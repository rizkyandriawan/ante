use bench_common::{make_payload, now_ms, print_result, Args, Histogram, Rng, Timer};
use redb::{Database, ReadableTable, TableDefinition};
use std::error::Error;
use std::fs;

type Res<T> = Result<T, Box<dyn Error>>;

const JOBS: TableDefinition<u64, &[u8]> = TableDefinition::new("jobs");
const READY: TableDefinition<u64, u64> = TableDefinition::new("ready");
const INFLIGHT: TableDefinition<u64, u64> = TableDefinition::new("inflight");
const DELAYED: TableDefinition<(u64, u64), u64> = TableDefinition::new("delayed");
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");

struct Store {
    db: Database,
}

impl Store {
    fn open(dir: &str) -> Res<Self> {
        let path = std::path::Path::new(dir).join("ante.redb");
        let db = redb::Builder::new()
            .set_cache_size(64 * 1024 * 1024)
            .create(path)?;
        {
            let w = db.begin_write()?;
            {
                let _ = w.open_table(JOBS)?;
                let _ = w.open_table(READY)?;
                let _ = w.open_table(INFLIGHT)?;
                let _ = w.open_table(DELAYED)?;
                let _ = w.open_table(META)?;
            }
            w.commit()?;
        }
        Ok(Store { db })
    }

    fn enqueue(&self, payload: &[u8], count: u64) -> Res<u64> {
        let w = self.db.begin_write()?;
        let mut next;
        {
            let mut jobs = w.open_table(JOBS)?;
            let mut ready = w.open_table(READY)?;
            let mut meta = w.open_table(META)?;
            next = meta.get("next_seq")?.map(|v| v.value()).unwrap_or(0);
            for _ in 0..count {
                let next_seq = next + 1;
                jobs.insert(next_seq, payload)?;
                ready.insert(next_seq, next_seq)?;
                next = next_seq;
            }
            meta.insert("next_seq", next)?;
        }
        w.commit()?;
        Ok(next)
    }

    fn claim(&self, count: u64) -> Res<Vec<u64>> {
        let w = self.db.begin_write()?;
        let picked: Vec<(u64, u64)> = {
            let ready = w.open_table(READY)?;
            let mut out = Vec::new();
            for item in ready.iter()?.take(count as usize) {
                let (k, v) = item?;
                out.push((k.value(), v.value()));
            }
            out
        };
        {
            let mut ready = w.open_table(READY)?;
            let mut inflight = w.open_table(INFLIGHT)?;
            let jobs = w.open_table(JOBS)?;
            for (seq, job_id) in &picked {
                jobs.get(*job_id)?.ok_or("ready entry points at missing job")?;
                ready.remove(*seq)?;
                inflight.insert(*seq, *job_id)?;
            }
        }
        w.commit()?;
        Ok(picked.into_iter().map(|(_, j)| j).collect())
    }

    fn ack(&self, count: u64) -> Res<u64> {
        let w = self.db.begin_write()?;
        let picked: Vec<(u64, u64)> = {
            let inflight = w.open_table(INFLIGHT)?;
            let mut out = Vec::new();
            for item in inflight.iter()?.take(count as usize) {
                let (k, v) = item?;
                out.push((k.value(), v.value()));
            }
            out
        };
        {
            let mut inflight = w.open_table(INFLIGHT)?;
            let mut jobs = w.open_table(JOBS)?;
            for (seq, job_id) in &picked {
                inflight.remove(*seq)?;
                jobs.remove(*job_id)?;
            }
        }
        w.commit()?;
        Ok(picked.len() as u64)
    }

    fn retry_round(&self, count: u64, retry_pct: u64, rng: &mut Rng) -> Res<(u64, u64)> {
        let w = self.db.begin_write()?;
        let picked: Vec<(u64, u64)> = {
            let inflight = w.open_table(INFLIGHT)?;
            let mut out = Vec::new();
            for item in inflight.iter()?.take(count as usize) {
                let (k, v) = item?;
                out.push((k.value(), v.value()));
            }
            out
        };
        let mut retried = 0u64;
        let mut acked = 0u64;
        {
            let mut inflight = w.open_table(INFLIGHT)?;
            let mut jobs = w.open_table(JOBS)?;
            let mut ready = w.open_table(READY)?;
            let mut delayed = w.open_table(DELAYED)?;
            let mut meta = w.open_table(META)?;
            let mut next = meta.get("next_seq")?.map(|v| v.value()).unwrap_or(0);
            let due = now_ms();
            for (seq, job_id) in &picked {
                inflight.remove(*seq)?;
                if rng.below(100) < retry_pct {
                    next += 1;
                    delayed.insert((due, next), *job_id)?;
                    retried += 1;
                } else {
                    jobs.remove(*job_id)?;
                    acked += 1;
                }
            }
            // Promote all due delayed entries back to ready in the same commit.
            let due_entries: Vec<((u64, u64), u64)> = {
                let mut scratch = Vec::new();
                for item in delayed.iter()? {
                    let (k, v) = item?;
                    let (t, _) = k.value();
                    if t > due {
                        break;
                    }
                    scratch.push((k.value(), v.value()));
                }
                scratch
            };
            for (key, job_id) in due_entries {
                delayed.remove(key)?;
                next += 1;
                ready.insert(next, job_id)?;
            }
            meta.insert("next_seq", next)?;
        }
        w.commit()?;
        Ok((acked, retried))
    }

}


fn verify(db: &Database) -> Res<serde_json::Value> {
    let r = db.begin_read()?;
    let jobs = r.open_table(JOBS)?;
    let ready = r.open_table(READY)?;
    let inflight = r.open_table(INFLIGHT)?;
    let delayed = r.open_table(DELAYED)?;

    let mut job_count = 0u64;
    for item in jobs.iter()? {
        item?;
        job_count += 1;
    }

    let mut live = std::collections::HashSet::new();
    let mut dup = 0u64;
    let mut missing = 0u64;
    for item in ready.iter()? {
        let (_, v) = item?;
        let id = v.value();
        if !live.insert(id) {
            dup += 1;
        }
        if jobs.get(id)?.is_none() {
            missing += 1;
        }
    }
    for item in inflight.iter()? {
        let (_, v) = item?;
        let id = v.value();
        if !live.insert(id) {
            dup += 1;
        }
        if jobs.get(id)?.is_none() {
            missing += 1;
        }
    }
    for item in delayed.iter()? {
        let (_, v) = item?;
        let id = v.value();
        if !live.insert(id) {
            dup += 1;
        }
        if jobs.get(id)?.is_none() {
            missing += 1;
        }
    }

    Ok(serde_json::json!({
        "jobs": job_count,
        "ready": count_inside(&ready)?,
        "inflight": count_inside32(&inflight)?,
        "delayed": count_inside_delayed(&delayed)?,
        "duplicate_live_references": dup,
        "references_to_missing_jobs": missing,
        "invariants_ok": dup == 0 && missing == 0,
    }))
}

fn count_inside(t: &redb::ReadOnlyTable<u64, u64>) -> Res<u64> {
    let mut n = 0;
    for item in t.iter()? {
        item?;
        n += 1;
    }
    Ok(n)
}

fn count_inside32(t: &redb::ReadOnlyTable<u64, u64>) -> Res<u64> {
    count_inside(t)
}

fn count_inside_delayed(t: &redb::ReadOnlyTable<(u64, u64), u64>) -> Res<u64> {
    let mut n = 0;
    for item in t.iter()? {
        item?;
        n += 1;
    }
    Ok(n)
}

const BACKEND: &str = "redb";

fn fill_all(store: &Store, payload: &[u8], total: u64, batch: u64) -> Res<()> {
    let mut done = 0u64;
    while done < total {
        let n = (total - done).min(batch);
        store.enqueue(payload, n)?;
        done += n;
    }
    Ok(())
}

fn main() -> Res<()> {
    let args = Args::parse();
    fs::create_dir_all(&args.db)?;
    let open_timer = Timer::start();
    let store = Store::open(&args.db)?;
    let open_us = open_timer.micros();
    let payload = make_payload(args.payload, args.seed);
    let mut rng = Rng::new(args.seed);

    match args.mode.as_str() {
        "stream" => {
            let mut enq = Histogram::new();
            let mut claim = Histogram::new();
            let mut ack = Histogram::new();
            let mut ops = 0u64;
            let mut cycles = 0u64;
            let mut per_second: Vec<u64> = Vec::new();
            let mut window_ops = 0u64;
            let start = Timer::start();
            let limit_cycles = if args.duration_ms == 0 {
                args.total / args.batch
            } else {
                u64::MAX
            };
            loop {
                if cycles >= limit_cycles {
                    break;
                }
                if args.duration_ms > 0 && start.micros() / 1000 >= args.duration_ms {
                    break;
                }
                let t = Timer::start();
                store.enqueue(&payload, args.batch)?;
                enq.record(t.micros());
                let t = Timer::start();
                store.claim(args.batch)?;
                claim.record(t.micros());
                let t = Timer::start();
                store.ack(args.batch)?;
                ack.record(t.micros());
                cycles += 1;
                ops += args.batch;
                window_ops += args.batch;
                let elapsed_ms = start.micros() / 1000;
                if args.duration_ms > 0 && elapsed_ms / 1000 >= per_second.len() as u64 + 1 {
                    per_second.push(window_ops);
                    window_ops = 0;
                }
            }
            let wall = start.micros() / 1000;
            let mut extra = serde_json::json!({
                "enqueue_batch_us_p50": enq.p50(),
                "enqueue_batch_us_p99": enq.p99(),
                "claim_batch_us_p50": claim.p50(),
                "claim_batch_us_p99": claim.p99(),
                "ack_batch_us_p50": ack.p50(),
                "ack_batch_us_p99": ack.p99(),
                "commits": cycles * 3,
                "cycles": cycles,
            });
            if args.duration_ms > 0 {
                let min = per_second.iter().copied().min().unwrap_or(0);
                let max = per_second.iter().copied().max().unwrap_or(0);
                let mut sorted = per_second.clone();
                sorted.sort_unstable();
                let med = if sorted.is_empty() { 0 } else { sorted[sorted.len() / 2] };
                if let Some(obj) = extra.as_object_mut() {
                    obj.insert("per_second_ops".into(), serde_json::json!(per_second));
                    obj.insert("per_second_min".into(), serde_json::json!(min));
                    obj.insert("per_second_max".into(), serde_json::json!(max));
                    obj.insert("per_second_median".into(), serde_json::json!(med));
                }
            }
            print_result(BACKEND, &args, "lifecycle", ops, wall, &mut enq, extra);
        }
        "backlog" => {
            let start = Timer::start();
            fill_all(&store, &payload, args.total, args.batch)?;
            let enqueue_ms = start.micros() / 1000;
            let rss_after_enqueue = bench_common::rss_kb();
            let hwm_after_enqueue = bench_common::hwm_kb();
            let disk_after_enqueue = bench_common::dir_alloc_bytes(std::path::Path::new(&args.db));

            let mut claim = Histogram::new();
            let mut ack = Histogram::new();
            let start = Timer::start();
            let mut completed = 0u64;
            loop {
                let t = Timer::start();
                let claimed = store.claim(args.batch)?.len() as u64;
                claim.record(t.micros());
                if claimed == 0 {
                    break;
                }
                let t = Timer::start();
                completed += store.ack(args.batch)?;
                ack.record(t.micros());
            }
            let drain_ms = start.micros() / 1000;
            let extra = serde_json::json!({
                "enqueue_ms": enqueue_ms,
                "enqueue_ops_per_sec": args.total as f64 * 1000.0 / enqueue_ms.max(1) as f64,
                "drain_ms": drain_ms,
                "rss_after_enqueue_kb": rss_after_enqueue,
                "hwm_after_enqueue_kb": hwm_after_enqueue,
                "disk_after_enqueue_bytes": disk_after_enqueue,
                "claim_batch_us_p50": claim.p50(),
                "claim_batch_us_p99": claim.p99(),
                "ack_batch_us_p50": ack.p50(),
                "ack_batch_us_p99": ack.p99(),
            });
            print_result(BACKEND, &args, "backlog_drain", completed, drain_ms, &mut claim, extra);
        }
        "retry" => {
            fill_all(&store, &payload, args.total, args.batch)?;
            let mut hist = Histogram::new();
            let start = Timer::start();
            let cycles = args.total / args.batch;
            let mut acked = 0u64;
            let mut retried = 0u64;
            for _ in 0..cycles {
                let t = Timer::start();
                let _ = store.claim(args.batch)?;
                let (a, r) = store.retry_round(args.batch, args.retry_pct, &mut rng)?;
                hist.record(t.micros());
                acked += a;
                retried += r;
            }
            let wall = start.micros() / 1000;
            let extra = serde_json::json!({
                "acked": acked,
                "retried": retried,
                "transitions_per_sec": (acked + retried) as f64 * 1000.0 / wall.max(1) as f64,
            });
            print_result(BACKEND, &args, "retry_round", acked + retried, wall, &mut hist, extra);
        }
        "fill" => {
            let start = Timer::start();
            fill_all(&store, &payload, args.total, args.batch)?;
            let wall = start.micros() / 1000;
            let mut hist = Histogram::new();
            hist.record(wall * 1000);
            print_result(BACKEND, &args, "enqueue_only", args.total, wall, &mut hist,
                serde_json::json!({ "db_alloc_bytes_final": bench_common::dir_alloc_bytes(std::path::Path::new(&args.db)) }));
        }
        "idle" => {
            std::thread::sleep(std::time::Duration::from_secs(2));
            let mut hist = Histogram::new();
            hist.record(0);
            print_result(BACKEND, &args, "idle_after_open", 0, 0, &mut hist,
                serde_json::json!({ "open_us": open_us }));
        }
        "recover" => {
            let start = Timer::start();
            let mut counts = verify(&store.db)?;
            let verify_us = start.micros();
            if let Some(obj) = counts.as_object_mut() {
                obj.insert("open_us".into(), serde_json::json!(open_us));
                obj.insert("verify_us".into(), serde_json::json!(verify_us));
            }
            let total_us = open_us + verify_us;
            let mut hist = Histogram::new();
            hist.record(total_us);
            print_result(BACKEND, &args, "recover", 0, total_us / 1000, &mut hist, counts);
        }
        other => panic!("unknown mode {other}"),
    }
    Ok(())
}