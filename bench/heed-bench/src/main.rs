use bench_common::{make_payload, now_ms, print_result, Args, Histogram, Rng, Timer};
use heed::types::{Bytes, Str};
use heed::{Database, Env, EnvOpenOptions, RoTxn, RwTxn};
use std::error::Error;
use std::fs;

type Res<T> = Result<T, Box<dyn Error>>;
type Table = Database<Bytes, Bytes>;

const MAP_SIZE: usize = 8 * 1024 * 1024 * 1024;

struct Store {
    env: Env,
    jobs: Table,
    ready: Table,
    inflight: Table,
    delayed: Table,
    meta: Database<Str, Bytes>,
}

fn k8(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

fn d8(b: &[u8]) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[..8]);
    u64::from_be_bytes(a)
}

fn k16(a: u64, b: u64) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&a.to_be_bytes());
    out[8..].copy_from_slice(&b.to_be_bytes());
    out
}

fn get_seq(meta: &Database<Str, Bytes>, txn: &RoTxn) -> Res<u64> {
    Ok(meta.get(txn, "next_seq")?.map(d8).unwrap_or(0))
}

fn set_seq(meta: &Database<Str, Bytes>, txn: &mut RwTxn, value: u64) -> Res<()> {
    meta.put(txn, "next_seq", &k8(value))?;
    Ok(())
}

impl Store {
    fn open(path: &str) -> Res<Self> {
        let mut opts = EnvOpenOptions::new();
        opts.map_size(MAP_SIZE).max_dbs(16);
        let env: Env = unsafe { opts.open(path)? };
        let mut wtxn = env.write_txn()?;
        let jobs: Table = env.create_database(&mut wtxn, Some("jobs"))?;
        let ready: Table = env.create_database(&mut wtxn, Some("ready"))?;
        let inflight: Table = env.create_database(&mut wtxn, Some("inflight"))?;
        let delayed: Table = env.create_database(&mut wtxn, Some("delayed"))?;
        let meta: Database<Str, Bytes> = env.create_database(&mut wtxn, Some("meta"))?;
        wtxn.commit()?;
        Ok(Store { env, jobs, ready, inflight, delayed, meta })
    }

    fn enqueue(&self, payload: &[u8], count: u64) -> Res<u64> {
        let mut wtxn = self.env.write_txn()?;
        let mut next = get_seq(&self.meta, &wtxn)?;
        for _ in 0..count {
            next += 1;
            self.jobs.put(&mut wtxn, &k8(next), payload)?;
            self.ready.put(&mut wtxn, &k8(next), &k8(next))?;
        }
        set_seq(&self.meta, &mut wtxn, next)?;
        wtxn.commit()?;
        Ok(next)
    }

    fn claim(&self, count: u64) -> Res<Vec<u64>> {
        let mut wtxn = self.env.write_txn()?;
        let picked: Vec<([u8; 8], u64)> = {
            let mut out = Vec::new();
            for item in self.ready.iter(&wtxn)?.take(count as usize) {
                let (k, v) = item?;
                let mut key = [0u8; 8];
                key.copy_from_slice(k);
                out.push((key, d8(v)));
            }
            out
        };
        for (key, job_id) in &picked {
            if self.jobs.get(&wtxn, &k8(*job_id))?.is_none() {
                return Err("ready entry points at missing job".into());
            }
            self.ready.delete(&mut wtxn, key)?;
            self.inflight.put(&mut wtxn, key, &k8(*job_id))?;
        }
        wtxn.commit()?;
        Ok(picked.into_iter().map(|(_, j)| j).collect())
    }

    fn ack(&self, count: u64) -> Res<u64> {
        let mut wtxn = self.env.write_txn()?;
        let picked: Vec<([u8; 8], u64)> = {
            let mut out = Vec::new();
            for item in self.inflight.iter(&wtxn)?.take(count as usize) {
                let (k, v) = item?;
                let mut key = [0u8; 8];
                key.copy_from_slice(k);
                out.push((key, d8(v)));
            }
            out
        };
        for (key, job_id) in &picked {
            self.inflight.delete(&mut wtxn, key)?;
            self.jobs.delete(&mut wtxn, &k8(*job_id))?;
        }
        wtxn.commit()?;
        Ok(picked.len() as u64)
    }

    fn retry_round(&self, count: u64, retry_pct: u64, rng: &mut Rng) -> Res<(u64, u64)> {
        let mut wtxn = self.env.write_txn()?;
        let picked: Vec<([u8; 8], u64)> = {
            let mut out = Vec::new();
            for item in self.inflight.iter(&wtxn)?.take(count as usize) {
                let (k, v) = item?;
                let mut key = [0u8; 8];
                key.copy_from_slice(k);
                out.push((key, d8(v)));
            }
            out
        };
        let mut next = get_seq(&self.meta, &wtxn)?;
        let due = now_ms();
        let mut retried = 0u64;
        let mut acked = 0u64;
        for (key, job_id) in &picked {
            self.inflight.delete(&mut wtxn, key)?;
            if rng.below(100) < retry_pct {
                next += 1;
                self.delayed.put(&mut wtxn, &k16(due, next), &k8(*job_id))?;
                retried += 1;
            } else {
                self.jobs.delete(&mut wtxn, &k8(*job_id))?;
                acked += 1;
            }
        }
        let due_entries: Vec<([u8; 16], u64)> = {
            let mut out = Vec::new();
            for item in self.delayed.iter(&wtxn)? {
                let (k, v) = item?;
                let mut key = [0u8; 16];
                key.copy_from_slice(k);
                let t = d8(&key[..8]);
                if t > due {
                    break;
                }
                out.push((key, d8(v)));
            }
            out
        };
        for (key, job_id) in due_entries {
            self.delayed.delete(&mut wtxn, &key)?;
            next += 1;
            self.ready.put(&mut wtxn, &k8(next), &k8(job_id))?;
        }
        set_seq(&self.meta, &mut wtxn, next)?;
        wtxn.commit()?;
        Ok((acked, retried))
    }
}

fn count(db: &Table, txn: &RoTxn) -> Res<u64> {
    let mut n = 0u64;
    for item in db.iter(txn)? {
        item?;
        n += 1;
    }
    Ok(n)
}

fn verify(store: &Store) -> Res<serde_json::Value> {
    let rtxn = store.env.read_txn()?;
    let mut live = std::collections::HashSet::new();
    let mut dup = 0u64;
    let mut missing = 0u64;

    let mut jobs = 0u64;
    for item in store.jobs.iter(&rtxn)? {
        item?;
        jobs += 1;
    }

    for db in [&store.ready, &store.inflight, &store.delayed] {
        for item in db.iter(&rtxn)? {
            let (_, v) = item?;
            let id = d8(v);
            if !live.insert(id) {
                dup += 1;
            }
            if store.jobs.get(&rtxn, &k8(id))?.is_none() {
                missing += 1;
            }
        }
    }

    Ok(serde_json::json!({
        "jobs": jobs,
        "ready": count(&store.ready, &rtxn)?,
        "inflight": count(&store.inflight, &rtxn)?,
        "delayed": count(&store.delayed, &rtxn)?,
        "duplicate_live_references": dup,
        "references_to_missing_jobs": missing,
        "invariants_ok": dup == 0 && missing == 0,
    }))
}

const BACKEND: &str = "lmdb-heed";

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
            let mut claim_hist = Histogram::new();
            let mut ack_hist = Histogram::new();
            let cycles = args.total / args.batch;
            let start = Timer::start();
            for _ in 0..cycles {
                let t = Timer::start();
                store.enqueue(&payload, args.batch)?;
                enq.record(t.micros());
                let t = Timer::start();
                store.claim(args.batch)?;
                claim_hist.record(t.micros());
                let t = Timer::start();
                store.ack(args.batch)?;
                ack_hist.record(t.micros());
            }
            let ops = cycles * args.batch;
            let wall = start.micros() / 1000;
            let extra = serde_json::json!({
                "enqueue_batch_us_p50": enq.p50(),
                "enqueue_batch_us_p99": enq.p99(),
                "claim_batch_us_p50": claim_hist.p50(),
                "claim_batch_us_p99": claim_hist.p99(),
                "ack_batch_us_p50": ack_hist.p50(),
                "ack_batch_us_p99": ack_hist.p99(),
                "commits": cycles * 3,
            });
            print_result(BACKEND, &args, "lifecycle", ops, wall, &mut enq, extra);
        }
        "backlog" => {
            let start = Timer::start();
            fill_all(&store, &payload, args.total, args.batch)?;
            let enqueue_ms = start.micros() / 1000;
            let rss_after_enqueue = bench_common::rss_kb();
            let hwm_after_enqueue = bench_common::hwm_kb();
            let disk_after_enqueue =
                bench_common::dir_alloc_bytes(std::path::Path::new(&args.db));

            let mut claim_hist = Histogram::new();
            let mut ack_hist = Histogram::new();
            let start = Timer::start();
            let mut completed = 0u64;
            loop {
                let t = Timer::start();
                let claimed = store.claim(args.batch)?.len() as u64;
                claim_hist.record(t.micros());
                if claimed == 0 {
                    break;
                }
                let t = Timer::start();
                completed += store.ack(args.batch)?;
                ack_hist.record(t.micros());
            }
            let drain_ms = start.micros() / 1000;
            let extra = serde_json::json!({
                "enqueue_ms": enqueue_ms,
                "enqueue_ops_per_sec": args.total as f64 * 1000.0 / enqueue_ms.max(1) as f64,
                "drain_ms": drain_ms,
                "rss_after_enqueue_kb": rss_after_enqueue,
                "hwm_after_enqueue_kb": hwm_after_enqueue,
                "disk_after_enqueue_bytes": disk_after_enqueue,
                "claim_batch_us_p50": claim_hist.p50(),
                "claim_batch_us_p99": claim_hist.p99(),
                "ack_batch_us_p50": ack_hist.p50(),
                "ack_batch_us_p99": ack_hist.p99(),
            });
            print_result(BACKEND, &args, "backlog_drain", completed, drain_ms, &mut claim_hist, extra);
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
            let mut counts = verify(&store)?;
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