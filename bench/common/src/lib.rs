use std::fs;
use std::path::Path;
use std::time::Instant;

pub struct Args {
    pub mode: String,
    pub db: String,
    pub total: u64,
    pub batch: u64,
    pub payload: usize,
    pub retry_pct: u64,
    pub seed: u64,
    pub rep: u64,
    pub duration_ms: u64,
}

impl Args {
    pub fn parse() -> Args {
        let mut mode = String::from("stream");
        let mut db = String::from("./data");
        let mut total = 20_000u64;
        let mut batch = 32u64;
        let mut payload = 1024usize;
        let mut retry_pct = 20u64;
        let mut seed = 42u64;
        let mut rep = 0u64;
        let mut duration_ms = 0u64;
        let mut positional = 0;

        for arg in std::env::args().skip(1) {
            if let Some((k, v)) = arg.split_once('=') {
                match k {
                    "--db" => db = v.to_string(),
                    "--total" => total = v.parse().expect("total"),
                    "--batch" => batch = v.parse().expect("batch"),
                    "--payload" => payload = v.parse().expect("payload"),
                    "--retry-pct" => retry_pct = v.parse().expect("retry-pct"),
                    "--seed" => seed = v.parse().expect("seed"),
                    "--rep" => rep = v.parse().expect("rep"),
                    "--duration" => duration_ms = (v.parse::<f64>().expect("duration") * 1000.0) as u64,
                    other => panic!("unknown flag {other}"),
                }
            } else if positional == 0 {
                mode = arg;
                positional += 1;
            } else {
                panic!("unexpected argument {arg}");
            }
        }

        assert!(batch > 0 && (total > 0 || duration_ms > 0) && payload > 0 && retry_pct <= 100);
        Args { mode, db, total, batch, payload, retry_pct, seed, rep, duration_ms }
    }
}

pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed | 1)
    }
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

pub fn make_payload(size: usize, seed: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(size);
    let mut rng = Rng::new(seed);
    while out.len() < size {
        out.extend_from_slice(&rng.next_u64().to_le_bytes());
    }
    out.truncate(size);
    out
}

#[derive(Default)]
pub struct Histogram {
    samples: Vec<u64>,
}

impl Histogram {
    pub fn new() -> Self {
        Histogram { samples: Vec::new() }
    }
    pub fn record(&mut self, micros: u64) {
        self.samples.push(micros);
    }
    pub fn percentile(&mut self, p: f64) -> u64 {
        if self.samples.is_empty() {
            return 0;
        }
        self.samples.sort_unstable();
        let idx = ((self.samples.len() as f64 - 1.0) * p).round() as usize;
        self.samples[idx]
    }
    pub fn p50(&mut self) -> u64 {
        self.percentile(0.50)
    }
    pub fn p99(&mut self) -> u64 {
        self.percentile(0.99)
    }
    pub fn max(&self) -> u64 {
        self.samples.iter().copied().max().unwrap_or(0)
    }
}

pub fn rss_kb() -> u64 {
    proc_kb("VmRSS:")
}

pub fn hwm_kb() -> u64 {
    proc_kb("VmHWM:")
}

pub fn mem_breakdown() -> (u64, u64, u64) {
    let status = fs::read_to_string("/proc/self/status").unwrap_or_default();
    let mut anon = 0;
    let mut file = 0;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("RssAnon:") {
            anon = rest.trim().trim_end_matches("kB").trim().parse().unwrap_or(0);
        } else if let Some(rest) = line.strip_prefix("RssFile:") {
            file = rest.trim().trim_end_matches("kB").trim().parse().unwrap_or(0);
        } else if let Some(rest) = line.strip_prefix("RssShmem:") {
            file += rest.trim().trim_end_matches("kB").trim().parse::<u64>().unwrap_or(0);
        }
    }
    let rollup = fs::read_to_string("/proc/self/smaps_rollup").unwrap_or_default();
    let mut pss = 0;
    for line in rollup.lines() {
        if let Some(rest) = line.strip_prefix("Pss:") {
            pss = rest.trim().trim_end_matches("kB").trim().parse().unwrap_or(0);
        }
    }
    (anon, file, pss)
}

fn proc_kb(key: &str) -> u64 {
    let status = fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix(key) {
            let value = rest.trim().trim_end_matches("kB").trim();
            return value.parse().unwrap_or(0);
        }
    }
    0
}

#[cfg(unix)]
pub fn dir_alloc_bytes(path: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    let mut total = 0u64;
    let Ok(entries) = fs::read_dir(path) else {
        return 0;
    };
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else { continue };
        if meta.is_dir() {
            total += dir_alloc_bytes(&entry.path());
        } else {
            total += meta.blocks() * 512;
        }
    }
    total
}

#[cfg(not(unix))]
pub fn dir_alloc_bytes(_path: &Path) -> u64 {
    0
}

pub struct Timer {
    start: Instant,
}

impl Timer {
    pub fn start() -> Self {
        Timer { start: Instant::now() }
    }
    pub fn micros(&self) -> u64 {
        self.start.elapsed().as_micros() as u64
    }
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn print_result(
    backend: &str,
    args: &Args,
    phase: &str,
    operations: u64,
    wall_ms: u64,
    hist: &mut Histogram,
    extra: serde_json::Value,
) {
    let mut obj = serde_json::json!({
        "backend": backend,
        "phase": phase,
        "mode": args.mode,
        "total": args.total,
        "batch": args.batch,
        "payload_bytes": args.payload,
        "retry_pct": args.retry_pct,
        "seed": args.seed,
        "rep": args.rep,
        "duration_ms": args.duration_ms,
        "operations": operations,
        "wall_ms": wall_ms,
        "ops_per_sec": if wall_ms > 0 { operations as f64 * 1000.0 / wall_ms as f64 } else { 0.0 },
        "lat_us_p50": hist.p50(),
        "lat_us_p99": hist.p99(),
        "lat_us_max": hist.max(),
        "rss_kb": rss_kb(),
        "hwm_kb": hwm_kb(),
        "db_alloc_bytes": dir_alloc_bytes(Path::new(&args.db)),
        "anon_kb": mem_breakdown().0,
        "file_kb": mem_breakdown().1,
        "pss_kb": mem_breakdown().2,
    });
    if let (Some(dst), Some(src)) = (obj.as_object_mut(), extra.as_object()) {
        for (k, v) in src {
            dst.insert(k.clone(), v.clone());
        }
    }
    println!("{obj}");
}