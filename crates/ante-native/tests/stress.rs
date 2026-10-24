use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ante_core::Ante;
use ante_native::codec::Value;
use ante_native::{serve_until, NativeClient};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> Self {
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "ante-native-stress-{}-{}-{}",
            std::process::id(),
            id,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        TempDir { path }
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

struct Harness {
    address: String,
    ante: Arc<Ante>,
    shutdown: Option<oneshot::Sender<()>>,
    server: tokio::task::JoinHandle<()>,
    _dir: TempDir,
}

impl Harness {
    async fn start() -> Self {
        let dir = TempDir::new();
        let ante = Arc::new(Ante::open(dir.path.join("ante.sqlite3")).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (tx, rx) = oneshot::channel();
        let server_ante = ante.clone();
        let server = tokio::spawn(async move {
            serve_until(listener, server_ante, rx).await.unwrap();
        });
        Harness {
            address,
            ante,
            shutdown: Some(tx),
            server,
            _dir: dir,
        }
    }

    async fn client(&self) -> NativeClient {
        NativeClient::connect(&self.address).await.unwrap()
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        self.server.abort();
    }
}

struct Record {
    token: String,
    attempts: u64,
    message_id: String,
    payload: Vec<u8>,
}

fn parse_records(value: Value) -> Vec<Record> {
    let Value::Array(records) = value else {
        panic!("expected array, got {value:?}");
    };
    records
        .into_iter()
        .map(|record| {
            let Value::Array(fields) = record else {
                panic!("expected record array");
            };
            assert_eq!(fields.len(), 7);
            let text = |index: usize| match &fields[index] {
                Value::Bulk(bytes) => String::from_utf8(bytes.clone()).unwrap(),
                other => panic!("expected bulk at {index}, got {other:?}"),
            };
            let payload = match &fields[6] {
                Value::Bulk(bytes) => bytes.clone(),
                other => panic!("expected payload, got {other:?}"),
            };
            Record {
                token: text(0),
                message_id: text(1),
                attempts: text(5).parse().unwrap(),
                payload,
            }
        })
        .collect()
}

async fn publish_batch(client: &mut NativeClient, queue: &str, payloads: &[Vec<u8>]) {
    let mut args = vec![b"PUBLISH_BATCH".to_vec(), queue.as_bytes().to_vec()];
    args.extend(payloads.iter().cloned());
    match client.call(&args).await.unwrap() {
        Value::Array(ids) => assert_eq!(ids.len(), payloads.len()),
        other => panic!("publish_batch failed: {other:?}"),
    }
}

async fn take(
    client: &mut NativeClient,
    queue: &str,
    max: usize,
    lease_ms: u64,
    wait_ms: u64,
) -> Vec<Record> {
    let value = client
        .call(&[
            b"TAKE".to_vec(),
            queue.as_bytes().to_vec(),
            max.to_string().into_bytes(),
            lease_ms.to_string().into_bytes(),
            wait_ms.to_string().into_bytes(),
        ])
        .await
        .unwrap();
    parse_records(value)
}

async fn info_field(client: &mut NativeClient, queue: &str, field: &str) -> u64 {
    let value = client
        .call(&[b"INFO".to_vec(), queue.as_bytes().to_vec()])
        .await
        .unwrap();
    let Value::Array(items) = value else {
        panic!("expected info array");
    };
    for pair in items.chunks(2) {
        if let Value::Bulk(key) = &pair[0] {
            if key == field.as_bytes() {
                if let Value::Bulk(value) = &pair[1] {
                    return String::from_utf8(value.clone()).unwrap().parse().unwrap();
                }
            }
        }
    }
    panic!("field {field} not found");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_network_workers_process_each_message_once() {
    let harness = Harness::start().await;
    harness.ante.declare_queue("jobs").unwrap();

    let total = 2_000usize;
    let mut publisher = harness.client().await;
    for chunk in 0..(total / 100) {
        let payloads: Vec<Vec<u8>> = (0..100)
            .map(|offset| format!("job-{}", chunk * 100 + offset).into_bytes())
            .collect();
        publish_batch(&mut publisher, "jobs", &payloads).await;
    }

    let processed = Arc::new(AtomicUsize::new(0));
    let duplicates = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(HashSet::new()));
    let counts = Arc::new(Mutex::new(Vec::new()));
    let workers = 8;
    let barrier = Arc::new(tokio::sync::Barrier::new(workers));

    let mut handles = Vec::new();
    for worker in 0..workers {
        let mut client = harness.client().await;
        let processed = processed.clone();
        let duplicates = duplicates.clone();
        let seen = seen.clone();
        let counts = counts.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let mut mine = 0usize;
            let deadline = Instant::now() + Duration::from_secs(30);
            while processed.load(Ordering::Relaxed) < total {
                if Instant::now() > deadline {
                    break;
                }
                let records = take(&mut client, "jobs", 16, 30_000, 100).await;
                if records.is_empty() {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    continue;
                }
                {
                    let mut seen = seen.lock().unwrap();
                    for record in &records {
                        if !seen.insert(record.payload.clone()) {
                            duplicates.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                let mut args = vec![b"ACK".to_vec()];
                args.extend(
                    records
                        .iter()
                        .map(|record| record.token.as_bytes().to_vec()),
                );
                let value = client.call(&args).await.unwrap();
                let acked = match value {
                    Value::Array(results) => {
                        for result in &results {
                            assert!(
                                matches!(result, Value::Simple(text) if text == "OK"),
                                "{result:?}"
                            );
                        }
                        results.len()
                    }
                    Value::Simple(text) if text == "OK" => 1,
                    other => panic!("ack failed: {other:?}"),
                };
                processed.fetch_add(acked, Ordering::Relaxed);
                mine += acked;
            }
            let _ = worker;
            counts.lock().unwrap().push(mine);
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }

    assert_eq!(duplicates.load(Ordering::Relaxed), 0);
    assert_eq!(processed.load(Ordering::Relaxed), total);
    assert_eq!(seen.lock().unwrap().len(), total);
    assert_eq!(info_field(&mut publisher, "jobs", "ready").await, 0);
    assert_eq!(info_field(&mut publisher, "jobs", "inflight").await, 0);

    let counts = counts.lock().unwrap();
    let active = counts.iter().filter(|count| **count > 0).count();
    assert!(
        active >= 6,
        "only {active}/{workers} workers got work: {counts:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lease_expiry_over_the_wire_redelivers_with_higher_attempt() {
    let harness = Harness::start().await;
    harness.ante.declare_queue("slow").unwrap();
    let mut client = harness.client().await;
    let mut publisher = harness.client().await;
    publish_batch(&mut publisher, "slow", &[b"slow-job".to_vec()]).await;

    let first = take(&mut client, "slow", 1, 150, 0).await.remove(0);
    assert_eq!(first.attempts, 1);
    assert_eq!(first.payload, b"slow-job");
    assert_eq!(info_field(&mut client, "slow", "inflight").await, 1);

    let deadline = Instant::now() + Duration::from_secs(6);
    loop {
        if info_field(&mut client, "slow", "ready").await == 1 {
            break;
        }
        assert!(Instant::now() < deadline, "lease was never reclaimed");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let second = take(&mut client, "slow", 1, 30_000, 0).await.remove(0);
    assert_eq!(second.attempts, 2);
    assert_eq!(second.message_id, first.message_id);

    let stale = client
        .call(&[b"ACK".to_vec(), first.token.as_bytes().to_vec()])
        .await
        .unwrap();
    match stale {
        Value::Error(message) => assert!(message.starts_with("STALE_DELIVERY"), "{message}"),
        other => panic!("stale ack should fail, got {other:?}"),
    }
    assert_eq!(info_field(&mut client, "slow", "inflight").await, 1);

    let ok = client
        .call(&[b"ACK".to_vec(), second.token.as_bytes().to_vec()])
        .await
        .unwrap();
    assert!(
        matches!(ok, Value::Simple(ref text) if text == "OK"),
        "{ok:?}"
    );
    assert_eq!(info_field(&mut client, "slow", "total").await, 0);
}
