use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bench_common::{dir_alloc_bytes, hwm_kb, Histogram, Timer};
use futures_util::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions, QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Connection, ConnectionProperties};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, watch, Semaphore};

use ante_core::Ante;
use ante_native::{serve_until, NativeClient, Value};
use fe2o3_amqp::types::messaging::{Body, Message};
use fe2o3_amqp::types::primitives::Value as Amqp1Value;
use fe2o3_amqp::{
    Connection as Amqp1Connection, Receiver as Amqp1Receiver, Sender as Amqp1Sender,
    Session as Amqp1Session,
};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct Config {
    protocol: String,
    total: u64,
    batch: usize,
    payload: usize,
    rep: u64,
    window_batches: u64,
    mode: String,
}

#[derive(Serialize)]
struct Stats {
    phase: &'static str,
    protocol: String,
    jobs: u64,
    batch: usize,
    payload: usize,
    rep: u64,
    window_batches: u64,
    mode: String,
    wall_ms: u64,
    jobs_per_sec: f64,
    publish_batch_us_p50: u64,
    publish_batch_us_p99: u64,
    consume_batch_us_p50: u64,
    consume_batch_us_p99: u64,
    peak_rss_kb: u64,
    db_bytes: u64,
    duplicates: u64,
    verified: bool,
}

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> Self {
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "ante-e2e-bench-{}-{}-{}",
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

fn make_job(index: u64, size: usize) -> Vec<u8> {
    let mut payload = vec![0u8; size];
    payload[..8].copy_from_slice(&index.to_be_bytes());
    for (offset, byte) in payload.iter_mut().enumerate().skip(8) {
        *byte = (offset as u8) ^ (index as u8);
    }
    payload
}

fn job_index(payload: &[u8]) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&payload[..8]);
    u64::from_be_bytes(bytes)
}

struct Verifier {
    total: u64,
    seen: Vec<bool>,
    seen_count: u64,
    duplicates: u64,
}

impl Verifier {
    fn new(total: u64) -> Self {
        Verifier {
            total,
            seen: vec![false; total as usize],
            seen_count: 0,
            duplicates: 0,
        }
    }

    fn observe(&mut self, payload: &[u8]) {
        assert!(payload.len() >= 8, "payload too small to verify");
        let index = job_index(payload);
        assert!(index < self.total, "unexpected job index {index}");
        if self.seen[index as usize] {
            self.duplicates += 1;
        } else {
            self.seen[index as usize] = true;
            self.seen_count += 1;
        }
    }

    fn check(self) -> u64 {
        assert_eq!(self.duplicates, 0, "duplicate deliveries");
        assert_eq!(self.seen_count, self.total, "missing deliveries");
        self.duplicates
    }
}

#[tokio::main]
async fn main() {
    let config = parse_args();
    let run = async {
        match config.protocol.as_str() {
            "native" => run_native(&config).await,
            "amqp" => run_amqp(&config).await,
            "amqp1" => run_amqp1(&config).await,
            "core" => run_core(&config).await,
            other => panic!("unknown protocol {other}"),
        }
    };
    let stats = tokio::time::timeout(Duration::from_secs(300), run)
        .await
        .expect("benchmark timed out after 300s");
    println!("{}", serde_json::to_string(&stats).unwrap());
}

async fn start_server(
    ante: Arc<Ante>,
) -> (String, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (stop, rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        serve_until(listener, ante, rx).await.unwrap();
    });
    (address, stop, task)
}

async fn start_amqp_server(
    ante: Arc<Ante>,
) -> (String, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (stop, rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        ante_amqp::serve_until(listener, ante, rx).await.unwrap();
    });
    (address, stop, task)
}

async fn run_core(config: &Config) -> Stats {
    assert_eq!(
        config.mode, "both",
        "core benchmark only supports --mode=both"
    );
    let dir = TempDir::new();
    let ante = Arc::new(Ante::open(dir.path.join("ante.redb")).unwrap());
    ante.declare_queue("jobs").unwrap();

    let total = config.total;
    let batch = config.batch.min(256);
    let payload_size = config.payload;
    let permits = Arc::new(Semaphore::new(batch * config.window_batches as usize));
    let start = Timer::start();

    let producer_permits = permits.clone();
    let producer_ante = ante.clone();
    let produce = tokio::spawn(async move {
        let mut hist = Histogram::new();
        let mut sent = 0u64;
        while sent < total {
            let count = batch.min((total - sent) as usize);
            producer_permits
                .clone()
                .acquire_many_owned(count as u32)
                .await
                .unwrap()
                .forget();
            let messages: Vec<ante_core::NewMessage> = (0..count)
                .map(|offset| ante_core::NewMessage {
                    payload: make_job(sent + offset as u64, payload_size),
                    ..Default::default()
                })
                .collect();
            let timer = Timer::start();
            producer_ante.publish_batch("jobs", messages).unwrap();
            hist.record(timer.micros());
            sent += count as u64;
        }
        (hist, sent)
    });

    let consumer_permits = permits.clone();
    let consumer_ante = ante.clone();
    let consume = tokio::spawn(async move {
        let mut hist = Histogram::new();
        let mut verifier = Verifier::new(total);
        let mut acked = 0u64;
        while acked < total {
            let timer = Timer::start();
            let deliveries = consumer_ante.take("jobs", batch, 30_000).unwrap();
            if deliveries.is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
                continue;
            }
            for delivery in &deliveries {
                verifier.observe(&delivery.payload);
            }
            let tokens: Vec<_> = deliveries
                .iter()
                .map(|delivery| delivery.token.clone())
                .collect();
            consumer_ante.ack_batch(&tokens).unwrap();
            hist.record(timer.micros());
            acked += deliveries.len() as u64;
            consumer_permits.add_permits(deliveries.len());
        }
        (hist, acked, verifier)
    });

    let (mut publish_hist, sent) = produce.await.unwrap();
    let (mut consume_hist, acked, verifier) = consume.await.unwrap();
    let wall_ms = (start.micros() / 1000).max(1);
    assert_eq!(sent, total);
    assert_eq!(acked, total);
    let duplicates = verifier.check();

    Stats {
        phase: "e2e_lifecycle",
        protocol: "core".into(),
        jobs: total,
        batch,
        payload: payload_size,
        rep: config.rep,
        window_batches: config.window_batches,
        mode: config.mode.clone(),
        wall_ms,
        jobs_per_sec: total as f64 * 1000.0 / wall_ms as f64,
        publish_batch_us_p50: publish_hist.p50(),
        publish_batch_us_p99: publish_hist.p99(),
        consume_batch_us_p50: consume_hist.p50(),
        consume_batch_us_p99: consume_hist.p99(),
        peak_rss_kb: hwm_kb(),
        db_bytes: dir_alloc_bytes(&dir.path),
        duplicates,
        verified: true,
    }
}

async fn run_native(config: &Config) -> Stats {
    assert_eq!(
        config.mode, "both",
        "native benchmark only supports --mode=both"
    );
    let dir = TempDir::new();
    let ante = Arc::new(Ante::open(dir.path.join("ante.redb")).unwrap());
    let (address, stop, server) = start_server(ante.clone()).await;

    let mut producer = NativeClient::connect(&address).await.unwrap();
    let mut consumer = NativeClient::connect(&address).await.unwrap();
    producer
        .call(&[b"DECLARE".to_vec(), b"jobs".to_vec()])
        .await
        .unwrap();

    let total = config.total;
    let batch = config.batch.min(256);
    let payload_size = config.payload;
    let permits = Arc::new(Semaphore::new(batch * config.window_batches as usize));
    let start = Timer::start();

    let producer_permits = permits.clone();
    let produce = tokio::spawn(async move {
        let mut hist = Histogram::new();
        let mut sent = 0u64;
        while sent < total {
            let count = batch.min((total - sent) as usize);
            producer_permits
                .clone()
                .acquire_many_owned(count as u32)
                .await
                .unwrap()
                .forget();
            let mut args = Vec::with_capacity(count + 2);
            args.push(b"PUBLISH_BATCH".to_vec());
            args.push(b"jobs".to_vec());
            for offset in 0..count {
                args.push(make_job(sent + offset as u64, payload_size));
            }
            let timer = Timer::start();
            let value = producer.call(&args).await.unwrap();
            hist.record(timer.micros());
            match value {
                Value::Array(ids) => assert_eq!(ids.len(), count),
                other => panic!("publish_batch failed: {other:?}"),
            }
            sent += count as u64;
        }
        (hist, sent)
    });

    let consumer_permits = permits.clone();
    let consume = tokio::spawn(async move {
        let mut hist = Histogram::new();
        let mut verifier = Verifier::new(total);
        let mut acked = 0u64;
        while acked < total {
            let timer = Timer::start();
            let value = consumer
                .call(&[
                    b"TAKE".to_vec(),
                    b"jobs".to_vec(),
                    batch.to_string().into_bytes(),
                    b"30000".to_vec(),
                    b"0".to_vec(),
                ])
                .await
                .unwrap();
            let Value::Array(records) = value else {
                panic!("take failed: {value:?}");
            };
            if records.is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
                continue;
            }
            let mut ack = vec![b"ACK".to_vec()];
            for record in &records {
                let Value::Array(fields) = record else {
                    panic!("bad take record");
                };
                let Value::Bulk(payload) = &fields[6] else {
                    panic!("bad payload");
                };
                verifier.observe(payload);
                let Value::Bulk(token) = &fields[0] else {
                    panic!("bad token");
                };
                ack.push(token.clone());
            }
            let value = consumer.call(&ack).await.unwrap();
            let count = match value {
                Value::Array(results) => {
                    assert!(
                        results
                            .iter()
                            .all(|result| matches!(result, Value::Simple(text) if text == "OK")),
                        "ack failed"
                    );
                    results.len()
                }
                Value::Simple(ref text) if text == "OK" => 1,
                other => panic!("ack failed: {other:?}"),
            };
            hist.record(timer.micros());
            acked += count as u64;
            consumer_permits.add_permits(count);
        }
        (hist, acked, verifier)
    });

    let (mut publish_hist, sent) = produce.await.unwrap();
    let (mut consume_hist, acked, verifier) = consume.await.unwrap();
    let wall_ms = (start.micros() / 1000).max(1);
    assert_eq!(sent, total);
    assert_eq!(acked, total);
    let duplicates = verifier.check();

    let _ = stop.send(());
    let _ = server.await;

    Stats {
        phase: "e2e_lifecycle",
        protocol: "native".into(),
        jobs: total,
        batch,
        payload: payload_size,
        rep: config.rep,
        window_batches: config.window_batches,
        mode: config.mode.clone(),
        wall_ms,
        jobs_per_sec: total as f64 * 1000.0 / wall_ms as f64,
        publish_batch_us_p50: publish_hist.p50(),
        publish_batch_us_p99: publish_hist.p99(),
        consume_batch_us_p50: consume_hist.p50(),
        consume_batch_us_p99: consume_hist.p99(),
        peak_rss_kb: hwm_kb(),
        db_bytes: dir_alloc_bytes(&dir.path),
        duplicates,
        verified: true,
    }
}

async fn start_amqp1_server(
    ante: Arc<Ante>,
) -> (String, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (stop, rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        ante_amqp1::serve_until(listener, ante, rx).await.unwrap();
    });
    (address, stop, task)
}

fn amqp1_payload(body: &Body<Amqp1Value>) -> Vec<u8> {
    match body {
        Body::Data(batch) => batch
            .iter()
            .flat_map(|data| data.0.as_ref().iter().copied())
            .collect(),
        Body::Value(fe2o3_amqp::types::messaging::AmqpValue(Amqp1Value::Binary(binary))) => {
            binary.to_vec()
        }
        Body::Value(fe2o3_amqp::types::messaging::AmqpValue(Amqp1Value::String(text))) => {
            text.as_bytes().to_vec()
        }
        other => panic!("unexpected AMQP 1.0 body: {other:?}"),
    }
}

async fn amqp1_produce(
    url: String,
    name: String,
    start: u64,
    end: u64,
    payload_size: usize,
) -> (Histogram, u64) {
    let mut connection = Amqp1Connection::open(name.as_str(), url.as_str())
        .await
        .unwrap();
    let mut session = Amqp1Session::begin(&mut connection).await.unwrap();
    let mut sender = Amqp1Sender::attach(&mut session, name.as_str(), "jobs")
        .await
        .unwrap();
    let mut hist = Histogram::new();
    for index in start..end {
        let timer = Timer::start();
        let message = Message::builder()
            .data(make_job(index, payload_size))
            .build();
        let outcome = sender.send(message).await.unwrap();
        assert!(outcome.is_accepted(), "send rejected for job {index}");
        hist.record(timer.micros());
    }
    sender.close().await.unwrap();
    session.end().await.unwrap();
    connection.close().await.unwrap();
    (hist, end - start)
}

async fn amqp1_consume(
    url: String,
    name: String,
    total: u64,
    acked: Arc<AtomicU64>,
    done: watch::Sender<bool>,
    mut done_rx: watch::Receiver<bool>,
    mut release: watch::Receiver<bool>,
) -> (Histogram, Vec<u64>) {
    let mut connection = Amqp1Connection::open(name.as_str(), url.as_str())
        .await
        .unwrap();
    let mut session = Amqp1Session::begin(&mut connection).await.unwrap();
    let mut receiver = Amqp1Receiver::attach(&mut session, name.as_str(), "jobs")
        .await
        .unwrap();
    let mut hist = Histogram::new();
    let mut received = Vec::new();
    while acked.load(Ordering::Relaxed) < total {
        let received_message = tokio::select! {
            _ = done_rx.changed() => break,
            result = tokio::time::timeout(Duration::from_secs(30), receiver.recv::<Body<Amqp1Value>>()) => {
                match result {
                    Ok(Ok(delivery)) => delivery,
                    Ok(Err(error)) => panic!("{name} receive failed: {error}"),
                    Err(_) => {
                        if acked.load(Ordering::Relaxed) >= total {
                            break;
                        }
                        panic!("{name} stalled before all jobs were consumed");
                    }
                }
            }
        };
        let payload = amqp1_payload(&received_message.message().body);
        let timer = Timer::start();
        receiver.accept(&received_message).await.unwrap();
        hist.record(timer.micros());
        received.push(job_index(&payload));
        if acked.fetch_add(1, Ordering::Relaxed) + 1 == total {
            let _ = done.send(true);
        }
    }
    while !*release.borrow() {
        release.changed().await.unwrap();
    }
    receiver.close().await.unwrap();
    session.end().await.unwrap();
    connection.close().await.unwrap();
    (hist, received)
}

async fn run_amqp1(config: &Config) -> Stats {
    assert_eq!(
        config.mode, "both",
        "amqp1 benchmark only supports --mode=both"
    );
    let directory = TempDir::new();
    let ante = Arc::new(Ante::open(directory.path.join("ante.sqlite3")).unwrap());
    ante.declare_queue("jobs").unwrap();
    let (address, stop, server) = start_amqp1_server(ante.clone()).await;
    let url = format!("amqp://{address}");

    let total = config.total;
    let payload_size = config.payload;
    let workers = 4u64;
    let acked = Arc::new(AtomicU64::new(0));
    let (done_tx, done_rx) = watch::channel(false);
    let (release_tx, release_rx) = watch::channel(false);
    let start = Timer::start();

    let mut producers = Vec::new();
    for index in 0..workers {
        let first = total * index / workers;
        let last = total * (index + 1) / workers;
        producers.push(tokio::spawn(amqp1_produce(
            url.clone(),
            format!("producer-{index}"),
            first,
            last,
            payload_size,
        )));
    }
    let mut consumers = Vec::new();
    for index in 0..workers {
        consumers.push(tokio::spawn(amqp1_consume(
            url.clone(),
            format!("consumer-{index}"),
            total,
            acked.clone(),
            done_tx.clone(),
            done_rx.clone(),
            release_rx.clone(),
        )));
    }

    let mut publish_hist = Histogram::new();
    let mut sent = 0u64;
    for producer in producers {
        let (hist, count) = producer.await.unwrap();
        publish_hist.merge(hist);
        sent += count;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while acked.load(Ordering::Relaxed) < total {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for consumers"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let info = ante.info("jobs").unwrap();
        if info.total() == 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "server did not settle accepted deliveries: ready={} inflight={}",
            info.ready,
            info.inflight
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let _ = release_tx.send(true);
    let mut consume_hist = Histogram::new();
    let mut seen = vec![false; total as usize];
    let mut duplicates = 0u64;
    for consumer in consumers {
        let (hist, indexes) = consumer.await.unwrap();
        consume_hist.merge(hist);
        for index in indexes {
            let slot = &mut seen[index as usize];
            if *slot {
                duplicates += 1;
            } else {
                *slot = true;
            }
        }
    }
    let wall_ms = (start.micros() / 1000).max(1);
    assert_eq!(sent, total);
    let missing = seen.iter().filter(|seen| !**seen).count();
    assert_eq!(missing, 0, "{missing} jobs were not delivered");
    assert_eq!(duplicates, 0, "{duplicates} duplicate deliveries");
    let info = ante.info("jobs").unwrap();
    assert_eq!(
        info.total(),
        0,
        "queue not drained: ready={} inflight={}",
        info.ready,
        info.inflight
    );

    let _ = stop.send(());
    let _ = server.await;

    Stats {
        phase: "e2e_lifecycle",
        protocol: "amqp1".into(),
        jobs: total,
        batch: config.batch,
        payload: payload_size,
        rep: config.rep,
        window_batches: config.window_batches,
        mode: config.mode.clone(),
        wall_ms,
        jobs_per_sec: total as f64 * 1000.0 / wall_ms as f64,
        publish_batch_us_p50: publish_hist.p50(),
        publish_batch_us_p99: publish_hist.p99(),
        consume_batch_us_p50: consume_hist.p50(),
        consume_batch_us_p99: consume_hist.p99(),
        peak_rss_kb: hwm_kb(),
        db_bytes: dir_alloc_bytes(&directory.path),
        duplicates,
        verified: true,
    }
}

async fn run_amqp(config: &Config) -> Stats {
    let dir = TempDir::new();
    let ante = Arc::new(Ante::open(dir.path.join("ante.redb")).unwrap());
    let (address, stop, server) = start_amqp_server(ante.clone()).await;

    let url = format!(
        "amqp://guest:guest@127.0.0.1:{}/%2f",
        address.rsplit(':').next().unwrap()
    );
    let connection = Connection::connect(&url, ConnectionProperties::default())
        .await
        .unwrap();

    let producer = connection.create_channel().await.unwrap();
    producer
        .queue_declare(
            "jobs",
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();

    let total = config.total;
    let batch = config.batch.min(256);
    let payload_size = config.payload;
    let publish_mode = config.mode != "consume";
    let consume_mode = config.mode != "publish";

    if publish_mode {
        producer
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .unwrap();
    }

    if config.mode == "consume" {
        let mut filled = 0u64;
        while filled < total {
            let count = batch.min((total - filled) as usize);
            let messages: Vec<ante_core::NewMessage> = (0..count)
                .map(|offset| ante_core::NewMessage {
                    payload: make_job(filled + offset as u64, payload_size),
                    ..Default::default()
                })
                .collect();
            ante.publish_batch("jobs", messages).unwrap();
            filled += count as u64;
        }
    }

    let consumer_channel = connection.create_channel().await.unwrap();
    consumer_channel
        .basic_qos(batch as u16, BasicQosOptions::default())
        .await
        .unwrap();

    let window = if consume_mode {
        batch * config.window_batches as usize
    } else {
        total as usize
    };
    let permits = Arc::new(Semaphore::new(window));
    let start = Timer::start();

    let consume = if consume_mode {
        let mut consumer = consumer_channel
            .basic_consume(
                "jobs",
                "bench",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        let consumer_permits = permits.clone();
        let barrier = consumer_channel.clone();
        Some(tokio::spawn(async move {
            let mut hist = Histogram::new();
            let mut verifier = Verifier::new(total);
            let mut acked = 0u64;
            while acked < total {
                let timer = Timer::start();
                let mut last_tag = 0;
                let mut received = 0u64;
                while received < batch as u64 {
                    let delivery = consumer.next().await.unwrap().unwrap();
                    verifier.observe(&delivery.data);
                    last_tag = delivery.delivery_tag;
                    received += 1;
                }
                consumer_channel
                    .basic_ack(last_tag, BasicAckOptions { multiple: true })
                    .await
                    .unwrap();
                hist.record(timer.micros());
                acked += received;
                consumer_permits.add_permits(received as usize);
            }
            // basic.ack has no reply: a passive declare on the same channel is a
            // wire-level barrier proving the server committed the final ack.
            barrier
                .queue_declare(
                    "jobs",
                    QueueDeclareOptions {
                        passive: true,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
                .unwrap();
            (hist, acked, verifier)
        }))
    } else {
        None
    };

    let produce = if publish_mode {
        let producer_permits = permits.clone();
        Some(tokio::spawn(async move {
            let mut hist = Histogram::new();
            let mut sent = 0u64;
            while sent < total {
                let count = batch.min((total - sent) as usize);
                if consume_mode {
                    producer_permits
                        .clone()
                        .acquire_many_owned(count as u32)
                        .await
                        .unwrap()
                        .forget();
                }
                let timer = Timer::start();
                for offset in 0..count {
                    producer
                        .basic_publish(
                            "",
                            "jobs",
                            BasicPublishOptions::default(),
                            &make_job(sent + offset as u64, payload_size),
                            BasicProperties::default(),
                        )
                        .await
                        .unwrap();
                }
                let returned = producer.wait_for_confirms().await.unwrap();
                assert!(
                    returned.is_empty(),
                    "{} messages were returned",
                    returned.len()
                );
                hist.record(timer.micros());
                sent += count as u64;
            }
            (hist, sent)
        }))
    } else {
        None
    };

    let (mut publish_hist, sent) = match produce {
        Some(handle) => handle.await.unwrap(),
        None => (Histogram::new(), 0),
    };
    let (mut consume_hist, acked, verifier) = match consume {
        Some(handle) => handle.await.unwrap(),
        None => (Histogram::new(), 0, Verifier::new(total)),
    };
    let wall_ms = (start.micros() / 1000).max(1);

    if publish_mode {
        assert_eq!(sent, total);
    }
    if consume_mode {
        assert_eq!(acked, total);
    }
    let duplicates = if consume_mode {
        verifier.check()
    } else {
        verify_stored(&ante, total, batch)
    };
    let info = ante.info("jobs").unwrap();
    assert_eq!(
        info.total(),
        0,
        "queue not drained: ready={} inflight={}",
        info.ready,
        info.inflight
    );

    let _ = connection.close(200, "done").await;
    let _ = stop.send(());
    let _ = server.await;

    Stats {
        phase: "e2e_lifecycle",
        protocol: "amqp".into(),
        jobs: total,
        batch,
        payload: payload_size,
        rep: config.rep,
        window_batches: config.window_batches,
        mode: config.mode.clone(),
        wall_ms,
        jobs_per_sec: total as f64 * 1000.0 / wall_ms as f64,
        publish_batch_us_p50: publish_hist.p50(),
        publish_batch_us_p99: publish_hist.p99(),
        consume_batch_us_p50: consume_hist.p50(),
        consume_batch_us_p99: consume_hist.p99(),
        peak_rss_kb: hwm_kb(),
        db_bytes: dir_alloc_bytes(&dir.path),
        duplicates,
        verified: true,
    }
}

fn verify_stored(ante: &Ante, total: u64, batch: usize) -> u64 {
    let mut verifier = Verifier::new(total);
    let mut acked = 0u64;
    while acked < total {
        let deliveries = ante.take("jobs", batch, 30_000).unwrap();
        assert!(!deliveries.is_empty(), "queue drained before {total} jobs");
        for delivery in &deliveries {
            verifier.observe(&delivery.payload);
        }
        let tokens: Vec<_> = deliveries
            .iter()
            .map(|delivery| delivery.token.clone())
            .collect();
        ante.ack_batch(&tokens).unwrap();
        acked += deliveries.len() as u64;
    }
    verifier.check()
}

fn parse_args() -> Config {
    let mut protocol = String::from("native");
    let mut total = 0u64;
    let mut batch = 128usize;
    let mut payload = 1024usize;
    let mut rep = 0u64;
    let mut window_batches = 8u64;
    let mut mode = String::from("both");
    let mut positional = 0;

    for arg in std::env::args().skip(1) {
        if let Some((key, value)) = arg.split_once('=') {
            match key {
                "--total" => total = value.parse().expect("total"),
                "--batch" => batch = value.parse().expect("batch"),
                "--payload" => payload = value.parse().expect("payload"),
                "--rep" => rep = value.parse().expect("rep"),
                "--window" => window_batches = value.parse().expect("window"),
                "--mode" => mode = value.to_string(),
                other => panic!("unknown flag {other}"),
            }
        } else if positional == 0 {
            protocol = arg;
            positional += 1;
        } else {
            panic!("unexpected argument {arg}");
        }
    }

    assert!(batch > 0 && batch <= 256, "batch must be 1..=256");
    assert!(payload >= 8, "payload must be at least 8 bytes");
    if total == 0 {
        total = if batch == 1 { 2_000 } else { 102_400 };
    }
    assert_eq!(total % batch as u64, 0, "total must be divisible by batch");
    assert!(window_batches > 0, "window must be positive");
    assert!(
        matches!(mode.as_str(), "both" | "publish" | "consume"),
        "mode must be both|publish|consume"
    );
    Config {
        protocol,
        total,
        batch,
        payload,
        rep,
        window_batches,
        mode,
    }
}
