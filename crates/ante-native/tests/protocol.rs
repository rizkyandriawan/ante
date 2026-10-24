use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
            "ante-native-test-{}-{}-{}",
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
        let server = tokio::spawn(async move {
            serve_until(listener, ante, rx).await.unwrap();
        });
        Harness {
            address,
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

fn expect_error(value: &Value, code: &str) {
    match value {
        Value::Error(message) => assert!(
            message.starts_with(code),
            "expected error {code}, got {message}"
        ),
        other => panic!("expected error {code}, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_take_ack_over_the_wire() {
    let harness = Harness::start().await;
    let mut client = harness.client().await;

    assert_eq!(
        client.hello().await.unwrap(),
        Value::Simple("ANTE 1".into())
    );
    assert_eq!(client.declare("jobs").await.unwrap(), Value::Integer(1));
    assert_eq!(client.ping().await.unwrap(), Value::Simple("PONG".into()));

    let payload: Vec<u8> = (0..=255).collect();
    let published = client.publish("jobs", &payload).await.unwrap();
    assert_eq!(published, Value::Integer(1));

    let taken = client.take("jobs", 1).await.unwrap();
    let Value::Array(deliveries) = taken else {
        panic!("expected delivery array, got {taken:?}");
    };
    assert_eq!(deliveries.len(), 1);
    let Value::Array(fields) = &deliveries[0] else {
        panic!("expected delivery record");
    };
    assert_eq!(fields.len(), 7);
    let token = fields[0].as_str().unwrap().to_string();
    assert_eq!(fields[1].as_str().unwrap(), "1");
    assert_eq!(fields[5].as_str().unwrap(), "1");
    assert_eq!(fields[6], Value::Bulk(payload));

    assert_eq!(
        client.ack(&token).await.unwrap(),
        Value::Simple("OK".into())
    );
    let info = client.info("jobs").await.unwrap();
    let Value::Array(pairs) = info else {
        panic!("expected info array")
    };
    assert_eq!(pairs[7].as_str().unwrap(), "0");
    assert_eq!(pairs[9].as_str().unwrap(), "0");
}

fn field<'a>(pairs: &'a [Value], name: &str) -> &'a Value {
    pairs
        .as_chunks::<2>()
        .0
        .iter()
        .find(|pair| pair[0].as_str() == Some(name))
        .map(|pair| &pair[1])
        .unwrap_or_else(|| panic!("missing field {name}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_reports_attempt_history_over_the_wire() {
    let harness = Harness::start().await;
    let mut client = harness.client().await;
    client.declare("jobs").await.unwrap();
    let published = client.publish("jobs", b"tracked").await.unwrap();
    let Value::Integer(message_id) = published else {
        panic!("expected message id");
    };

    let status = client
        .call(&[
            b"STATUS".to_vec(),
            b"jobs".to_vec(),
            message_id.to_string().into_bytes(),
        ])
        .await
        .unwrap();
    let Value::Array(pairs) = status else {
        panic!("expected status pairs");
    };
    assert_eq!(field(&pairs, "state").as_str(), Some("ready"));
    assert_eq!(field(&pairs, "attempts").as_str(), Some("0"));
    let Value::Array(history) = field(&pairs, "history") else {
        panic!("expected history array");
    };
    assert!(history.is_empty());

    let delivery = take_one(&mut client).await;
    let status = client
        .call(&[
            b"STATUS".to_vec(),
            b"jobs".to_vec(),
            message_id.to_string().into_bytes(),
        ])
        .await
        .unwrap();
    let Value::Array(pairs) = status else {
        panic!("expected status pairs");
    };
    assert_eq!(field(&pairs, "state").as_str(), Some("inflight"));
    assert_eq!(field(&pairs, "attempts").as_str(), Some("1"));
    let Value::Array(history) = field(&pairs, "history") else {
        panic!("expected history array");
    };
    assert_eq!(history.len(), 1);
    let Value::Array(attempt) = &history[0] else {
        panic!("expected attempt pairs");
    };
    assert_eq!(field(attempt, "outcome").as_str(), Some("inflight"));
    assert_eq!(field(attempt, "finished-at-ms").as_str(), Some(""));

    client.ack(&delivery.token).await.unwrap();
    let status = client
        .call(&[
            b"STATUS".to_vec(),
            b"jobs".to_vec(),
            message_id.to_string().into_bytes(),
        ])
        .await
        .unwrap();
    let Value::Array(pairs) = status else {
        panic!("expected status pairs");
    };
    assert_eq!(field(&pairs, "state").as_str(), Some("acked"));
    let Value::Array(history) = field(&pairs, "history") else {
        panic!("expected history array");
    };
    assert_eq!(history.len(), 1);
    let Value::Array(attempt) = &history[0] else {
        panic!("expected attempt pairs");
    };
    assert_eq!(field(attempt, "outcome").as_str(), Some("acked"));
    assert_ne!(field(attempt, "finished-at-ms").as_str(), Some(""));

    let missing = client
        .call(&[b"STATUS".to_vec(), b"jobs".to_vec(), b"999".to_vec()])
        .await
        .unwrap();
    expect_error(&missing, "MESSAGE_NOT_FOUND");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discard_settles_without_acknowledging() {
    let harness = Harness::start().await;
    let mut client = harness.client().await;
    client.declare("jobs").await.unwrap();
    let published = client.publish("jobs", b"poison").await.unwrap();
    let Value::Integer(message_id) = published else {
        panic!("expected message id");
    };
    let delivery = take_one(&mut client).await;

    let discarded = client
        .call(&[b"DISCARD".to_vec(), delivery.token.as_bytes().to_vec()])
        .await
        .unwrap();
    assert_eq!(discarded, Value::Simple("OK".into()));

    let status = client
        .call(&[
            b"STATUS".to_vec(),
            b"jobs".to_vec(),
            message_id.to_string().into_bytes(),
        ])
        .await
        .unwrap();
    let Value::Array(pairs) = status else {
        panic!("expected status pairs");
    };
    assert_eq!(field(&pairs, "state").as_str(), Some("discarded"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn complete_publishes_the_result_and_acks() {
    let harness = Harness::start().await;
    let mut client = harness.client().await;
    client.declare("jobs").await.unwrap();
    client.declare("results").await.unwrap();
    client.publish("jobs", b"input").await.unwrap();
    let delivery = take_one(&mut client).await;

    let completed = client
        .call(&[
            b"COMPLETE".to_vec(),
            delivery.token.into_bytes(),
            b"results".to_vec(),
            b"output".to_vec(),
            b"text/plain".to_vec(),
            b"result-1".to_vec(),
            b"ACK".to_vec(),
        ])
        .await
        .unwrap();
    assert!(matches!(completed, Value::Integer(_)));
    assert_eq!(
        client.info("jobs").await.unwrap(),
        client.info("jobs").await.unwrap()
    );

    let taken = client.take("results", 1).await.unwrap();
    let Value::Array(deliveries) = taken else {
        panic!("expected delivery array");
    };
    let Value::Array(fields) = &deliveries[0] else {
        panic!("expected delivery record");
    };
    assert_eq!(fields[6], Value::Bulk(b"output".to_vec()));
    client.ack(fields[0].as_str().unwrap()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requeue_and_stale_delivery_detection() {
    let harness = Harness::start().await;
    let mut client = harness.client().await;
    client.declare("jobs").await.unwrap();
    client.publish("jobs", b"first").await.unwrap();

    let first = take_one(&mut client).await;
    let requeued = client
        .call(&[b"REQUEUE".to_vec(), first.token.as_bytes().to_vec()])
        .await
        .unwrap();
    assert_eq!(requeued, Value::Simple("OK".into()));

    let second = take_one(&mut client).await;
    assert_ne!(first.token, second.token);

    let stale = client.ack(&first.token).await.unwrap();
    expect_error(&stale, "STALE_DELIVERY");

    assert_eq!(
        client.ack(&second.token).await.unwrap(),
        Value::Simple("OK".into())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_publish_and_batch_ack() {
    let harness = Harness::start().await;
    let mut client = harness.client().await;
    client.declare("batch").await.unwrap();

    let ids = client
        .call(&[
            b"PUBLISH_BATCH".to_vec(),
            b"batch".to_vec(),
            b"a".to_vec(),
            b"b".to_vec(),
            b"c".to_vec(),
        ])
        .await
        .unwrap();
    let Value::Array(ids) = ids else {
        panic!("expected ids")
    };
    assert_eq!(ids.len(), 3);

    let Value::Array(deliveries) = client.take("batch", 10).await.unwrap() else {
        panic!("expected deliveries");
    };
    assert_eq!(deliveries.len(), 3);

    let mut args = vec![b"ACK".to_vec()];
    for delivery in &deliveries {
        let Value::Array(fields) = delivery else {
            panic!()
        };
        args.push(fields[0].as_str().unwrap().as_bytes().to_vec());
    }
    let reply = client.call(&args).await.unwrap();
    let Value::Array(outcomes) = reply else {
        panic!("expected outcomes")
    };
    assert_eq!(outcomes.len(), 3);
    assert!(outcomes.iter().all(|o| *o == Value::Simple("OK".into())));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_queue_and_unknown_command_are_errors() {
    let harness = Harness::start().await;
    let mut client = harness.client().await;

    let missing = client.publish("nope", b"x").await.unwrap();
    expect_error(&missing, "QUEUE_NOT_FOUND");

    let unknown = client.call(&[b"WAT".to_vec()]).await.unwrap();
    expect_error(&unknown, "UNKNOWN_COMMAND");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn long_poll_returns_when_a_message_arrives() {
    let harness = Harness::start().await;
    let mut consumer = harness.client().await;
    let mut producer = harness.client().await;
    consumer.declare("wait").await.unwrap();

    let take = tokio::spawn(async move {
        consumer
            .call(&[
                b"TAKE".to_vec(),
                b"wait".to_vec(),
                b"1".to_vec(),
                b"5000".to_vec(),
                b"5000".to_vec(),
            ])
            .await
            .unwrap()
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    producer.publish("wait", b"late").await.unwrap();

    let reply = take.await.unwrap();
    let Value::Array(deliveries) = reply else {
        panic!("expected deliveries")
    };
    assert_eq!(deliveries.len(), 1);
    let Value::Array(fields) = &deliveries[0] else {
        panic!()
    };
    assert_eq!(fields[6], Value::Bulk(b"late".to_vec()));
}

struct Taken {
    token: String,
}

async fn take_one(client: &mut NativeClient) -> Taken {
    let reply = client.take("jobs", 1).await.unwrap();
    let Value::Array(deliveries) = reply else {
        panic!("expected deliveries")
    };
    let Value::Array(fields) = &deliveries[0] else {
        panic!()
    };
    Taken {
        token: fields[0].as_str().unwrap().to_string(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_closes_open_connections() {
    let mut harness = Harness::start().await;
    let mut client = harness.client().await;
    assert_eq!(client.ping().await.unwrap(), Value::Simple("PONG".into()));

    harness.shutdown.take().unwrap().send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), &mut harness.server)
        .await
        .expect("serve_until did not stop")
        .unwrap();

    let after = tokio::time::timeout(Duration::from_secs(2), client.ping()).await;
    assert!(
        matches!(after, Ok(Err(_)) | Err(_)),
        "an open connection still served a command after shutdown"
    );
}
