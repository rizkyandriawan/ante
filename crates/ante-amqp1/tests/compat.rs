use std::sync::Arc;
use std::time::Duration;

use ante_core::Ante;
use fe2o3_amqp::link::receiver::CreditMode;
use fe2o3_amqp::types::messaging::{AmqpValue, Body, Message, MessageId, Modified, Properties};
use fe2o3_amqp::types::primitives::Value;
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

#[tokio::test]
async fn amqp1_sender_and_receiver_round_trip() {
    let directory = tempfile::tempdir().unwrap();
    let ante = Arc::new(Ante::open(directory.path().join("ante.sqlite3")).unwrap());
    ante.declare_queue("jobs").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (stop, shutdown) = oneshot::channel();
    let server_ante = ante.clone();
    let server = tokio::spawn(async move {
        ante_amqp1::serve_until(listener, server_ante, shutdown)
            .await
            .unwrap();
    });

    let url = format!("amqp://127.0.0.1:{port}");
    let mut connection = Connection::open("ante-amqp1-test", url.as_str())
        .await
        .unwrap();
    let mut session = Session::begin(&mut connection).await.unwrap();
    let mut sender = Sender::attach(&mut session, "sender-1", "jobs")
        .await
        .unwrap();
    let outcome = sender
        .send(Message::builder().data(b"data body".to_vec()).build())
        .await
        .unwrap();
    assert!(outcome.is_accepted());
    let outcome = sender.send("hello AMQP 1.0").await.unwrap();
    assert!(outcome.is_accepted());
    sender.close().await.unwrap();

    let mut receiver = Receiver::attach(&mut session, "receiver-1", "jobs")
        .await
        .unwrap();
    let delivery = receiver.recv::<Body<Value>>().await.unwrap();
    assert_eq!(payload(&delivery.message().body), b"data body");
    receiver.accept(&delivery).await.unwrap();
    let delivery = receiver.recv::<Body<Value>>().await.unwrap();
    assert_eq!(payload(&delivery.message().body), b"hello AMQP 1.0");
    receiver.accept(&delivery).await.unwrap();
    receiver.close().await.unwrap();
    session.end().await.unwrap();
    connection.close().await.unwrap();

    let info = ante.info("jobs").unwrap();
    assert_eq!(info.total(), 0);

    let _ = stop.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn amqp1_receiver_releases_and_rejects() {
    let directory = tempfile::tempdir().unwrap();
    let ante = Arc::new(Ante::open(directory.path().join("ante.sqlite3")).unwrap());
    ante.declare_queue("jobs").unwrap();
    ante.publish(
        "jobs",
        ante_core::NewMessage {
            payload: b"first".to_vec(),
            content_type: Some("text/plain".to_string()),
            message_id: Some("external-1".to_string()),
            correlation_id: None,
            reply_to: None,
        },
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (stop, shutdown) = oneshot::channel();
    let server_ante = ante.clone();
    let server = tokio::spawn(async move {
        ante_amqp1::serve_until(listener, server_ante, shutdown)
            .await
            .unwrap();
    });

    let url = format!("amqp://127.0.0.1:{port}");
    let mut connection = Connection::open("ante-amqp1-release", url.as_str())
        .await
        .unwrap();
    let mut session = Session::begin(&mut connection).await.unwrap();
    let mut receiver = Receiver::attach(&mut session, "receiver-2", "jobs")
        .await
        .unwrap();
    let delivery = receiver.recv::<Body<Value>>().await.unwrap();
    let message = delivery.message();
    assert_eq!(
        message
            .properties
            .as_ref()
            .and_then(|properties| properties.message_id.as_ref())
            .map(|id| format!("{id:?}")),
        Some("String(\"external-1\")".to_string())
    );
    receiver.release(&delivery).await.unwrap();
    let delivery = receiver.recv::<Body<Value>>().await.unwrap();
    receiver.reject(&delivery, None).await.unwrap();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    while ante.info("jobs").unwrap().total() != 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(ante.info("jobs").unwrap().total(), 0);
    receiver.close().await.unwrap();
    session.end().await.unwrap();
    connection.close().await.unwrap();

    let _ = stop.send(());
    server.await.unwrap();
}

fn payload(body: &Body<Value>) -> Vec<u8> {
    match body {
        Body::Data(batch) => batch
            .iter()
            .flat_map(|data| data.0.as_ref().iter().copied())
            .collect(),
        Body::Value(AmqpValue(Value::Binary(binary))) => binary.to_vec(),
        Body::Value(AmqpValue(Value::String(text))) => text.as_bytes().to_vec(),
        other => panic!("unexpected body: {other:?}"),
    }
}

#[tokio::test]
async fn amqp1_modified_requeues_delivery() {
    let directory = tempfile::tempdir().unwrap();
    let ante = Arc::new(Ante::open(directory.path().join("ante.sqlite3")).unwrap());
    ante.declare_queue("jobs").unwrap();
    ante.publish(
        "jobs",
        ante_core::NewMessage {
            payload: b"retry me".to_vec(),
            content_type: None,
            message_id: None,
            correlation_id: None,
            reply_to: None,
        },
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (stop, shutdown) = oneshot::channel();
    let server_ante = ante.clone();
    let server = tokio::spawn(async move {
        ante_amqp1::serve_until(listener, server_ante, shutdown)
            .await
            .unwrap();
    });
    let url = format!("amqp://127.0.0.1:{port}");
    let mut connection = Connection::open("ante-amqp1-modified", url.as_str())
        .await
        .unwrap();
    let mut session = Session::begin(&mut connection).await.unwrap();
    let mut receiver = Receiver::attach(&mut session, "modified-receiver", "jobs")
        .await
        .unwrap();
    let delivery = receiver.recv::<Body<Value>>().await.unwrap();
    receiver
        .modify(
            &delivery,
            Modified {
                delivery_failed: None,
                undeliverable_here: None,
                message_annotations: None,
            },
        )
        .await
        .unwrap();
    let delivery = receiver.recv::<Body<Value>>().await.unwrap();
    assert_eq!(payload(&delivery.message().body), b"retry me");
    receiver.accept(&delivery).await.unwrap();
    receiver.close().await.unwrap();
    session.end().await.unwrap();
    connection.close().await.unwrap();
    let info = ante.info("jobs").unwrap();
    assert_eq!(info.total(), 0);
    let _ = stop.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn amqp1_stale_consumer_does_not_consume_recreated_queue() {
    let directory = tempfile::tempdir().unwrap();
    let ante = Arc::new(Ante::open(directory.path().join("ante.sqlite3")).unwrap());
    ante.declare_queue("jobs").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (stop, shutdown) = oneshot::channel();
    let server_ante = ante.clone();
    let server = tokio::spawn(async move {
        ante_amqp1::serve_until(listener, server_ante, shutdown)
            .await
            .unwrap();
    });
    let url = format!("amqp://127.0.0.1:{port}");
    let mut connection = Connection::open("ante-amqp1-stale", url.as_str())
        .await
        .unwrap();
    let mut session = Session::begin(&mut connection).await.unwrap();
    let mut receiver = Receiver::attach(&mut session, "stale-receiver", "jobs")
        .await
        .unwrap();

    ante.delete_queue("jobs").unwrap();
    ante.declare_queue("jobs").unwrap();
    ante.publish(
        "jobs",
        ante_core::NewMessage {
            payload: b"new generation".to_vec(),
            content_type: None,
            message_id: None,
            correlation_id: None,
            reply_to: None,
        },
    )
    .unwrap();

    let stale = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        receiver.recv::<Body<Value>>(),
    )
    .await;
    assert!(stale.is_err() || stale.unwrap().is_err());

    let mut fresh = Receiver::attach(&mut session, "fresh-receiver", "jobs")
        .await
        .unwrap();
    let delivery = fresh.recv::<Body<Value>>().await.unwrap();
    assert_eq!(payload(&delivery.message().body), b"new generation");
    fresh.accept(&delivery).await.unwrap();
    fresh.close().await.unwrap();
    session.end().await.unwrap();
    connection.close().await.unwrap();
    let _ = stop.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn amqp1_send_failure_requeues_and_stops_link() {
    use ante_core::MessageState;

    let directory = tempfile::tempdir().unwrap();
    let ante = Arc::new(Ante::open(directory.path().join("ante.sqlite3")).unwrap());
    ante.declare_queue("jobs").unwrap();
    let message_id = ante
        .publish(
            "jobs",
            ante_core::NewMessage {
                payload: b"do not lose me".to_vec(),
                content_type: None,
                message_id: None,
                correlation_id: None,
                reply_to: None,
            },
        )
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (stop, shutdown) = oneshot::channel();
    let server_ante = ante.clone();
    let server = tokio::spawn(async move {
        ante_amqp1::serve_until(listener, server_ante, shutdown)
            .await
            .unwrap();
    });
    let url = format!("amqp://127.0.0.1:{port}");
    let mut connection = Connection::open("ante-amqp1-failure", url.as_str())
        .await
        .unwrap();
    let mut session = Session::begin(&mut connection).await.unwrap();
    let receiver = Receiver::attach(&mut session, "failure-receiver", "jobs")
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let status = ante.message_status("jobs", message_id).unwrap().unwrap();
        if status.state == MessageState::Inflight {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "server never claimed the message"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    drop(receiver);
    let _ = connection.close().await;

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let status = ante.message_status("jobs", message_id).unwrap().unwrap();
        if status.state == MessageState::Ready {
            assert_eq!(
                status.history.last().map(|attempt| attempt.outcome),
                Some(MessageState::Requeued)
            );
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "message was not requeued after the link failed: {status:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let _ = stop.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn amqp1_recovers_expired_leases_without_consumers() {
    let directory = tempfile::tempdir().unwrap();
    let ante = Arc::new(Ante::open(directory.path().join("ante.sqlite3")).unwrap());
    ante.declare_queue("jobs").unwrap();
    ante.publish(
        "jobs",
        ante_core::NewMessage {
            payload: b"expired".to_vec(),
            content_type: None,
            message_id: None,
            correlation_id: None,
            reply_to: None,
        },
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (stop, shutdown) = oneshot::channel();
    let server_ante = ante.clone();
    let server = tokio::spawn(async move {
        ante_amqp1::serve_until(listener, server_ante, shutdown)
            .await
            .unwrap();
    });
    let deliveries = ante.take("jobs", 1, 10).unwrap();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(ante.info("jobs").unwrap().inflight, 1);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let info = ante.info("jobs").unwrap();
        if info.ready == 1 && info.inflight == 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "maintenance did not recover the lease: {info:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let _ = stop.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn amqp1_zero_credit_receiver_does_not_claim() {
    let directory = tempfile::tempdir().unwrap();
    let ante = Arc::new(Ante::open(directory.path().join("ante.sqlite3")).unwrap());
    ante.declare_queue("jobs").unwrap();
    ante.publish(
        "jobs",
        ante_core::NewMessage {
            payload: b"wait for credit".to_vec(),
            content_type: None,
            message_id: None,
            correlation_id: None,
            reply_to: None,
        },
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (stop, shutdown) = oneshot::channel();
    let server_ante = ante.clone();
    let options = ante_amqp1::ServerOptions {
        credit_wait: Duration::from_millis(50),
        ..Default::default()
    };
    let server = tokio::spawn(async move {
        ante_amqp1::serve_until_with_options(listener, server_ante, options, shutdown)
            .await
            .unwrap();
    });
    let url = format!("amqp://127.0.0.1:{port}");
    let mut connection = Connection::open("ante-amqp1-zero-credit", url.as_str())
        .await
        .unwrap();
    let mut session = Session::begin(&mut connection).await.unwrap();
    let mut receiver = Receiver::builder()
        .name("zero-credit-receiver")
        .source("jobs")
        .credit_mode(CreditMode::Manual)
        .attach(&mut session)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    let info = ante.info("jobs").unwrap();
    assert_eq!(info.ready, 1, "message was claimed without credit");
    assert_eq!(info.inflight, 0, "message was claimed without credit");

    receiver.set_credit(1).await.unwrap();
    let delivery = receiver.recv::<Body<Value>>().await.unwrap();
    assert_eq!(payload(&delivery.message().body), b"wait for credit");
    receiver.accept(&delivery).await.unwrap();
    receiver.close().await.unwrap();
    session.end().await.unwrap();
    connection.close().await.unwrap();
    assert_eq!(ante.info("jobs").unwrap().total(), 0);
    let _ = stop.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn amqp1_rejects_binary_message_ids() {
    let directory = tempfile::tempdir().unwrap();
    let ante = Arc::new(Ante::open(directory.path().join("ante.sqlite3")).unwrap());
    ante.declare_queue("jobs").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (stop, shutdown) = oneshot::channel();
    let server_ante = ante.clone();
    let server = tokio::spawn(async move {
        ante_amqp1::serve_until(listener, server_ante, shutdown)
            .await
            .unwrap();
    });
    let url = format!("amqp://127.0.0.1:{port}");
    let mut connection = Connection::open("ante-amqp1-binary-id", url.as_str())
        .await
        .unwrap();
    let mut session = Session::begin(&mut connection).await.unwrap();
    let mut sender = Sender::attach(&mut session, "binary-id-sender", "jobs")
        .await
        .unwrap();
    let message = Message::builder()
        .properties(Properties {
            message_id: Some(MessageId::Binary(vec![0x80].into())),
            ..Properties::default()
        })
        .data(b"reject me".to_vec())
        .build();
    let outcome = sender.send(message).await.unwrap();
    assert!(
        outcome.is_rejected(),
        "binary id must be rejected: {outcome:?}"
    );
    sender.close().await.unwrap();
    session.end().await.unwrap();
    connection.close().await.unwrap();
    assert_eq!(ante.info("jobs").unwrap().total(), 0);
    let _ = stop.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn amqp1_zero_session_window_aborts_instead_of_stalling() {
    let directory = tempfile::tempdir().unwrap();
    let ante = Arc::new(Ante::open(directory.path().join("ante.sqlite3")).unwrap());
    ante.declare_queue("jobs").unwrap();
    ante.publish(
        "jobs",
        ante_core::NewMessage {
            payload: b"session-window".to_vec(),
            ..Default::default()
        },
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (stop, shutdown) = oneshot::channel();
    let server_ante = ante.clone();
    let options = ante_amqp1::ServerOptions {
        credit_wait: Duration::from_millis(150),
        ..Default::default()
    };
    let server = tokio::spawn(async move {
        ante_amqp1::serve_until_with_options(listener, server_ante, options, shutdown)
            .await
            .unwrap();
    });
    let url = format!("amqp://127.0.0.1:{port}");
    let mut connection = Connection::open("ante-amqp1-window-zero", url.as_str())
        .await
        .unwrap();
    let mut session = Session::builder()
        .incoming_window(0)
        .begin(&mut connection)
        .await
        .unwrap();
    let mut receiver = Receiver::builder()
        .name("window-zero")
        .source("jobs")
        .credit_mode(CreditMode::Manual)
        .attach(&mut session)
        .await
        .unwrap();
    receiver.set_credit(1).await.unwrap();
    let recv = tokio::time::timeout(Duration::from_secs(3), receiver.recv::<Body<Value>>()).await;
    assert!(
        matches!(recv, Ok(Err(_))),
        "server must abort the blocked session instead of waiting: {recv:?}"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let info = ante.info("jobs").unwrap();
        if info.ready == 1 && info.inflight == 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "message was not recovered: {info:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    drop(receiver);
    drop(session);
    drop(connection);

    let mut connection = Connection::open("ante-amqp1-window-zero-retry", url.as_str())
        .await
        .unwrap();
    let mut session = Session::begin(&mut connection).await.unwrap();
    let mut receiver = Receiver::attach(&mut session, "retry", "jobs")
        .await
        .unwrap();
    let delivery = tokio::time::timeout(Duration::from_secs(3), receiver.recv::<Body<Value>>())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(payload(&delivery.message().body), b"session-window");
    receiver.accept(&delivery).await.unwrap();
    receiver.close().await.unwrap();
    session.end().await.unwrap();
    connection.close().await.unwrap();
    assert_eq!(ante.info("jobs").unwrap().total(), 0);
    let _ = stop.send(());
    server.await.unwrap();
}

async fn partial_transfer_timeout(body_size: usize) {
    let directory = tempfile::tempdir().unwrap();
    let ante = Arc::new(Ante::open(directory.path().join("ante.sqlite3")).unwrap());
    ante.declare_queue("jobs").unwrap();
    let body = vec![7u8; body_size];
    ante.publish(
        "jobs",
        ante_core::NewMessage {
            payload: body.clone(),
            ..Default::default()
        },
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (stop, shutdown) = oneshot::channel();
    let server_ante = ante.clone();
    let options = ante_amqp1::ServerOptions {
        credit_wait: Duration::from_millis(150),
        ..Default::default()
    };
    let server = tokio::spawn(async move {
        ante_amqp1::serve_until_with_options(listener, server_ante, options, shutdown)
            .await
            .unwrap();
    });
    let url = format!("amqp://127.0.0.1:{port}");
    let gate = Arc::new(ReadGate::default());
    let stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let mut connection = Connection::builder()
        .container_id("ante-amqp1-partial")
        .max_frame_size(512)
        .open_with_stream(GatedRead {
            stream,
            gate: gate.clone(),
        })
        .await
        .unwrap();
    let mut session = Session::builder()
        .incoming_window(1)
        .session_flow_top_up(false)
        .begin(&mut connection)
        .await
        .unwrap();
    let mut receiver = Receiver::builder()
        .name("partial")
        .source("jobs")
        .credit_mode(CreditMode::Manual)
        .attach(&mut session)
        .await
        .unwrap();
    let before = gate.bytes_read.load(std::sync::atomic::Ordering::SeqCst);
    receiver.set_credit(1).await.unwrap();
    let recv = tokio::time::timeout(Duration::from_secs(3), receiver.recv::<Body<Value>>()).await;
    assert!(
        matches!(recv, Ok(Err(_))),
        "server must abort a stalled multi-frame transfer"
    );

    let received = gate.bytes_read.load(std::sync::atomic::Ordering::SeqCst) - before;
    assert!(
        received >= 400 && received < body_size,
        "expected one partial frame, received {received} bytes for a {body_size}-byte body"
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let info = ante.info("jobs").unwrap();
        if info.ready == 1 && info.inflight == 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "partial transfer was not recovered: {info:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    drop(receiver);
    drop(session);
    drop(connection);

    let mut connection = Connection::open("ante-amqp1-partial-retry", url.as_str())
        .await
        .unwrap();
    let mut session = Session::begin(&mut connection).await.unwrap();
    let mut receiver = Receiver::attach(&mut session, "partial-retry", "jobs")
        .await
        .unwrap();
    let delivery = tokio::time::timeout(Duration::from_secs(3), receiver.recv::<Body<Value>>())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(payload(&delivery.message().body), body);
    receiver.accept(&delivery).await.unwrap();
    receiver.close().await.unwrap();
    session.end().await.unwrap();
    connection.close().await.unwrap();
    assert_eq!(ante.info("jobs").unwrap().total(), 0);
    let _ = stop.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn amqp1_partial_transfer_timeout_does_not_poison_link() {
    partial_transfer_timeout(64 * 1024).await;
}

#[tokio::test]
async fn amqp1_two_frame_transfer_waits_for_final_frame() {
    partial_transfer_timeout(700).await;
}

#[tokio::test]
async fn amqp1_accepted_outcome_is_durable_before_close() {
    use ante_core::MessageState;

    let directory = tempfile::tempdir().unwrap();
    let ante = Arc::new(Ante::open(directory.path().join("ante.sqlite3")).unwrap());
    ante.declare_queue("jobs").unwrap();
    let ids: Vec<u64> = (0..20)
        .map(|index| {
            ante.publish(
                "jobs",
                ante_core::NewMessage {
                    payload: format!("close-race-{index}").into_bytes(),
                    ..Default::default()
                },
            )
            .unwrap()
        })
        .collect();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (stop, shutdown) = oneshot::channel();
    let server_ante = ante.clone();
    let server = tokio::spawn(async move {
        ante_amqp1::serve_until(listener, server_ante, shutdown)
            .await
            .unwrap();
    });
    let url = format!("amqp://127.0.0.1:{port}");
    for (iteration, message_id) in ids.into_iter().enumerate() {
        let mut connection = Connection::open(format!("close-race-{iteration}"), url.as_str())
            .await
            .unwrap();
        let mut session = Session::begin(&mut connection).await.unwrap();
        let mut receiver = Receiver::builder()
            .name("close-race")
            .source("jobs")
            .credit_mode(CreditMode::Manual)
            .attach(&mut session)
            .await
            .unwrap();
        receiver.set_credit(1).await.unwrap();
        let delivery = receiver.recv::<Body<Value>>().await.unwrap();
        receiver.accept(&delivery).await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let status = ante.message_status("jobs", message_id).unwrap().unwrap();
            if status.state == MessageState::Acked {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "accepted outcome was not recorded: {status:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        connection.close().await.unwrap();
        assert_eq!(
            ante.message_status("jobs", message_id)
                .unwrap()
                .unwrap()
                .state,
            MessageState::Acked
        );
    }
    assert_eq!(ante.info("jobs").unwrap().total(), 0);
    let _ = stop.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn amqp1_accept_then_immediate_close_does_not_stall() {
    use ante_core::MessageState;

    let directory = tempfile::tempdir().unwrap();
    let ante = Arc::new(Ante::open(directory.path().join("ante.sqlite3")).unwrap());
    ante.declare_queue("jobs").unwrap();
    let ids: Vec<u64> = (0..20)
        .map(|index| {
            ante.publish(
                "jobs",
                ante_core::NewMessage {
                    payload: format!("immediate-close-{index}").into_bytes(),
                    ..Default::default()
                },
            )
            .unwrap()
        })
        .collect();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (stop, shutdown) = oneshot::channel();
    let server_ante = ante.clone();
    let server = tokio::spawn(async move {
        ante_amqp1::serve_until(listener, server_ante, shutdown)
            .await
            .unwrap();
    });
    let url = format!("amqp://127.0.0.1:{port}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let acked = ids
            .iter()
            .filter(|&&message_id| {
                ante.message_status("jobs", message_id)
                    .unwrap()
                    .unwrap()
                    .state
                    == MessageState::Acked
            })
            .count();
        if acked == ids.len() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "immediate close stalled: {}/{acked} acked",
            ids.len()
        );

        let mut connection = Connection::open(format!("immediate-close-{acked}"), url.as_str())
            .await
            .unwrap();
        let mut session = Session::begin(&mut connection).await.unwrap();
        let mut receiver = Receiver::builder()
            .name("immediate-close")
            .source("jobs")
            .credit_mode(CreditMode::Manual)
            .attach(&mut session)
            .await
            .unwrap();
        receiver.set_credit(1).await.unwrap();
        let delivery = tokio::time::timeout(Duration::from_secs(2), receiver.recv::<Body<Value>>())
            .await
            .unwrap()
            .unwrap();
        receiver.accept(&delivery).await.unwrap();
        connection.close().await.unwrap();
    }
    assert_eq!(ante.info("jobs").unwrap().total(), 0);
    let _ = stop.send(());
    server.await.unwrap();
}

/// Stops application reads without closing the peer socket, reproducing TCP
/// backpressure while the protocol's session window still has capacity.
#[derive(Debug)]
struct GatedRead {
    stream: tokio::net::TcpStream,
    gate: Arc<ReadGate>,
}

#[derive(Debug, Default)]
struct ReadGate {
    paused: std::sync::atomic::AtomicBool,
    bytes_read: std::sync::atomic::AtomicUsize,
    waker: std::sync::Mutex<Option<std::task::Waker>>,
}

impl ReadGate {
    fn pause(&self) {
        self.paused.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn resume(&self) {
        self.paused
            .store(false, std::sync::atomic::Ordering::SeqCst);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}

impl tokio::io::AsyncRead for GatedRead {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        {
            let mut waker = self.gate.waker.lock().unwrap();
            if self.gate.paused.load(std::sync::atomic::Ordering::SeqCst) {
                *waker = Some(cx.waker().clone());
                return std::task::Poll::Pending;
            }
        }
        let before = buf.filled().len();
        let result = std::pin::Pin::new(&mut self.stream).poll_read(cx, buf);
        self.gate.bytes_read.fetch_add(
            buf.filled().len() - before,
            std::sync::atomic::Ordering::SeqCst,
        );
        result
    }
}

impl tokio::io::AsyncWrite for GatedRead {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.stream).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn amqp1_exhausted_window_after_complete_transfer_does_not_requeue() {
    use ante_core::MessageState;

    let directory = tempfile::tempdir().unwrap();
    let ante = Arc::new(Ante::open(directory.path().join("ante.sqlite3")).unwrap());
    ante.declare_queue("jobs").unwrap();
    let id = ante
        .publish(
            "jobs",
            ante_core::NewMessage {
                payload: b"one-frame".to_vec(),
                ..Default::default()
            },
        )
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, shutdown) = oneshot::channel();
    let server_ante = ante.clone();
    let server = tokio::spawn(async move {
        ante_amqp1::serve_until_with_options(
            listener,
            server_ante,
            ante_amqp1::ServerOptions {
                credit_wait: Duration::from_millis(100),
                ..Default::default()
            },
            shutdown,
        )
        .await
        .unwrap();
    });
    let mut connection =
        Connection::open("exhausted-complete", format!("amqp://{address}").as_str())
            .await
            .unwrap();
    let mut session = Session::builder()
        .incoming_window(1)
        .session_flow_top_up(false)
        .begin(&mut connection)
        .await
        .unwrap();
    let mut receiver = Receiver::builder()
        .name("one-frame")
        .source("jobs")
        .credit_mode(CreditMode::Manual)
        .attach(&mut session)
        .await
        .unwrap();
    receiver.set_credit(1).await.unwrap();
    let delivery = tokio::time::timeout(Duration::from_secs(2), receiver.recv::<Body<Value>>())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(payload(&delivery.message().body), b"one-frame");
    // Keep window zero for three transfer-timeout periods before settling.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(ante.info("jobs").unwrap().inflight, 1);
    assert_eq!(ante.info("jobs").unwrap().ready, 0);
    receiver.accept(&delivery).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while ante.message_status("jobs", id).unwrap().unwrap().state != MessageState::Acked {
        assert!(
            tokio::time::Instant::now() < deadline,
            "accepted delivery was not acked"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    connection.close().await.unwrap();
    let _ = stop.send(());
    server.await.unwrap();
}

async fn blocked_tcp_writer(options: ante_amqp1::ServerOptions, recovery_budget: Duration) {
    let directory = tempfile::tempdir().unwrap();
    let ante = Arc::new(
        Ante::open_with(
            directory.path().join("ante.sqlite3"),
            ante_core::Options {
                max_payload_bytes: 16 * 1024 * 1024,
                max_batch_bytes: 16 * 1024 * 1024,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    ante.declare_queue("jobs").unwrap();
    // Much larger than the kernel send buffer, but fewer frames than the
    // advertised 2048-frame session window. No session top-up is required.
    ante.publish(
        "jobs",
        ante_core::NewMessage {
            payload: vec![9u8; 16 * 1024 * 1024],
            ..Default::default()
        },
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, shutdown) = oneshot::channel();
    let server_ante = ante.clone();
    let server = tokio::spawn(async move {
        ante_amqp1::serve_until_with_options(listener, server_ante, options, shutdown)
            .await
            .unwrap();
    });
    let stream = tokio::net::TcpStream::connect(address).await.unwrap();
    socket2::SockRef::from(&stream)
        .set_recv_buffer_size(4096)
        .unwrap();
    let gate = Arc::new(ReadGate::default());
    let mut connection = Connection::builder()
        .container_id("blocked-tcp-writer")
        .open_with_stream(GatedRead {
            stream,
            gate: gate.clone(),
        })
        .await
        .unwrap();
    let mut session = Session::builder()
        .incoming_window(2048)
        .session_flow_top_up(false)
        .begin(&mut connection)
        .await
        .unwrap();
    let mut receiver = Receiver::builder()
        .name("blocked-tcp-writer")
        .source("jobs")
        .credit_mode(CreditMode::Manual)
        .attach(&mut session)
        .await
        .unwrap();
    gate.pause();
    receiver.set_credit(1).await.unwrap();
    let started = tokio::time::Instant::now();
    let claimed_deadline = started + Duration::from_secs(2);
    while ante.info("jobs").unwrap().inflight == 0 {
        assert!(
            tokio::time::Instant::now() < claimed_deadline,
            "message was never claimed"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let claimed_at = tokio::time::Instant::now();
    while ante.info("jobs").unwrap().ready == 0 {
        assert!(
            claimed_at.elapsed() < recovery_budget,
            "writer stayed blocked beyond {:?}",
            recovery_budget
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(ante.info("jobs").unwrap().inflight, 0);
    gate.resume();
    drop(receiver);
    drop(session);
    drop(connection);
    let _ = stop.send(());
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn amqp1_positive_window_blocked_tcp_writer_times_out() {
    blocked_tcp_writer(
        ante_amqp1::ServerOptions {
            credit_wait: Duration::from_millis(150),
            ..Default::default()
        },
        Duration::from_millis(800),
    )
    .await;
}

#[tokio::test]
async fn amqp1_short_lease_caps_blocked_transfer_deadline() {
    blocked_tcp_writer(
        ante_amqp1::ServerOptions {
            lease_ms: 200,
            credit_wait: Duration::from_secs(3),
            ..Default::default()
        },
        Duration::from_millis(800),
    )
    .await;
}
