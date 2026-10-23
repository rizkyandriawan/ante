use std::sync::Arc;
use std::time::{Duration, Instant};

use ante_core::{AckOutcome, Ante, Delivery, DeliveryToken, Error, NewMessage, RequeueOutcome};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use crate::codec::{array, bulk, write_value, RespReader, Value};
use crate::protocol::{Command, PROTOCOL_VERSION};

const MAX_TAKE: usize = 1024;
const POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Clone, Default)]
struct Drain(Arc<DrainInner>);

#[derive(Default)]
struct DrainInner {
    active: std::sync::atomic::AtomicUsize,
    idle: tokio::sync::Notify,
}

struct DrainGuard(Arc<DrainInner>);

impl Drop for DrainGuard {
    fn drop(&mut self) {
        if self
            .0
            .active
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel)
            == 1
        {
            self.0.idle.notify_one();
        }
    }
}

impl Drain {
    fn begin(&self) -> DrainGuard {
        self.0
            .active
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        DrainGuard(self.0.clone())
    }

    fn active(&self) -> usize {
        self.0.active.load(std::sync::atomic::Ordering::Acquire)
    }

    async fn wait(&self) {
        loop {
            if self.active() == 0 {
                return;
            }
            self.0.idle.notified().await;
        }
    }

    async fn wait_until(&self, deadline: tokio::time::Instant) -> bool {
        tokio::time::timeout_at(deadline, self.wait()).await.is_ok()
    }
}

pub async fn serve(listener: TcpListener, ante: Arc<Ante>) -> std::io::Result<()> {
    let (_tx, rx) = oneshot::channel::<()>();
    serve_until(listener, ante, rx).await
}

pub async fn serve_until(
    listener: TcpListener,
    ante: Arc<Ante>,
    mut shutdown: oneshot::Receiver<()>,
) -> std::io::Result<()> {
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let drain = Drain::default();
    let mut connections = tokio::task::JoinSet::new();

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let ante = ante.clone();
                let guard = drain.begin();
                tokio::task::spawn_blocking(move || {
                    let _guard = guard;
                    if let Err(err) = ante.recover_expired() {
                        eprintln!("ante: lease recovery failed: {err}");
                    }
                });
            }
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(pair) => pair,
                    Err(err) => {
                        eprintln!("ante: accept failed: {err}");
                        continue;
                    }
                };
                let ante = ante.clone();
                let stop = stop_rx.clone();
                let drain = drain.clone();
                connections.spawn(async move {
                    if let Err(err) = handle_connection(stream, ante, drain, stop).await {
                        if err.kind() != std::io::ErrorKind::UnexpectedEof {
                            eprintln!("ante: connection {peer} closed: {err}");
                        }
                    }
                });
            }
            _ = &mut shutdown => break,
            _ = connections.join_next(), if !connections.is_empty() => {},
        }
    }

    let _ = stop_tx.send(true);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !connections.is_empty() {
        match tokio::time::timeout_at(deadline, connections.join_next()).await {
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(_) => {
                connections.abort_all();
                while connections.join_next().await.is_some() {}
                break;
            }
        }
    }
    if !drain.wait_until(deadline).await {
        eprintln!(
            "ante: shutdown deadline reached with {} storage operations still running",
            drain.active()
        );
    }
    Ok(())
}

async fn handle_connection(
    stream: TcpStream,
    ante: Arc<Ante>,
    drain: Drain,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> std::io::Result<()> {
    stream.set_nodelay(true)?;
    let (read_half, write_half) = stream.into_split();
    let mut writer = BufWriter::with_capacity(64 * 1024, write_half);
    let mut reader = RespReader::new(read_half);

    loop {
        let value = tokio::select! {
            _ = stop.changed() => return Ok(()),
            value = reader.read_value() => match value? {
                Some(value) => value,
                None => return Ok(()),
            },
        };

        let command = match Command::parse(value) {
            Ok(command) => command,
            Err(err) => {
                write_value(&mut writer, &error_value(err.code, &err.message)).await?;
                writer.flush().await?;
                continue;
            }
        };

        let quit = matches!(command, Command::Quit);
        let reply = dispatch(command, &ante, &drain).await;
        write_value(&mut writer, &reply).await?;
        writer.flush().await?;
        if quit {
            return Ok(());
        }
    }
}

async fn dispatch(command: Command, ante: &Arc<Ante>, drain: &Drain) -> Value {
    match command {
        Command::Hello { .. } => Value::Simple(format!("ANTE {PROTOCOL_VERSION}")),
        Command::Ping => Value::Simple("PONG".into()),
        Command::Quit => Value::Simple("OK".into()),
        Command::Declare { queue } => {
            let ante = ante.clone();
            match blocking(drain, move || ante.declare_queue(&queue)).await {
                Ok(id) => Value::Integer(id as i64),
                Err(err) => error_value(err.code(), &err.to_string()),
            }
        }
        Command::Publish {
            queue,
            payload,
            content_type,
            message_id,
        } => {
            let ante = ante.clone();
            let message = NewMessage {
                payload,
                content_type,
                message_id,
                ..Default::default()
            };
            match blocking(drain, move || ante.publish(&queue, message)).await {
                Ok(id) => Value::Integer(id as i64),
                Err(err) => error_value(err.code(), &err.to_string()),
            }
        }
        Command::PublishBatch { queue, payloads } => {
            let ante = ante.clone();
            let messages: Vec<NewMessage> = payloads
                .into_iter()
                .map(|payload| NewMessage {
                    payload,
                    ..Default::default()
                })
                .collect();
            match blocking(drain, move || ante.publish_batch(&queue, messages)).await {
                Ok(ids) => array(
                    ids.into_iter()
                        .map(|id| Value::Integer(id as i64))
                        .collect(),
                ),
                Err(err) => error_value(err.code(), &err.to_string()),
            }
        }
        Command::Take {
            queue,
            max,
            lease_ms,
            wait_ms,
        } => take_with_wait(ante, drain, queue, max, lease_ms, wait_ms).await,
        Command::Ack { tokens } => {
            let parsed: Result<Vec<DeliveryToken>, Error> = tokens
                .iter()
                .map(|token| DeliveryToken::parse(token))
                .collect();
            let parsed = match parsed {
                Ok(parsed) => parsed,
                Err(err) => return error_value(err.code(), &err.to_string()),
            };
            let single = parsed.len() == 1;
            let ante = ante.clone();
            match blocking(drain, move || ante.ack_batch(&parsed)).await {
                Ok(outcomes) => {
                    if single {
                        ack_value(outcomes[0])
                    } else {
                        array(outcomes.into_iter().map(ack_value).collect())
                    }
                }
                Err(err) => error_value(err.code(), &err.to_string()),
            }
        }
        Command::Requeue { token } => {
            let token = match DeliveryToken::parse(&token) {
                Ok(token) => token,
                Err(err) => return error_value(err.code(), &err.to_string()),
            };
            let ante = ante.clone();
            match blocking(drain, move || ante.requeue(&token)).await {
                Ok(outcome) => requeue_value(outcome),
                Err(err) => error_value(err.code(), &err.to_string()),
            }
        }
        Command::Info { queue } => {
            let ante = ante.clone();
            match blocking(drain, move || ante.info(&queue)).await {
                Ok(info) => info_value(&info),
                Err(err) => error_value(err.code(), &err.to_string()),
            }
        }
        Command::Purge { queue } => {
            let ante = ante.clone();
            match blocking(drain, move || ante.purge(&queue)).await {
                Ok(count) => Value::Integer(count as i64),
                Err(err) => error_value(err.code(), &err.to_string()),
            }
        }
        Command::DeleteQueue { queue } => {
            let ante = ante.clone();
            match blocking(drain, move || ante.delete_queue(&queue)).await {
                Ok(count) => Value::Integer(count as i64),
                Err(err) => error_value(err.code(), &err.to_string()),
            }
        }
        Command::List => {
            let ante = ante.clone();
            match blocking(drain, move || ante.list_queues()).await {
                Ok(queues) => array(
                    queues
                        .iter()
                        .map(|q| {
                            array(vec![
                                bulk(q.name.clone()),
                                bulk(q.ready.to_string()),
                                bulk(q.inflight.to_string()),
                            ])
                        })
                        .collect(),
                ),
                Err(err) => error_value(err.code(), &err.to_string()),
            }
        }
    }
}

async fn take_with_wait(
    ante: &Arc<Ante>,
    drain: &Drain,
    queue: String,
    max: usize,
    lease_ms: u64,
    wait_ms: u64,
) -> Value {
    let max = max.min(MAX_TAKE);
    if max == 0 {
        return array(Vec::new());
    }
    let deadline = Instant::now() + Duration::from_millis(wait_ms);
    loop {
        let ante = ante.clone();
        let queue_for_call = queue.clone();
        let result = blocking(drain, move || ante.take(&queue_for_call, max, lease_ms)).await;
        match result {
            Ok(deliveries) if !deliveries.is_empty() => {
                return array(deliveries.iter().map(delivery_value).collect())
            }
            Ok(_) => {
                if wait_ms == 0 || Instant::now() >= deadline {
                    return array(Vec::new());
                }
                tokio::time::sleep(POLL_INTERVAL).await;
            }
            Err(err) => return error_value(err.code(), &err.to_string()),
        }
    }
}

fn delivery_value(delivery: &Delivery) -> Value {
    array(vec![
        bulk(delivery.token.to_wire()),
        bulk(delivery.message_id.to_string()),
        bulk(delivery.content_type.clone().unwrap_or_default()),
        bulk(delivery.correlation_id.clone().unwrap_or_default()),
        bulk(delivery.reply_to.clone().unwrap_or_default()),
        bulk(delivery.attempts.to_string()),
        Value::Bulk(delivery.payload.clone()),
    ])
}

fn info_value(info: &ante_core::QueueInfo) -> Value {
    array(vec![
        bulk("name"),
        bulk(info.name.clone()),
        bulk("id"),
        bulk(info.id.to_string()),
        bulk("ready"),
        bulk(info.ready.to_string()),
        bulk("inflight"),
        bulk(info.inflight.to_string()),
        bulk("total"),
        bulk(info.total().to_string()),
    ])
}

fn ack_value(outcome: AckOutcome) -> Value {
    match outcome {
        AckOutcome::Acked => Value::Simple("OK".into()),
        AckOutcome::Stale => Value::Error("STALE_DELIVERY".into()),
        AckOutcome::Unknown => Value::Error("UNKNOWN_DELIVERY".into()),
    }
}

fn requeue_value(outcome: RequeueOutcome) -> Value {
    match outcome {
        RequeueOutcome::Requeued => Value::Simple("OK".into()),
        RequeueOutcome::Stale => Value::Error("STALE_DELIVERY".into()),
        RequeueOutcome::Unknown => Value::Error("UNKNOWN_DELIVERY".into()),
    }
}

fn error_value(code: &str, message: &str) -> Value {
    Value::Error(format!("{code} {message}"))
}

async fn blocking<T, F>(drain: &Drain, f: F) -> ante_core::Result<T>
where
    F: FnOnce() -> ante_core::Result<T> + Send + 'static,
    T: Send + 'static,
{
    let guard = drain.begin();
    match tokio::task::spawn_blocking(move || {
        let _guard = guard;
        f()
    })
    .await
    {
        Ok(result) => result,
        Err(err) => Err(Error::Invalid(format!("engine task failed: {err}"))),
    }
}
