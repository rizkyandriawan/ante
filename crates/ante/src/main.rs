use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use ante_core::Ante;
use ante_native::{NativeClient, Value};
use ante_security::Security;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

const DEFAULT_SERVER: &str = "127.0.0.1:7300";
const DEFAULT_NATIVE: &str = "127.0.0.1:7300";
const DEFAULT_AMQP: &str = "127.0.0.1:5672";
const DEFAULT_DB: &str = "ante.sqlite3";

const USAGE: &str = "\
ante - durable work queue

USAGE:
  ante serve [--db PATH] [--native ADDR] [--amqp ADDR] [--no-native] [--no-amqp]
             [--security PATH]
  ante hash-password
  ante init [--db PATH]
  ante declare QUEUE [--server ADDR]
  ante list [--server ADDR]
  ante info QUEUE [--server ADDR]
  ante status QUEUE MESSAGE_ID [--server ADDR]
  ante publish QUEUE [PAYLOAD] [--file PATH] [--content-type TYPE] [--message-id ID]
  ante take QUEUE [--max N] [--lease-ms N] [--wait-ms N] [--json] [--server ADDR]
  ante ack TOKEN... [--server ADDR]
  ante discard TOKEN... [--server ADDR]
  ante complete TOKEN QUEUE [PAYLOAD] [--file PATH] [--content-type TYPE]
                [--message-id ID] [--discard] [--server ADDR]
  ante requeue TOKEN... [--server ADDR]
  ante purge QUEUE [--server ADDR]
  ante delete QUEUE [--server ADDR]
  ante ping [--server ADDR]

CLIENT TLS:
  --tls-ca PATH          trust this PEM CA and use TLS
  --tls-server-name NAME override the certificate name (default: host)
  --user NAME            native protocol username (or ANTE_USER)
  --password-file PATH   read the secret from PATH (or ANTE_PASSWORD)

ENVIRONMENT:
  ANTE_DB       default database path (default: ante.sqlite3)
  ANTE_SERVER   default native server address (default: 127.0.0.1:7300)
  ANTE_USER     native protocol username
  ANTE_PASSWORD native protocol secret

TAKE output is tab separated:
  token  message_id  content_type  correlation_id  reply_to  attempts  payload
";

#[tokio::main]
async fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let code = match run(argv).await {
        Ok(()) => 0,
        Err((code, message)) => {
            eprintln!("ante: {message}");
            code
        }
    };
    std::process::exit(code);
}

async fn run(argv: Vec<String>) -> Result<(), (i32, String)> {
    let Some(command) = argv.first().cloned() else {
        print!("{USAGE}");
        return Ok(());
    };
    if command == "-h" || command == "--help" || command == "help" {
        print!("{USAGE}");
        return Ok(());
    }

    let rest = &argv[1..];
    let (positional, flags) = parse_args(rest)?;
    match command.as_str() {
        "serve" => serve(&flags).await,
        "hash-password" => hash_password_command(),
        "init" => init(&flags),
        "declare" => {
            let queue = positional
                .first()
                .ok_or((2, "usage: ante declare QUEUE".into()))?;
            let mut client = connect(&flags).await?;
            let value = client.declare(queue).await.map_err(io_err)?;
            expect_ok(value, &mut std::io::stdout())
        }
        "list" => {
            let mut client = connect(&flags).await?;
            let value = client.list().await.map_err(io_err)?;
            let queues = expect_array(value)?;
            let mut out = std::io::stdout();
            for queue in queues {
                let fields = expect_array(queue)?;
                let name = field(&fields, 0);
                let ready = field(&fields, 1);
                let inflight = field(&fields, 2);
                let _ = writeln!(out, "{name}\tready={ready}\tinflight={inflight}");
            }
            Ok(())
        }
        "info" => {
            let queue = positional
                .first()
                .ok_or((2, "usage: ante info QUEUE".into()))?;
            let mut client = connect(&flags).await?;
            let value = client.info(queue).await.map_err(io_err)?;
            let fields = expect_array(value)?;
            let mut out = std::io::stdout();
            let mut iter = fields.chunks(2);
            if let Some(first) = iter.next() {
                let name = bulk_text(first.get(1));
                let mut rest = Vec::new();
                for pair in iter {
                    rest.push(format!(
                        "{}={}",
                        bulk_text(pair.first()),
                        bulk_text(pair.get(1))
                    ));
                }
                let _ = writeln!(out, "{name}\t{}", rest.join("\t"));
            }
            Ok(())
        }
        "status" => {
            if positional.len() != 2 {
                return Err((2, "usage: ante status QUEUE MESSAGE_ID".into()));
            }
            let message_id: u64 = positional[1]
                .parse()
                .map_err(|_| (2, "MESSAGE_ID must be an unsigned integer".into()))?;
            let mut client = connect(&flags).await?;
            let value = client
                .call(&[
                    b"STATUS".to_vec(),
                    positional[0].as_bytes().to_vec(),
                    message_id.to_string().into_bytes(),
                ])
                .await
                .map_err(io_err)?;
            let fields = expect_array(value)?;
            let mut out = std::io::stdout();
            let mut iter = fields.chunks(2);
            if let Some(first) = iter.next() {
                let queue = bulk_text(first.get(1));
                let mut rest = Vec::new();
                let mut history = Vec::new();
                for pair in iter {
                    let key = bulk_text(pair.first());
                    let value = pair.get(1).cloned().unwrap_or(Value::Bulk(Vec::new()));
                    if key == "history" {
                        if let Value::Array(attempts) = value {
                            history = attempts;
                        }
                    } else {
                        rest.push(format!("{key}={}", bulk_text(pair.get(1))));
                    }
                }
                let _ = writeln!(out, "{queue}\t{}", rest.join("\t"));
                for attempt in history {
                    let Value::Array(pairs) = attempt else {
                        continue;
                    };
                    let mut values = Vec::new();
                    for pair in pairs.chunks(2) {
                        values.push(format!(
                            "{}={}",
                            bulk_text(pair.first()),
                            bulk_text(pair.get(1))
                        ));
                    }
                    let _ = writeln!(out, "  {}", values.join("\t"));
                }
            }
            Ok(())
        }
        "publish" => publish(&positional, &flags).await,
        "complete" => complete(&positional, &flags).await,
        "take" => take(&positional, &flags).await,
        "ack" | "discard" | "requeue" => {
            if positional.is_empty() {
                return Err((2, format!("usage: ante {command} TOKEN...")));
            }
            let mut client = connect(&flags).await?;
            let mut out = std::io::stdout();
            let mut failed = false;

            if command == "requeue" {
                for token in &positional {
                    let value = client
                        .call(&[b"REQUEUE".to_vec(), token.as_bytes().to_vec()])
                        .await
                        .map_err(io_err)?;
                    failed |= !report_outcome(token, value, &mut out);
                }
            } else {
                let method = if command == "discard" {
                    b"DISCARD".to_vec()
                } else {
                    b"ACK".to_vec()
                };
                let mut call_args = vec![method];
                call_args.extend(positional.iter().map(|token| token.as_bytes().to_vec()));
                let value = client.call(&call_args).await.map_err(io_err)?;
                match value {
                    Value::Array(results) if results.len() == positional.len() => {
                        for (token, result) in positional.iter().zip(results) {
                            failed |= !report_outcome(token, result, &mut out);
                        }
                    }
                    single if positional.len() == 1 => {
                        failed |= !report_outcome(&positional[0], single, &mut out);
                    }
                    other => {
                        failed = true;
                        let _ = writeln!(out, "unexpected reply: {other:?}");
                    }
                }
            }

            if failed {
                Err((1, format!("{command} failed for at least one token")))
            } else {
                Ok(())
            }
        }
        "purge" | "delete" => {
            let queue = positional
                .first()
                .ok_or((2, format!("usage: ante {command} QUEUE")))?;
            let mut client = connect(&flags).await?;
            let value = if command == "purge" {
                client.purge(queue).await
            } else {
                client.delete_queue(queue).await
            }
            .map_err(io_err)?;
            let mut out = std::io::stdout();
            match value {
                Value::Integer(count) => {
                    let _ = writeln!(out, "OK\t{count}");
                    Ok(())
                }
                other => expect_ok(other, &mut out),
            }
        }
        "ping" => {
            let mut client = connect(&flags).await?;
            let value = client.ping().await.map_err(io_err)?;
            expect_ok(value, &mut std::io::stdout())
        }
        other => Err((2, format!("unknown command {other}\n{USAGE}"))),
    }
}

async fn serve(flags: &HashMap<String, String>) -> Result<(), (i32, String)> {
    let db = db_path(flags);
    let ante =
        Arc::new(Ante::open(&db).map_err(|err| (1, format!("open {}: {err}", db.display())))?);
    let native_addr = flags
        .get("native")
        .cloned()
        .unwrap_or_else(|| DEFAULT_NATIVE.into());
    let amqp_addr = flags
        .get("amqp")
        .cloned()
        .unwrap_or_else(|| DEFAULT_AMQP.into());
    let with_native = !flags.contains_key("no-native");
    let with_amqp = !flags.contains_key("no-amqp");
    if !with_native && !with_amqp {
        return Err((2, "at least one listener must be enabled".into()));
    }
    let security = match flags.get("security") {
        Some(path) => {
            Some(Arc::new(Security::load(path).map_err(|err| {
                (2, format!("security config {path}: {err}"))
            })?))
        }
        None => None,
    };

    let mut native_task = None;
    let mut native_stop = None;
    if with_native {
        let listener = TcpListener::bind(&native_addr)
            .await
            .map_err(|err| (1, format!("bind native {native_addr}: {err}")))?;
        let address = listener.local_addr().map_err(|err| (1, err.to_string()))?;
        let (tx, rx) = oneshot::channel();
        native_stop = Some(tx);
        let task = match security.clone() {
            Some(security) => tokio::spawn(ante_native::serve_secure_until(
                listener,
                ante.clone(),
                security,
                rx,
            )),
            None => tokio::spawn(ante_native::serve_until(listener, ante.clone(), rx)),
        };
        native_task = Some(task);
        println!(
            "native  {address}{}",
            if security.is_some() { " (tls)" } else { "" }
        );
    }

    let mut amqp_task = None;
    let mut amqp_stop = None;
    if with_amqp {
        let listener = TcpListener::bind(&amqp_addr)
            .await
            .map_err(|err| (1, format!("bind amqp {amqp_addr}: {err}")))?;
        let address = listener.local_addr().map_err(|err| (1, err.to_string()))?;
        let (tx, rx) = oneshot::channel();
        amqp_stop = Some(tx);
        let task = match security.clone() {
            Some(security) => tokio::spawn(ante_amqp::serve_secure_until_with_options(
                listener,
                ante.clone(),
                ante_amqp::ServerOptions::default(),
                security,
                rx,
            )),
            None => tokio::spawn(ante_amqp::serve_until(listener, ante.clone(), rx)),
        };
        amqp_task = Some(task);
        println!(
            "amqp    {address}{}",
            if security.is_some() { " (tls)" } else { "" }
        );
    }
    println!("db      {}", db.display());

    let reason = tokio::select! {
        _ = async {
            match native_task.as_mut() {
                Some(task) => { let _ = task.await; }
                None => std::future::pending::<()>().await,
            }
        } => "native listener stopped".to_string(),
        _ = async {
            match amqp_task.as_mut() {
                Some(task) => { let _ = task.await; }
                None => std::future::pending::<()>().await,
            }
        } => "amqp listener stopped".to_string(),
        _ = tokio::signal::ctrl_c() => "interrupted".to_string(),
    };
    eprintln!("ante: {reason}, shutting down");

    if let Some(tx) = native_stop {
        let _ = tx.send(());
    }
    if let Some(tx) = amqp_stop {
        let _ = tx.send(());
    }
    if let Some(task) = native_task {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(6), task).await;
    }
    if let Some(task) = amqp_task {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(6), task).await;
    }
    Ok(())
}

fn init(flags: &HashMap<String, String>) -> Result<(), (i32, String)> {
    let db = db_path(flags);
    let ante = Ante::open(&db).map_err(|err| (1, format!("open {}: {err}", db.display())))?;
    let queues = ante.list_queues().map_err(|err| (1, err.to_string()))?;
    println!("db      {}", db.display());
    println!("queues  {}", queues.len());
    Ok(())
}

async fn publish(
    positional: &[String],
    flags: &HashMap<String, String>,
) -> Result<(), (i32, String)> {
    let queue = positional
        .first()
        .ok_or((2, "usage: ante publish QUEUE [PAYLOAD]".into()))?;
    let payload = match flags.get("file") {
        Some(path) => std::fs::read(path).map_err(|err| (1, format!("read {path}: {err}")))?,
        None => positional
            .get(1)
            .map(|value| value.as_bytes().to_vec())
            .unwrap_or_default(),
    };
    let mut client = connect(flags).await?;
    let content_type = flags.get("content-type").cloned();
    let message_id = flags.get("message-id").cloned();
    let mut call_args = vec![b"PUBLISH".to_vec(), queue.as_bytes().to_vec(), payload];
    if content_type.is_some() || message_id.is_some() {
        call_args.push(content_type.unwrap_or_default().into_bytes());
    }
    if let Some(message_id) = message_id {
        call_args.push(message_id.into_bytes());
    }
    let value = client.call(&call_args).await.map_err(io_err)?;
    let mut out = std::io::stdout();
    match value {
        Value::Integer(id) => {
            let _ = writeln!(out, "OK\t{id}");
            Ok(())
        }
        other => expect_ok(other, &mut out),
    }
}

async fn complete(
    positional: &[String],
    flags: &HashMap<String, String>,
) -> Result<(), (i32, String)> {
    let token = positional.first().ok_or((
        2,
        "usage: ante complete TOKEN QUEUE [PAYLOAD] [--file PATH]".into(),
    ))?;
    let queue = positional.get(1).ok_or((
        2,
        "usage: ante complete TOKEN QUEUE [PAYLOAD] [--file PATH]".into(),
    ))?;
    let payload = match flags.get("file") {
        Some(path) => std::fs::read(path).map_err(|err| (1, format!("read {path}: {err}")))?,
        None => positional
            .get(2)
            .map(|value| value.as_bytes().to_vec())
            .unwrap_or_default(),
    };
    let content_type = flags.get("content-type").cloned();
    let message_id = flags.get("message-id").cloned();
    let settlement = flags.contains_key("discard").then_some("DISCARD");
    let mut call_args = vec![
        b"COMPLETE".to_vec(),
        token.as_bytes().to_vec(),
        queue.as_bytes().to_vec(),
        payload,
    ];
    if content_type.is_some() || message_id.is_some() || settlement.is_some() {
        call_args.push(content_type.unwrap_or_default().into_bytes());
    }
    if message_id.is_some() || settlement.is_some() {
        call_args.push(message_id.unwrap_or_default().into_bytes());
    }
    if let Some(settlement) = settlement {
        call_args.push(settlement.as_bytes().to_vec());
    }
    let mut client = connect(flags).await?;
    let value = client.call(&call_args).await.map_err(io_err)?;
    let mut out = std::io::stdout();
    match value {
        Value::Integer(id) => {
            let _ = writeln!(out, "OK\t{id}");
            Ok(())
        }
        other => expect_ok(other, &mut out),
    }
}

async fn take(positional: &[String], flags: &HashMap<String, String>) -> Result<(), (i32, String)> {
    let queue = positional
        .first()
        .ok_or((2, "usage: ante take QUEUE".into()))?;
    let max = flag_u64(flags, "max")?.unwrap_or(1);
    let lease_ms = flag_u64(flags, "lease-ms")?.unwrap_or(30_000);
    let wait_ms = flag_u64(flags, "wait-ms")?.unwrap_or(0);
    let json = flags.contains_key("json");

    let mut client = connect(flags).await?;
    let value = client
        .take_with(queue, max as usize, lease_ms, wait_ms)
        .await
        .map_err(io_err)?;
    let records = expect_array(value)?;
    let mut out = std::io::stdout();
    if json {
        let mut rows = Vec::new();
        for record in records {
            let fields = expect_array(record)?;
            rows.push(format!(
                "{{\"token\":{},\"message_id\":{},\"content_type\":{},\"correlation_id\":{},\"reply_to\":{},\"attempts\":{},\"payload\":{}}}",
                json_string(&field(&fields, 0)),
                json_string(&field(&fields, 1)),
                json_string(&field(&fields, 2)),
                json_string(&field(&fields, 3)),
                json_string(&field(&fields, 4)),
                field(&fields, 5),
                json_string(&String::from_utf8_lossy(&bulk_bytes(fields.get(6)))),
            ));
        }
        let _ = writeln!(out, "[{}]", rows.join(","));
        return Ok(());
    }
    for record in records {
        let fields = expect_array(record)?;
        let token = field(&fields, 0);
        let header = format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t",
            token,
            field(&fields, 1),
            field(&fields, 2),
            field(&fields, 3),
            field(&fields, 4),
            field(&fields, 5),
        );
        out.write_all(header.as_bytes())
            .map_err(|err| (1, err.to_string()))?;
        out.write_all(&bulk_bytes(fields.get(6)))
            .map_err(|err| (1, err.to_string()))?;
        out.write_all(b"\n").map_err(|err| (1, err.to_string()))?;
    }
    Ok(())
}

async fn connect(flags: &HashMap<String, String>) -> Result<NativeClient, (i32, String)> {
    let address = flags
        .get("server")
        .cloned()
        .or_else(|| std::env::var("ANTE_SERVER").ok())
        .unwrap_or_else(|| DEFAULT_SERVER.into());
    let mut client = if let Some(ca) = flags.get("tls-ca") {
        let name = flags
            .get("tls-server-name")
            .cloned()
            .unwrap_or_else(|| server_host(&address));
        NativeClient::connect_tls(&address, std::path::Path::new(ca), &name)
            .await
            .map_err(|err| (1, format!("connect {address} over TLS: {err}")))?
    } else {
        NativeClient::connect(&address)
            .await
            .map_err(|err| (1, format!("connect {address}: {err}")))?
    };
    let username = flags
        .get("user")
        .cloned()
        .or_else(|| std::env::var("ANTE_USER").ok());
    let password = match flags.get("password-file") {
        Some(path) => Some(
            std::fs::read_to_string(path)
                .map_err(|err| (2, format!("read password file {path}: {err}")))?
                .trim_end_matches(['\r', '\n'])
                .to_string(),
        ),
        None => std::env::var("ANTE_PASSWORD").ok(),
    };
    if let Some(username) = username {
        let password = password.ok_or((
            2,
            "a password is required (--password-file or ANTE_PASSWORD)".into(),
        ))?;
        client
            .authenticate(&username, &password)
            .await
            .map_err(|err| (1, format!("authenticate as {username}: {err}")))?;
    }
    Ok(client)
}

fn server_host(address: &str) -> String {
    address
        .rsplit_once(':')
        .map(|(host, _)| host.trim_matches(['[', ']']).to_string())
        .unwrap_or_else(|| address.to_string())
}

fn hash_password_command() -> Result<(), (i32, String)> {
    use std::io::Read;
    let mut secret = String::new();
    std::io::stdin()
        .read_to_string(&mut secret)
        .map_err(|err| (1, format!("read stdin: {err}")))?;
    let secret = secret.trim_end_matches(['\r', '\n']);
    let hash =
        ante_security::hash_password(secret).map_err(|err| (2, format!("hash password: {err}")))?;
    println!("{hash}");
    Ok(())
}

fn io_err(err: std::io::Error) -> (i32, String) {
    (1, err.to_string())
}

fn report_outcome(token: &str, value: Value, out: &mut impl Write) -> bool {
    match value {
        Value::Simple(text) => {
            let _ = writeln!(out, "{token}\t{text}");
            true
        }
        Value::Error(message) => {
            let _ = writeln!(out, "{token}\tERR\t{message}");
            false
        }
        other => {
            let _ = writeln!(out, "{token}\tERR\t{other:?}");
            false
        }
    }
}

fn expect_ok(value: Value, out: &mut impl Write) -> Result<(), (i32, String)> {
    match value {
        Value::Simple(text) => {
            let _ = writeln!(out, "{text}");
            Ok(())
        }
        Value::Integer(number) => {
            let _ = writeln!(out, "{number}");
            Ok(())
        }
        Value::Error(message) => Err((1, message)),
        other => Err((1, format!("unexpected reply: {other:?}"))),
    }
}

fn expect_array(value: Value) -> Result<Vec<Value>, (i32, String)> {
    match value {
        Value::Array(items) => Ok(items),
        Value::Error(message) => Err((1, message)),
        other => Err((1, format!("unexpected reply: {other:?}"))),
    }
}

fn field(fields: &[Value], index: usize) -> String {
    bulk_text(fields.get(index))
}

fn bulk_text(value: Option<&Value>) -> String {
    String::from_utf8_lossy(&bulk_bytes(value)).into_owned()
}

fn bulk_bytes(value: Option<&Value>) -> Vec<u8> {
    match value {
        Some(Value::Bulk(bytes)) => bytes.clone(),
        Some(Value::Simple(text)) | Some(Value::Error(text)) => text.as_bytes().to_vec(),
        Some(Value::Integer(number)) => number.to_string().into_bytes(),
        _ => Vec::new(),
    }
}

fn json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn db_path(flags: &HashMap<String, String>) -> PathBuf {
    flags
        .get("db")
        .map(PathBuf::from)
        .or_else(|| std::env::var("ANTE_DB").ok().map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from(DEFAULT_DB))
}

fn flag_u64(flags: &HashMap<String, String>, name: &str) -> Result<Option<u64>, (i32, String)> {
    match flags.get(name) {
        Some(raw) => raw
            .parse()
            .map(Some)
            .map_err(|_| (2, format!("--{name} must be a number"))),
        None => Ok(None),
    }
}

type ParsedArgs = (Vec<String>, HashMap<String, String>);

fn parse_args(args: &[String]) -> Result<ParsedArgs, (i32, String)> {
    let mut positional = Vec::new();
    let mut flags = HashMap::new();
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if let Some(rest) = arg.strip_prefix("--") {
            if rest.is_empty() {
                return Err((2, "empty flag".into()));
            }
            if let Some((key, value)) = rest.split_once('=') {
                flags.insert(key.to_string(), value.to_string());
                index += 1;
                continue;
            }
            let key = rest.to_string();
            let needs_value = !matches!(
                key.as_str(),
                "json" | "no-native" | "no-amqp" | "help" | "quiet" | "passive" | "durable"
            );
            if needs_value {
                let value = args
                    .get(index + 1)
                    .ok_or((2, format!("--{key} requires a value")))?;
                flags.insert(key, value.clone());
                index += 2;
            } else {
                flags.insert(key, String::new());
                index += 1;
            }
        } else {
            positional.push(arg.clone());
            index += 1;
        }
    }
    Ok((positional, flags))
}
