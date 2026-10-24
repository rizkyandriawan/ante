use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ante_core::Ante;
use ante_security::{client_config, hash_password, Security, ServerName};
use fe2o3_amqp::sasl_profile::SaslProfile;
use fe2o3_amqp::types::messaging::Data;
use fe2o3_amqp::{Connection, Receiver};
use rcgen::{CertificateParams, KeyPair, SanType};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

struct Identity {
    cert: PathBuf,
    key: PathBuf,
}

fn server_identity(dir: &Path) -> Identity {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    params
        .subject_alt_names
        .push(SanType::IpAddress("127.0.0.1".parse().unwrap()));
    let cert = params.self_signed(&key).unwrap();
    let cert_path = dir.join("server-cert.pem");
    let key_path = dir.join("server-key.pem");
    fs::write(&cert_path, cert.pem()).unwrap();
    fs::write(&key_path, key.serialize_pem()).unwrap();
    Identity {
        cert: cert_path,
        key: key_path,
    }
}

fn connector(ca: &Path) -> tokio_rustls::TlsConnector {
    tokio_rustls::TlsConnector::from(client_config(ca).unwrap())
}

async fn start_server(
    ante: Arc<Ante>,
    security: Arc<Security>,
) -> (u16, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    start_server_with_options(ante, security, ante_amqp1::ServerOptions::default()).await
}

async fn start_server_with_options(
    ante: Arc<Ante>,
    security: Arc<Security>,
    options: ante_amqp1::ServerOptions,
) -> (u16, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (stop, shutdown) = oneshot::channel();
    let task = tokio::spawn(async move {
        ante_amqp1::serve_secure_until_with_options(listener, ante, options, security, shutdown)
            .await
            .unwrap();
    });
    (port, stop, task)
}

#[tokio::test]
async fn amqp1_tls_plain_auth() {
    let directory = tempfile::tempdir().unwrap();
    let identity = server_identity(directory.path());
    let config = serde_json::json!({
        "tls": {"cert": identity.cert, "key": identity.key},
        "users": {
            "worker": {
                "password_hash": hash_password("secret").unwrap(),
                "permissions": {"consume": ["jobs"], "inspect": ["jobs"]}
            }
        }
    });
    let security_path = directory.path().join("security.json");
    fs::write(&security_path, serde_json::to_vec(&config).unwrap()).unwrap();
    let security = Arc::new(Security::load(&security_path).unwrap());
    let ante = Arc::new(Ante::open(directory.path().join("ante.sqlite3")).unwrap());
    ante.declare_queue("jobs").unwrap();
    ante.publish(
        "jobs",
        ante_core::NewMessage {
            payload: b"secured".to_vec(),
            content_type: None,
            message_id: None,
            correlation_id: None,
            reply_to: None,
        },
    )
    .unwrap();
    let (port, stop, server) = start_server(ante.clone(), security).await;
    let ca = directory.path().join("server-cert.pem");
    let url = format!("amqps://127.0.0.1:{port}");
    let mut connection = Connection::builder()
        .container_id("plain-client")
        .tls_connector(connector(&ca))
        .sasl_profile(SaslProfile::Plain {
            username: "worker".to_string(),
            password: "secret".to_string(),
        })
        .open(url.as_str())
        .await
        .unwrap();
    let mut session = fe2o3_amqp::Session::begin(&mut connection).await.unwrap();
    let mut receiver = Receiver::attach(&mut session, "secure-receiver", "jobs")
        .await
        .unwrap();
    let delivery = receiver.recv::<Data>().await.unwrap();
    assert_eq!(delivery.message().body.0.as_ref(), b"secured");
    receiver.accept(&delivery).await.unwrap();
    receiver.close().await.unwrap();
    session.end().await.unwrap();
    connection.close().await.unwrap();
    let _ = stop.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn amqp1_accepts_direct_tls_without_upgrade_header() {
    let directory = tempfile::tempdir().unwrap();
    let identity = server_identity(directory.path());
    let config = serde_json::json!({
        "tls": {"cert": identity.cert, "key": identity.key},
        "users": {
            "worker": {
                "password_hash": hash_password("secret").unwrap(),
                "permissions": {"consume": ["jobs"], "inspect": ["jobs"]}
            }
        }
    });
    let security_path = directory.path().join("security.json");
    fs::write(&security_path, serde_json::to_vec(&config).unwrap()).unwrap();
    let security = Arc::new(Security::load(&security_path).unwrap());
    let ante = Arc::new(Ante::open(directory.path().join("ante.sqlite3")).unwrap());
    ante.declare_queue("jobs").unwrap();
    ante.publish(
        "jobs",
        ante_core::NewMessage {
            payload: b"direct tls".to_vec(),
            content_type: None,
            message_id: None,
            correlation_id: None,
            reply_to: None,
        },
    )
    .unwrap();
    let (port, stop, server) = start_server(ante.clone(), security).await;
    let ca = directory.path().join("server-cert.pem");
    let tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let tls = connector(&ca)
        .connect(ServerName::try_from("127.0.0.1").unwrap(), tcp)
        .await
        .unwrap();
    let mut connection = Connection::builder()
        .container_id("direct-tls-client")
        .scheme("amqp")
        .sasl_profile(SaslProfile::Plain {
            username: "worker".to_string(),
            password: "secret".to_string(),
        })
        .open_with_stream(tls)
        .await
        .unwrap();
    let mut session = fe2o3_amqp::Session::begin(&mut connection).await.unwrap();
    let mut receiver = Receiver::attach(&mut session, "direct-tls-receiver", "jobs")
        .await
        .unwrap();
    let delivery = receiver.recv::<Data>().await.unwrap();
    assert_eq!(delivery.message().body.0.as_ref(), b"direct tls");
    receiver.accept(&delivery).await.unwrap();
    receiver.close().await.unwrap();
    session.end().await.unwrap();
    connection.close().await.unwrap();
    let _ = stop.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn amqp1_tls_without_sasl_times_out() {
    let directory = tempfile::tempdir().unwrap();
    let identity = server_identity(directory.path());
    let config = serde_json::json!({
        "tls": {"cert": identity.cert, "key": identity.key},
        "users": {
            "worker": {
                "password_hash": hash_password("secret").unwrap(),
                "permissions": {"consume": ["jobs"]}
            }
        }
    });
    let security_path = directory.path().join("security.json");
    fs::write(&security_path, serde_json::to_vec(&config).unwrap()).unwrap();
    let security = Arc::new(Security::load(&security_path).unwrap());
    let ante = Arc::new(Ante::open(directory.path().join("ante.sqlite3")).unwrap());
    ante.declare_queue("jobs").unwrap();
    let options = ante_amqp1::ServerOptions {
        handshake_timeout: Duration::from_millis(100),
        ..Default::default()
    };
    let (port, stop, server) = start_server_with_options(ante, security, options).await;
    let ca = directory.path().join("server-cert.pem");
    let tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut tls = connector(&ca)
        .connect(ServerName::try_from("127.0.0.1").unwrap(), tcp)
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut buffer = [0u8; 16];
    loop {
        match tokio::time::timeout_at(deadline, tls.read(&mut buffer)).await {
            Ok(Ok(0)) => break,
            Ok(Err(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Ok(Ok(read)) => assert!(read > 0),
            Ok(Err(error)) => panic!("unexpected read error: {error}"),
            Err(_) => panic!("server did not close an idle SASL handshake"),
        }
    }
    let _ = stop.send(());
    server.await.unwrap();
}
