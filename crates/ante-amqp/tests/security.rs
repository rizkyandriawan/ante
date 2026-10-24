use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ante_amqp::ServerOptions;
use ante_core::Ante;
use ante_security::{hash_password, Security};
use lapin::options::{
    BasicGetOptions, BasicPublishOptions, QueueDeclareOptions, QueuePurgeOptions,
};
use lapin::tcp::OwnedTLSConfig;
use lapin::{Connection, ConnectionProperties};
use rcgen::{CertificateParams, KeyPair, SanType};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

struct Harness {
    port: u16,
    ca: PathBuf,
    ante: Arc<Ante>,
    shutdown: Option<oneshot::Sender<()>>,
    server: tokio::task::JoinHandle<()>,
    _directory: tempfile::TempDir,
}

fn identity(dir: &Path) -> (PathBuf, PathBuf) {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    params
        .subject_alt_names
        .push(SanType::IpAddress("127.0.0.1".parse().unwrap()));
    let cert = params.self_signed(&key).unwrap();
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    fs::write(&cert_path, cert.pem()).unwrap();
    fs::write(&key_path, key.serialize_pem()).unwrap();
    (cert_path, key_path)
}

impl Harness {
    async fn start() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let (cert, key) = identity(directory.path());
        let config = serde_json::json!({
            "tls": {"cert": cert, "key": key},
            "users": {
                "producer": {
                    "password_hash": hash_password("producer-secret").unwrap(),
                    "permissions": {"publish": ["jobs"]}
                },
                "worker": {
                    "password_hash": hash_password("worker-secret").unwrap(),
                    "permissions": {"consume": ["jobs"], "inspect": ["jobs"]}
                }
            }
        });
        let security_path = directory.path().join("security.json");
        fs::write(&security_path, serde_json::to_vec(&config).unwrap()).unwrap();
        let security = Arc::new(Security::load(&security_path).unwrap());
        let ante = Arc::new(Ante::open(directory.path().join("ante.redb")).unwrap());
        ante.declare_queue("jobs").unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = oneshot::channel();
        let server_ante = ante.clone();
        let server = tokio::spawn(async move {
            ante_amqp::serve_secure_until_with_options(
                listener,
                server_ante,
                ServerOptions::default(),
                security,
                rx,
            )
            .await
            .unwrap();
        });
        Harness {
            port,
            ca: directory.path().join("cert.pem"),
            ante,
            shutdown: Some(tx),
            server,
            _directory: directory,
        }
    }

    fn uri(&self, user: &str, password: &str, vhost: &str) -> String {
        format!(
            "amqps://{user}:{password}@127.0.0.1:{}/{}",
            self.port, vhost
        )
    }

    async fn connect(&self, user: &str, password: &str) -> lapin::Result<Connection> {
        let ca = fs::read_to_string(&self.ca).unwrap();
        Connection::connect_with_config(
            &self.uri(user, password, "%2f"),
            ConnectionProperties::default(),
            OwnedTLSConfig {
                identity: None,
                cert_chain: Some(ca),
            },
        )
        .await
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.server.abort();
    }
}

#[tokio::test]
async fn tls_amqp_auth_and_queue_permissions() {
    let harness = Harness::start().await;

    let producer = harness
        .connect("producer", "producer-secret")
        .await
        .unwrap();
    let channel = producer.create_channel().await.unwrap();
    channel
        .basic_publish(
            "",
            "jobs",
            BasicPublishOptions::default(),
            b"hello",
            lapin::BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    assert!(channel
        .queue_declare(
            "jobs",
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            Default::default(),
        )
        .await
        .is_err());

    let channel = producer.create_channel().await.unwrap();
    channel
        .confirm_select(lapin::options::ConfirmSelectOptions::default())
        .await
        .unwrap();
    let denied = channel
        .basic_publish(
            "",
            "private",
            BasicPublishOptions::default(),
            b"nope",
            lapin::BasicProperties::default(),
        )
        .await
        .unwrap();
    assert!(denied.await.is_err());
    let channel = producer.create_channel().await.unwrap();
    channel
        .confirm_select(lapin::options::ConfirmSelectOptions::default())
        .await
        .unwrap();
    let denied = channel
        .basic_publish(
            "other",
            "jobs",
            BasicPublishOptions::default(),
            b"named exchange",
            lapin::BasicProperties::default(),
        )
        .await
        .unwrap();
    assert!(denied.await.is_err());
    let channel = producer.create_channel().await.unwrap();
    channel
        .basic_publish(
            "",
            "jobs",
            BasicPublishOptions::default(),
            b"still connected",
            lapin::BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();

    let worker = harness.connect("worker", "worker-secret").await.unwrap();
    let channel = worker.create_channel().await.unwrap();
    let message = channel
        .basic_get("jobs", BasicGetOptions { no_ack: true })
        .await
        .unwrap()
        .expect("queued message");
    assert_eq!(message.delivery.data, b"hello");
    assert!(channel
        .queue_purge("jobs", QueuePurgeOptions::default())
        .await
        .is_err());

    assert!(harness.connect("producer", "wrong").await.is_err());
    assert!(harness.connect("producer", "producer-secret").await.is_ok());
    assert!(Connection::connect(
        &format!(
            "amqp://producer:producer-secret@127.0.0.1:{}/%2f",
            harness.port
        ),
        ConnectionProperties::default(),
    )
    .await
    .is_err());
    assert!(Connection::connect_with_config(
        &harness.uri("producer", "producer-secret", "other"),
        ConnectionProperties::default(),
        OwnedTLSConfig {
            identity: None,
            cert_chain: Some(fs::read_to_string(&harness.ca).unwrap()),
        },
    )
    .await
    .is_err());

    let _ = &harness.ante;
}
