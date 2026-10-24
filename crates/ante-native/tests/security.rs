use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ante_core::Ante;
use ante_native::NativeClient;
use ante_security::{hash_password, Security};
use rcgen::{CertificateParams, KeyPair, SanType};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

struct Identity {
    cert: PathBuf,
    key: PathBuf,
}

fn identity(dir: &Path, name: &str) -> Identity {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    params
        .subject_alt_names
        .push(SanType::IpAddress("127.0.0.1".parse().unwrap()));
    let cert = params.self_signed(&key).unwrap();
    let cert_path = dir.join(format!("{name}-cert.pem"));
    let key_path = dir.join(format!("{name}-key.pem"));
    fs::write(&cert_path, cert.pem()).unwrap();
    fs::write(&key_path, key.serialize_pem()).unwrap();
    Identity {
        cert: cert_path,
        key: key_path,
    }
}

fn server(dir: &Path) -> Arc<Security> {
    let identity = identity(dir, "server");
    let config = serde_json::json!({
        "tls": {"cert": identity.cert, "key": identity.key},
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
    let path = dir.join("security.json");
    fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    Arc::new(Security::load(&path).unwrap())
}

#[tokio::test]
async fn tls_native_auth_and_queue_permissions() {
    let directory = tempfile::tempdir().unwrap();
    let security = server(directory.path());
    let ca = directory.path().join("server-cert.pem");
    let db = directory.path().join("ante.redb");
    let ante = Arc::new(Ante::open(&db).unwrap());
    ante.declare_queue("jobs").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, shutdown) = oneshot::channel();
    let server = tokio::spawn(ante_native::serve_secure_until(
        listener,
        ante.clone(),
        security,
        shutdown,
    ));

    let anonymous = NativeClient::connect_tls(&address.to_string(), &ca, "127.0.0.1")
        .await
        .unwrap();
    let mut anonymous = anonymous;
    assert!(matches!(
        anonymous.call(&[b"TAKE".to_vec(), b"jobs".to_vec(), b"1".to_vec()]).await.unwrap(),
        ante_native::Value::Error(message) if message.starts_with("AUTH_REQUIRED")
    ));

    let mut wrong = NativeClient::connect_tls(&address.to_string(), &ca, "127.0.0.1")
        .await
        .unwrap();
    assert!(matches!(
        wrong.authenticate("worker", "wrong").await,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied
    ));

    let mut producer = NativeClient::connect_tls(&address.to_string(), &ca, "127.0.0.1")
        .await
        .unwrap();
    producer
        .authenticate("producer", "producer-secret")
        .await
        .unwrap();
    let published = producer
        .call(&[b"PUBLISH".to_vec(), b"jobs".to_vec(), b"payload".to_vec()])
        .await
        .unwrap();
    assert!(matches!(published, ante_native::Value::Integer(_)));
    assert!(matches!(
        producer.call(&[b"TAKE".to_vec(), b"jobs".to_vec(), b"1".to_vec()]).await.unwrap(),
        ante_native::Value::Error(message) if message.starts_with("PERMISSION_DENIED")
    ));
    assert!(matches!(
        producer.call(&[b"LIST".to_vec()]).await.unwrap(),
        ante_native::Value::Array(items) if items.is_empty()
    ));

    let mut worker = NativeClient::connect_tls(&address.to_string(), &ca, "127.0.0.1")
        .await
        .unwrap();
    worker
        .authenticate("worker", "worker-secret")
        .await
        .unwrap();
    let taken = worker.take_with("jobs", 1, 30_000, 0).await.unwrap();
    let ante_native::Value::Array(records) = taken else {
        panic!("expected records, got {taken:?}");
    };
    assert_eq!(records.len(), 1);
    let ante_native::Value::Array(fields) = &records[0] else {
        panic!("expected record array");
    };
    let Some(ante_native::Value::Bulk(token)) = fields.first() else {
        panic!("expected token");
    };
    let token = String::from_utf8(token.clone()).unwrap();

    assert!(matches!(
        producer.call(&[b"ACK".to_vec(), token.as_bytes().to_vec()]).await.unwrap(),
        ante_native::Value::Error(message) if message.starts_with("PERMISSION_DENIED")
    ));
    let mut worker2 = NativeClient::connect_tls(&address.to_string(), &ca, "127.0.0.1")
        .await
        .unwrap();
    worker2
        .authenticate("worker", "worker-secret")
        .await
        .unwrap();
    assert!(matches!(
        worker2.call(&[b"ACK".to_vec(), token.as_bytes().to_vec()]).await.unwrap(),
        ante_native::Value::Simple(message) if message == "OK"
    ));

    let _ = stop.send(());
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn untrusted_ca_is_rejected() {
    let directory = tempfile::tempdir().unwrap();
    let security = server(directory.path());
    let other = identity(directory.path(), "other");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (_stop, shutdown) = oneshot::channel();
    let server = tokio::spawn(ante_native::serve_secure_until(
        listener,
        Arc::new(Ante::open(directory.path().join("ante.redb")).unwrap()),
        security,
        shutdown,
    ));
    let result = NativeClient::connect_tls(&address.to_string(), &other.cert, "127.0.0.1").await;
    assert!(result.is_err());
    server.abort();
}
