use std::fs;
use std::path::Path;

use ante_security::{hash_password, Permission, Security};
use rcgen::{CertificateParams, KeyPair, SanType};

fn identity(dir: &Path) -> (String, String) {
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
    (
        cert_path.display().to_string(),
        key_path.display().to_string(),
    )
}

#[test]
fn loads_users_and_authenticates() {
    let directory = tempfile::tempdir().unwrap();
    let (cert, key) = identity(directory.path());
    let password = hash_password("secret").unwrap();
    let config = serde_json::json!({
        "tls": {"cert": cert, "key": key},
        "users": {
            "worker": {
                "password_hash": password,
                "permissions": {"consume": ["jobs"], "inspect": ["jobs"]}
            }
        }
    });
    let path = directory.path().join("security.json");
    fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let security = Security::load(&path).unwrap();
    let principal = security.authenticate("worker", "secret").unwrap();
    assert!(principal.allows(Permission::Consume, "jobs"));
    assert!(!principal.allows(Permission::Publish, "jobs"));
    assert!(security.authenticate("worker", "wrong").is_none());
    assert!(security.authenticate("missing", "secret").is_none());
}

#[test]
fn rejects_insecure_or_malformed_configs() {
    let directory = tempfile::tempdir().unwrap();
    let (cert, key) = identity(directory.path());
    let password = hash_password("secret").unwrap();

    let no_tls = serde_json::json!({
        "users": {"worker": {"password_hash": password, "permissions": {"consume": ["jobs"]}}}
    });
    let path = directory.path().join("no-tls.json");
    fs::write(&path, serde_json::to_vec(&no_tls).unwrap()).unwrap();
    assert!(Security::load(&path).is_err());

    let weak = serde_json::json!({
        "tls": {"cert": cert, "key": key},
        "users": {"worker": {"password_hash": "plaintext", "permissions": {"consume": ["jobs"]}}}
    });
    let path = directory.path().join("weak.json");
    fs::write(&path, serde_json::to_vec(&weak).unwrap()).unwrap();
    assert!(Security::load(&path).is_err());

    let wildcard = serde_json::json!({
        "tls": {"cert": cert, "key": key},
        "users": {"worker": {"password_hash": hash_password("secret").unwrap(), "permissions": {"consume": ["jobs.*"]}}}
    });
    let path = directory.path().join("wildcard.json");
    fs::write(&path, serde_json::to_vec(&wildcard).unwrap()).unwrap();
    assert!(Security::load(&path).is_err());
}
