#!/usr/bin/env bash
set -euo pipefail
export PATH="$HOME/.cargo/bin:$PATH"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

mkdir -p "$work/src"
cat > "$work/Cargo.toml" <<TOML
[package]
name = "ante-downstream-smoke"
version = "0.1.0"
edition = "2021"

[dependencies]
ante-amqp1 = { path = "$root/crates/ante-amqp1" }
TOML
cat > "$work/src/main.rs" <<'RS'
fn main() {
    let options = ante_amqp1::ServerOptions::default();
    assert!(options.lease_ms > 0);
}
RS

cd "$work"
cargo check --quiet
cargo check --release --quiet
echo "downstream build: passed (debug + release, no [patch.crates-io])"
