#!/usr/bin/env bash
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$BENCH_DIR"
RESULT_DIR="${RESULT_DIR:-$BENCH_DIR/results}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$RESULT_DIR/$RUN_ID-e2e.jsonl"
mkdir -p "$RESULT_DIR"
: > "$OUT"

export PATH="$HOME/.cargo/bin:$PATH"
cargo build --release -p e2e-bench >/dev/null

log() { echo "[$(date -u +%H:%M:%S)] $*" >&2; }

meta() {
  {
    echo "run_id=$RUN_ID"
    echo "cpu=$(lscpu | awk -F': +' '/Model name/{print $2; exit}')"
    echo "cores=$(nproc)"
    echo "kernel=$(uname -r)"
    echo "rustc=$(rustc --version)"
    echo "topology=loopback TCP, server and client in one process"
    echo "durability=redb immediate, default cache 64 MiB"
  } | tee "$RESULT_DIR/$RUN_ID-e2e.meta"
}

record() {
  "$@" | tee -a "$OUT"
}

meta
for payload in 64 1024; do
  for batch in 1 128 256; do
    if [ "$batch" = 1 ]; then
      native_total=2000
      amqp_total=512
    else
      native_total=51200
      amqp_total=12800
    fi
    for rep in 1 2 3; do
      log "native payload=$payload batch=$batch rep=$rep"
      record ./target/release/e2e-bench native \
        "--total=$native_total" "--batch=$batch" "--payload=$payload" "--rep=$rep" "--mode=both"
      for mode in publish consume both; do
        log "amqp payload=$payload batch=$batch rep=$rep mode=$mode"
        record ./target/release/e2e-bench amqp \
          "--total=$amqp_total" "--batch=$batch" "--payload=$payload" "--rep=$rep" "--mode=$mode"
      done
    done
  done
done
log "done -> $OUT"
