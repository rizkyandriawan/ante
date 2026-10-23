#!/usr/bin/env bash
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$BENCH_DIR"
DATA_ROOT="${DATA_ROOT:-/tmp/ante-bench}"
RESULT_DIR="${RESULT_DIR:-$BENCH_DIR/results}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$RESULT_DIR/$RUN_ID.jsonl"
mkdir -p "$DATA_ROOT" "$RESULT_DIR"
: > "$OUT"

bins=(redb-bench heed-bench)

log() { echo "[$(date -u +%H:%M:%S)] $*" >&2; }

record() {
  local line="$1"
  echo "$line" >> "$OUT"
}

meta() {
  {
    echo "run_id=$RUN_ID"
    echo "cpu=$(lscpu | awk -F': +' '/Model name/{print $2; exit}')"
    echo "cores=$(nproc)"
    echo "kernel=$(uname -r)"
    echo "fs=$(stat -f -c %T "$DATA_ROOT")"
    echo "rustc=$(~/.cargo/bin/rustc --version 2>/dev/null || rustc --version)"
    echo "redb=$(grep -A1 '^name = "redb"' Cargo.lock | awk -F'"' '/version/{print $2; exit}')"
    echo "heed=$(grep -A1 '^name = "heed"' Cargo.lock | awk -F'"' '/version/{print $2; exit}')"
    echo "lmdb_sys=$(grep -A1 '^name = "lmdb-master-sys"' Cargo.lock | awk -F'"' '/version/{print $2; exit}')"
  } | tee "$RESULT_DIR/$RUN_ID.meta"
  echo "--- binary sizes (bytes) ---" | tee -a "$RESULT_DIR/$RUN_ID.meta"
  stat -c '%n %s' target/release/redb-bench target/release/heed-bench | tee -a "$RESULT_DIR/$RUN_ID.meta"
}

run_one() {
  local bin="$1" mode="$2" db="$3"; shift 3
  rm -rf "$db"
  local line
  line=$("./target/release/$bin" "$mode" "--db=$db" "$@")
  record "$line"
  echo "$line" >&2
}

phase_lifecycle() {
  log "phase: lifecycle matrix"
  for payload in 64 1024 8192; do
    for batch in 1 32 128; do
      local total=20000
      if [ "$batch" = 1 ]; then total=5000; fi
      for bin in "${bins[@]}"; do
        for rep in 1 2 3; do
          run_one "$bin" stream "$DATA_ROOT/lifecycle" \
            "--total=$total" "--batch=$batch" "--payload=$payload" "--rep=$rep"
          rm -rf "$DATA_ROOT/lifecycle"
        done
      done
    done
  done
}

phase_backlog() {
  log "phase: backlog"
  for spec in "64 500000" "1024 200000" "8192 50000"; do
    set -- $spec
    local payload=$1 total=$2
    for bin in "${bins[@]}"; do
      run_one "$bin" backlog "$DATA_ROOT/backlog" \
        "--total=$total" "--batch=64" "--payload=$payload"
      rm -rf "$DATA_ROOT/backlog"
    done
  done
}

run_existing() {
  local bin="$1" mode="$2" db="$3"; shift 3
  local line
  line=$("./target/release/$bin" "$mode" "--db=$db" "$@")
  record "$line"
  echo "$line" >&2
}

phase_retry() {
  log "phase: retry"
  for bin in "${bins[@]}"; do
    run_one "$bin" retry "$DATA_ROOT/retry" \
      "--total=200000" "--batch=64" "--payload=512" "--retry-pct=20"
    run_existing "$bin" recover "$DATA_ROOT/retry" "--total=1" "--batch=1" "--payload=512"
    rm -rf "$DATA_ROOT/retry"
  done
}

phase_million() {
  log "phase: million-job footprint"
  for bin in "${bins[@]}"; do
    run_one "$bin" fill "$DATA_ROOT/million" \
      "--total=1000000" "--batch=64" "--payload=1024"
    run_existing "$bin" recover "$DATA_ROOT/million" "--total=1" "--batch=1" "--payload=1024"
    rm -rf "$DATA_ROOT/million"
  done
}

phase_crash() {
  log "phase: kill -9 recovery"
  for bin in "${bins[@]}"; do
    local dir="$DATA_ROOT/crash-$bin"
    rm -rf "$dir"
    for i in $(seq 1 30); do
      local delay
      delay=$(python3 -c 'import random; print(round(random.uniform(0.05,0.40),3))')
      "./target/release/$bin" stream "--db=$dir" "--total=50000000" "--batch=64" "--payload=256" \
        >/dev/null 2>"$DATA_ROOT/crash.err" &
      local pid=$!
      sleep "$delay"
      kill -9 "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      local out
      out=$("./target/release/$bin" recover "--db=$dir" "--total=1" "--batch=1" "--payload=256")
      if [ "$(echo "$out" | python3 -c 'import json,sys; print(json.load(sys.stdin)["invariants_ok"])')" != "True" ]; then
        echo "CRASH INVARIANT FAILURE bin=$bin iteration=$i" >&2
        echo "$out" >&2
        exit 1
      fi
      python3 -c 'import json,sys; d=json.loads(sys.argv[1]); d.update(phase="crash_recover", iteration=int(sys.argv[2]), delay_s=float(sys.argv[3])); print(json.dumps(d))' "$out" "$i" "$delay" >> "$OUT"
    done
    log "crash test ok for $bin (30 cycles)"
    run_existing "$bin" recover "$dir" "--total=1" "--batch=1" "--payload=256"
    rm -rf "$dir"
  done
}

meta
phase_lifecycle
phase_backlog
phase_retry
phase_million
phase_crash
log "done -> $OUT"