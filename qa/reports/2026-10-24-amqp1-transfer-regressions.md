# AMQP 1.0 ordered transport completion — 2026-10-24

Follow-up to the review of the session-window snapshot and socket-abort path.

## Implementation

- Removed `TransferStatus` as a completion mechanism. `Sender::wait_for_transport()`
  places a local marker on the same FIFO as the delivery's transfer fragments.
- The session retains markers in its outgoing buffer behind blocked fragments.
  Markers do not consume session window: a marker immediately following the last
  fragment can proceed even when that fragment exhausted the window.
- The connection writer acknowledges the marker only after transport flush
  succeeds. This is local write completion, not a peer receipt or settlement ACK.
- Enqueue and writer completion share one absolute transfer deadline, capped by
  the absolute lease deadline. Settlement and shutdown grace use the same lease
  deadline, without adding another full duration after transfer completion.
- Real terminal settlement already available at transfer cancellation is
  processed. Pre-settled sends still require writer completion before ACKing the
  queue message.
- A cloned socket descriptor is mandatory at connection admission. Descriptor
  duplication failure rejects that connection. Socket shutdown is checked before
  immediate requeue; if shutdown fails, the claim remains leased. Zero linger is
  requested, and protocol-abort control is subsequently bounded cleanup.
- Send errors also disable the connection before releasing an ambiguous claim.

## Verification

| Gate | Result |
|---|---|
| `cargo test --workspace` | 131 passed, 0 failed |
| AMQP1 compatibility tests | 16 passed, 0 failed |
| Socket shutdown failure injection | 1 passed; claim stays in-flight |
| Ante crates, all-target Clippy with `--no-deps -- -D warnings` | passed, 0 warnings |
| `cargo fmt --all -- --check` | passed |
| Vendored acceptor + rustls + transaction feature build | passed |
| Downstream build without a Cargo patch | debug + release passed |
| Python extension release rebuild | passed |
| Python unittest suite | 33 passed, 0 failed |

The all-target Clippy gate selects Ante's six packages explicitly so vendored
fe2o3 unit-test targets (whose development dependencies are not shipped) are not
selected as workspace test targets.

### Regression cases

- **One-frame delivery consumes the final session slot:** initial window 1,
  automatic window top-up disabled. The peer holds the delivery for 300 ms,
  three times the 100 ms transfer timeout, before accepting it. The claim stays
  in-flight and the accepted outcome becomes durable without opening a new slot.
- **Partial two-frame and many-frame deliveries:** max frame size 512 bytes,
  initial window 1, top-up disabled; payloads 700 bytes and 64 KiB. The test
  counts bytes read from the socket and asserts that a partial frame arrived,
  but not the complete body, before server-side cancellation and recovery.
- **Positive session window, TCP backpressure:** the peer stops reading after
  attach, has a 4 KiB receive buffer, and advertises 2048 session slots. A 16 MiB
  message fits within that frame allowance but cannot fit in the socket buffer.
  The 150 ms transfer deadline recovers the claim within an 800 ms assertion
  budget, rather than waiting for the default 30-minute settlement lease.
- **Lease caps transfer wait:** the same blocked writer has a 200 ms lease and
  a 3-second transfer wait. Recovery must occur within 800 ms; it cannot wait
  for the longer transfer timeout or the maintenance recovery interval.
- **Abort syscall fails:** an owned non-socket descriptor injects `ENOTSOCK`.
  The abort/requeue helper must leave 1 in-flight and 0 ready messages.

## Post-change load smoke

One release-mode run, four producers and four consumers, 20,000 messages of
1 KiB, SQLite durable commits, loopback plaintext AMQP 1.0:

| Wall | Throughput | Publish p50 | Publish p99 | Peak RSS | DB | Duplicates |
|---:|---:|---:|---:|---:|---:|---:|
| 37.377 s | 535.1 jobs/s | 3.749 ms | 9.748 ms | 28.05 MiB | 17.41 MiB | 0 |

The payload verifier passed and the queue drained. This is one regression smoke
run, not a new three-run median or capacity measurement. Earlier load medians
remain historical in [the baseline report](2026-10-24-amqp1-load.md).

Reproduce the main correctness and lint gates:

```bash
cargo test --workspace
cargo clippy -p ante-core -p ante-native -p ante-amqp -p ante-amqp1 -p ante-security -p ante --all-targets --no-deps -- -D warnings
cargo fmt --all -- --check
./qa/downstream_build.sh
```
