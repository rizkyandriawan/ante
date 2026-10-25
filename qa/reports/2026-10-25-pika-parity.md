# Pika, standalone Ante, and embedded Python parity — 2026-10-25

## Review findings and changes

### Standalone and embedded AMQP adapter

- A queue existence lookup previously swallowed storage errors with
  `unwrap_or(false)`, allowing success confirms on the unroutable path. Storage
  failure now closes the channel without a success confirm.
- ACK/NACK/reject/recover previously removed delivery tracking before storage
  succeeded; some failures were only logged or ignored. Tracking and the
  delivery lock now remain held through settlement. Failed settlement closes
  the channel and leaves claims available to cleanup/recovery.
- Auto-ACK storage errors now close the channel instead of silently forgetting
  claims. Bodies may already have reached a peer before an auto-ACK failure;
  this is not an exactly-once guarantee.
- QoS now follows RabbitMQ's split: non-global limits are captured per new
  consumer; global QoS adds a shared push-consumer limit. Zero is unlimited;
  `basic.get` and auto-ACK do not consume QoS credit. Internal takes stay bounded.
- Exclusive/auto-delete queue flags, queue/consumer arguments, exclusive
  consumers, and byte QoS are rejected rather than silently ignored.

### Direct embedded Python adapter

- `basic_nack(requeue=False)` / reject now use discard, preserving correct
  attempt history instead of recording success.
- Unknown upper delivery tags are validated before multiple settlement, so an
  invalid multiple ACK cannot settle valid preceding deliveries.
- Callback exceptions stop/propagate from the dispatcher, leaving manual
  settlement under application control. The separate high-level API retains
  its configured retry/drop behavior.
- Added `confirm_delivery`, `add_on_return_callback`, `basic_recover`, and
  `is_closed`; aligned defaults/parameters for the 15 tested channel methods.
- Channel publishes return `None`, accept bytes/UTF-8 strings, preserve optional
  metadata, and implement mandatory routing. `ante.UnroutableError.messages`
  carries returned-message details. Missing and explicitly empty properties
  remain distinct through the PyO3 publish binding.
- Added per-consumer/global/unlimited QoS with a shared atomic take gate for
  global credit. Auto-ACK bypasses credit. Channel leases match the AMQP
  30-minute timeout; high-level `Ante.take/consume` keep their 30-second default.
- Auto-generated consumer tags are monotonic and skip existing explicit tags.
- Embedded multiple ACK uses one core batch settlement, marking Python delivery
  objects settled only after the core result is known.

The updated AMQP and core sources/tests were mirrored into `ante-python/vendor`.
The separate in-tree `ante/sdks/python` work was not edited.

## Verification

| Gate | Result |
|---|---|
| Rust workspace | 132 passed, 0 failed |
| Ante all-target Clippy, six packages, `--no-deps -- -D warnings` | 0 warnings |
| Rust formatting | passed |
| Downstream packaging without Cargo patch | debug + release passed |
| Python extension release rebuild | passed |
| Python suite with Pika 1.4.4 | 46 passed, 0 failed |
| Python suite with Pika 1.3.2 | 46 passed, 0 failed |
| Shared contract transports | 4/4 passed: direct embedded, embedded AMQP, standalone AMQP, RabbitMQ |
| Parameter contract | 15/15 channel methods, both Pika versions |
| Python line coverage | 87% overall, 87% channel module (590/675 statements covered overall) |
| Ruff | 0 findings |
| Python native package Clippy | 0 warnings |
| Independent Pika producer/consumer processes | 100 messages per listener, 2 listener modes, both Pika versions; all ordered/drained |
| SIGKILL fault matrix | 15/15 passed: 3 each publish, claim, ACK, complete, ownership |
| AgentQueue downstream regression | 57 passed, 0 failed |

Reference broker: local isolated `rabbitmq:3.13-management`, ephemeral loopback
port 32768. Standalone Ante: release binary on loopback port 15679. The embedded
listener uses an OS-assigned port. RabbitMQ and Ante were used only with
disposable test queues/storage and stopped after verification.

The new suite contributes 13 tests, including a shared multi-transport scenario,
parameter checks, independent process checks, process-exit recovery, and SQLite
fault injection. All 33 pre-existing Python tests still pass.

The independent processes assert every payload and drain the queue. Failure
tests distinguish producer confirms from consumer ACK method returns by using
an ordered RPC and observing broker state. SQLite trigger injection verifies
that failed manual/auto-ACK and discard preserve pending messages. Renaming the
queue table injects a lookup failure and verifies no false publish confirm.

## Reproduction

From `ante-python`, with rebuilt extension installed into a test venv:

```bash
ANTE_STANDALONE_AMQP=127.0.0.1:15679 \
ANTE_REFERENCE_AMQP=127.0.0.1:32768 \
python -m unittest discover -s tests -v
```

`ANTE_REFERENCE_AMQP` must point to an actual disposable RabbitMQ instance.
Without environment endpoints the suite still runs direct embedded and embedded
AMQP tests, but does not exercise standalone or reference variants.

From Ante, using the Python environment containing the rebuilt extension:

```bash
cargo test --workspace
cargo clippy -p ante-core -p ante-native -p ante-amqp -p ante-amqp1 -p ante-security -p ante --all-targets --no-deps -- -D warnings
cargo fmt --all -- --check
./qa/downstream_build.sh
python qa/fault_matrix.py
```

## Remaining scope and release handoff

- This verifies a named-queue/default-exchange work-queue profile. Named
  exchanges, server-generated queues, TTL/dead-letter/priority policies, and
  complete RabbitMQ/Pika compatibility are outside the supported profile.
- Direct Python conditional queue deletion remains explicitly unsupported;
  the AMQP listener supports it. `BasicProperties` supports five fields, not
  every AMQP basic property.
- Direct errors use `AnteError`, not Pika's exception hierarchy, and invalid
  calls do not emulate every protocol-level channel closure. `basic_cancel`
  returns `None` and drains Ante workers, rather than returning Pika's buffered
  message list. Callback concurrency is an Ante extension.
- AgentQueue tests used the locally rebuilt editable extension. Its dependency
  is still pinned to upstream `ante-python@ce6034a`. Publishing these changes
  and updating that pin to the new tested commit is required for fresh installs
  to receive the fixes. No unpublished commit hash was invented.
- No group-commit/cycle-based fsync implementation was introduced. This suite
  is the compatibility gate for any future storage scheduling change.
- Process crash checks do not prove physical power-loss behavior of the device.
