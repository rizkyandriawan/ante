# POSIX mqueue source investigation for Ante

Date: 2026-10-23. Source inspection only; no performance benchmark or kernel
build was performed. Exact upstream snapshots are recorded in `sources.json`.
The kernel snapshot is upstream master, not the running host's 7.0.0-38-generic
kernel. Downloaded sources retain their upstream copyright/license headers.

## Layers

POSIX specifies the API; it is not a storage engine. On Linux:

1. Application calls libc `mq_send`/`mq_receive`.
2. libc forwards to timed variants, using a null timeout.
3. Timed variants enter kernel syscalls.
4. `ipc/mqueue.c` implements the queue, ordering, synchronization and wakeups.
5. `ipc/msgutil.c` allocates message memory and copies payloads across the
   user/kernel boundary.
6. Kernel allocators, scheduler, virtual memory and CPU memory primitives
   provide the lower-level infrastructure. There is no database underneath.

## Source-backed observations

References are to the downloaded snapshots, using 1-indexed line numbers.

| Observation | Reference |
| --- | --- |
| libc send is a thin forwarding function | `glibc/sysdeps/unix/sysv/linux/mq_send.c:24-30` |
| libc timed send invokes a syscall, with ABI/time compatibility handling | `glibc/sysdeps/unix/sysv/linux/mq_timedsend.c:25-59` |
| Each queue owns a spinlock, priority tree, cached tree node and wait lists | `linux/ipc/mqueue.c:133-155` |
| Red-black tree nodes correspond to priorities; messages within a priority use a list | `linux/ipc/mqueue.c:190-229` |
| Highest-priority tree node is cached | `linux/ipc/mqueue.c:247-269` |
| A waiting receiver can receive a message directly without tree insertion | `linux/ipc/mqueue.c:1013-1023`, `1127-1135` |
| Payload allocation/copy-in occurs before the queue lock | `linux/ipc/mqueue.c:1083-1099` |
| Wakeup occurs after releasing the queue lock | `linux/ipc/mqueue.c:1139-1141` |
| Receive removes a message, copies it out, then frees it; there is no later application ack | `linux/ipc/mqueue.c:1217-1234` |
| Allocation uses kernel buckets and extra segments for larger messages | `linux/ipc/msgutil.c:39-83` |
| Explicit payload copy-in and copy-out | `linux/ipc/msgutil.c:92-119`, `158-174` |

Ordinary send/receive therefore involves two explicit payload-copy stages:
sender user memory to kernel message memory, then kernel message memory to
receiver user memory. Direct handoff avoids queue insertion, not those copies.
Tree insertion searches distinct active priorities, not every queued message.
With one priority, tree traversal is already shallow; removing it alone is
unlikely to be a revolutionary optimization. These are candidate cost centers,
not measured bottlenecks.

## What Rust changes

- A Rust wrapper calling `mq_*` uses this same kernel implementation.
- A kernel Rust replacement would be a separate kernel engineering project,
  requiring appropriate Rust abstractions/interoperability and integration;
  it cannot replace the existing syscall implementation by installing a normal
  systemd service. Equivalent algorithms have no automatic speed advantage.
- A new userspace Rust queue can specialize semantics, allocation and batching,
  but is a new implementation rather than a faster POSIX wrapper.

## Candidate Ante design

Recommended initial shape: a userspace Rust daemon/library with a bounded
FIFO ready queue, event-driven delivery, batched durable state changes and
optional AMQP 0-9-1 adapter. Use Unix sockets as the baseline local transport.
Use a separate due-time index for retries. Multiple worker executions may run
concurrently even when queue state changes are serialized.

Potential optimizations to test:

- FIFO/ring or chunked structures instead of priorities, when priorities are
  explicitly outside Ante's contract.
- Bounded buffers/pools to reduce per-message allocation.
- Batch publish/delivery/ack to amortize calls and storage synchronization.
- Shared-memory rings as a later alternative local transport. They require
  ownership, crash recovery, memory ordering and backpressure design; they do
  not remove the copies or semantics required by existing AMQP TCP clients.
- Keep payloads immutable across retries, changing only small state records.

Shared memory is volatile IPC, not durable storage. A journal or transactional
store remains necessary for confirmed-message recovery after reboot. POSIX
mqueue is useful as a baseline, not a replacement for that store.

## Proposed experiment (not executed)

Compare POSIX mqueue, Unix-socket Ante prototype and, only if justified, a
shared-memory prototype. Test volatile IPC separately from durable job queues.

- Payload sizes: 64 B, 1 KiB, 8 KiB.
- Producer/consumer counts: 1/1, 4/4, 16/16.
- Batch sizes where supported: 1, 16, 64. POSIX mq_* remains one message per call.
- Report completed messages/s, p50/p95/p99, CPU time/message and context
  switches/message; record affinity, kernel, compiler, hardware and queue caps.
- Account for userspace RSS/PSS, shared-memory resident pages and kernel IPC
  allocations; daemon RSS alone is not a fair memory comparison.
- Durable candidates: match sync/confirm contracts, include ack-state writes,
  retry-heavy workloads, stalled workers, disk reclamation and recovery time.
- Crash tests should count confirmed messages, completed/acked messages,
  duplicates and lost messages separately. Process-kill tests do not establish
  power-loss safety; OS page cache survives a process kill.

Decision: pursue a specialized userspace engine if measurements show a useful
footprint/throughput advantage while preserving Ante's delivery contract.
Do not claim a Rust performance improvement from source inspection alone.
