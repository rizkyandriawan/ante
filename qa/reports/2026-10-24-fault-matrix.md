# SQLite process-fault matrix — 2026-10-24

Fifteen `SIGKILL` injection cycles across five durability boundaries: three
cycles per scenario. No scenario lost a committed message, replayed an
acknowledged message, or split an atomic completion.

Each child process opened the database, completed the named operation, wrote a
ready marker, and then slept. The parent only sent `SIGKILL` after the marker
appeared, so every kill happened after the operation had returned.

| Scenario | Cycles | Assertion after reopen | Result |
|---|---:|---|---|
| Committed publish, then kill | 3 | 200 ready, 0 in-flight; exact message IDs and payloads | 3/3 |
| In-flight delivery, then kill | 3 | 5 ready, 0 in-flight; exact IDs recovered, history `recovered` | 3/3 |
| Acknowledged delivery, then kill | 3 | 4 ready; the exact acked ID stays `acked` and is not redelivered | 3/3 |
| Result + input ack in one transaction, then kill | 3 | 4 exact inputs ready; input `acked`; exactly one result with the expected ID | 3/3 |
| Second owner while first is alive | 3 | second open rejected; after kill, reopen succeeds | 3/3 |

The test now checks every ACK result count and status, exact message-ID-to-payload
pairing, the atomic input state, the result identity, and that every injected child
died from the expected `SIGKILL` before assertions run. The in-flight and atomic-completion cases
exercise startup recovery after an unclean process exit. The owner-lock case
confirms recovery is not run by a second opener.

Limits: these tests prove process-crash safety, not power-loss durability.
`SIGKILL` cannot interrupt a flush that the device has not physically persisted,
and filesystems/devices may reorder or lose writes across power loss. They also
do not cover a kill racing inside a commit before the API returns.

Reproduce with `qa/fault_matrix.py` using the rebuilt Python extension.
