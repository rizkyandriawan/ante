# Security: TLS, authentication, and queue permissions

Ante's network endpoints are loopback-first and intentionally small. Before
exposing them to another host, enable the secure profile: TLS, credential
authentication, and default-deny queue permissions.

A security file configures both adapters:

```bash
ante serve --db ./ante.redb --security ./security.json
```

The file is loaded and fully validated **before** any listener binds. A
configuration with users but no TLS is rejected. The same `Security` object is
used by the native and AMQP listeners.

## Security file

```json
{
  "tls": {
    "cert": "server.pem",
    "key": "server.key"
  },
  "users": {
    "producer": {
      "password_hash": "$argon2id$v=19$m=19456,t=2,p=1$...",
      "permissions": {
        "publish": ["jobs"],
        "inspect": ["jobs"]
      }
    },
    "worker": {
      "password_hash": "$argon2id$v=19$m=19456,t=2,p=1$...",
      "permissions": {
        "consume": ["jobs"],
        "inspect": ["jobs"]
      }
    }
  }
}
```

- `tls.cert` and `tls.key` are PEM paths, resolved relative to the security
  file's directory when not absolute.
- `password_hash` must be an **Argon2id** PHC string. Generate one from stdin:

  ```bash
  printf '%s' 'secret' | ante hash-password
  ```

- `permissions` is default-deny. Queue names are exact; wildcards (`*`, `#`)
  are rejected.
- A user with no permissions is rejected rather than silently granted nothing.

## Permission matrix

| Permission | Native operations | AMQP operations |
|---|---|---|
| `publish` | `PUBLISH`, `PUBLISH_BATCH` | `basic.publish` |
| `consume` | `TAKE`, `ACK`, `REQUEUE` | `basic.consume`, `basic.get`, `basic.ack/reject/nack`, `basic.recover` |
| `inspect` | `INFO`, filtered `LIST`, passive declare | `queue.declare(passive)`, `queue.bind/unbind` |
| `declare` | `DECLARE` | `queue.declare` |
| `purge` | `PURGE` | `queue.purge` |
| `delete` | `DELETE` | `queue.delete` |

`ACK` and `REQUEUE` authorize the queue that owns the in-flight delivery; a
delivery token is never accepted as proof of authorization. An unauthorized token
is rejected without settling the message. `LIST` only returns queues the user may
inspect.

AMQP `basic.consume` only needs `consume`; the queue must already exist (as it
must for any consumer). A producer that should not read its own work needs only
`publish`.

## TLS

- TLS 1.2 and 1.3 via rustls.
- The listener speaks **only** TLS in secure mode; there is no plaintext
  fallback.
- Clients verify the certificate against a CA. For internal CAs, point the client
  at the CA PEM.
- TLS and authentication handshakes time out after 10 seconds.
- Password verification runs off the async runtime (Argon2 is deliberately
  expensive); it is not performed under the Python GIL.

## Native protocol clients

```bash
ante declare jobs \
  --server 127.0.0.1:7300 \
  --tls-ca ./ca.pem \
  --tls-server-name 127.0.0.1 \
  --user producer \
  --password-file ./producer.secret
```

- `ANTE_USER` and `ANTE_PASSWORD` are the environment equivalents.
- `--password-file` strips trailing CR/LF; prefer it over a command-line secret.
- `--tls-ca` enables TLS. Without it the client attempts plaintext.
- A secure server replies `AUTH_REQUIRED` until a valid `AUTH` command arrives.

Wire sequence:

```text
HELLO 1
AUTH producer <secret>
PUBLISH jobs payload
```

`AUTH` may be retried on the same connection; every other queue command is
rejected until it succeeds. Unknown users and wrong secrets return the same
`AUTHENTICATION_FAILED` error. Missing authentication returns `AUTH_REQUIRED`.

## AMQP clients

Use `amqps://` and a CA file:

```python
import ssl
import pika

context = ssl.create_default_context(cafile="ca.pem")
params = pika.ConnectionParameters(
    "broker.internal",
    5671,
    credentials=pika.PlainCredentials("producer", "secret"),
    ssl_options=pika.SSLOptions(context, "broker.internal"),
)
conn = pika.BlockingConnection(params)
```

Only SASL `PLAIN` is offered in secure mode. The virtual host must be `/`
(`%2f` in a URI). Failed authentication closes the connection with
`ACCESS_REFUSED` (403). Missing permissions close the channel with
`ACCESS_REFUSED`; the connection remains usable for other channels.

`AMQPLAIN` remains accepted only by the legacy plaintext profile. It is not a
replacement for modern credentials.

## Embedded Python

`ante-embedded` exposes the same secure listener:

```python
ante = Ante.open("ante.redb")
host, port = ante.serve_amqp(
    "127.0.0.1", 5671, security_path="/etc/myapp/security.json"
)
```

The security file is loaded before the socket binds. The embedded API never
returns plaintext when `security_path` is set; callers must configure TLS
clients accordingly.

## Operational notes

- Generate a CA and a server certificate with a SAN for every client-visible
  hostname/IP. Do not use a self-signed leaf as both server certificate and CA;
  rustls rejects `CAUsedAsEndEntity`.
- Restrict file permissions on the security JSON and key: `0600`/`0640` with a
  dedicated group. Ante does not chmod them for you.
- Rotate certificates and credentials by editing the file and restarting the
  daemon. Hot reload is not implemented.
- There is no mTLS, OAuth, token expiry, revocation, rate limit, or audit log in
  v1. Use network policy and short-lived credentials if you expose the endpoint.
- The queue database remains a single-writer file. Ante's security does not
  sandbox local processes that can open it directly; protect the filesystem.
- Password hashes are verified with Argon2's default parameters
  (`m=19456, t=2, p=1`). Generating a hash is CPU- and memory-intensive by
  design.
