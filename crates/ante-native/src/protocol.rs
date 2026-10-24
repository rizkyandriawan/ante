use crate::codec::Value;

pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Hello {
        version: u32,
    },
    Auth {
        username: String,
        secret: String,
    },
    Declare {
        queue: String,
    },
    Publish {
        queue: String,
        payload: Vec<u8>,
        content_type: Option<String>,
        message_id: Option<String>,
    },
    PublishBatch {
        queue: String,
        payloads: Vec<Vec<u8>>,
    },
    Take {
        queue: String,
        max: usize,
        lease_ms: u64,
        wait_ms: u64,
    },
    Ack {
        tokens: Vec<String>,
    },
    Discard {
        tokens: Vec<String>,
    },
    Complete {
        token: String,
        queue: String,
        payload: Vec<u8>,
        content_type: Option<String>,
        message_id: Option<String>,
        settlement: ante_core::Settlement,
    },
    Touch {
        token: String,
        lease_ms: u64,
    },
    Requeue {
        token: String,
    },
    Info {
        queue: String,
    },
    Status {
        queue: String,
        message_id: u64,
    },
    Purge {
        queue: String,
    },
    DeleteQueue {
        queue: String,
    },
    List,
    Ping,
    Quit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub code: &'static str,
    pub message: String,
}

fn fail(code: &'static str, message: impl Into<String>) -> ParseError {
    ParseError {
        code,
        message: message.into(),
    }
}

impl Command {
    pub fn parse(value: Value) -> Result<Command, ParseError> {
        let items = match value {
            Value::Array(items) => items,
            _ => return Err(fail("PROTOCOL", "expected an array of bulk strings")),
        };
        if items.is_empty() {
            return Err(fail("PROTOCOL", "empty command"));
        }

        let mut fields = Vec::with_capacity(items.len());
        for item in items {
            match item {
                Value::Bulk(bytes) => fields.push(bytes),
                Value::Simple(text) => fields.push(text.into_bytes()),
                other => {
                    return Err(fail(
                        "PROTOCOL",
                        format!("command arguments must be strings, got {other:?}"),
                    ))
                }
            }
        }

        let name = String::from_utf8_lossy(&fields[0]).to_ascii_uppercase();
        let args = &fields[1..];
        match name.as_str() {
            "HELLO" => {
                let version = match args.first() {
                    Some(raw) => parse_u32(raw, "version")?,
                    None => PROTOCOL_VERSION,
                };
                if version != PROTOCOL_VERSION {
                    return Err(fail(
                        "VERSION",
                        format!("unsupported protocol version {version}"),
                    ));
                }
                Ok(Command::Hello { version })
            }
            "AUTH" => {
                expect(args, 2, "AUTH username secret")?;
                Ok(Command::Auth {
                    username: text(&args[0], "username")?,
                    secret: text(&args[1], "secret")?,
                })
            }
            "DECLARE" => {
                expect(args, 1, "DECLARE queue")?;
                Ok(Command::Declare {
                    queue: text(&args[0], "queue")?,
                })
            }
            "PUBLISH" => {
                if args.len() < 2 || args.len() > 4 {
                    return Err(fail(
                        "PROTOCOL",
                        "PUBLISH queue payload [content-type] [message-id]",
                    ));
                }
                Ok(Command::Publish {
                    queue: text(&args[0], "queue")?,
                    payload: args[1].clone(),
                    content_type: optional_text(args.get(2)),
                    message_id: optional_text(args.get(3)),
                })
            }
            "PUBLISH_BATCH" => {
                if args.len() < 2 {
                    return Err(fail("PROTOCOL", "PUBLISH_BATCH queue payload..."));
                }
                Ok(Command::PublishBatch {
                    queue: text(&args[0], "queue")?,
                    payloads: args[1..].to_vec(),
                })
            }
            "TAKE" => {
                if args.len() < 2 || args.len() > 4 {
                    return Err(fail("PROTOCOL", "TAKE queue max [lease-ms] [wait-ms]"));
                }
                Ok(Command::Take {
                    queue: text(&args[0], "queue")?,
                    max: parse_usize(&args[1], "max")?,
                    lease_ms: args
                        .get(2)
                        .map(|v| parse_u64(v, "lease-ms"))
                        .transpose()?
                        .unwrap_or(0),
                    wait_ms: args
                        .get(3)
                        .map(|v| parse_u64(v, "wait-ms"))
                        .transpose()?
                        .unwrap_or(0),
                })
            }
            "ACK" | "ACK_BATCH" => {
                if args.is_empty() {
                    return Err(fail("PROTOCOL", "ACK token..."));
                }
                Ok(Command::Ack {
                    tokens: args
                        .iter()
                        .map(|raw| text(raw, "delivery token"))
                        .collect::<Result<_, _>>()?,
                })
            }
            "DISCARD" => {
                if args.is_empty() {
                    return Err(fail("PROTOCOL", "DISCARD token..."));
                }
                Ok(Command::Discard {
                    tokens: args
                        .iter()
                        .map(|raw| text(raw, "delivery token"))
                        .collect::<Result<_, _>>()?,
                })
            }
            "TOUCH" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(fail("PROTOCOL", "TOUCH token [lease-ms]"));
                }
                Ok(Command::Touch {
                    token: text(&args[0], "delivery token")?,
                    lease_ms: args
                        .get(1)
                        .map(|value| parse_u64(value, "lease-ms"))
                        .transpose()?
                        .unwrap_or(0),
                })
            }
            "COMPLETE" => {
                if args.len() < 3 || args.len() > 6 {
                    return Err(fail(
                        "PROTOCOL",
                        "COMPLETE token queue payload [content-type] [message-id] [ACK|DISCARD]",
                    ));
                }
                let settlement = match args.get(5) {
                    Some(value) if value.eq_ignore_ascii_case(b"discard") => {
                        ante_core::Settlement::Discard
                    }
                    Some(value) if value.eq_ignore_ascii_case(b"ack") => ante_core::Settlement::Ack,
                    Some(_) => return Err(fail("PROTOCOL", "settlement must be ACK or DISCARD")),
                    None => ante_core::Settlement::Ack,
                };
                Ok(Command::Complete {
                    token: text(&args[0], "delivery token")?,
                    queue: text(&args[1], "queue")?,
                    payload: args[2].clone(),
                    content_type: optional_text(args.get(3)),
                    message_id: optional_text(args.get(4)),
                    settlement,
                })
            }
            "REQUEUE" => {
                expect(args, 1, "REQUEUE token")?;
                Ok(Command::Requeue {
                    token: text(&args[0], "delivery token")?,
                })
            }
            "INFO" => {
                expect(args, 1, "INFO queue")?;
                Ok(Command::Info {
                    queue: text(&args[0], "queue")?,
                })
            }
            "STATUS" => {
                expect(args, 2, "STATUS queue message-id")?;
                Ok(Command::Status {
                    queue: text(&args[0], "queue")?,
                    message_id: parse_u64(&args[1], "message-id")?,
                })
            }
            "PURGE" => {
                expect(args, 1, "PURGE queue")?;
                Ok(Command::Purge {
                    queue: text(&args[0], "queue")?,
                })
            }
            "DELETE" => {
                expect(args, 1, "DELETE queue")?;
                Ok(Command::DeleteQueue {
                    queue: text(&args[0], "queue")?,
                })
            }
            "LIST" => {
                expect(args, 0, "LIST")?;
                Ok(Command::List)
            }
            "PING" => Ok(Command::Ping),
            "QUIT" => Ok(Command::Quit),
            other => Err(fail("UNKNOWN_COMMAND", format!("unknown command {other}"))),
        }
    }
}

fn expect(args: &[Vec<u8>], count: usize, usage: &str) -> Result<(), ParseError> {
    if args.len() == count {
        Ok(())
    } else {
        Err(fail("PROTOCOL", format!("usage: {usage}")))
    }
}

fn text(raw: &[u8], field: &str) -> Result<String, ParseError> {
    String::from_utf8(raw.to_vec()).map_err(|_| fail("PROTOCOL", format!("{field} is not UTF-8")))
}

fn optional_text(raw: Option<&Vec<u8>>) -> Option<String> {
    raw.filter(|bytes| !bytes.is_empty())
        .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
}

fn parse_usize(raw: &[u8], field: &str) -> Result<usize, ParseError> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| fail("PROTOCOL", format!("invalid {field}")))
}

fn parse_u64(raw: &[u8], field: &str) -> Result<u64, ParseError> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| fail("PROTOCOL", format!("invalid {field}")))
}

fn parse_u32(raw: &[u8], field: &str) -> Result<u32, ParseError> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| fail("PROTOCOL", format!("invalid {field}")))
}
