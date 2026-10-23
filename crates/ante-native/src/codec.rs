use std::io;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};

use crate::{MAX_ARRAY_ITEMS, MAX_BULK_BYTES};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Vec<u8>),
    Array(Vec<Value>),
    Null,
}

impl Value {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Simple(s) | Value::Error(s) => Some(s),
            Value::Bulk(bytes) => std::str::from_utf8(bytes).ok(),
            _ => None,
        }
    }

    pub fn integer(&self) -> Option<i64> {
        match self {
            Value::Integer(v) => Some(*v),
            _ => None,
        }
    }
}

pub struct RespReader<R> {
    inner: BufReader<R>,
    max_bulk: usize,
    max_items: usize,
}

impl<R: AsyncReadExt + Unpin> RespReader<R> {
    pub fn new(inner: R) -> Self {
        RespReader {
            inner: BufReader::new(inner),
            max_bulk: MAX_BULK_BYTES,
            max_items: MAX_ARRAY_ITEMS,
        }
    }

    pub fn with_limits(inner: R, max_bulk: usize, max_items: usize) -> Self {
        RespReader {
            inner: BufReader::new(inner),
            max_bulk,
            max_items,
        }
    }

    /// Returns Ok(None) on a clean end-of-stream before a value.
    pub async fn read_value(&mut self) -> io::Result<Option<Value>> {
        let mut prefix = [0u8; 1];
        match self.inner.read_exact(&mut prefix).await {
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(err) => return Err(err),
        }
        let value = match prefix[0] {
            b'+' => Value::Simple(self.read_line().await?),
            b'-' => Value::Error(self.read_line().await?),
            b':' => {
                let line = self.read_line().await?;
                Value::Integer(parse_int(&line)?)
            }
            b'$' => {
                let line = self.read_line().await?;
                let len = parse_int(&line)?;
                if len < 0 {
                    Value::Null
                } else {
                    let len = usize::try_from(len)
                        .map_err(|_| protocol_error("bulk length out of range"))?;
                    if len > self.max_bulk {
                        return Err(protocol_error("bulk value exceeds limit"));
                    }
                    let mut buf = vec![0u8; len];
                    self.inner.read_exact(&mut buf).await?;
                    let mut terminator = [0u8; 2];
                    self.inner.read_exact(&mut terminator).await?;
                    if terminator != *b"\r\n" {
                        return Err(protocol_error("bulk value is not terminated by CRLF"));
                    }
                    Value::Bulk(buf)
                }
            }
            b'*' => {
                let line = self.read_line().await?;
                let count = parse_int(&line)?;
                if count < 0 {
                    Value::Null
                } else {
                    let count = usize::try_from(count)
                        .map_err(|_| protocol_error("array length out of range"))?;
                    if count > self.max_items {
                        return Err(protocol_error("array exceeds item limit"));
                    }
                    let mut items = Vec::with_capacity(count);
                    for _ in 0..count {
                        match Box::pin(self.read_value()).await? {
                            Some(value) => items.push(value),
                            None => return Err(protocol_error("truncated array")),
                        }
                    }
                    Value::Array(items)
                }
            }
            other => {
                return Err(protocol_error(&format!(
                    "unknown RESP prefix {:?}",
                    other as char
                )))
            }
        };
        Ok(Some(value))
    }

    async fn read_line(&mut self) -> io::Result<String> {
        let mut line = String::new();
        let n = self.inner.read_line(&mut line).await?;
        if n == 0 {
            return Err(protocol_error("unexpected end of stream"));
        }
        if !line.ends_with("\r\n") {
            return Err(protocol_error("line is not terminated by CRLF"));
        }
        line.truncate(line.len() - 2);
        Ok(line)
    }
}

fn parse_int(value: &str) -> io::Result<i64> {
    value.parse().map_err(|_| protocol_error("invalid integer"))
}

fn protocol_error(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

pub async fn write_value<W: AsyncWrite + Unpin>(writer: &mut W, value: &Value) -> io::Result<()> {
    match value {
        Value::Simple(text) => {
            writer.write_all(b"+").await?;
            writer.write_all(text.as_bytes()).await?;
            writer.write_all(b"\r\n").await?;
        }
        Value::Error(text) => {
            writer.write_all(b"-").await?;
            writer.write_all(text.as_bytes()).await?;
            writer.write_all(b"\r\n").await?;
        }
        Value::Integer(number) => {
            writer
                .write_all(format!(":{number}\r\n").as_bytes())
                .await?;
        }
        Value::Bulk(bytes) => {
            writer
                .write_all(format!("${}\r\n", bytes.len()).as_bytes())
                .await?;
            writer.write_all(bytes).await?;
            writer.write_all(b"\r\n").await?;
        }
        Value::Null => writer.write_all(b"$-1\r\n").await?,
        Value::Array(items) => {
            writer
                .write_all(format!("*{}\r\n", items.len()).as_bytes())
                .await?;
            for item in items {
                Box::pin(write_value(writer, item)).await?;
            }
        }
    }
    Ok(())
}

pub fn bulk(value: impl Into<Vec<u8>>) -> Value {
    Value::Bulk(value.into())
}

pub fn array(items: Vec<Value>) -> Value {
    Value::Array(items)
}
