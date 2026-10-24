use std::io;
use std::path::Path;
use std::sync::Arc;

use ante_security::{client_config, BoxedStream, ServerName, TlsConnector};
use tokio::io::{AsyncWriteExt, BufWriter, ReadHalf, WriteHalf};
use tokio::net::TcpStream;

use crate::codec::{bulk, write_value, RespReader, Value};

pub struct NativeClient {
    reader: RespReader<ReadHalf<BoxedStream>>,
    writer: BufWriter<WriteHalf<BoxedStream>>,
}

impl NativeClient {
    pub async fn connect(address: &str) -> io::Result<Self> {
        let stream = TcpStream::connect(address).await?;
        stream.set_nodelay(true)?;
        Self::from_stream(Box::new(stream))
    }

    /// Connect using TLS and verify the server against `ca` and `server_name`.
    pub async fn connect_tls(address: &str, ca: &Path, server_name: &str) -> io::Result<Self> {
        let stream = TcpStream::connect(address).await?;
        stream.set_nodelay(true)?;
        let config = client_config(ca).map_err(to_io)?;
        let connector = TlsConnector::from(Arc::clone(&config));
        let name = ServerName::try_from(server_name.to_string())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let stream = connector
            .connect(name, stream)
            .await
            .map_err(|error| io::Error::new(io::ErrorKind::ConnectionAborted, error))?;
        Self::from_stream(Box::new(stream))
    }

    fn from_stream(stream: BoxedStream) -> io::Result<Self> {
        let (read_half, writer) = tokio::io::split(stream);
        Ok(NativeClient {
            reader: RespReader::new(read_half),
            writer: BufWriter::with_capacity(64 * 1024, writer),
        })
    }

    pub async fn authenticate(&mut self, username: &str, secret: &str) -> io::Result<()> {
        let value = self
            .call(&[
                b"AUTH".to_vec(),
                username.as_bytes().to_vec(),
                secret.as_bytes().to_vec(),
            ])
            .await?;
        match value {
            Value::Simple(text) if text == "OK" => Ok(()),
            Value::Error(text) => Err(io::Error::new(io::ErrorKind::PermissionDenied, text)),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected AUTH reply: {other:?}"),
            )),
        }
    }

    pub async fn call(&mut self, args: &[Vec<u8>]) -> io::Result<Value> {
        let request = Value::Array(args.iter().map(|arg| bulk(arg.clone())).collect());
        write_value(&mut self.writer, &request).await?;
        self.writer.flush().await?;
        match self.reader.read_value().await? {
            Some(value) => Ok(value),
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "server closed the connection",
            )),
        }
    }

    pub async fn hello(&mut self) -> io::Result<Value> {
        self.call(&[b"HELLO".to_vec(), b"1".to_vec()]).await
    }

    pub async fn declare(&mut self, queue: &str) -> io::Result<Value> {
        self.call(&[b"DECLARE".to_vec(), queue.as_bytes().to_vec()])
            .await
    }

    pub async fn publish(&mut self, queue: &str, payload: &[u8]) -> io::Result<Value> {
        self.call(&[
            b"PUBLISH".to_vec(),
            queue.as_bytes().to_vec(),
            payload.to_vec(),
        ])
        .await
    }

    pub async fn take(&mut self, queue: &str, max: usize) -> io::Result<Value> {
        self.call(&[
            b"TAKE".to_vec(),
            queue.as_bytes().to_vec(),
            max.to_string().into_bytes(),
        ])
        .await
    }

    pub async fn take_with(
        &mut self,
        queue: &str,
        max: usize,
        lease_ms: u64,
        wait_ms: u64,
    ) -> io::Result<Value> {
        self.call(&[
            b"TAKE".to_vec(),
            queue.as_bytes().to_vec(),
            max.to_string().into_bytes(),
            lease_ms.to_string().into_bytes(),
            wait_ms.to_string().into_bytes(),
        ])
        .await
    }

    pub async fn ack(&mut self, token: &str) -> io::Result<Value> {
        self.call(&[b"ACK".to_vec(), token.as_bytes().to_vec()])
            .await
    }

    pub async fn info(&mut self, queue: &str) -> io::Result<Value> {
        self.call(&[b"INFO".to_vec(), queue.as_bytes().to_vec()])
            .await
    }

    pub async fn purge(&mut self, queue: &str) -> io::Result<Value> {
        self.call(&[b"PURGE".to_vec(), queue.as_bytes().to_vec()])
            .await
    }

    pub async fn delete_queue(&mut self, queue: &str) -> io::Result<Value> {
        self.call(&[b"DELETE".to_vec(), queue.as_bytes().to_vec()])
            .await
    }

    pub async fn list(&mut self) -> io::Result<Value> {
        self.call(&[b"LIST".to_vec()]).await
    }

    pub async fn ping(&mut self) -> io::Result<Value> {
        self.call(&[b"PING".to_vec()]).await
    }
}

fn to_io(error: ante_security::SecurityError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error.to_string())
}
