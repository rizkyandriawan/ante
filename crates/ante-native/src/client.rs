use std::io;

use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

use crate::codec::{bulk, write_value, RespReader, Value};

pub struct NativeClient {
    reader: RespReader<OwnedReadHalf>,
    writer: BufWriter<OwnedWriteHalf>,
}

impl NativeClient {
    pub async fn connect(address: &str) -> io::Result<Self> {
        let stream = TcpStream::connect(address).await?;
        stream.set_nodelay(true)?;
        let (read_half, writer) = stream.into_split();
        Ok(NativeClient {
            reader: RespReader::new(read_half),
            writer: BufWriter::with_capacity(64 * 1024, writer),
        })
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
