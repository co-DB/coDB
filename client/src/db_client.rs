use std::io::ErrorKind;

use log::info;
use protocol::{ArchivedResponse, Request, Response};
use rkyv::rancor::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
};

use crate::ClientError;

pub enum ReadResult {
    Disconnected,
    Response(Response),
}

pub trait ProtocolHandler {
    async fn send_request(&mut self, request: Request) -> Result<(), ClientError>;
    async fn read_response(&mut self) -> Result<ReadResult, ClientError>;
}

pub struct TextProtocolHandler {
    reader: BufReader<OwnedReadHalf>,
    writer: BufWriter<OwnedWriteHalf>,
}

impl ProtocolHandler for TextProtocolHandler {
    async fn send_request(&mut self, request: Request) -> Result<(), ClientError> {
        let json = serde_json::to_string(&request)?;
        self.writer.write_all(json.as_bytes()).await?;
        self.writer.write_all(b"\n").await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn read_response(&mut self) -> Result<ReadResult, ClientError> {
        let mut buffer = String::new();

        match self.reader.read_line(&mut buffer).await {
            Ok(0) => Ok(ReadResult::Disconnected),
            Ok(_) => {
                let trimmed = buffer.trim();
                let response = serde_json::from_str(trimmed)?;
                Ok(ReadResult::Response(response))
            }
            Err(e) => Err(ClientError::IoError(e)),
        }
    }
}

impl From<TcpStream> for TextProtocolHandler {
    fn from(value: TcpStream) -> Self {
        let (reader, writer) = value.into_split();
        Self {
            reader: BufReader::new(reader),
            writer: BufWriter::new(writer),
        }
    }
}

pub struct BinaryProtocolHandler {
    reader: BufReader<OwnedReadHalf>,
    writer: BufWriter<OwnedWriteHalf>,
}

impl ProtocolHandler for BinaryProtocolHandler {
    async fn send_request(&mut self, request: Request) -> Result<(), ClientError> {
        let bytes =
            rkyv::to_bytes::<Error>(&request).map_err(ClientError::BinarySerializationError)?;
        self.writer.write_u32(bytes.len() as u32).await?;
        self.writer.write_all(&bytes).await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn read_response(&mut self) -> Result<ReadResult, ClientError> {
        let length = match self.reader.read_u32().await {
            Ok(len) => len,
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                return Ok(ReadResult::Disconnected);
            }
            Err(e) => {
                return Err(ClientError::IoError(e));
            }
        };

        let mut buffer = vec![0u8; length as _];
        match self.reader.read_exact(&mut buffer).await {
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                return Ok(ReadResult::Disconnected);
            }
            Err(e) => {
                return Err(ClientError::IoError(e));
            }
        }
        let archived_response = rkyv::access::<ArchivedResponse, Error>(&buffer[..])
            .map_err(ClientError::BinaryDeserializationError)?;
        let response = rkyv::deserialize::<Response, Error>(archived_response)
            .map_err(ClientError::BinaryDeserializationError)?;

        Ok(ReadResult::Response(response))
    }
}

impl From<TcpStream> for BinaryProtocolHandler {
    fn from(value: TcpStream) -> Self {
        let (reader, writer) = value.into_split();
        Self {
            reader: BufReader::new(reader),
            writer: BufWriter::new(writer),
        }
    }
}

pub struct DbClient<P>
where
    P: ProtocolHandler,
{
    protocol: P,
}

impl<P> DbClient<P>
where
    P: ProtocolHandler,
{
    pub fn new(p: P) -> Self {
        DbClient { protocol: p }
    }

    pub async fn send_request(&mut self, request: Request) -> Result<(), ClientError> {
        info!("Sending request: {:?}", request);

        self.protocol.send_request(request).await
    }

    pub async fn read_response(&mut self) -> Result<ReadResult, ClientError> {
        self.protocol.read_response().await
    }
}
