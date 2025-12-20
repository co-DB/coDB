use protocol::{Request, Response};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use crate::client_handler::ClientError;

pub(crate) enum ReadResult {
    Request(Request),
    Disconnected,
    Empty,
}

pub(crate) trait ProtocolHandler {
    async fn read_request(&mut self) -> Result<ReadResult, ClientError>;
    async fn send_response(&mut self, response: Response) -> Result<(), ClientError>;
}

pub(crate) struct BinaryProtocolHandler {
    reader: BufReader<OwnedReadHalf>,
    writer: BufWriter<OwnedWriteHalf>,
}

impl From<TcpStream> for BinaryProtocolHandler {
    fn from(value: TcpStream) -> Self {
        let (reader, writer) = value.into_split();
        BinaryProtocolHandler {
            reader: BufReader::new(reader),
            writer: BufWriter::new(writer),
        }
    }
}

impl ProtocolHandler for BinaryProtocolHandler {
    async fn read_request(&mut self) -> Result<ReadResult, ClientError> {
        todo!()
    }

    async fn send_response(&mut self, response: Response) -> Result<(), ClientError> {
        todo!()
    }
}

pub(crate) struct TextProtocolHandler {
    reader: BufReader<OwnedReadHalf>,
    writer: BufWriter<OwnedWriteHalf>,
}

impl From<TcpStream> for TextProtocolHandler {
    fn from(value: TcpStream) -> Self {
        let (reader, writer) = value.into_split();
        TextProtocolHandler {
            reader: BufReader::new(reader),
            writer: BufWriter::new(writer),
        }
    }
}

impl ProtocolHandler for TextProtocolHandler {
    async fn read_request(&mut self) -> Result<ReadResult, ClientError> {
        let mut buffer = String::new();

        match self.reader.read_line(&mut buffer).await {
            Ok(0) => Ok(ReadResult::Disconnected),
            Ok(_) => {
                let trimmed = buffer.trim();
                if trimmed.is_empty() {
                    Ok(ReadResult::Empty)
                } else {
                    let request = serde_json::from_str(trimmed)?;
                    Ok(ReadResult::Request(request))
                }
            }
            Err(e) => Err(ClientError::IoError(e)),
        }
    }

    async fn send_response(&mut self, response: Response) -> Result<(), ClientError> {
        let json = serde_json::to_string(&response)?;
        self.writer.write_all(json.as_bytes()).await?;
        self.writer.write_all(b"\n").await?;
        self.writer.flush().await?;
        Ok(())
    }
}
