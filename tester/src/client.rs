use std::net::SocketAddr;

use protocol::{Request, Response};
use rkyv::rancor::Error as RkyvError;
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
};

use crate::TesterError;

pub enum ReadResult {
    Disconnected,
    Response(Response),
}

pub struct BinaryClient {
    reader: BufReader<OwnedReadHalf>,
    writer: BufWriter<OwnedWriteHalf>,
}

impl BinaryClient {
    pub async fn connect(addr: SocketAddr) -> Result<Self, TesterError> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let (reader, writer) = stream.into_split();
        Ok(Self {
            reader: BufReader::new(reader),
            writer: BufWriter::new(writer),
        })
    }

    pub async fn send_request(&mut self, request: &Request) -> Result<(), TesterError> {
        let bytes = rkyv::to_bytes::<RkyvError>(request)?;
        self.writer.write_u32(bytes.len() as u32).await?;
        self.writer.write_all(&bytes).await?;
        self.writer.flush().await?;
        Ok(())
    }

    pub async fn read_response(&mut self) -> Result<ReadResult, TesterError> {
        let length = match self.reader.read_u32().await {
            Ok(len) => len,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(ReadResult::Disconnected);
            }
            Err(e) => return Err(TesterError::Io(e)),
        };

        let mut buffer = vec![0u8; length as usize];
        match self.reader.read_exact(&mut buffer).await {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(ReadResult::Disconnected);
            }
            Err(e) => return Err(TesterError::Io(e)),
        }

        let archived = rkyv::access::<protocol::ArchivedResponse, RkyvError>(&buffer[..])
            .map_err(TesterError::BinaryDeserialization)?;
        let response = rkyv::deserialize::<Response, RkyvError>(archived)
            .map_err(TesterError::BinaryDeserialization)?;

        Ok(ReadResult::Response(response))
    }

    pub async fn execute_and_wait(&mut self, request: Request) -> Result<(), TesterError> {
        self.send_request(&request).await?;

        loop {
            match self.read_response().await? {
                ReadResult::Disconnected => return Err(TesterError::Disconnected),
                ReadResult::Response(resp) => {
                    if let Response::Error { message, .. } = &resp {
                        return Err(TesterError::ServerError {
                            message: message.clone(),
                        });
                    }

                    if is_final_response(&resp) {
                        return Ok(());
                    }
                }
            }
        }
    }
}

fn is_final_response(response: &Response) -> bool {
    matches!(
        response,
        Response::QueryCompleted
            | Response::Connected { .. }
            | Response::DatabaseCreated { .. }
            | Response::DatabaseDeleted { .. }
            | Response::DatabasesListed { .. }
            | Response::Error { .. }
    )
}
