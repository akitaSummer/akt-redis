use bytes::Bytes;
use redis_protocol::resp2::prelude::*;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::task::JoinHandle;
use tracing::{debug, instrument};

use crate::command::get::Get;
use crate::command::set::Set;
use crate::connection::Connection;
use crate::server;
use crate::types::Result;

pub struct Client {
    connection: Connection,
}

pub async fn connect<T: ToSocketAddrs>(addr: T) -> Result<Client> {
    let socket = TcpStream::connect(addr).await?;

    let connection = Connection::new(socket);

    Ok(Client { connection })
}

impl Client {
    #[instrument(skip(self))]
    pub async fn get(&mut self, key: &str) -> Result<Option<Bytes>> {
        let frame = Get::new(key).into_frame();

        debug!(request = ?frame);

        self.connection.write_frame(&frame).await;

        match self.read_response().await? {
            Frame::SimpleString(value) => Ok(Some(value.into())),
            Frame::BulkString(value) => Ok(Some(value.into())),
            Frame::Null => Ok(None),
            frame => Err(format!("unexpected frame: {}", frame.as_str().unwrap()).into()),
        }
    }

    #[instrument(skip(self))]
    pub async fn set(&mut self, key: &str, value: Bytes) -> Result<()> {
        self.set_cmd(Set::new(key, value, None)).await
    }

    #[instrument(skip(self))]
    pub async fn set_expires(
        &mut self,
        key: &str,
        value: Bytes,
        expiration: Duration,
    ) -> Result<()> {
        self.set_cmd(Set::new(key, value, Some(expiration))).await
    }

    async fn set_cmd(&mut self, cmd: Set) -> Result<()> {
        let frame = cmd.into_frame();

        debug!(request = ?frame);

        self.connection.write_frame(&frame).await?;

        match self.read_response().await? {
            Frame::SimpleString(response) if response == "OK" => Ok(()),
            frame => Err(format!("unexpected frame: {}", frame.as_str().unwrap()).into()),
        }
    }

    async fn read_response(&mut self) -> Result<Frame> {
        let response = self.connection.read_frame().await?;

        debug!(?response);

        match response {
            Some(Frame::Error(msg)) => Err(msg.into()),
            Some(frame) => Ok(frame),
            None => {
                let err = Error::new(ErrorKind::ConnectionReset, "connection reset by server");

                Err(err.into())
            }
        }
    }
}

#[tokio::test]
async fn key_value_get_set() {
    let (addr, _) = start_server().await;

    let mut client = connect(addr).await.unwrap();
    client.set("hello", "world".into()).await.unwrap();

    let value = client.get("hello").await.unwrap().unwrap();
    assert_eq!(b"world", &value[..])
}

async fn start_server() -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = tokio::spawn(async move { server::run(listener, tokio::signal::ctrl_c()).await });

    (addr, handle)
}
