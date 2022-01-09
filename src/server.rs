use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};

use crate::command::Command;
use crate::connection::Connection;
use crate::db::{Db, DbHolder};
use crate::shutdown::Shutdown;
use crate::types::Result;
use tracing::{debug, error, info};

#[derive(Debug)]
struct Listener {
    db_holder: DbHolder,
    listener: TcpListener,
    // 限制数量
    limit_connections: Arc<Semaphore>,
    // 广播终止信号
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

impl Listener {
    async fn run(&mut self) -> Result<()> {
        loop {
            // 当未达限制数量时，获得可执行信息
            self.limit_connections.acquire().await.unwrap().forget();

            // 得到句柄
            let socket = self.accept().await?;

            let mut handler = Handler {
                db: self.db_holder.db(),
                connection: Connection::new(socket),
                limit_connections: self.limit_connections.clone(),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    error!(cause = ?err, "connection error");
                }
            });
        }
    }

    // 获取tcp流的句柄
    async fn accept(&mut self) -> Result<TcpStream> {
        // 获取失败的重试时间
        let mut try_time = 1;

        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if try_time > 64 {
                        return Err(err.into());
                    }
                }
            }

            time::sleep(Duration::from_secs(try_time)).await;

            try_time *= 2
        }
    }
}

#[derive(Debug)]
struct Handler {
    db: Db,
    connection: Connection,
    limit_connections: Arc<Semaphore>,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
}

impl Handler {
    async fn run(&mut self) -> Result<()> {
        while !self.shutdown.is_shutdown() {
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    return Ok(());
                }
            };

            // None表示结束
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            let cmd = Command::from_frame(frame)?;

            debug!(?cmd);

            cmd.apply(&self.db, &mut self.connection).await?;
        }

        Ok(())
    }
}

impl Drop for Handler {
    fn drop(&mut self) {
        self.limit_connections.add_permits(1);
    }
}

pub async fn run<T: Future>(listener: TcpListener, shutdown: T) {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
    let mut server = Listener {
        listener,
        db_holder: DbHolder::new(),
        limit_connections: Arc::new(Semaphore::new(2)),
        notify_shutdown,
        shutdown_complete_tx,
        shutdown_complete_rx,
    };

    tokio::select! {
        res = server.run() => {
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            info!("shutting down");
        }
    }

    drop(server.notify_shutdown);
    drop(server.shutdown_complete_tx);
    let _ = server.shutdown_complete_rx.recv().await;
}

#[tokio::test]
async fn key_value_get_set() {
    let addr = start_server().await;

    let mut stream = TcpStream::connect(addr).await.unwrap();

    stream
        .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n")
        .await
        .unwrap();

    let mut response = [0; 5];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"$-1\r\n", &response);

    stream
        .write_all(b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
        .await
        .unwrap();

    let mut response = [0; 5];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"+OK\r\n", &response);

    stream
        .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n")
        .await
        .unwrap();

    stream.shutdown().await.unwrap();

    let mut response = [0; 11];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"$5\r\nworld\r\n", &response);

    assert_eq!(0, stream.read(&mut response).await.unwrap());
}

#[tokio::test]
async fn key_value_timeout() {
    tokio::time::pause();

    let addr = start_server().await;

    let mut stream = TcpStream::connect(addr).await.unwrap();

    stream
        .write_all(
            b"*5\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n\
                     +EX\r\n:1\r\n",
        )
        .await
        .unwrap();

    let mut response = [0; 5];

    stream.read_exact(&mut response).await.unwrap();

    assert_eq!(b"+OK\r\n", &response);

    stream
        .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n")
        .await
        .unwrap();

    let mut response = [0; 11];

    stream.read_exact(&mut response).await.unwrap();

    assert_eq!(b"$5\r\nworld\r\n", &response);

    time::advance(Duration::from_secs(1)).await;

    stream
        .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n")
        .await
        .unwrap();

    let mut response = [0; 5];

    stream.read_exact(&mut response).await.unwrap();

    assert_eq!(b"$-1\r\n", &response);
}

#[tokio::test]
async fn send_error_unknown_command() {
    let addr = start_server().await;

    let mut stream = TcpStream::connect(addr).await.unwrap();

    stream
        .write_all(b"*2\r\n$3\r\nFOO\r\n$5\r\nhello\r\n")
        .await
        .unwrap();

    let mut response = [0; 28];

    stream.read_exact(&mut response).await.unwrap();

    assert_eq!(b"-ERR unknown command \'foo\'\r\n", &response);
}

async fn start_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move { run(listener, tokio::signal::ctrl_c()).await });

    addr
}
