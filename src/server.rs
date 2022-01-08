use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};

use crate::command::Command;
use crate::connection::Connection;
use crate::db::{Db, DbHolder};
use crate::types::Result;
use tracing::{debug, error, info, instrument};

#[derive(Debug)]
struct Listener {
    db_holder: DbHolder,
    listener: TcpListener,
    // 限制数量
    limit_connections: Arc<Semaphore>,
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
}

impl Handler {
    async fn run(&mut self) -> Result<()> {
        loop {
            let maybe_frame = self.connection.read_frame().await?;

            // None表示结束
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            let cmd = Command::from_frame(frame)?;

            debug!(?cmd);
        }

        Ok(())
    }
}

impl Drop for Handler {
    fn drop(&mut self) {
        self.limit_connections.add_permits(1);
    }
}

pub fn run(listener: TcpListener) {
    let mut server = Listener {
        listener,
        db_holder: DbHolder::new(),
        limit_connections: Arc::new(Semaphore::new(500)),
    };

    tokio::select! {
        res = server.run() => {
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
    }
}
