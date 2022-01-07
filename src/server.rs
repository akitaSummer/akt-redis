use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};

use crate::connection::Connection;
use crate::db::{Db, DbHolder};
use crate::types::Result;

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

            let socket = self.accept().await?;
        }
    }

    // 获取tcp流
    async fn accept(&mut self) -> Result<TcpStream> {
        // 未找到时的休眠时间
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
        loop {}
    }
}

pub fn run(listener: TcpListener) {
    let mut server = Listener {
        listener,
        db_holder: DbHolder::new(),
        limit_connections: Arc::new(Semaphore::new(500)),
    };
}
