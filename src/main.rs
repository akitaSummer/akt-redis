mod connection;
mod db;
mod server;
mod types;

use std::io;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    server::run(listener);

    Ok(())
}
