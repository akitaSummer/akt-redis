mod client;
mod command;
mod connection;
mod db;
mod parse;
mod server;
mod shutdown;
mod types;

use std::io;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::spawn;

#[tokio::main]
async fn main() -> io::Result<()> {
    tracing_subscriber::fmt::try_init();
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    let addr = listener.local_addr().unwrap();

    let handle = spawn(async move {
        server::run(listener, signal::ctrl_c()).await;
    });

    let mut client = client::connect(addr).await.unwrap();

    client.set("hello", "world".into()).await.unwrap();

    let value = client.get("hello").await.unwrap().unwrap();
    assert_eq!(b"world", &value[..]);

    Ok(())
}
