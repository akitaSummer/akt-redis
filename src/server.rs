use tokio::net::TcpListener;

use crate::db::DbHolder;

#[derive(Debug)]
struct Listener {
    db_holder: DbHolder,
    listener: TcpListener,
}

pub fn run(listener: TcpListener) {
    let mut server = Listener {
        listener,
        db_holder: DbHolder::new(),
    };
}
