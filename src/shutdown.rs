use tokio::sync::broadcast;

#[derive(Debug)]
pub(crate) struct Shutdown {
    shutdown: bool,
    // 监听关闭信息
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    pub fn new(notify: broadcast::Receiver<()>) -> Shutdown {
        Shutdown {
            shutdown: false,
            notify,
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.shutdown
    }

    pub async fn recv(&mut self) {
        if self.shutdown {
            return;
        }

        // 接受关闭信号
        let _ = self.notify.recv().await;

        self.shutdown = true;
    }
}
