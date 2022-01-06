use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, Notify};
use tokio::time::{self, Duration, Instant};

#[derive(Debug)]
pub struct DbHolder {
    db: Db,
}

impl DbHolder {
    pub fn new() -> DbHolder {
        DbHolder { db: Db::new() }
    }

    pub fn db(&self) -> Db {
        // Arc的克隆只是指针复制，底层实例只有一个
        self.db.clone()
    }
}

impl Drop for DbHolder {
    fn drop(&mut self) {
        self.db.shutdown_purge_task();
    }
}

#[derive(Debug, Clone)]
pub struct Db {
    shared: Arc<Shared>,
}

impl Db {
    pub fn new() -> Db {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                expirations: BTreeMap::new(),
                next_id: 0,
                shutdown: false,
            }),
            background_task: Notify::new(),
        });

        // 启动过期回收线程
        tokio::spawn(purge_expired_tasks(shared.clone()));

        Db { shared }
    }

    // 通知清除后台
    fn shutdown_purge_task(&self) {
        let mut state = self.shared.state.lock().unwrap();

        state.shutdown = true;

        drop(state);
        self.shared.background_task.notify_one();
    }
}

#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
    // 检查过期值或关闭信号
    background_task: Notify,
}

impl Shared {
    // 清除过期键
    fn purge_expired_keys(&self) -> Option<Instant> {
        // 获取state原始值
        let mut state = &mut *(self.state.lock().unwrap());
        if state.shutdown {
            return None;
        }

        // 当前时间
        let now = Instant::now();

        while let Some(((expired_time, id), key)) = state.expirations.iter().next() {
            if *expired_time > now {
                return Some(*expired_time);
            }

            state.entries.remove(key);
            state.expirations.remove(&(*expired_time, *id));
        }

        None
    }

    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }
}

#[derive(Debug)]
struct State {
    // key val储存
    entries: HashMap<String, Entry>,

    // 根据到期时间维护BTreeMap
    expirations: BTreeMap<(Instant, u64), String>,

    // 下一个到期id
    next_id: u64,

    // 后台是否在退出
    shutdown: bool,
}

#[derive(Debug)]
struct Entry {
    id: u64,
    data: Bytes,
    // 删除时间
    expires_at: Option<Instant>,
}

// 后台用于清除过期任务的线程
async fn purge_expired_tasks(shared: Arc<Shared>) {
    while !shared.is_shutdown() {
        // 清除过期键
        if let Some(next_time) = shared.purge_expired_keys() {
            // 等待下一个过期键
            tokio::select! {
                _ = time::sleep_until(next_time) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            // 没有过期的密钥了
            shared.background_task.notified().await;
        }
    }
}
