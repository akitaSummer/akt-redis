use redis_protocol::resp2::prelude::*;

use crate::connection::Connection;
use crate::types::Result;

use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Unknown {
    command_name: String,
}

impl Unknown {
    pub fn new<T: ToString>(key: T) -> Unknown {
        Unknown {
            command_name: key.to_string(),
        }
    }
    pub fn get_name(&self) -> &str {
        &self.command_name
    }

    // 返回消息表示不认识命令
    #[instrument(skip(self, dst))]
    pub async fn apply(&self, dst: &mut Connection) -> Result<()> {
        let response = Frame::Error(format!("ERR unknown command '{}'", self.command_name));

        debug!(?response);

        dst.write_frame(&response).await?;
        Ok(())
    }
}
