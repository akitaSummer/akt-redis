use redis_protocol::resp2::prelude::*;

use bytes::Bytes;
use tracing::{debug, instrument};

use crate::connection::Connection;
use crate::db::Db;
use crate::parse::Parse;
use crate::types::Result;

#[derive(Debug)]
pub struct Get {
    key: String,
}

impl Get {
    pub fn new<T: ToString>(key: T) -> Get {
        Get {
            key: key.to_string(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn parse_frames(parse: &mut Parse) -> Result<Get> {
        let key = parse.next_string()?;
        Ok(Get {
            key: key.to_string(),
        })
    }

    #[instrument(skip(self, db, dst))]
    pub async fn apply(&self, db: &Db, dst: &mut Connection) -> Result<()> {
        let response = if let Some(value) = db.get(&self.key) {
            Frame::BulkString(value.to_vec())
        } else {
            Frame::Null
        };

        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub fn into_frame(self) -> Frame {
        let v = &mut vec![Frame::BulkString(Bytes::from("get".as_bytes()).to_vec())];
        v.push(Frame::BulkString(
            Bytes::from(self.key.into_bytes()).to_vec(),
        ));
        Frame::Array(v.to_vec())
    }
}
