use redis_protocol::resp2::prelude::*;

use bytes::Bytes;
use std::time::Duration;
use tracing::{debug, instrument};

use crate::connection::Connection;
use crate::db::Db;
use crate::parse::Parse;
use crate::types::Result;

#[derive(Debug)]
pub struct Set {
    key: String,
    value: Bytes,
    expire: Option<Duration>,
}

impl Set {
    pub fn new(key: impl ToString, value: Bytes, expire: Option<Duration>) -> Set {
        Set {
            key: key.to_string(),
            value,
            expire,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &Bytes {
        &self.value
    }

    pub fn expire(&self) -> Option<Duration> {
        self.expire
    }

    pub fn parse_frames(parse: &mut Parse) -> Result<Set> {
        let key = parse.next_string()?;
        let value = parse.next_bytes()?;
        let mut expire = None;

        match parse.next_string() {
            Ok(s) if s.to_uppercase() == "EX" => {
                let secs = parse.next_int()?;
                expire = Some(Duration::from_secs(secs as u64));
            }
            Ok(s) if s.to_uppercase() == "PX" => {
                let ms = parse.next_int()?;
                expire = Some(Duration::from_millis(ms as u64));
            }
            Ok(_) => return Err("currently `SET` only supports the expiration option".into()),
            Err(EndOfStream) => {}
            Err(err) => return Err(err.into()),
        }
        Ok(Set { key, value, expire })
    }

    #[instrument(skip(self, db, dst))]
    pub async fn apply(&self, db: &Db, dst: &mut Connection) -> Result<()> {
        db.set(self.key.to_string(), self.value.clone(), self.expire);
        let response = Frame::SimpleString("OK".to_string());
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub fn into_frame(self) -> Frame {
        let v = &mut vec![Frame::BulkString(Bytes::from("set".as_bytes()).to_vec())];
        v.push(Frame::BulkString(
            Bytes::from(self.key.into_bytes()).to_vec(),
        ));
        v.push(Frame::BulkString(Bytes::from(self.value).to_vec()));
        if let Some(ms) = self.expire {
            v.push(Frame::BulkString(Bytes::from("px".as_bytes()).to_vec()));
            v.push(Frame::Integer(ms.as_millis() as i64));
        }
        Frame::Array(v.to_vec())
    }
}
