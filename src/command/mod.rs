pub mod get;
pub mod set;
pub mod unknow;

use redis_protocol::resp2::prelude::*;

use crate::connection::Connection;
use crate::db::Db;
use crate::parse::Parse;
use crate::types::Result;

use get::Get;
use set::Set;
use unknow::Unknown;

#[derive(Debug)]
pub enum Command {
    Get(Get),
    Unknown(Unknown),
    Set(Set),
}

impl Command {
    pub fn from_frame(frame: Frame) -> Result<Command> {
        let mut parse = Parse::new(frame)?;

        // 读取命令开头
        let command_name = parse.next_string()?.to_lowercase();

        let command = match &command_name[..] {
            "get" => Command::Get(Get::parse_frames(&mut parse)?),
            "set" => Command::Set(Set::parse_frames(&mut parse)?),
            _ => {
                return Ok(Command::Unknown(Unknown::new(command_name)));
            }
        };

        parse.finish()?;
        Ok(command)
    }

    pub async fn apply(&self, db: &Db, dst: &mut Connection) -> Result<()> {
        match self {
            Command::Get(cmd) => cmd.apply(db, dst).await,
            Command::Set(cmd) => cmd.apply(db, dst).await,
            Command::Unknown(cmd) => cmd.apply(dst).await,
        }
    }
}
