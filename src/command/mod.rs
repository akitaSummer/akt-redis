pub mod unknow;
use redis_protocol::resp2::prelude::*;

use crate::parse::Parse;
use crate::types::Result;

use unknow::Unknown;

pub enum Command {
    Get,
    Unknown(Unknown),
}

impl Command {
    pub fn from_frame(frame: Frame) -> Result<Command> {
        let mut parse = Parse::new(frame)?;

        // 读取命令开头
        let command_name = parse.next_string()?.to_lowercase();

        let command = match &command_name[..] {
            _ => {
                return Ok(Command::Unknown(Unknown::new(command_name)));
            }
        };

        parse.finish()?;
        Ok(command)
    }
}
