use atoi::atoi;
use bytes::Bytes;
use redis_protocol::resp2::prelude::*;
use std::{fmt, str, vec};

use crate::types::Error;

#[derive(Debug)]
pub struct Parse {
    frames: vec::IntoIter<Frame>,
}

#[derive(Debug)]
pub enum ParseError {
    EndOfStream,
    Other(Error),
}

impl From<String> for ParseError {
    fn from(src: String) -> ParseError {
        ParseError::Other(src.into())
    }
}

impl From<&str> for ParseError {
    fn from(src: &str) -> ParseError {
        src.to_string().into()
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::EndOfStream => "protocol error; unexpected end of stream".fmt(f),
            ParseError::Other(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for ParseError {}

impl Parse {
    pub fn new(frame: Frame) -> Result<Parse, ParseError> {
        let array = match frame {
            Frame::Array(array) => array,
            frame => return Err(format!("protocol error; expected array, got {:?}", frame).into()),
        };

        Ok(Parse {
            frames: array.into_iter(),
        })
    }

    fn next(&mut self) -> Result<Frame, ParseError> {
        self.frames.next().ok_or(ParseError::EndOfStream)
    }

    // 处理字符串
    pub fn next_string(&mut self) -> Result<String, ParseError> {
        match self.next()? {
            Frame::SimpleString(s) => Ok(s),
            Frame::BulkString(data) => str::from_utf8(&data[..])
                .map(|s| s.to_string())
                .map_err(|_| "protocol error; invalid string".into()),
            frame => Err(format!(
                "protocol error; expected simple frame or bulk frame, got {:?}",
                frame
            )
            .into()),
        }
    }

    // 处理bytes
    pub fn next_bytes(&mut self) -> Result<Bytes, ParseError> {
        match self.next()? {
            Frame::SimpleString(s) => Ok(Bytes::from(s.into_bytes())),
            Frame::BulkString(data) => Ok(Bytes::from(data)),
            frame => Err(format!(
                "protocol error; expected simple frame or bulk frame, got {:?}",
                frame
            )
            .into()),
        }
    }

    // int
    pub fn next_int(&mut self) -> Result<i64, ParseError> {
        const MSG: &str = "protocol error; invalid number";

        match self.next()? {
            Frame::Integer(v) => Ok(v),
            Frame::SimpleString(data) => atoi::<i64>(data.as_bytes()).ok_or_else(|| MSG.into()),
            Frame::BulkString(data) => atoi::<i64>(&data).ok_or_else(|| MSG.into()),
            frame => Err(format!("protocol error; expected int frame but got {:?}", frame).into()),
        }
    }

    // 解析完毕
    pub fn finish(&mut self) -> Result<(), ParseError> {
        if self.frames.next().is_none() {
            Ok(())
        } else {
            Err("protocol error; expected end of frame, but there was more".into())
        }
    }
}
