use redis_protocol::resp2::prelude::*;

use crate::types::Result;
use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // 0 时为结束流
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                    // 缓冲区不为空时则对方关闭了流
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    fn parse_frame(&mut self) -> Result<Option<Frame>> {
        let mut buf = &self.buffer[..];
        // 正常情况下应自己解析，偷懒用了库
        match decode(&buf) {
            Ok(Some((f, _))) => {
                self.buffer.clear();
                Ok(Some(f))
            }
            Ok(None) => Ok(None),
            Err(e) => panic!("Error parsing bytes: {:?}", e),
        }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Array(val) => {
                // 数组类型前缀是 *
                self.stream.write_u8(b'*').await?;

                // 数据传入长度
                self.write_decimal(val.len() as i64).await?;

                // 每一项进行解析
                for entry in &**val {
                    self.write_value(entry).await?;
                }
            }
            // 非数组直接解析
            _ => self.write_value(frame).await?,
        }

        // 剩余部分调用冲洗
        self.stream.flush().await
    }

    // 解析
    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::SimpleString(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(val) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::BulkString(val) => {
                let len = val.len();

                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as i64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            // 数组不做
            Frame::Array(_val) => unreachable!(),
        }

        Ok(())
    }

    /// 写数据进入流
    async fn write_decimal(&mut self, val: i64) -> io::Result<()> {
        use std::io::Write;

        // 将值转换为字符串
        let mut buf = [0u8; 20];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}
