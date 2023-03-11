use bytes::Buf;
use bytes::BytesMut;
use mini_redis::Frame;
use mini_redis::Result;
use tokio::io::AsyncReadExt;
use tokio::io::{self, AsyncWriteExt};
use tokio::net::TcpStram;

pub struct Connection {
    stream: TcpStream,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Connection {
        Connection {
            stream,
            // allocate the buffer with 4kb of capacity
            buffer: BytesMut::with_capactiy(4096),
        }
    }

    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            // Attempt to parse a frame from the buffered data. If
            // enough data has been buffered, the frame s
            // returned.
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // there is not enough buffered data to read a frame.
            // attempt to read more data from the socket.
            //
            // on success, the number of bytes is retunred 0
            // indicates "end of stream"
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // the remote closed the connection. for this to be
                // a clean shutdown, theer should be no data in the
                // read buffer. If there is, this means that the
                // peer closed the socket while sending the frame
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(val) => {
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
            Frame::Bulk(val) => {
                let len = val.len();

                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Array(_val) => unimplemented!(),
        }

        self.stream.flush().await;

        Ok(())
    }
}
