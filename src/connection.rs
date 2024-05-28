use crate::frame::Frame;
use bytes::{Buf, BufMut, BytesMut};
use futures::{pin_mut, task::noop_waker_ref};
use std::task::{Context, Poll};
use tokio::{
    io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::TcpStream,
};
use tracing::{instrument, trace};

pub trait AsyncStream:
    AsyncRead + AsyncReadExt + AsyncWrite + AsyncWriteExt + Unpin + Send
{
}
impl<T: AsyncRead + AsyncReadExt + AsyncWrite + AsyncWriteExt + Unpin + Send> AsyncStream for T {}

#[derive(Debug)]
pub struct Connection<S = TcpStream>
where
    S: AsyncStream,
{
    pub stream: S,
    reader_buf: BytesMut,
    writer_buf: BytesMut,
    /// 支持批处理
    count: usize,
}

impl<S: AsyncStream> From<S> for Connection<S> {
    fn from(value: S) -> Self {
        Self {
            stream: value,
            reader_buf: BytesMut::with_capacity(1024),
            writer_buf: BytesMut::with_capacity(1024),
            count: 0,
        }
    }
}

impl<S: AsyncStream> Connection<S> {
    pub const fn count(&self) -> usize {
        self.count
    }

    #[inline]
    pub async fn shutdown(&mut self) -> io::Result<()> {
        self.stream.shutdown().await
    }

    #[inline]
    pub async fn flush(&mut self) -> io::Result<()> {
        self.stream.flush().await
    }

    #[inline]
    pub async fn read_buf<B: BufMut + ?Sized>(&mut self, buf: &mut B) -> io::Result<usize> {
        self.stream.read_buf(buf).await
    }

    #[inline]
    pub async fn write_buf<B: Buf>(&mut self, buf: &mut B) -> io::Result<usize> {
        self.stream.write_buf(buf).await
    }

    #[inline]
    pub async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.stream.write_all(buf).await
    }

    #[inline]
    #[instrument(level = "trace", skip(self), ret, err)]
    pub async fn read_frame(&mut self) -> io::Result<Option<Frame>> {
        Frame::parse_frame(&mut self.stream, &mut self.reader_buf)
            .await
            .map_err(Into::into)
    }

    // 尝试读取多个frame，直到buffer和stream都为空
    #[inline]
    #[instrument(level = "trace", skip(self), ret, err)]
    pub async fn read_frames(&mut self) -> io::Result<Option<Vec<Frame>>> {
        let mut frames = Vec::with_capacity(32);

        loop {
            let frame = match Frame::parse_frame(&mut self.stream, &mut self.reader_buf).await? {
                Some(frame) => frame,
                None => return Ok(None),
            };

            trace!(?frame, "read frame");
            frames.push(frame);
            self.count += 1;

            // 最大支持1024容量的批处理
            if self.count > 1024 {
                return Ok(Some(frames));
            }

            // 尝试继续从stream读取数据到buffer，如果buffer为空则继续读取，如果阻塞则返回结果
            while self.reader_buf.is_empty() {
                let s = &mut self.stream;
                pin_mut!(s);

                let mut cx = Context::from_waker(noop_waker_ref());
                match s.poll_read(&mut cx, &mut ReadBuf::new(&mut self.reader_buf)) {
                    Poll::Ready(Ok(())) => {}
                    // 读取失败，如果客户端已关闭则正常关闭连接，否则返回错误
                    Poll::Ready(Err(e)) => return Err(e),
                    // 已读取所有可读的frame，返回
                    Poll::Pending => return Ok(Some(frames)),
                }
            }
        }
    }

    #[inline]
    #[instrument(level = "trace", skip(self), err)]
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        frame.to_raw_in_buf(&mut self.writer_buf);

        if self.count >= 1 {
            self.count -= 1;
        }

        if self.count == 0 {
            self.stream.write_buf(&mut self.writer_buf).await?;
            self.flush().await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod conn_tests {
    use super::*;
    use crate::util::test_init;

    #[tokio::test]
    async fn test_read_frames() {
        test_init();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (socket, _addr) = listener.accept().await.unwrap();
            let mut server_conn = Connection::from(socket);

            // 测试简单字符串
            let _ = server_conn
                .write_frame(&Frame::new_simple_borrowed("OK"))
                .await;
            // 测试错误消息
            let _ = server_conn
                .write_frame(&Frame::new_error_borrowed("Error message"))
                .await;
            // 测试整数
            let _ = server_conn.write_frame(&Frame::Integer(1000)).await;
            // 测试大容量字符串
            let _ = server_conn
                .write_frame(&Frame::new_bulk_by_copying(b"foobar"))
                .await;
            // 测试空字符串
            let _ = server_conn
                .write_frame(&Frame::new_bulk_by_copying(b""))
                .await;
            // 测试空值
            let _ = server_conn.write_frame(&Frame::Null).await;
            // 测试数组
            let _ = server_conn
                .write_frame(&Frame::Array(vec![
                    Frame::new_simple_borrowed("simple"),
                    Frame::new_error_borrowed("error"),
                    Frame::Integer(1000),
                    Frame::new_bulk_by_copying(b"bulk"),
                    Frame::Null,
                    Frame::Array(vec![
                        Frame::new_bulk_by_copying(b"foo"),
                        Frame::new_bulk_by_copying(b"bar"),
                    ]),
                ]))
                .await;
            // 测试空数组
            let _ = server_conn.write_frame(&Frame::Array(vec![])).await;

            tx.send(()).unwrap();
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let mut conn = Connection::from(stream);

        rx.await.unwrap();

        let mut res = vec![];
        while let Some(frames) = conn.read_frames().await.unwrap() {
            res.extend(frames);
        }

        assert_eq!(
            res,
            vec![
                Frame::new_simple_borrowed("OK"),
                Frame::new_error_borrowed("Error message"),
                Frame::Integer(1000),
                Frame::new_bulk_by_copying(b"foobar"),
                Frame::new_bulk_by_copying(b""),
                Frame::Null,
                Frame::Array(vec![
                    Frame::new_simple_borrowed("simple"),
                    Frame::new_error_borrowed("error"),
                    Frame::Integer(1000),
                    Frame::new_bulk_by_copying(b"bulk"),
                    Frame::Null,
                    Frame::Array(vec![
                        Frame::new_bulk_by_copying(b"foo"),
                        Frame::new_bulk_by_copying(b"bar"),
                    ]),
                ]),
                Frame::Array(vec![]),
            ]
        );
    }
}
