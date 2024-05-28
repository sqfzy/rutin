use crate::frame::Frame;
use bytes::BytesMut;
use futures::{pin_mut, task::noop_waker_ref};
use std::task::{Context, Poll};
use tokio::{
    io::{self, AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf},
    net::TcpStream,
};
use tokio_rustls::server::TlsStream;
use tracing::instrument;

#[derive(Debug)]
pub struct Connection {
    inner: ConnectionInner,
    /// stream读取的数据会先写入buf，然后再从buf中读取数据。
    /// buf读取数据的行为：使用游标从buf中读取数据时，如果游标到达末尾，则从stream中读取数据到buf，再次尝试。
    /// 从buf中读数据时，有两种错误情况：
    /// 1. 读取时发现内容错误，证明frame格式错误。
    /// 2. 读取时发现游标到达末尾，但stream中也没有数据，证明frame不完整
    reader_buf: BytesMut,
    writer_buf: BytesMut,

    /// 支持批处理
    pub count: usize,
}

#[derive(Debug)]
pub enum ConnectionInner {
    TlsStream(TlsStream<TcpStream>),
    TcpStream(TcpStream),
}

impl Connection {
    pub fn new(socket: TcpStream) -> Self {
        Self {
            inner: ConnectionInner::TcpStream(socket),
            reader_buf: BytesMut::with_capacity(1024),
            writer_buf: BytesMut::with_capacity(1024),
            count: 0,
            // authed: AtomicCell::new(false),
        }
    }

    pub fn new_with_tls(socket: TlsStream<TcpStream>) -> Self {
        Self {
            inner: ConnectionInner::TlsStream(socket),
            reader_buf: BytesMut::with_capacity(1024),
            writer_buf: BytesMut::with_capacity(1024),
            count: 0,
            // authed: AtomicCell::new(false),
        }
    }

    #[inline]
    pub async fn shutdown(&mut self) -> io::Result<()> {
        match self.inner {
            ConnectionInner::TcpStream(ref mut stream) => stream.shutdown().await,
            ConnectionInner::TlsStream(ref mut stream) => stream.shutdown().await,
        }
    }

    #[inline]
    pub async fn flush(&mut self) -> io::Result<()> {
        match self.inner {
            ConnectionInner::TcpStream(ref mut stream) => stream.flush().await,
            ConnectionInner::TlsStream(ref mut stream) => stream.flush().await,
        }
    }

    // 尝试读取多个frame，直到buffer和stream都为空
    #[inline]
    #[instrument(level = "trace", skip(self), ret, err)]
    pub async fn read_frames<'a>(&mut self) -> anyhow::Result<Option<Vec<Frame>>> {
        match &mut self.inner {
            ConnectionInner::TcpStream(stream) => {
                // 如果buffer为空，则尝试从stream中读入数据到buffer
                if self.reader_buf.is_empty() && stream.read_buf(&mut self.reader_buf).await? == 0 {
                    return Ok(None);
                }

                let mut frames = Vec::with_capacity(32);
                loop {
                    frames.push(Frame::parse_frame(stream, &mut self.reader_buf).await?);
                    self.count += 1;

                    if self.reader_buf.is_empty()
                        && stream.try_read_buf(&mut self.reader_buf).unwrap_or(0) == 0
                    {
                        break;
                    }
                }

                Ok(Some(frames))
            }
            ConnectionInner::TlsStream(stream) => {
                // 如果buffer为空，则尝试从stream中读入数据到buffer
                if self.reader_buf.is_empty() && stream.read_buf(&mut self.reader_buf).await? == 0 {
                    return Ok(None);
                }

                let mut frames = Vec::with_capacity(32);
                loop {
                    let s = &mut *stream;
                    frames.push(Frame::parse_frame(s, &mut self.reader_buf).await?);
                    self.count += 1;

                    let waker = noop_waker_ref();
                    let mut cx = Context::from_waker(waker);

                    pin_mut!(s);
                    if self.reader_buf.is_empty() {
                        let mut buf = [0u8; 1024];
                        match s.poll_read(&mut cx, &mut ReadBuf::new(&mut buf)) {
                            Poll::Ready(Ok(())) => {}
                            Poll::Ready(Err(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
                                break;
                            }
                            Poll::Ready(Err(e)) => return Err(e.into()),
                            Poll::Pending => break,
                        }
                    }
                }

                Ok(Some(frames))
            }
        }
    }

    #[inline]
    #[instrument(level = "trace", skip(self), err)]
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        match &mut self.inner {
            ConnectionInner::TcpStream(stream) => {
                frame.to_raw_in_buf(&mut self.writer_buf);

                if self.count >= 1 {
                    self.count -= 1;
                }

                if self.count == 0 {
                    stream.write_buf(&mut self.writer_buf).await?;
                    self.flush().await?;
                }
            }
            ConnectionInner::TlsStream(stream) => {
                frame.to_raw_in_buf(&mut self.writer_buf);

                if self.count >= 1 {
                    self.count -= 1;
                }

                if self.count == 0 {
                    stream.write_buf(&mut self.writer_buf).await?;
                    self.flush().await?;
                }
            }
        };

        Ok(())
    }
}

impl From<ConnectionInner> for Connection {
    fn from(inner: ConnectionInner) -> Self {
        Self {
            inner,
            reader_buf: BytesMut::with_capacity(1024),
            writer_buf: BytesMut::with_capacity(1024),
            count: 0,
        }
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
            let mut server_conn = Connection::new(socket);

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
        let mut conn = Connection::new(stream);

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

    #[tokio::test]
    async fn test_read_frame() {
        test_init();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (tx, rx) = tokio::sync::oneshot::channel();
        // 模拟服务器
        tokio::spawn(async move {
            let (socket, _addr) = listener.accept().await.unwrap();
            let mut server_conn = Connection::new(socket);

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
        let mut conn = Connection::new(stream);

        rx.await.unwrap();

        let mut res = vec![];
        while res.len() != 8 {
            let frame = conn.read_frames().await.unwrap().unwrap();
            res.extend(frame);
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
                Frame::Array(vec![])
            ]
        )
    }
}
