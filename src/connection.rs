use crate::frame::Frame;
use bytes::{Buf, BufMut, BytesMut};
use flume::{
    r#async::{RecvFut, SendFut},
    Receiver, Sender,
};
use futures::{pin_mut, task::noop_waker_ref, Future};
use pin_project::{pin_project, pinned_drop};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::TcpStream,
};
use tracing::{error, instrument, trace};

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
    stream: S,
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

    pub fn set_count(&mut self, count: usize) {
        self.count = count;
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

        if self.count > 0 {
            self.count -= 1;
        }

        if self.count == 0 {
            self.stream.write_buf(&mut self.writer_buf).await?;
            self.flush().await?;
        }

        Ok(())
    }
}

impl Connection<FakeStream> {
    pub fn shutdown_signal(&self) -> ShutdownSignal {
        ShutdownSignal(self.stream.tx.clone())
    }

    pub fn write_frame_blocking(&mut self, frame: &Frame) -> io::Result<()> {
        frame.to_raw_in_buf(&mut self.writer_buf);

        if self.count > 0 {
            self.count -= 1;
        }

        if self.count == 0 {
            self.stream
                .tx
                .send(self.writer_buf.split())
                .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e))?;
        }

        Ok(())
    }

    pub fn read_frame_blocking(&mut self) -> io::Result<Option<Frame>> {
        let mut buf = BytesMut::new();
        let data = self
            .stream
            .rx
            .recv()
            .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e))?;

        if data.is_empty() {
            return Ok(None);
        }

        buf.extend_from_slice(&data);

        Ok(Some(Frame::from_raw(&mut buf)?))
    }
}

pub struct ShutdownSignal(Sender<BytesMut>);

impl ShutdownSignal {
    pub fn shutdown(&self) {
        let _ = self.0.send(BytesMut::new()).map_err(|e| {
            error!("fake connection failed to send shutdown signal: {:?}", e);
        });
    }
}

#[pin_project(project = StateProj)]
enum State<'a> {
    Start,
    Recv(#[pin] RecvFut<'a, BytesMut>),
    Send(#[pin] SendFut<'a, BytesMut>),
    Remain(BytesMut),
}

#[pin_project(PinnedDrop)]
pub struct FakeStream {
    tx: Sender<BytesMut>,
    rx: Receiver<BytesMut>,
    #[pin]
    state: State<'static>,
}

impl FakeStream {
    pub fn new(tx: Sender<BytesMut>, rx: Receiver<BytesMut>) -> Self {
        Self {
            tx,
            rx,
            state: State::Start,
        }
    }
}

impl AsyncRead for FakeStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut this = self.project();

        loop {
            match this.state.as_mut().project() {
                StateProj::Start => {
                    let future = this.rx.recv_async();
                    this.state.set(unsafe {
                        State::Recv(std::mem::transmute::<
                            flume::r#async::RecvFut<'_, bytes::BytesMut>,
                            flume::r#async::RecvFut<'_, bytes::BytesMut>,
                        >(future))
                    });
                }
                StateProj::Recv(fut) => match fut.poll(cx) {
                    Poll::Ready(Ok(mut data)) => {
                        if buf.remaining() == 0 {
                            this.state.set(State::Remain(data));
                            return Poll::Pending;
                        }

                        if data.len() > buf.remaining() {
                            buf.put_slice(&data.split_to(buf.remaining()));
                            this.state.set(State::Remain(data));
                        } else {
                            buf.put_slice(&data.split());
                            this.state.set(State::Start);
                        }
                        return Poll::Ready(Ok(()));
                    }
                    // 通道已经关闭
                    Poll::Ready(Err(_)) => return Poll::Ready(Ok(())),
                    Poll::Pending => return Poll::Pending,
                },
                StateProj::Remain(data) => {
                    if buf.remaining() == 0 {
                        return Poll::Pending;
                    }

                    if data.len() > buf.remaining() {
                        buf.put_slice(&data.split_to(buf.remaining()));
                    } else {
                        buf.put_slice(&data.split());
                        this.state.set(State::Start);
                    }
                    return Poll::Ready(Ok(()));
                }
                _ => unreachable!(),
            }
        }
    }
}

impl AsyncWrite for FakeStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        // 不发送空数据
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let mut this = self.project();

        loop {
            match this.state.as_mut().project() {
                StateProj::Start => {
                    let future = this.tx.send_async(BytesMut::from(buf));
                    this.state.set(unsafe {
                        State::Send(std::mem::transmute::<
                            flume::r#async::SendFut<'_, bytes::BytesMut>,
                            flume::r#async::SendFut<'_, bytes::BytesMut>,
                        >(future))
                    });
                }
                StateProj::Send(fut) => match fut.poll(cx) {
                    Poll::Ready(Ok(_)) => {
                        this.state.set(State::Start);
                        return Poll::Ready(Ok(buf.len()));
                    }
                    Poll::Ready(Err(_)) => {
                        return Poll::Ready(Ok(0));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                _ => unreachable!(),
            }
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let _ = self.tx.send(BytesMut::new()); // 代表连接关闭
        std::task::Poll::Ready(Ok(()))
    }
}

#[pinned_drop]
impl PinnedDrop for FakeStream {
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        let _ = this.tx.send(BytesMut::new()); // 发送空数据，代表连接关闭
    }
}

#[cfg(test)]
mod fake_cs_tests {
    use super::*;
    use crate::util::test_init;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

    #[tokio::test]
    async fn fake_poll_test() {
        let data = BytesMut::from(b"a".as_slice());
        let data2 = BytesMut::from(b"b".as_slice());

        let (server_tx, client_rx) = flume::bounded(1);
        let (client_tx, server_rx) = flume::unbounded();
        let mut server = FakeStream::new(server_tx, server_rx);

        let handle = tokio::spawn(async move {
            let mut client = FakeStream::new(client_tx, client_rx);

            tokio::time::sleep(Duration::from_millis(100)).await;
            client.write_all(&data).await.unwrap(); // 写入数据，解除server.read_u8()的阻塞
            println!("client write data done");

            tokio::time::sleep(Duration::from_millis(100)).await;
            let mut buf = [0; 3];
            let _ = client.read_exact(&mut buf).await.unwrap(); // 读取数据，解除server.write_all(&data2)的阻塞
            println!("client read data: {:?}", buf);
            assert_eq!(buf, b"bbb".as_slice());
        });

        println!("server reading data...");
        let a = server.read_u8().await.unwrap(); // async阻塞
        println!("server read data: {:?}", a);
        assert_eq!(a, b'a');

        println!("server writing data...");
        server.write_all(&data2).await.unwrap();
        server.write_all(&data2).await.unwrap();
        server.write_all(&data2).await.unwrap(); // async阻塞
        println!("server write data done");
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn fake_stream_test() {
        use crate::frame::Frame;
        use crate::server::Handler;

        crate::util::test_init();

        let (mut handler, mut client) = Handler::new_fake();

        tokio::spawn(async move {
            // 测试简单字符串
            let _ = client.write_frame(&Frame::new_simple_borrowed("OK")).await;
            // 测试错误消息
            let _ = client
                .write_frame(&Frame::new_error_borrowed("Error message"))
                .await;
            // 测试整数
            let _ = client.write_frame(&Frame::Integer(1000)).await;
            // 测试大容量字符串
            let _ = client
                .write_frame(&Frame::new_bulk_by_copying(b"foobar"))
                .await;
            // 测试空字符串
            let _ = client.write_frame(&Frame::new_bulk_by_copying(b"")).await;
            // 测试空值
            let _ = client.write_frame(&Frame::Null).await;
            // 测试数组
            let _ = client
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
            let _ = client.write_frame(&Frame::Array(vec![])).await;
        });

        let mut res = vec![];
        while let Some(frames) = handler.conn.read_frames().await.unwrap() {
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
