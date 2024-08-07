use crate::{error::RutinResult, frame::Resp3};
use bytes::{Buf, BufMut, BytesMut};
use flume::{
    r#async::{RecvFut, SendFut},
    Receiver, Sender,
};
use futures::{pin_mut, task::noop_waker_ref, Future};
use pin_project::{pin_project, pinned_drop};
use smallvec::SmallVec;
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
    batch: usize,
    pub max_batch: usize,
}

impl<S: AsyncStream> Connection<S> {
    pub fn new(stream: S, max_batch_count: usize) -> Self {
        Self {
            stream,
            reader_buf: BytesMut::with_capacity(1024),
            writer_buf: BytesMut::with_capacity(1024),
            batch: 0,
            max_batch: max_batch_count,
        }
    }

    pub const fn unhandled_count(&self) -> usize {
        self.batch
    }

    pub fn set_count(&mut self, count: usize) {
        self.batch = count;
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
    pub async fn write_all(&mut self, src: &[u8]) -> io::Result<()> {
        self.stream.write_all(src).await
    }

    #[inline]
    #[instrument(level = "trace", skip(self), ret, err)]
    pub async fn read_frame(&mut self) -> RutinResult<Option<Resp3>> {
        Resp3::decode_async(&mut self.stream, &mut self.reader_buf).await
    }

    // 尝试读取多个frame，直到buffer和stream都为空
    #[inline]
    #[instrument(level = "trace", skip(self), ret, err)]
    pub async fn read_frames(&mut self) -> RutinResult<Option<SmallVec<[Resp3; 32]>>> {
        let mut frames = SmallVec::new();

        self.reader_buf.reserve(1024);

        loop {
            let frame = match Resp3::decode_async(&mut self.stream, &mut self.reader_buf).await? {
                Some(frame) => frame,
                None => return Ok(None),
            };

            trace!(?frame, "read frame");
            frames.push(frame);
            self.batch += 1;

            // PERF: 该值影响pipeline的性能，以及内存占用
            if self.batch > self.max_batch {
                return Ok(Some(frames));
            }

            // 如果buffer为空则尝试继续从stream读取数据到buffer。如果阻塞或连接
            // 断开(内核中暂无数据)则返回目前解析到frame；如果读取到数据则继续
            // 解析(服务端总是假定客户端会发送完整的RESP3 frame，如果出现半包情
            // 况，则需要等待该frame的完整数据，其它frame请求的处理也会被阻塞)。
            if self.reader_buf.is_empty() {
                let fut = self.stream.read_buf(&mut self.reader_buf);
                pin_mut!(fut);

                let mut cx = Context::from_waker(noop_waker_ref());
                match fut.poll(&mut cx) {
                    Poll::Ready(Ok(n)) if n != 0 => {}
                    _ => return Ok(Some(frames)),
                }

                debug_assert!(!self.reader_buf.is_empty());
            }
        }
    }

    #[inline]
    #[instrument(level = "trace", skip(self), err)]
    pub async fn write_frame<B, St>(&mut self, frame: &Resp3<B, St>) -> io::Result<()>
    where
        B: AsRef<[u8]> + PartialEq + std::fmt::Debug,
        St: AsRef<str> + PartialEq + std::fmt::Debug,
    {
        frame.encode_buf(&mut self.writer_buf);

        if self.batch > 0 {
            self.batch -= 1;
        }

        if self.batch == 0 {
            self.stream.write_buf(&mut self.writer_buf).await?;
            self.flush().await?;
        }

        Ok(())
    }

    #[instrument(level = "trace", skip(self), err)]
    pub async fn write_frame_force<B, St>(&mut self, frame: &Resp3<B, St>) -> io::Result<()>
    where
        B: AsRef<[u8]> + PartialEq + std::fmt::Debug,
        St: AsRef<str> + PartialEq + std::fmt::Debug,
    {
        frame.encode_buf(&mut self.writer_buf);

        self.stream.write_buf(&mut self.writer_buf).await?;
        self.flush().await?;

        Ok(())
    }
}

impl Connection<FakeStream> {
    pub fn shutdown_signal(&self) -> ShutdownSignal {
        ShutdownSignal(self.stream.tx.clone())
    }

    // pub fn write_frame_blocking(&mut self, frame: &Resp3) -> io::Result<()> {
    //     frame.encode_buf(&mut self.writer_buf);
    //
    //     if self.batch_count > 0 {
    //         self.batch_count -= 1;
    //     }
    //
    //     if self.batch_count == 0 {
    //         self.stream
    //             .tx
    //             .send(self.writer_buf.split())
    //             .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e))?;
    //     }
    //
    //     Ok(())
    // }
    //
    // pub fn read_frame_blocking(&mut self) -> io::Result<Option<Resp3>> {
    //     let mut buf = BytesMut::new();
    //     let data = self
    //         .stream
    //         .rx
    //         .recv()
    //         .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e))?;
    //
    //     if data.is_empty() {
    //         return Ok(None);
    //     }
    //
    //     buf.extend_from_slice(&data);
    //
    //     Ok(Some(Resp3::new_blob_string(buf.freeze())))
    // }
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

/// 使用channel模拟客户端与服务端的连接。目前主要用于测试以及lua script功能的实现
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
    use bytes::Bytes;
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
            let mut server_conn = Connection::new(socket, 0);

            // 测试简单字符串
            let _ = server_conn
                .write_frame::<Bytes, String>(&Resp3::new_simple_string("OK".into()))
                .await;
            // 测试错误消息
            let _ = server_conn
                .write_frame::<Bytes, String>(&Resp3::new_simple_error("Error message".into()))
                .await;
            // 测试整数
            let _ = server_conn
                .write_frame::<Bytes, String>(&Resp3::new_integer(1000))
                .await;
            // 测试大容量字符串
            let _ = server_conn
                .write_frame::<Bytes, String>(&Resp3::new_blob_string("foobar".into()))
                .await;
            // 测试空字符串
            let _ = server_conn
                .write_frame::<Bytes, String>(&Resp3::new_blob_string("".into()))
                .await;
            // 测试空值
            let _ = server_conn
                .write_frame::<Bytes, String>(&Resp3::new_null())
                .await;
            // 测试数组
            let _ = server_conn
                .write_frame::<Bytes, String>(&Resp3::new_array(vec![
                    Resp3::new_simple_string("simple".into()),
                    Resp3::new_simple_error("error".into()),
                    Resp3::new_integer(1000),
                    Resp3::new_blob_string("bulk".into()),
                    Resp3::new_null(),
                    Resp3::new_array(vec![
                        Resp3::new_blob_string("foo".into()),
                        Resp3::new_blob_string("bar".into()),
                    ]),
                ]))
                .await;
            // 测试空数组
            let _ = server_conn
                .write_frame::<Bytes, String>(&Resp3::new_array(vec![]))
                .await;

            tx.send(()).unwrap();
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let mut conn = Connection::new(stream, 0);

        rx.await.unwrap();

        let mut res = vec![];
        while let Some(frames) = conn.read_frames().await.unwrap() {
            res.extend(frames);
        }

        let right_res = vec![
            Resp3::new_simple_string("OK".into()),
            Resp3::new_simple_error("Error message".into()),
            Resp3::new_integer(1000),
            Resp3::new_blob_string("foobar".into()),
            Resp3::new_blob_string("".into()),
            Resp3::new_null(),
            Resp3::new_array(vec![
                Resp3::new_simple_string("simple".into()),
                Resp3::new_simple_error("error".into()),
                Resp3::new_integer(1000),
                Resp3::new_blob_string("bulk".into()),
                Resp3::new_null(),
                Resp3::new_array(vec![
                    Resp3::new_blob_string("foo".into()),
                    Resp3::new_blob_string("bar".into()),
                ]),
            ]),
            Resp3::new_array(vec![]),
        ];

        for (a, b) in res.into_iter().zip(right_res) {
            assert_eq!(a, b);
        }
    }

    #[tokio::test]
    async fn fake_poll_test() {
        use tracing::debug;

        let data = BytesMut::from(b"a".as_slice());
        let data2 = BytesMut::from(b"b".as_slice());

        let (server_tx, client_rx) = flume::bounded(1);
        let (client_tx, server_rx) = flume::unbounded();
        let mut server = FakeStream::new(server_tx, server_rx);

        let handle = tokio::spawn(async move {
            let mut client = FakeStream::new(client_tx, client_rx);

            tokio::time::sleep(Duration::from_millis(100)).await;
            client.write_all(&data).await.unwrap(); // 写入数据，解除server.read_u8()的阻塞
            debug!("client write data done");

            tokio::time::sleep(Duration::from_millis(100)).await;
            let mut buf = [0; 3];
            let _ = client.read_exact(&mut buf).await.unwrap(); // 读取数据，解除server.write_all(&data2)的阻塞
            debug!("client read data: {:?}", buf);
            assert_eq!(buf, b"bbb".as_slice());
        });

        debug!("server reading data...");
        let a = server.read_u8().await.unwrap(); // async阻塞
        debug!("server read data: {:?}", a);
        assert_eq!(a, b'a');

        debug!("server writing data...");
        server.write_all(&data2).await.unwrap();
        server.write_all(&data2).await.unwrap();
        server.write_all(&data2).await.unwrap(); // async阻塞
        debug!("server write data done");
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn fake_stream_test() {
        use crate::server::Handler;

        crate::util::test_init();

        let (mut handler, mut client) = Handler::new_fake();

        tokio::spawn(async move {
            // 测试简单字符串
            let _ = client
                .write_frame::<Bytes, String>(&Resp3::new_simple_string("OK".into()))
                .await;
            // 测试错误消息
            let _ = client
                .write_frame::<Bytes, String>(&Resp3::new_simple_error("Error message".into()))
                .await;
            // 测试整数
            let _ = client
                .write_frame::<Bytes, String>(&Resp3::new_integer(1000))
                .await;
            // 测试大容量字符串
            let _ = client
                .write_frame::<Bytes, String>(&Resp3::new_blob_string("foobar".into()))
                .await;
            // 测试空字符串
            let _ = client
                .write_frame::<Bytes, String>(&Resp3::new_blob_string("".into()))
                .await;
            // 测试空值
            let _ = client
                .write_frame::<Bytes, String>(&Resp3::new_null())
                .await;
            // 测试数组
            let _ = client
                .write_frame::<Bytes, String>(&Resp3::new_array(vec![
                    Resp3::new_simple_string("simple".into()),
                    Resp3::new_simple_error("error".into()),
                    Resp3::new_integer(1000),
                    Resp3::new_blob_string("bulk".into()),
                    Resp3::new_null(),
                    Resp3::new_array(vec![
                        Resp3::new_blob_string("foo".into()),
                        Resp3::new_blob_string("bar".into()),
                    ]),
                ]))
                .await;
            // 测试空数组
            let _ = client
                .write_frame::<Bytes, String>(&Resp3::new_array(vec![]))
                .await;
        });

        let mut res = vec![];
        while let Some(frames) = handler.conn.read_frames().await.unwrap() {
            res.extend(frames);
        }

        let right_res = vec![
            Resp3::new_simple_string("OK".into()),
            Resp3::new_simple_error("Error message".into()),
            Resp3::new_integer(1000),
            Resp3::new_blob_string("foobar".into()),
            Resp3::new_blob_string("".into()),
            Resp3::new_null(),
            Resp3::new_array(vec![
                Resp3::new_simple_string("simple".into()),
                Resp3::new_simple_error("error".into()),
                Resp3::new_integer(1000),
                Resp3::new_blob_string("bulk".into()),
                Resp3::new_null(),
                Resp3::new_array(vec![
                    Resp3::new_blob_string("foo".into()),
                    Resp3::new_blob_string("bar".into()),
                ]),
            ]),
            Resp3::new_array(vec![]),
        ];

        for (a, b) in res.into_iter().zip(right_res) {
            assert_eq!(a, b);
        }
    }
}
