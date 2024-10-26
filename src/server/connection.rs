use crate::{
    error::{RutinError, RutinResult},
    frame::{Resp3, StaticResp3},
    util::{StaticBytes, StaticStr},
};
use bytes::{Buf, BytesMut};
use flume::{
    r#async::{RecvFut, SendFut},
    Receiver, Sender,
};
use futures::{io, Future, FutureExt};
use pin_project::{pin_project, pinned_drop};
use rutin_resp3::codec::decode::{decode_async, decode_line_async, Resp3Decoder};
use std::{
    any::Any,
    collections::VecDeque,
    fmt::Debug,
    io::Cursor,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::TcpStream,
};
use tokio_util::codec::Decoder;
use tracing::{error, instrument};

pub trait AsyncStream:
    AsyncRead + AsyncReadExt + AsyncWrite + AsyncWriteExt + Unpin + Send + Any
{
}
impl<T: AsyncRead + AsyncReadExt + AsyncWrite + AsyncWriteExt + Unpin + Send + Any> AsyncStream
    for T
{
}

const BUF_SIZE: usize = 1024 * 8;

#[derive(derive_more::derive::Debug)]
pub struct Connection<S = TcpStream>
where
    S: AsyncStream,
{
    pub stream: S,
    pub reader_buf: Cursor<BytesMut>,
    pub writer_buf: BytesMut,
    /// 支持批处理
    batch: usize,
    pub max_batch: usize,

    pub requests: VecDeque<StaticResp3>,
}

impl<S: AsyncStream> Connection<S> {
    pub fn new(stream: S, max_batch: usize) -> Self {
        Self {
            stream,
            reader_buf: Cursor::new(BytesMut::with_capacity(BUF_SIZE)),
            writer_buf: BytesMut::with_capacity(BUF_SIZE),
            batch: 0,
            max_batch,
            requests: Default::default(),
        }
    }

    // 读取frame之后可以调用该函数清除reader_buf中已经使用过的数据
    // 不调用该函数只会导致内存占用增加，不会影响后续的数据读取
    pub fn flush_reader_buf(&mut self) {
        let reader_buf = &mut self.reader_buf;

        // // 清除reader_buf中已经使用过的数据，这会使RefMutResp3失效
        // if reader_buf.has_remaining() {
        //     // TEST:
        //
        //     // 如果reader_buf中还有数据，则将其复制到buf的开头
        //     let len = reader_buf.remaining();
        //     let pos = reader_buf.position() as usize;
        //
        //     let inner = reader_buf.get_mut();
        //     inner.copy_within(pos..pos + len, 0);
        //
        //     inner.truncate(len);
        // } else {
        //     reader_buf.get_mut().clear();
        // }

        reader_buf.get_mut().clear();
        reader_buf.set_position(0);
    }

    pub const fn unhandled_count(&self) -> usize {
        self.batch
    }

    #[inline]
    pub async fn shutdown(&mut self) -> io::Result<()> {
        self.stream.shutdown().await
    }

    #[inline]
    pub async fn flush(&mut self) -> io::Result<()> {
        self.stream.flush().await
    }

    pub async fn read_line(&mut self) -> RutinResult<StaticBytes> {
        Ok(decode_line_async(&mut self.stream, &mut self.reader_buf)
            .await?
            .into())
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
    pub async fn read_frame(&mut self) -> RutinResult<Option<StaticResp3>> {
        if !self.reader_buf.has_remaining() {
            self.flush_reader_buf();
        }

        let frame = decode_async(&mut self.stream, &mut self.reader_buf)
            .await
            .map_err(Into::into);

        self.batch += 1;

        frame
    }

    pub async fn try_read_frame(&mut self) -> RutinResult<Option<StaticResp3>> {
        let mut decoder = Resp3Decoder::<StaticBytes, StaticStr>::default();
        decoder.pos = self.reader_buf.position();
        let frame = decoder
            .decode(self.reader_buf.get_mut())
            .map_err(Into::into);

        self.batch += 1;

        frame
    }

    // 返回RutinError::Other而不是RutinError::Server
    #[inline]
    #[instrument(level = "trace", skip(self), ret, err)]
    pub async fn read_frame_force(&mut self) -> RutinResult<StaticResp3> {
        let frame = self
            .read_frame()
            .await?
            .ok_or_else(|| RutinError::from("ERR connection closed"));

        self.batch += 1;

        frame
    }

    // #[instrument(level = "trace", skip(self), ret, err)]
    // pub async fn read_frames(&mut self) -> RutinResult<Option<Vec<StaticResp3>>> {
    //     let mut frames = vec![];
    //
    //     loop {
    //         let frame = match self.read_frame().await? {
    //             Some(frame) => frame,
    //             None => {
    //                 if frames.is_empty() {
    //                     return Ok(None);
    //                 } else {
    //                     break;
    //                 }
    //             }
    //         };
    //
    //         frames.push(frame);
    //         self.batch += 1;
    //
    //         if self.batch > self.max_batch {
    //             break;
    //         }
    //
    //         // 如果buffer为空则尝试继续从stream读取数据到buffer。如果阻塞或连接
    //         // 断开(内核中暂无数据)则返回目前解析到frame；如果读取到数据则继续
    //         // 解析(服务端总是假定客户端会发送完整的RESP3 frame，如果出现半包情
    //         // 况，则需要等待该frame的完整数据，其它frame请求的处理也会被阻塞)。
    //         if !self.reader_buf.has_remaining() {
    //             match self
    //                 .stream
    //                 .read_buf(self.reader_buf.get_mut())
    //                 .now_or_never()
    //             {
    //                 Some(Ok(n)) if n != 0 => {}
    //                 _ => break,
    //             }
    //         }
    //     }
    //
    //     while self.reader_buf.has_remaining() {
    //         let frame = match self.read_frame().await? {
    //             Some(frame) => frame,
    //             None => {
    //                 if frames.is_empty() {
    //                     return Ok(None);
    //                 } else {
    //                     return Ok(Some(frames));
    //                 }
    //             }
    //         };
    //
    //         frames.push(frame);
    //         self.batch += 1;
    //     }
    //
    //     Ok(Some(frames))
    // }

    #[instrument(level = "trace", skip(self), ret, err)]
    pub async fn get_requests(&mut self) -> RutinResult<Option<usize>> {
        self.flush_reader_buf();

        loop {
            let frame = match self.read_frame().await? {
                Some(frame) => frame,
                None => {
                    if self.requests.is_empty() {
                        return Ok(None);
                    } else {
                        break;
                    }
                }
            };

            self.requests.push_back(frame);

            if self.batch > self.max_batch {
                break;
            }

            // 如果buffer为空则尝试继续从stream读取数据到buffer。如果阻塞或连接
            // 断开(内核中暂无数据)则返回目前解析到frame；如果读取到数据则继续
            // 解析(服务端总是假定客户端会发送完整的RESP3 frame，如果出现半包情
            // 况，则需要等待该frame的完整数据，其它frame请求的处理也会被阻塞)。
            if !self.reader_buf.has_remaining() {
                match self
                    .stream
                    .read_buf(self.reader_buf.get_mut())
                    .now_or_never()
                {
                    Some(Ok(n)) if n != 0 => {}
                    _ => break,
                }
            }
        }

        while self.reader_buf.has_remaining() {
            let frame = match self.read_frame().await? {
                Some(frame) => frame,
                None => {
                    if self.requests.is_empty() {
                        return Ok(None);
                    } else {
                        return Ok(Some(self.requests.len()));
                    }
                }
            };

            self.requests.push_back(frame);
        }

        Ok(Some(self.requests.len()))
    }

    #[instrument(level = "trace", skip(self), err)]
    pub async fn write_frame<B, St>(&mut self, frame: &Resp3<B, St>) -> io::Result<()>
    where
        B: AsRef<[u8]> + Debug,
        St: AsRef<str> + Debug,
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
    async fn fake_poll_test() {
        test_init();

        let data = BytesMut::from(b"a".as_slice());
        let data2 = BytesMut::from(b"b".as_slice());

        let (server_tx, client_rx) = flume::bounded(1);
        let (client_tx, server_rx) = flume::unbounded();
        let mut server = FakeStream::new(server_tx, server_rx);

        let handle = tokio::spawn(async move {
            let mut client = FakeStream::new(client_tx, client_rx);

            tokio::time::sleep(Duration::from_millis(100)).await;
            client.write_all(&data).await.unwrap(); // 写入数据，解除server.read_u8()的阻塞
            eprintln!("client write data done");

            tokio::time::sleep(Duration::from_millis(100)).await;
            let mut buf = [0; 3];
            let _ = client.read_exact(&mut buf).await.unwrap(); // 读取数据，解除server.write_all(&data2)的阻塞
            eprintln!("client read data: {:?}", buf);
            assert_eq!(buf, b"bbb".as_slice());
        });

        eprintln!("server reading data...");
        let a = server.read_u8().await.unwrap(); // async阻塞
        eprintln!("server read data: {:?}", a);
        assert_eq!(a, b'a');

        eprintln!("server writing data...");
        server.write_all(&data2).await.unwrap();
        server.write_all(&data2).await.unwrap();
        server.write_all(&data2).await.unwrap(); // async阻塞
        eprintln!("server write data done");
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
                .write_frame::<Bytes, String>(&Resp3::new_simple_string("OK"))
                .await;
            // 测试错误消息
            let _ = client
                .write_frame::<Bytes, String>(&Resp3::new_simple_error("Error message"))
                .await;
            // 测试整数
            let _ = client
                .write_frame::<Bytes, String>(&Resp3::new_integer(1000))
                .await;
            // 测试大容量字符串
            let _ = client
                .write_frame::<Bytes, String>(&Resp3::new_blob_string("foobar"))
                .await;
            // 测试空字符串
            let _ = client
                .write_frame::<Bytes, String>(&Resp3::new_blob_string(""))
                .await;
            // 测试空值
            let _ = client
                .write_frame::<Bytes, String>(&Resp3::new_null())
                .await;
            // 测试数组
            let _ = client
                .write_frame::<Bytes, String>(&Resp3::new_array(vec![
                    Resp3::new_simple_string("simple"),
                    Resp3::new_simple_error("error"),
                    Resp3::new_integer(1000),
                    Resp3::new_blob_string("bulk"),
                    Resp3::new_null(),
                    Resp3::new_array(vec![
                        Resp3::new_blob_string("foo"),
                        Resp3::new_blob_string("bar"),
                    ]),
                ]))
                .await;
            // 测试空数组
            let _ = client
                .write_frame::<Bytes, String>(&Resp3::new_array(vec![]))
                .await;
        });

        let right_res = vec![
            StaticResp3::new_simple_string("OK".to_string()),
            StaticResp3::new_simple_error("Error message".to_string()),
            StaticResp3::new_integer(1000),
            StaticResp3::new_blob_string(b"foobar".as_ref()),
            StaticResp3::new_blob_string(b"".as_ref()),
            StaticResp3::new_null(),
            StaticResp3::new_array(vec![
                StaticResp3::new_simple_string("simple".to_string()),
                StaticResp3::new_simple_error("error".to_string()),
                StaticResp3::new_integer(1000),
                StaticResp3::new_blob_string(b"bulk".as_ref()),
                StaticResp3::new_null(),
                StaticResp3::new_array(vec![
                    StaticResp3::new_blob_string(b"foo".as_ref()),
                    StaticResp3::new_blob_string(b"bar".as_ref()),
                ]),
            ]),
            Resp3::new_array(vec![]),
        ];

        let mut i = 0;
        while let Some(res) = handler.conn.read_frame().await.unwrap() {
            assert_eq!(res, right_res[i]);
            i += 1;
        }
    }
}
