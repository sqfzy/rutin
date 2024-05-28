use crate::{
    conf::Conf,
    server::{Handler, HandlerContext},
    shared::Shared,
    Connection,
};
use async_shutdown::ShutdownManager;
use bytes::BytesMut;
use flume::{
    r#async::{RecvFut, SendFut},
    Receiver, Sender,
};
use futures::Future;
use pin_project::{pin_project, pinned_drop};
use std::task::{Context, Poll};
use std::{pin::Pin, sync::Arc};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

impl<'a> Handler<FakeStream<'a>> {
    pub fn new_fake() -> (Self, Connection<FakeStream<'a>>) {
        Self::new_fake_with(
            Shared::default(),
            Arc::new(Conf::default()),
            ShutdownManager::default(),
        )
    }

    pub fn new_fake_with(
        shared: Shared,
        conf: Arc<Conf>,
        shutdown_manager: ShutdownManager<()>,
    ) -> (Self, Connection<FakeStream<'a>>) {
        let (server_tx, client_rx) = flume::unbounded();
        let (client_tx, server_rx) = flume::unbounded();
        let server_tx = Box::leak(Box::new(server_tx));
        let server_rx = Box::leak(Box::new(server_rx));
        let client_tx = Box::leak(Box::new(client_tx));
        let client_rx = Box::leak(Box::new(client_rx));
        (
            Self {
                shared,
                conn: Connection::from(FakeStream::new(server_tx, server_rx)),
                shutdown_manager,
                bg_task_channel: Default::default(),
                conf,
                context: HandlerContext::default(),
            },
            Connection::from(FakeStream::new(client_tx, client_rx)),
        )
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
pub struct FakeStream<'a> {
    tx: &'a Sender<BytesMut>,
    rx: &'a Receiver<BytesMut>,
    #[pin]
    state: State<'a>,
}

impl<'a> FakeStream<'a> {
    pub fn new(tx: &'a Sender<BytesMut>, rx: &'a Receiver<BytesMut>) -> Self {
        Self {
            tx,
            rx,
            state: State::Start,
        }
    }
}

#[pinned_drop]
impl PinnedDrop for FakeStream<'_> {
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        let _ = this.tx.send(BytesMut::new()); // 代表连接关闭
    }
}

impl AsyncRead for FakeStream<'_> {
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
                    this.state.set(State::Recv(future));
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

impl AsyncWrite for FakeStream<'_> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let mut this = self.project();

        loop {
            match this.state.as_mut().project() {
                StateProj::Start => {
                    let future = this.tx.send_async(BytesMut::from(buf));
                    this.state.set(State::Send(future));
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
        let this = self.project();
        let _ = this.tx.send(BytesMut::new()); // 代表连接关闭
        std::task::Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod fake_cs_tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn fake_poll_test() {
        let data = BytesMut::from(b"a".as_slice());
        let data2 = BytesMut::from(b"b".as_slice());

        let (server_tx, client_rx) = flume::bounded(1);
        let (client_tx, server_rx) = flume::unbounded();
        let mut server = FakeStream::new(&server_tx, &server_rx);

        let handle = tokio::spawn(async move {
            let mut client = FakeStream::new(&client_tx, &client_rx);

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
