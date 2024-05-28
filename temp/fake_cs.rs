use crate::{
    conf::Conf,
    server::{Handler, HandlerContext},
    shared::Shared,
    Connection,
};
use async_shutdown::ShutdownManager;
use bytes::{BufMut, BytesMut};
use flume::{r#async::RecvFut, Receiver, Sender};
use futures::{pin_mut, Future, FutureExt};
use pin_project::pin_project;
use std::task::{Context, Poll};
use std::{pin::Pin, sync::Arc};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};

impl Handler<FakeStream> {
    pub fn new_fake() -> (Handler<FakeStream>, Connection<FakeStream>) {
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
    ) -> (Handler<FakeStream>, Connection<FakeStream>) {
        let (server_tx, client_rx) = flume::unbounded();
        let (client_tx, server_rx) = flume::unbounded();
        (
            Handler::<FakeStream> {
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

#[pin_project]
pub struct FakeStream {
    tx: Sender<BytesMut>,
    rx: &'static Receiver<BytesMut>,
    #[pin]
    state: State<'static>,
}

impl FakeStream {
    pub fn new(tx: Sender<BytesMut>, rx: Receiver<BytesMut>) -> FakeStream {
        let rx = Box::leak(Box::new(rx));
        FakeStream {
            tx,
            rx,
            state: State::Start,
        }
    }
}

#[pin_project(project = StateProj)]
enum State<'r> {
    Start,
    Recv(#[pin] RecvFut<'r, BytesMut>),
    Done,
}

impl AsyncRead for FakeStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut this = self.project();

        loop {
            println!("debug0: loop");
            match this.state.as_mut().project() {
                StateProj::Start => {
                    println!("debug0: Start");
                    let future = this.rx.recv_async();
                    this.state.set(State::Recv(future));
                }
                StateProj::Recv(fut) => match fut.poll(cx) {
                    Poll::Ready(Ok(data)) => {
                        buf.put_slice(&data);
                        println!("debug read: {:?}", data);
                        return Poll::Ready(Ok(()));
                    }
                    // 通道关闭
                    Poll::Ready(Err(_)) => {
                        println!("debug0: Ready Err,");
                        this.state.set(State::Done);
                        return Poll::Ready(Ok(()));
                    }
                    // 等待数据
                    Poll::Pending => {
                        println!("debug0: Pending");
                        return Poll::Pending;
                    }
                },
                StateProj::Done => {
                    println!("debug0: Done");
                    this.state.set(State::Start);
                    return Poll::Ready(Err(std::io::ErrorKind::UnexpectedEof.into()));
                }
            }
        }
    }
}

impl AsyncWrite for FakeStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        // 如果阻塞则会等待，永远返回 Ready，
        println!("debug write: {:?}", buf);
        match self.tx.send(buf.into()) {
            Ok(_) => Poll::Ready(Ok(buf.len())),
            // 通道关闭
            Err(_) => Poll::Ready(Ok(0)),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let (tx, rx) = flume::bounded(0);
        self.tx = tx;
        self.rx = Box::leak(Box::new(rx));
        std::task::Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod fake_cs_tests {
    use super::*;
    use std::time::Duration;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        time::sleep,
    };

    #[tokio::test]
    async fn fake_poll_test() {
        let data = BytesMut::from(b"a".as_slice());
        let data2 = BytesMut::from(b"b".as_slice());

        let (server_tx, client_rx) = flume::unbounded();
        let (client_tx, server_rx) = flume::unbounded();
        let mut server = FakeStream::new(server_tx, server_rx);
        let mut client = FakeStream::new(client_tx, client_rx);

        let (t1, r1) = tokio::sync::oneshot::channel();
        let (t2, r2) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            r1.await.unwrap();
            client.write_all(&data).await.unwrap();

            r2.await.unwrap();
            let mut buf = [0; 2];
            let _ = client.read_exact(&mut buf).await.unwrap();
            assert_eq!(buf, b"bb".as_slice());
            println!("debug4");
        });

        t1.send(()).unwrap();
        let a = server.read_u8().await.unwrap(); // 阻塞
        assert_eq!(a, b'a');

        server.write_all(&data2).await.unwrap();
        t2.send(()).unwrap();
        server.write_all(&data2).await.unwrap(); // 阻塞
        println!("debug3");
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

            client.shutdown().await.unwrap();
            drop(client);
        });

        let mut res = vec![];
        println!("debug2");
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
