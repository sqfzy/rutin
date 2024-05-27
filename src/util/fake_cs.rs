use crate::{
    conf::Conf,
    server::{Handler, HandlerContext},
    shared::Shared,
    Connection,
};
use async_shutdown::ShutdownManager;
use bytes::BytesMut;
use flume::{Receiver, Sender};
use futures::{pin_mut, Future};
use std::task::{Context, Poll};
use std::{pin::Pin, sync::Arc};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

impl Handler<FakeStream> {
    pub fn new_fake_with(
        shared: Shared,
        shutdown_manager: ShutdownManager<()>,
        conf: Arc<Conf>,
    ) -> Handler<FakeStream> {
        Handler::<FakeStream> {
            shared,
            conn: FakeStream::default().into(),
            shutdown_manager,
            bg_task_channel: Default::default(),
            conf,
            context: HandlerContext::default(),
        }
    }

    pub fn new_client(&self) -> Connection<FakeStream> {
        Connection::from(FakeStream {
            // 交换两个端点
            local: self.conn.stream.peer.clone(),
            peer: self.conn.stream.local.clone(),
        })
    }
}

impl Default for Handler<FakeStream> {
    fn default() -> Self {
        Handler::<FakeStream> {
            shared: Default::default(),
            conn: FakeStream::default().into(),
            shutdown_manager: Default::default(),
            bg_task_channel: Default::default(),
            conf: Default::default(),
            context: Default::default(),
        }
    }
}

pub struct FakeStream {
    local: (Sender<BytesMut>, Receiver<BytesMut>),
    peer: (Sender<BytesMut>, Receiver<BytesMut>),
}

impl Default for FakeStream {
    fn default() -> Self {
        let (local_tx, peer_rx) = flume::unbounded();
        let (peer_tx, local_rx) = flume::unbounded();
        Self {
            local: (local_tx, local_rx),
            peer: (peer_tx, peer_rx),
        }
    }
}

impl AsyncRead for FakeStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let fut = self.local.1.recv_async();
        pin_mut!(fut);

        match fut.poll(cx) {
            Poll::Ready(Ok(data)) => {
                buf.put_slice(&data);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(_)) => Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into())),
            Poll::Pending => Poll::Pending,
        }
    }
}
impl AsyncWrite for FakeStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let fut = self.local.0.send_async(buf.into());
        pin_mut!(fut);

        match fut.poll(cx) {
            Poll::Ready(Ok(_)) => Poll::Ready(Ok(buf.len())),
            Poll::Ready(Err(_)) => Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into())),
            Poll::Pending => Poll::Pending,
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
        let channel = flume::bounded(0);
        self.local = channel.clone();
        self.peer = channel;
        std::task::Poll::Ready(Ok(()))
    }
}
