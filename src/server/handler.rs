use super::{BgTaskChannel, BgTaskSender, ServerError, CLIENT_ID_COUNT, ID};
use crate::{
    cmd::dispatch,
    conf::{AccessControl, DEFAULT_USER},
    connection::{AsyncStream, Connection, FakeStream},
    frame::Resp3,
    shared::Shared,
    Id, Key,
};
use bytes::BytesMut;
use std::sync::Arc;
use tracing::{debug, instrument};

pub struct Handler<S: AsyncStream> {
    pub shared: Shared,
    pub conn: Connection<S>,
    pub bg_task_channel: BgTaskChannel,
    pub context: HandlerContext,
}

impl<S: AsyncStream> Handler<S> {
    #[inline]
    pub fn new(shared: Shared, stream: S) -> Self {
        let bg_task_channel = BgTaskChannel::default();
        let client_id = Self::create_client_id(&shared, &bg_task_channel);
        // 使用默认ac
        let ac = shared.conf().security.default_ac.load_full();

        Self {
            conn: Connection::new(stream, shared.conf().server.max_batch),
            shared,
            bg_task_channel,
            context: HandlerContext::new(client_id, DEFAULT_USER, ac),
        }
    }

    #[inline]
    #[instrument(level = "debug", skip(self), fields(client_id), err)]
    pub async fn run(&mut self) -> anyhow::Result<()> {
        ID.scope(self.context.client_id, async {
            loop {
                tokio::select! {
                    // 等待shutdown信号
                    _signal = self.shared.shutdown().wait_shutdown_triggered() => {
                        debug!("handler received shutdown signal");
                        return Ok(());
                    }
                    // 等待客户端请求
                    frames =  self.conn.read_frames() => {
                        if let Some(frames) = frames? {
                            for f in frames.into_iter() {
                                if let Some(resp) = dispatch(f, self).await? {
                                    self.conn.write_frame(&resp).await?;
                                }
                            }
                        } else {
                            return Ok(());
                        }
                    },
                    // 从后台任务接收数据，并发送给客户端。只要拥有对应的BgTaskSender，
                    // 任何其它连接 都可以向当前连接的客户端发送消息
                    frame = self.bg_task_channel.recv_from_bg_task() => {
                        debug!("handler received from background task: {:?}", frame);
                        self.conn.write_frame(&frame).await?;
                    },
                };
            }
        })
        .await
    }

    #[inline]
    pub async fn dispatch(&mut self, cmd_frame: Resp3) -> Result<Option<Resp3>, ServerError> {
        ID.scope(self.context.client_id, dispatch(cmd_frame, self))
            .await
    }

    #[inline]
    pub fn create_client_id(shared: &Shared, bg_task_channel: &BgTaskChannel) -> Id {
        let id_may_occupied = CLIENT_ID_COUNT.fetch_add(1);
        let client_id = shared
            .db()
            .record_client_id(id_may_occupied, bg_task_channel.new_sender());
        if id_may_occupied != client_id {
            CLIENT_ID_COUNT.store(client_id);
        }
        client_id
    }
}

#[derive(Debug)]
pub struct HandlerContext {
    pub client_id: Id,
    // 客户端订阅的频道
    pub subscribed_channels: Option<Vec<Key>>,
    // 是否开启缓存追踪
    pub client_track: Option<BgTaskSender>,
    // 用于缓存需要传播的写命令
    pub wcmd_buf: BytesMut,
    pub user: bytes::Bytes,
    pub ac: Arc<AccessControl>,
}

impl HandlerContext {
    pub fn new(client_id: Id, user: bytes::Bytes, ac: Arc<AccessControl>) -> Self {
        Self {
            client_id,
            subscribed_channels: None,
            client_track: None,
            wcmd_buf: BytesMut::new(),
            user,
            ac,
        }
    }
}

impl Handler<FakeStream> {
    pub fn new_fake() -> (Self, Connection<FakeStream>) {
        Self::new_fake_with(Shared::default(), None, None)
    }

    pub fn with_capacity(capacity: usize) -> (Self, Connection<FakeStream>) {
        Self::new_fake_with(Shared::default(), Some(capacity), None)
    }

    pub fn with_shared(shared: Shared) -> (Self, Connection<FakeStream>) {
        Self::new_fake_with(shared, None, None)
    }

    pub fn new_fake_with(
        shared: Shared,
        capacity: Option<usize>,
        context: Option<HandlerContext>,
    ) -> (Self, Connection<FakeStream>) {
        let ((server_tx, client_rx), (client_tx, server_rx)) = if let Some(capacity) = capacity {
            (flume::bounded(capacity), flume::bounded(capacity))
        } else {
            (flume::unbounded(), flume::unbounded())
        };

        let bg_task_channel = BgTaskChannel::default();

        let context = if let Some(cx) = context {
            let client_id = Self::create_client_id(&shared, &bg_task_channel);

            // 继承Access Control
            HandlerContext::new(client_id, cx.user, cx.ac)
        } else {
            let client_id = Self::create_client_id(&shared, &bg_task_channel);

            let ac = shared.conf().security.default_ac.load_full();
            HandlerContext::new(client_id, DEFAULT_USER, ac)
        };

        let max_batch = shared.conf().server.max_batch;
        (
            Self {
                shared,
                conn: Connection::new(FakeStream::new(server_tx, server_rx), max_batch),
                bg_task_channel,
                context,
            },
            Connection::new(FakeStream::new(client_tx, client_rx), max_batch),
        )
    }
}
