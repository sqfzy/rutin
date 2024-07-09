use super::{BgTaskChannel, BgTaskSender, CLIENT_ID_COUNT, ID};
use crate::{
    cmd::dispatch,
    conf::{AccessControl, DEFAULT_USER},
    connection::{AsyncStream, Connection, FakeStream},
    error::RutinResult,
    frame::Resp3,
    server::{RESERVE_ID, RESERVE_MAX_ID},
    shared::Shared,
    Id, Key,
};
use bytes::BytesMut;
use std::sync::Arc;
use tracing::{debug, instrument};

pub struct Handler<S: AsyncStream> {
    pub shared: Shared,
    pub conn: Connection<S>,
    pub context: HandlerContext,
}

impl<S: AsyncStream> Handler<S> {
    #[inline]
    pub fn new(shared: Shared, stream: S) -> Self {
        Self {
            context: HandlerContext::new(&shared),
            conn: Connection::new(stream, shared.conf().server.max_batch),
            shared,
        }
    }

    #[inline]
    #[instrument(level = "debug", skip(self), fields(client_id), err)]
    pub async fn run(&mut self) -> RutinResult<()> {
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
                    frame = self.context.bg_task_channel.recv_from_bg_task() => {
                        debug!("handler received from background task: {:?}", frame);
                        self.conn.write_frame(&frame).await?;
                    },
                };
            }
        })
        .await
    }

    #[inline]
    pub async fn dispatch(&mut self, cmd_frame: Resp3) -> RutinResult<Option<Resp3>> {
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
    pub bg_task_channel: BgTaskChannel,
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
    pub fn new(shared: &Shared) -> Self {
        let bg_task_channel = BgTaskChannel::default();

        // 生成新的client_id
        let id_may_occupied = CLIENT_ID_COUNT.fetch_add(1);
        let client_id = shared
            .db()
            .record_client_id(id_may_occupied, bg_task_channel.new_sender());
        if id_may_occupied != client_id {
            CLIENT_ID_COUNT.store(client_id);
        }

        // 使用默认ac
        let ac = shared.conf().security.default_ac.load_full();

        Self {
            client_id,
            bg_task_channel,
            subscribed_channels: None,
            client_track: None,
            wcmd_buf: BytesMut::new(),
            user: DEFAULT_USER,
            ac,
        }
    }

    pub fn new_by_reserve_id(shared: &Shared, ac: Arc<AccessControl>) -> Self {
        let bg_task_channel = BgTaskChannel::default();

        let reserve_id = RESERVE_ID.fetch_add(1);

        // ID必须在0到RESERVE_MAX_ID之间
        debug_assert!(reserve_id <= RESERVE_MAX_ID);

        let client_id = shared
            .db()
            .record_client_id(reserve_id, bg_task_channel.new_sender());

        // ID必须没有被占用
        debug_assert!(client_id == reserve_id);

        Self {
            client_id,
            bg_task_channel,
            subscribed_channels: None,
            client_track: None,
            wcmd_buf: BytesMut::new(),
            user: DEFAULT_USER,
            ac,
        }
    }

    pub fn clear(&mut self) {
        self.subscribed_channels = None;
        self.client_track = None;
        self.wcmd_buf.clear();
    }
}

pub type FakeHandler = Handler<FakeStream>;

impl Handler<FakeStream> {
    pub fn new_fake() -> (Self, Connection<FakeStream>) {
        Self::new_fake_with(Shared::default(), None, None)
    }

    pub fn with_capacity(capacity: usize) -> (Self, Connection<FakeStream>) {
        Self::new_fake_with(Shared::default(), None, Some(capacity))
    }

    pub fn with_shared(shared: Shared) -> (Self, Connection<FakeStream>) {
        Self::new_fake_with(shared, None, None)
    }

    pub fn new_fake_with(
        shared: Shared,
        context: Option<HandlerContext>,
        capacity: Option<usize>,
    ) -> (Self, Connection<FakeStream>) {
        let ((server_tx, client_rx), (client_tx, server_rx)) = if let Some(capacity) = capacity {
            (flume::bounded(capacity), flume::bounded(capacity))
        } else {
            (flume::unbounded(), flume::unbounded())
        };

        let context = if let Some(ctx) = context {
            ctx
        } else {
            HandlerContext::new(&shared)
        };

        let max_batch = shared.conf().server.max_batch;
        (
            Self {
                shared,
                conn: Connection::new(FakeStream::new(server_tx, server_rx), max_batch),
                context,
            },
            Connection::new(FakeStream::new(client_tx, client_rx), max_batch),
        )
    }
}
