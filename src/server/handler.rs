use super::{BgTaskChannel, BgTaskSender, CLIENT_ID_COUNT, ID};
use crate::{
    cmd::dispatch,
    conf::{AccessControl, DEFAULT_USER},
    connection::{AsyncStream, Connection, FakeStream},
    error::RutinResult,
    frame::Resp3,
    server::{RESERVE_ID, RESERVE_MAX_ID},
    shared::{Shared, SignalManager},
    util::get_test_shared,
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
                    signal = self.shared.signal_manager().wait_all_signal() => {
                        // 如果信号是保留ID或者当前连接的ID，则关闭连接
                        if (0..=RESERVE_MAX_ID).contains(&signal)  {
                            return Ok(());
                        } else if signal == self.context.client_id {
                            debug!("client {} is closed by signal", self.context.client_id);
                            return Ok(());
                        }
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
}

impl<S: AsyncStream> Drop for Handler<S> {
    fn drop(&mut self) {
        self.shared
            .db()
            .remove_client_record(self.context.client_id);
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

        let client_id = create_client_id(shared, &bg_task_channel);

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

        let client_id = create_reserve_client_id(shared, &bg_task_channel);

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
        Self::new_fake_with(get_test_shared(), None, None)
    }

    pub fn with_capacity(capacity: usize) -> (Self, Connection<FakeStream>) {
        Self::new_fake_with(get_test_shared(), None, Some(capacity))
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

#[inline]
pub fn create_client_id(shared: &Shared, bg_task_channel: &BgTaskChannel) -> Id {
    let id_may_occupied = CLIENT_ID_COUNT.load();

    let new_id_count = id_may_occupied + 1;

    // 如果new_id_count在保留范围内，则从保留范围后开始
    if (0..=RESERVE_MAX_ID).contains(&new_id_count) {
        CLIENT_ID_COUNT.store(RESERVE_MAX_ID + 1);
    } else {
        CLIENT_ID_COUNT.store(new_id_count);
    };

    let client_id = shared
        .db()
        .insert_client_record(id_may_occupied, bg_task_channel.new_sender());

    if id_may_occupied != client_id {
        CLIENT_ID_COUNT.store(client_id);
    }
    client_id
}

pub fn create_reserve_client_id(shared: &Shared, bg_task_channel: &BgTaskChannel) -> Id {
    let reserve_id = RESERVE_ID.load();

    let new_reserve_id = reserve_id + 1;

    if new_reserve_id > RESERVE_MAX_ID {
        // 保留ID已经用完，也许需要扩大保留ID的范围或者这是bug导致的，需要修复
        panic!("reserve id is used up");
    }

    let client_id = shared
        .db()
        .insert_client_record(reserve_id, bg_task_channel.new_sender());

    // id必须没有被占用
    assert!(client_id == reserve_id);

    client_id
}
