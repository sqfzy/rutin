use super::{BgTaskChannel, BgTaskSender, CLIENT_ID_COUNT};

use crate::{
    cmd::dispatch,
    conf::Conf,
    connection::{AsyncStream, Connection, FakeStream},
    shared::Shared,
    Id, Key,
};
use bytes::BytesMut;
use either::for_both;
use std::sync::Arc;
use tracing::{debug, instrument};

pub struct Handler<S: AsyncStream> {
    pub shared: Shared,
    pub conn: Connection<S>,
    pub bg_task_channel: BgTaskChannel,
    pub conf: Arc<Conf>,
    pub context: HandlerContext,
}

impl<S: AsyncStream> Handler<S> {
    #[inline]
    pub fn new(shared: Shared, stream: S, conf: Arc<Conf>) -> Self {
        let bg_task_channel = BgTaskChannel::default();

        // 获取一个有效的客户端ID
        let id_may_occupied = CLIENT_ID_COUNT.fetch_add(1);
        let client_id = shared
            .db()
            .record_client_id(id_may_occupied, bg_task_channel.new_sender());
        if id_may_occupied != client_id {
            CLIENT_ID_COUNT.store(client_id);
        }
        let client_id = CLIENT_ID_COUNT.fetch_add(1);

        Self {
            shared,
            conn: Connection::new(stream, conf.server.max_batch),
            bg_task_channel,
            conf,
            context: HandlerContext::new(client_id),
        }
    }

    #[inline]
    #[instrument(level = "debug", skip(self), fields(client_id = %self.context.client_id), err)]
    pub async fn run(&mut self) -> anyhow::Result<()> {
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
                            for_both!(dispatch(f, self).await?, resp => {
                                if let Some(respond) = resp {
                                    self.conn.write_frame(&respond).await?;
                                }
                            });
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
    }
}

impl Handler<FakeStream> {
    pub fn new_fake() -> (Self, Connection<FakeStream>) {
        Self::new_fake_with(Shared::default(), Arc::new(Conf::default()), None)
    }

    pub fn with_capacity(capacity: usize) -> (Self, Connection<FakeStream>) {
        Self::new_fake_with(Shared::default(), Arc::new(Conf::default()), Some(capacity))
    }

    pub fn new_fake_with(
        shared: Shared,
        conf: Arc<Conf>,
        capacity: Option<usize>,
    ) -> (Self, Connection<FakeStream>) {
        let ((server_tx, client_rx), (client_tx, server_rx)) = if let Some(capacity) = capacity {
            (flume::bounded(capacity), flume::bounded(capacity))
        } else {
            (flume::unbounded(), flume::unbounded())
        };

        let max_batch = conf.server.max_batch;
        (
            Self {
                shared,
                conn: Connection::new(FakeStream::new(server_tx, server_rx), max_batch),
                bg_task_channel: Default::default(),
                conf,
                context: HandlerContext::default(),
            },
            Connection::new(FakeStream::new(client_tx, client_rx), max_batch),
        )
    }
}

#[derive(Default)]
pub struct HandlerContext {
    pub client_id: Id,
    // 客户端订阅的频道
    pub subscribed_channels: Option<Vec<Key>>,
    // 是否开启缓存追踪
    pub client_track: Option<BgTaskSender>,
    // 用于缓存需要传播的写命令
    pub wcmd_buf: BytesMut,
}

impl HandlerContext {
    pub fn new(client_id: Id) -> Self {
        Self {
            client_id,
            subscribed_channels: None,
            client_track: None,
            wcmd_buf: BytesMut::with_capacity(64),
        }
    }
}
