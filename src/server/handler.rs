use super::ID;
use crate::{
    cmd::dispatch,
    conf::{AccessControl, DEFAULT_USER},
    error::RutinResult,
    frame::{CheapResp3, Resp3, StaticResp3},
    server::{AsyncStream, Connection, FakeStream, SHARED},
    shared::{Inbox, Letter, Outbox, Shared, NULL_ID},
    util::BackLog,
    Id, Key,
};
use bytes::BytesMut;
use event_listener::{listener, Event, EventListener};
use futures::pin_mut;
use std::{sync::Arc, time::Duration};
use tracing::{error, instrument};

pub struct Handler<S: AsyncStream> {
    pub shared: Shared,
    pub conn: Connection<S>,
    pub context: HandlerContext,
}

impl<S: AsyncStream> Handler<S> {
    #[inline]
    pub fn new(shared: Shared, stream: S, context: HandlerContext) -> Self {
        Self {
            context,
            conn: Connection::new(stream, shared.conf().server.max_batch),
            shared,
        }
    }

    pub fn shutdown_listener(&self) -> EventListener<> {
        self.context.shutdown.listen()
    }

    #[inline]
    #[instrument(level = "debug", skip(self), fields(client_id), err)]
    pub async fn run(mut self) -> RutinResult<()> {
        ID.scope(self.context.id, async {
            let inbox = self.context.inbox.clone();
            let inbox_fut = inbox.recv_async();
            pin_mut!(inbox_fut);

            let mut frames_buf = Vec::with_capacity(16);
            loop {
                tokio::select! {
                    biased; // 有序轮询

                    // 等待客户端请求
                    res = self.conn.read_frames_buf(&mut frames_buf) => {
                        if res?.is_none() {
                            return Ok(());
                        }

                        for f in frames_buf.iter_mut() {
                            if let Some(resp) = dispatch(f, &mut self).await? {
                                self.conn.write_frames(&resp).await?;
                            }
                        }

                        // 清空读缓冲区，所有RefMutResp3失效
                        self.conn.finish_read_frames();
                    }
                    letter = &mut inbox_fut => {
                        match letter {
                            Letter::ShutdownServer | Letter::Reset => {
                                return Ok(());
                            }
                            Letter::Resp3(resp) => {
                                self.conn.write_frame(&resp).await?;

                            }
                            Letter::BlockAll { unblock_event } => {
                                listener!(unblock_event  => listener);
                                listener.await;
                            }
                            Letter::Wcmd(_) | Letter::Psync {..} => inbox_fut.set(inbox.recv_async()),
                        }

                        inbox_fut .set(inbox.recv_async());
                    } 
                };


                // let bg_tasks = &mut self.context.background_tasks;
                // if !bg_tasks.is_empty() {
                //     // 移除已经完成的后台任务
                //     bg_tasks.retain(|task| {
                //         !task.is_finished()
                //     });
                // }
            }
        })
        .await
    }

    // 1. 从节点向主节点不会返回响应，除非是REPLCONF GETACK命令
    // 2. 从节点每秒向主节点发送REPLCONF ACK <offset>命令，以便主节点知道当前同步进度
    // 3. offset记录了主节点发来的所有命令的字节数(不仅是写命令)
    pub async fn run_replica(
        mut self,
        mut offset: tokio::sync::MutexGuard<'_, u64>,
    ) -> RutinResult<()> {
        ID.scope(self.context.id, async {
            let mut interval = tokio::time::interval(Duration::from_secs(1));

            let mut buf = itoa::Buffer::new();
            let mut resp3: Resp3<BytesMut, String> = Resp3::new_array(vec![
                Resp3::new_blob_string("REPLCONF".into()),
                Resp3::new_blob_string("ACK".into()),
                Resp3::new_blob_string(buf.format(*offset).into()),
            ]);

            let inbox = self.context.inbox.clone();
            let inbox_fut = inbox.recv_async();
            pin_mut!(inbox_fut);


            loop {
                tokio::select! {
                    biased;

                    letter = &mut inbox_fut => {
                        match letter {
                            Letter::ShutdownServer | Letter::Reset => {
                                break;
                            }
                            Letter::BlockAll { unblock_event } => {
                                listener!(unblock_event  => listener);
                                listener.await;
                            }
                            Letter::Resp3(resp) => {
                                // TODO: REPLCONF GETACK命令返回响应
                                self.conn.write_frame(&resp).await?;
                            }
                            Letter::Wcmd(_) | Letter::Psync {..} => {}
                        }

                        inbox_fut .set(inbox.recv_async());
                    } 
                    // 等待master请求(一般为master传播的写命令)
                    res = self.conn.read_frames() => {
                        if let Some(mut frames) = res? {
                            for f in frames.iter_mut() {
                                // 如果从节点返回响应的话，主节点为了区分响应和请求
                                // 就需要同步等待响应，为了避免等待，从节点只发送请
                                // 求，不会发送响应(除了REPLCONF GETACK命令)
                                let size = f.size() as u64;
                                dispatch(f, &mut self).await?;
                                *offset += size;
                            }

                            // 清空读缓冲区，所有RefMutResp3失效
                            self.conn.finish_read_frames();
                        } else {
                            return Ok(());
                        }
                    }
                    // 每秒向master发送REPLCONF ACK <offset>命令，以便master知道当前
                    // 同步进度。
                    _ = interval.tick() => {
                        // 更新offset frame
                        resp3.try_array_mut().unwrap()[2] = Resp3::new_blob_string(buf.format(*offset).into());

                        self.conn.write_frame(&resp3).await?;
                        // 等待master响应"+OK\r\n"
                        let resp = self.conn.read_frame_force().await?;
                        if resp.is_simple_error() {
                            error!("REPLCONF ACK command get error: {}", resp);
                        }
                    }
                };
            }

            // let bg_tasks = &mut self.context.background_tasks;
            // if !bg_tasks.is_empty() {
            //     // 移除已经完成的后台任务
            //     bg_tasks.retain(|task| {
            //         !task.is_finished()
            //     });
            // }

            Ok(())
        })
        .await
    }

    #[inline]
    pub async fn dispatch(&mut self, cmd_frame: &mut StaticResp3) -> RutinResult<Option<CheapResp3>> {
        ID.scope(self.context.id, dispatch(cmd_frame, self)).await
    }
}

impl<S: AsyncStream> Drop for Handler<S> {
    fn drop(&mut self) {
        let Handler { shared, context: HandlerContext {id, /* background_tasks: background_task , */ .. }  ,.. } = self;
        // 移除注册的信箱
        shared.post_office().remove(id);
        
        // // 中断后台任务
        // for task in background_task {
        //     task.abort();
        // }
    } 
}

#[derive(Debug)]
pub struct HandlerContext {
    pub id: Id,
    pub ac: Arc<AccessControl>,
    pub user: bytes::Bytes,

    pub inbox: Inbox,
    pub outbox: Outbox,

    // 客户端订阅的频道
    pub subscribed_channels: Vec<Key>,

    // 是否开启缓存追踪
    pub client_track: Option<Outbox>,

    // // 后台任务
    // pub background_tasks: Vec<tokio::task::AbortHandle>,

    // 用于缓存需要传播的写命令
    pub wcmd_buf: BytesMut,
    pub back_log: Option<BackLog>,

    pub shutdown: Event,
}

impl HandlerContext {
    pub fn new(shared: Shared, id: Id, outbox: Outbox, inbox: Inbox) -> Self {
        // 使用默认ac
        let ac = shared.conf().security.default_ac.load_full();

        Self::with_ac(id, outbox, inbox, ac, DEFAULT_USER)
    }

    pub fn with_ac(
        id: Id,
        outbox: Outbox,
        inbox: Inbox,
        ac: Arc<AccessControl>,
        user: bytes::Bytes,
    ) -> Self {
        Self {
            id,
            ac,
            user,
            inbox,
            outbox,
            subscribed_channels: Vec::new(),
            client_track: None,
            wcmd_buf: BytesMut::new(),
            back_log: None,
            shutdown: Event::new(),
        }
    }

    // pub fn clear(&mut self) {
    //     self.subscribed_channels = None;
    //     self.client_track = None;
    //     self.wcmd_buf.clear();
    // }
}

// impl Clone for HandlerContext {
//     fn clone(&self) -> Self {
//         assert!(self.back_log.is_none());
//
//         Self {
//             id: self.id,
//             ac: self.ac.clone(),
//             user: self.user.clone(),
//             inbox: self.inbox.clone(),
//             outbox: self.outbox.clone(),
//             subscribed_channels: self.subscribed_channels.clone(),
//             client_track: self.client_track.clone(),
//             wcmd_buf: self.wcmd_buf.clone(),
//             back_log: None,
//         }
//     }
// }

pub type FakeHandler = Handler<FakeStream>;

impl Handler<FakeStream> {
    pub fn new_fake() -> (Self, Connection<FakeStream>) {
        Self::new_fake_with(*SHARED, None, None)
    }

    pub fn with_capacity(capacity: usize) -> (Self, Connection<FakeStream>) {
        Self::new_fake_with(*SHARED, None, Some(capacity))
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
            let (outbox, inbox) = shared.post_office().new_mailbox_with_special_id(NULL_ID);
            HandlerContext::new(shared, NULL_ID, outbox, inbox)
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
