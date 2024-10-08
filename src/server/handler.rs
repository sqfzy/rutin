use super::ID;
use crate::{
    cmd::{dispatch, CmdUnparsed},
    conf::{AccessControl, DEFAULT_USER},
    error::RutinResult,
    frame::{CheapResp3, Resp3, StaticResp3},
    server::{AsyncStream, Connection, FakeStream, SHARED},
    shared::{db::Key, Letter, MailboxGuard, Outbox, Shared, NULL_ID},
    util::BackLog,
    Id,
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

    pub fn shutdown_listener(&self) -> EventListener {
        self.context.shutdown.listen()
    }

    #[inline]
    #[instrument(level = "debug", skip(self), fields(client_id), err)]
    pub async fn run(mut self) -> RutinResult<()> {
        let res = ID.scope(self.context.id, async {
            let inbox = self.context.mailbox.inbox.clone();
            let inbox_fut = inbox.recv_async();
            pin_mut!(inbox_fut);

            let mut frames_buf = Vec::with_capacity(16);
            loop {
                tokio::select! {
                    biased; // 有序轮询

                    letter = &mut inbox_fut => {
                        // mailbox没有drop，所以不可能出现Err
                        match letter.unwrap() {
                            Letter::Shutdown => {
                                return Ok(());
                            }
                            Letter::Resp3(resp) => {
                                self.conn.write_frame(&resp).await?;

                            }
                            Letter::Block { unblock_event } => {
                                listener!(unblock_event  => listener);
                                listener.await;
                            }
                            Letter::Wcmd(_) | Letter::Psync {..} | Letter::ModifyShared(..) => inbox_fut.set(inbox.recv_async()),
                        }

                        inbox_fut .set(inbox.recv_async());
                    }
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
                        self.conn.finish_read();
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
        .await;

        if res.is_err() {
            eprintln!("reader_buf={:?}", self.conn.reader_buf.get_ref());
        }

        res
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

            let inbox = self.context.mailbox.inbox.clone();
            let inbox_fut = inbox.recv_async();
            pin_mut!(inbox_fut);


            loop {
                tokio::select! {
                    biased;

                    letter = &mut inbox_fut => {
                        match letter.unwrap() {
                            Letter::Shutdown => {
                                break;
                            }
                            Letter::Block { unblock_event } => {
                                listener!(unblock_event  => listener);
                                listener.await;
                            }
                            Letter::Resp3(resp) => {
                                // TODO: REPLCONF GETACK命令返回响应
                                self.conn.write_frame(&resp).await?;
                            }
                            Letter::Wcmd(_) | Letter::Psync {..} | Letter::ModifyShared(..) => {}
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
                            self.conn.finish_read();
                        } else {
                            return Ok(());
                        }
                    }
                    // 每秒向master发送REPLCONF ACK <offset>命令，以便master知道当前
                    // 同步进度。
                    _ = interval.tick() => {
                        // 更新offset frame
                        resp3.as_array_mut().unwrap()[2] = Resp3::new_blob_string(buf.format(*offset).into());

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
    pub async fn dispatch(
        &mut self,
        cmd_frame: &mut StaticResp3,
    ) -> RutinResult<Option<CheapResp3>> {
        ID.scope(self.context.id, dispatch(cmd_frame, self)).await
    }
}

// impl<S: AsyncStream> Drop for Handler<S> {
//     fn drop(&mut self) {
//         let Handler {
//             shared,
//             context:
//                 HandlerContext {
//                     id, /* background_tasks: background_task , */
//                     ..
//                 },
//             ..
//         } = self;
//
//         // // 中断后台任务
//         // for task in background_task {
//         //     task.abort();
//         // }
//     }
// }

#[derive(Debug)]
pub struct HandlerContext {
    pub id: Id,
    pub ac: Arc<AccessControl>,
    pub user: bytes::Bytes,

    pub mailbox: MailboxGuard,

    // 客户端订阅的频道
    pub subscribed_channels: Vec<Key>,

    // 是否开启缓存追踪
    pub client_track: Option<ClientTrack>,

    // // 后台任务
    // pub background_tasks: Vec<tokio::task::AbortHandle>,

    // 用于缓存需要传播的写命令
    pub wcmd_buf: WcmdBuf,
    pub back_log: Option<BackLog>,

    pub shutdown: Event,
}

impl HandlerContext {
    pub fn new(shared: Shared, id: Id, mailbox: MailboxGuard) -> Self {
        // 使用默认ac
        let ac = shared.conf().security.default_ac.load_full();

        Self::with_ac(id, mailbox, ac, DEFAULT_USER)
    }

    pub fn with_ac(
        id: Id,
        mailbox: MailboxGuard,
        ac: Arc<AccessControl>,
        user: bytes::Bytes,
    ) -> Self {
        Self {
            id,
            ac,
            user,
            mailbox,
            subscribed_channels: Vec::new(),
            client_track: None,
            wcmd_buf: WcmdBuf {
                buf: BytesMut::with_capacity(1024),
                len: 0,
            },
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

#[derive(Debug)]
pub struct WcmdBuf {
    buf: BytesMut,
    len: usize,
}

impl WcmdBuf {
    #[inline]
    pub fn buffer_wcmd(&mut self, cmd_name: &'static str, cmd: &CmdUnparsed) {
        use crate::frame::{ARRAY_PREFIX, CRLF};
        use bytes::BufMut;

        let buf = &mut self.buf;
        buf.put_u8(ARRAY_PREFIX);
        buf.put_slice(itoa::Buffer::new().format(cmd.len() + 1).as_bytes());
        buf.put_slice(CRLF);

        let cmd_name_frame: Resp3<&[u8], String> = Resp3::new_blob_string(cmd_name.as_bytes());
        cmd_name_frame.encode_buf(buf);

        for frame in cmd.inner.iter() {
            frame.encode_buf(buf);
        }
    }

    pub fn rollback(&mut self) {
        self.buf.truncate(self.len);
    }
}

impl<S: AsyncStream> Handler<S> {
    #[inline]
    pub async fn may_send_wmcd_buf(&mut self) {
        let wcmd_buf = &mut self.context.wcmd_buf;

        // 如果使用了pipline则该批命令视为一个整体命令。当最后一个命令执行完毕后，才发送写命令，
        // 避免性能瓶颈。如果执行过程中程序崩溃，则客户端会报错，用户会视为该批命令没有执行成
        // 功，也不会传播该批命令，符合一致性。
        if self.conn.unhandled_count() <= 1 {
            self.shared
                .post_office()
                .send_wcmd(wcmd_buf.buf.split())
                .await;
            wcmd_buf.len = 0;
        } else {
            wcmd_buf.len = wcmd_buf.buf.len();
        }
    }
}

#[derive(Debug)]
pub struct ClientTrack {
    pub tracker: Outbox,
    pub keys: Vec<Key>,
}

impl ClientTrack {
    pub fn new(tracker: Outbox) -> Self {
        Self {
            tracker,
            keys: Vec::new(),
        }
    }

    pub fn rollback(&mut self) {
        self.keys.clear();
    }
}

impl<S: AsyncStream> Handler<S> {
    #[inline]
    pub async fn track(&mut self) {
        use crate::{
            error::RutinError,
            server::NEVER_EXPIRE,
            shared::db::{ObjectValue, WriteEvent},
        };
        use std::collections::VecDeque;
        use tokio::time::Instant;

        let Handler {
            shared,
            context: HandlerContext { client_track, .. },
            ..
        } = self;
        let ClientTrack { tracker, keys } = client_track
            .as_mut()
            .expect("tracker must exist while calling track()");
        let db = shared.db();

        while let Some(key) = keys.pop() {
            let tracker = tracker.clone();

            if let Ok(obj) = db.object_entry(&key).await {
                let invalidation = CheapResp3::new_array(VecDeque::from([
                    Resp3::new_blob_string("INVALIDATE".into()),
                    Resp3::new_blob_string(key.clone().into()),
                ]));

                let callback = move |_obj: &mut ObjectValue, _ex: Instant| {
                    if tracker.send(Letter::Resp3(invalidation.clone())).is_err() {
                        // 发送失败，证明客户端已经断开连接，事件完成
                        Ok(())
                    } else {
                        Err(RutinError::Null)
                    }
                };

                obj.add_write_event(WriteEvent::FnMut {
                    deadline: *NEVER_EXPIRE,
                    callback: Box::new(callback),
                });
            }
        }

        keys.clear();
    }
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
            let mailbox = shared.post_office().register_special_mailbox(NULL_ID);
            HandlerContext::new(shared, NULL_ID, mailbox)
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
