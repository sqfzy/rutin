// 目前Inbox存在与三种类型的任务中：Server, Wcmd Propagate, Handler，
//
// Server需要处理ShutdownServer和BlockAllClients消息，
// Wcmd Propagate需要处理Wcmd，AddReplica和RemoveReplica消息，
// Handler需要处理ShutdownServer和BlockAllClients消息。

use core::panic;
use std::{fmt::Debug, sync::Arc};

use crate::{
    conf::Conf,
    frame::Resp3,
    server::{AsyncStream, Handler, ID},
    Id,
};
use bytes::{Bytes, BytesMut};
use dashmap::{DashMap, Entry};
use event_listener::Event;
use flume::{Receiver, Sender};
use tokio::net::{TcpSocket, TcpStream};
use tracing::{debug, instrument};

pub const NULL_ID: Id = 0; // 用于测试以及创建FakeHandler。该ID没有统一对应的mailbox
pub const DELAY_ID: Id = 1; // 用于延迟任务

pub const SPECIAL_ID_RANGE: std::ops::Range<Id> = 0..64;

// INFO: 特殊ID的数量。每次新增特殊ID时，都需要更新这个值
pub const USED_SPECIAL_ID_COUNT: usize = 4;

pub const MAIN_ID: Id = 2;
pub const AOF_ID: Id = 3;
pub const SET_MASTER_ID: Id = 4;
pub const SET_REPLICA_ID: Id = 5;

#[derive(Debug)]
pub struct PostOffice {
    pub(super) inner: DashMap<Id, (Outbox, Inbox), nohash::BuildNoHashHasher<u64>>,

    /// 传播写命令到aof或replica。由于需要频繁使用该OutBox，因此直接保存在字段中
    ///
    /// **编译期确定是否需要传播**：是否需要传播wcmd取决于是否有aof或replica。aof
    /// 在编译期就确定了，而replica则是运行时(在多线程中)可变的。为了避免额外的
    /// 同步开销(每次执行写命令都需要判断是否需要传播，这会使得开销变得很大)，增
    /// 加了conf.server.standalone在编译期决定是否需要replica。当
    /// **conf.server.standalone为true且没有开启aof时**，该字段为None，不允许添加从节
    /// 点；否则为Some，无论是否存在从节点，都会传播wcmd。
    ///
    /// **wcmd仅传播一次**：考虑到性能原因，写命令最好是只发送一次到一个task中处
    /// 理，因此aof和replica需要在同一个task中处理。但是replica的数目是运行时可
    /// 变的，为避免额外得同步措施，wcmd_propagator既可以传播wcmd，也可以发送一个
    /// replica的handler
    aof_outbox: Option<Outbox>,
    set_master_outbox: Option<Outbox>,
}

impl PostOffice {
    pub fn new(aof_outbox: Option<Outbox>, set_master_outbox: Option<Outbox>) -> Self {
        Self {
            inner: DashMap::with_hasher(nohash::BuildNoHashHasher::default()),
            aof_outbox,
            set_master_outbox,
        }
    }

    pub fn new_mailbox(&'static self, mut id: Id) -> (Id, Outbox, Inbox) {
        let (tx, rx) = flume::unbounded();

        // 跳过特殊ID
        if SPECIAL_ID_RANGE.contains(&id) {
            id = SPECIAL_ID_RANGE.end;
        }

        let mut rx = Inbox {
            inner: rx,
            post_office: self,
            id,
        };

        loop {
            match self.inner.entry(id) {
                Entry::Vacant(entry) => {
                    entry.insert((tx.clone(), rx.clone()));
                    break;
                }
                Entry::Occupied(_) => {
                    id += 1;
                    rx.id = id;
                }
            }
        }

        (id, tx, rx)
    }

    pub fn reset(&'static mut self, conf: &Conf) {
        match (
            conf.aof.is_some() && self.aof_outbox.is_none(),
            conf.master.is_some() && self.set_master_outbox.is_none(),
        ) {
            (true, true) => {
                let (aof_outbox, aof_inbox) = flume::unbounded();
                let (set_master_outbox, set_master_inbox) = flume::unbounded();

                self.aof_outbox = Some(aof_outbox.clone());
                self.set_master_outbox = Some(set_master_outbox.clone());

                self.inner.insert(
                    AOF_ID,
                    (
                        aof_outbox,
                        Inbox {
                            inner: aof_inbox,
                            post_office: self as &'static Self,
                            id: AOF_ID,
                        },
                    ),
                );

                self.inner.insert(
                    SET_MASTER_ID,
                    (
                        set_master_outbox,
                        Inbox {
                            inner: set_master_inbox,
                            post_office: self as &'static Self,
                            id: SET_MASTER_ID,
                        },
                    ),
                );
            }
            (true, false) => {
                let (outbox, inbox) = flume::unbounded();

                self.aof_outbox = Some(outbox.clone());
                self.inner.insert(
                    AOF_ID,
                    (
                        outbox,
                        Inbox {
                            inner: inbox,
                            post_office: self as &'static Self,
                            id: AOF_ID,
                        },
                    ),
                );
            }
            (false, true) => {
                let (outbox, inbox) = flume::unbounded();

                self.set_master_outbox = Some(outbox.clone());
                self.inner.insert(
                    SET_MASTER_ID,
                    (
                        outbox,
                        Inbox {
                            inner: inbox,
                            post_office: self as &'static Self,
                            id: SET_MASTER_ID,
                        },
                    ),
                );
            }
            _ => {}
        }
    }

    #[inline]
    pub fn delay_token(&'static self) -> Inbox {
        self.new_mailbox_with_special_id(DELAY_ID).1.clone()
    }

    pub fn new_mailbox_with_special_id(&'static self, id: Id) -> (Outbox, Inbox) {
        debug_assert!(SPECIAL_ID_RANGE.contains(&id));

        if id == NULL_ID {
            let (outbox, inbox) = flume::unbounded();
            let inbox = Inbox {
                inner: inbox,
                post_office: self,
                id,
            };
            return (outbox, inbox);
        }

        let (tx, rx) = flume::unbounded();

        let rx = Inbox {
            inner: rx,
            post_office: self,
            id,
        };

        self.inner.insert(id, (tx.clone(), rx.clone()));

        (tx, rx)
    }

    #[inline]
    pub fn get_clients_count(&self) -> usize {
        self.inner.len() - USED_SPECIAL_ID_COUNT
    }

    #[inline]
    pub fn get_outbox(&self, id: Id) -> Option<Outbox> {
        self.inner.get(&id).map(|entry| entry.0.clone())
    }

    #[inline]
    pub fn get_inbox(&self, id: Id) -> Option<Inbox> {
        self.inner.get(&id).map(|entry| entry.1.clone())
    }

    #[inline]
    pub fn need_send_wcmd(&self) -> bool {
        self.aof_outbox.is_some() || self.set_master_outbox.is_some()
    }

    #[inline]
    pub fn remove(&self, id: Id) -> Option<(Id, (Outbox, Inbox))> {
        self.inner.remove(&id)
    }

    #[inline]
    pub async fn send_wcmd(&self, wcmd: BytesMut) {
        // Aof和SetMaster任务都需要处理写命令，但Aof只需要&wcmd，为了避免clone，
        // 如果存在Aof任务，则由Aof任务在使用完&wcmd后发送给SetMaster任务
        if let Some(outbox) = &self.aof_outbox {
            outbox.send_async(Letter::Wcmd(wcmd)).await.ok();
        } else if let Some(outbox) = &self.set_master_outbox {
            outbox.send_async(Letter::Wcmd(wcmd)).await.ok();
        }
    }

    pub async fn wait_shutdown_complete(&self) {
        loop {
            if self.inner.is_empty() {
                break;
            }

            if tracing::enabled!(tracing::Level::DEBUG) {
                for entry in self.inner.iter() {
                    debug!("wait id={} shutdown", entry.key());
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
        }
    }

    // 广播消息。如果想要单播，可以使用get_outbox
    #[instrument(level = "debug", skip(self))]
    pub async fn send_all(&self, msg: Letter) {
        match &msg {
            Letter::ShutdownServer => {
                for entry in self.inner.iter() {
                    entry.0.send_async(msg.clone()).await.ok();
                }
            }
            Letter::Reset => {
                // 服务还未初始化
                if !self.inner.contains_key(&MAIN_ID) {
                    return;
                }

                // 重置服务，关闭除主任务以外的所有任务
                for entry in self.inner.iter() {
                    if *entry.key() == MAIN_ID {
                        continue;
                    }

                    entry.0.send_async(msg.clone()).await.ok();
                }

                // 循环等待主任务以外的任务关闭
                loop {
                    if self.inner.len() == 1 {
                        break;
                    }

                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }

                // 向主任务发送Reset，由主任务重置Shared
                self.get_outbox(MAIN_ID)
                    .unwrap()
                    .send_async(msg.clone())
                    .await
                    .ok();
            }
            Letter::BlockAll {
                unblock_event: event,
            } => {
                // 不存在任何任务
                if self.inner.is_empty() {
                    return;
                }

                // 阻塞服务，不允许新连接
                self.inner
                    .get(&MAIN_ID)
                    .unwrap()
                    .0
                    .send_async(msg.clone())
                    .await
                    .ok();

                // 循环等待服务准备好监听
                loop {
                    if event.total_listeners() == 1 {
                        break;
                    }

                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }

                let src_id = ID.get();
                let len = self.inner.len();

                // 向除自己以外的task发送
                for entry in self.inner.iter() {
                    if *entry.key() == src_id {
                        continue;
                    }

                    entry.0.send_async(msg.clone()).await.ok();
                }

                // 循环等待它们(主服务以及除自己以外的连接)准备好监听
                loop {
                    if event.total_listeners() == len - 1 {
                        break;
                    }

                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }
            _ => unreachable!(),
        }
    }
}

pub enum Letter {
    // 关闭服务。
    ShutdownServer,
    // 重置服务。关闭除主任务以外的所有任务，并重置Shared
    Reset,

    // // 阻塞服务，断开所有连接。
    // BlockServer {
    //     unblock_event: Arc<Event>,
    // },
    /// 阻塞服务(不允许新增连接)以及所有客户端连接。
    /// 阻塞后必须解除阻塞，否则服务无法正常关闭。
    ///
    ///```plaintext
    ///
    ///        +--------+                   
    ///        | Task X |                   
    ///        +---+----+                   
    ///            |    BlockAll                 +-----------+
    ///            +---------------------------->| Main Task |
    ///            |                             +-----+-----+
    ///            |                             |
    ///            |                             |
    ///            |                             |
    ///  Wait until main task                  main task
    ///  begins listening                   start listening
    ///            |                             |
    ///            |                             |
    ///            |                             |
    ///            |                             |
    ///            |    BlockAll                 +------------+
    ///            +---------------------------->| Other Task |
    ///            |                             +------------+
    ///            |                             |
    ///            |                             |
    ///            |                             |
    ///            |                             |
    ///  Wait until all task                  all task     
    ///  begins listening                   start listening
    ///            |                             |
    ///            |                             |
    ///            |                             |
    ///            |                             |
    ///            +-----------------------------+
    ///```
    BlockAll {
        unblock_event: Arc<Event>,
    },
    // ShutdownReplicas,

    // 用于客户端之间重定向
    Resp3(Resp3),

    Wcmd(BytesMut),
    Psync {
        handle_replica: Handler<TcpStream>,
        repl_id: Bytes,
        repl_offset: u64,
    },
    // RemoveReplica,

    // 关闭某个任务
    ShutdownTask,
    // Block

    // // do nothing
    // Null,
}

impl Debug for Letter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Letter::Resp3(resp) => write!(f, "Letter::Resp3({:?})", resp),
            Letter::ShutdownServer => write!(f, "Letter::ShutdownServer"),
            Letter::Reset => write!(f, "Letter::Reset"),
            Letter::BlockAll {
                unblock_event: event,
            } => write!(f, "Letter::BlockAll({:?})", event),
            Letter::Wcmd(cmd) => write!(f, "Letter::Wcmd({:?})", cmd),
            Letter::Psync {
                repl_id,
                repl_offset,
                ..
            } => write!(f, "Letter::Psync({:?}, {:?})", repl_id, repl_offset),
            // Letter::RemoveReplica => write!(f, "Letter::RemoveReplica"),
            Letter::ShutdownTask => write!(f, "Letter::ShutdownClient"),
        }
    }
}

impl Clone for Letter {
    fn clone(&self) -> Self {
        match self {
            Letter::Resp3(resp) => Letter::Resp3(resp.clone()),
            Letter::ShutdownServer => Letter::ShutdownServer,
            Letter::Reset => Letter::Reset,
            Letter::BlockAll {
                unblock_event: event,
            } => Letter::BlockAll {
                unblock_event: event.clone(),
            },
            Letter::Wcmd(cmd) => Letter::Wcmd(cmd.clone()),
            Letter::Psync { .. } => unreachable!(),
            // Letter::RemoveReplica => Letter::RemoveReplica,
            Letter::ShutdownTask => Letter::ShutdownTask,
        }
    }
}

impl Letter {
    // TODO: 使用过程宏自动生成
    pub fn as_resp3_unchecked(&self) -> &Resp3 {
        match self {
            Letter::Resp3(resp) => resp,
            _ => panic!("Letter is not Resp3"),
        }
    }
}

pub type Outbox = Sender<Letter>;

pub struct Inbox {
    pub inner: Receiver<Letter>,
    pub(super) post_office: &'static PostOffice,
    pub(super) id: Id,
}

impl Inbox {
    pub async fn recv_async(&self) -> Letter {
        self.inner.recv_async().await.unwrap()
    }
}

impl Drop for Inbox {
    fn drop(&mut self) {
        // 如果当前receiver drop之后，只有一个receiver了，那么删除对应的sender
        if self.inner.receiver_count() <= 2 {
            self.post_office.inner.remove(&self.id);
        }
    }
}

impl Clone for Inbox {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            post_office: self.post_office,
            id: self.id,
        }
    }
}

impl Debug for Inbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inbox").field("id", &self.id).finish()
    }
}

pub struct UnblockEvent(pub Arc<Event>);

// WARN: 必须执行该步骤否则阻塞的服务无法正常关闭，因此不能设置panic="abort"
impl Drop for UnblockEvent {
    fn drop(&mut self) {
        self.0.notify(usize::MAX);
    }
}
