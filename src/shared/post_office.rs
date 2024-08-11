// 目前Inbox存在与三种类型的任务中：Server, Wcmd Propagate, Handler，
//
// Server需要处理ShutdownServer和BlockAllClients消息，
// Wcmd Propagate需要处理Wcmd，AddReplica和RemoveReplica消息，
// Handler需要处理ShutdownServer和BlockAllClients消息。

use std::{
    fmt::Debug,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use crate::{
    frame::Resp3,
    server::{Handler, ID},
    Id,
};
use bytes::BytesMut;
use dashmap::{DashMap, Entry};
use event_listener::Event;
use flume::{Receiver, Sender};
use tokio::net::TcpStream;
use tracing::debug;

pub const SPECIAL_ID_RANGE: std::ops::Range<Id> = 0..64;

// INFO: 特殊ID的数量。每次新增特殊ID时，都需要更新这个值
pub const USED_SPECIAL_ID_COUNT: usize = 3;

pub const MAIN_ID: Id = 0;
pub const WCMD_PROPAGATE_ID: Id = 1;
pub const RUN_REPLICA_ID: Id = 2;

pub enum Letter {
    // 关闭服务。
    ShutdownServer,

    // 阻塞服务，断开所有连接。
    BlockServer {
        unblock_event: Arc<Event>,
    },

    /// 如果不是向所有client handler发送，那么需要指定ID
    /// 阻塞服务(不允许新增连接)，阻塞所有客户端连接
    ///
    ///        +-------+                   
    ///        \ Task X\                   
    ///        +---+---+                   
    ///            \    BlockAllClients          +------------+
    ///            +---------------------------->\ Main Task  \
    ///            \                             +-----+------+
    ///            \                             \
    ///            \                             \
    ///            \                             \
    ///  Wait until main task               start listening     
    ///  begins listening                        \
    ///            \                             \
    ///            \                             \
    ///            \                             \
    ///            \                             \
    ///            \                             +------------+
    ///            \    BlockAllClients          \Other Client\
    ///            +---------------------------->\ Handlers   \
    ///            \                             +------+-----+
    ///            \                             \
    ///            \                             \
    ///            \                             \
    ///            \                             \
    ///  Wait until main task                 all clients     
    ///  begins listening                   start listening
    ///            \                             \
    ///            \                             \
    ///            \                             \
    ///            \                             \
    ///            +-----------------------------+
    ///
    BlockAll {
        unblock_event: Arc<Event>,
    },
    ShutdownReplicas,

    // 用于客户端之间重定向
    Resp3(Resp3),

    Wcmd(BytesMut),
    AddReplica(Handler<TcpStream>),
    // RemoveReplica,

    // 关闭某个连接
    ShutdownClient,
    // Block

    // // do nothing
    // Null,
}

impl Debug for Letter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Letter::Resp3(resp) => write!(f, "Letter::Resp3({:?})", resp),
            Letter::ShutdownServer => write!(f, "Letter::ShutdownServer"),
            Letter::BlockServer {
                unblock_event: event,
            } => write!(f, "Letter::BlockServer({:?})", event),
            Letter::BlockAll {
                unblock_event: event,
            } => write!(f, "Letter::BlockAll({:?})", event),
            Letter::ShutdownReplicas => write!(f, "Letter::ShutdownReplicas"),
            Letter::Wcmd(cmd) => write!(f, "Letter::Wcmd({:?})", cmd),
            Letter::AddReplica(_) => write!(f, "Letter::AddReplica"),
            // Letter::RemoveReplica => write!(f, "Letter::RemoveReplica"),
            Letter::ShutdownClient => write!(f, "Letter::ShutdownClient"),
        }
    }
}

impl Clone for Letter {
    fn clone(&self) -> Self {
        match self {
            Letter::Resp3(resp) => Letter::Resp3(resp.clone()),
            Letter::ShutdownServer => Letter::ShutdownServer,
            Letter::BlockServer {
                unblock_event: event,
            } => Letter::BlockServer {
                unblock_event: event.clone(),
            },
            Letter::BlockAll {
                unblock_event: event,
            } => Letter::BlockAll {
                unblock_event: event.clone(),
            },
            Letter::ShutdownReplicas => Letter::ShutdownReplicas,
            Letter::Wcmd(cmd) => Letter::Wcmd(cmd.clone()),
            Letter::AddReplica(_) => panic!("AddReplica cannot be cloned"),
            // Letter::RemoveReplica => Letter::RemoveReplica,
            Letter::ShutdownClient => Letter::ShutdownClient,
        }
    }
}

#[derive(Debug)]
pub struct PostOffice {
    pub(super) inner: DashMap<Id, (OutBox, Inbox), nohash::BuildNoHashHasher<u64>>,
    current_id: AtomicU64,

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
    pub wcmd_outbox: Option<OutBox>,
}

impl PostOffice {
    pub fn new(wcmd_outbox: Option<OutBox>) -> Self {
        Self {
            inner: DashMap::with_hasher(nohash::BuildNoHashHasher::default()),
            current_id: AtomicU64::new(SPECIAL_ID_RANGE.end),
            wcmd_outbox,
        }
    }

    pub fn new_mailbox(&'static self) -> (Id, OutBox, Inbox) {
        let (tx, rx) = flume::unbounded();
        let mut id = self.current_id.fetch_add(1, Ordering::Relaxed);

        // 自动分配ID时，跳过特殊ID
        if SPECIAL_ID_RANGE.contains(&id) {
            id = SPECIAL_ID_RANGE.end;
            self.current_id.store(id + 1, Ordering::Relaxed);
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
                    id = self.current_id.fetch_add(1, Ordering::Relaxed);
                    rx.id = id;
                }
            }
        }

        (id, tx, rx)
    }

    #[inline]
    pub fn delay_token(&'static self) -> Inbox {
        self.new_mailbox().2.clone()
    }

    pub fn new_mailbox_with_special_id(&'static self, id: Id) -> (OutBox, Inbox) {
        debug_assert!(SPECIAL_ID_RANGE.contains(&id));

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
    pub fn get_outbox(&self, id: Id) -> Option<Sender<Letter>> {
        self.inner.get(&id).map(|entry| entry.0.clone())
    }

    #[inline]
    pub fn get_inbox(&self, id: Id) -> Option<Inbox> {
        self.inner.get(&id).map(|entry| entry.1.clone())
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
    pub async fn send_all(&self, msg: Letter) {
        match &msg {
            Letter::ShutdownServer => {
                for entry in self.inner.iter() {
                    let _ = entry.0.send_async(msg.clone()).await;
                }
            }
            Letter::BlockServer {
                unblock_event: event,
            } => {
                let _ = self
                    .inner
                    .get(&MAIN_ID)
                    .unwrap()
                    .0
                    .send_async(msg.clone())
                    .await;

                // 循环等待服务准备好监听
                loop {
                    if event.total_listeners() == 1 {
                        break;
                    }

                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }
            Letter::BlockAll {
                unblock_event: event,
            } => {
                // 阻塞服务，不允许新连接
                let _ = self
                    .inner
                    .get(&MAIN_ID)
                    .unwrap()
                    .0
                    .send_async(msg.clone())
                    .await;

                // 循环等待服务准备好监听
                loop {
                    if event.total_listeners() == 1 {
                        break;
                    }

                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }

                let src_id = ID.get();

                // 向除自己以外的client handler发送
                for entry in self.inner.iter() {
                    if SPECIAL_ID_RANGE.contains(entry.key()) || *entry.key() == src_id {
                        continue;
                    }

                    let _ = entry.0.send_async(msg.clone()).await;
                }

                // 循环等待它们(主服务以及除自己以外的连接)准备好监听
                loop {
                    if event.total_listeners() == self.get_clients_count() + 1 - 1 {
                        break;
                    }

                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }
            _ => unreachable!(),
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

pub type OutBox = Sender<Letter>;

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
