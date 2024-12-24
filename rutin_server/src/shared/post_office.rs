use crate::{frame::CheapResp3, server::Handler, Id};
use bytes::BytesMut;
use bytestring::ByteString;
use rutin_dashmap::{DashMap, Entry};
use event_listener::{listener, Event};
use flume::{Receiver, Sender};
use std::collections::HashMap;
use std::{
    fmt::Debug,
    sync::{Arc, RwLock},
};
use tokio::{net::TcpStream, time::Duration};
use tracing::instrument;

pub const NULL_ID: Id = 0; // 用于测试以及创建FakeHandler。该ID没有统一对应的mailbox

pub const SPECIAL_ID_RANGE: std::ops::Range<Id> = 0..64;

// // INFO: 特殊ID的数量。每次新增特殊ID时，都需要更新这个值
// pub const USED_SPECIAL_ID_COUNT: usize = 4;

pub const CTRL_C_ID: Id = 1;
pub const MAIN_ID: Id = 2;
pub const AOF_ID: Id = 3;
pub const SET_MASTER_ID: Id = 4;
pub const SET_REPLICA_ID: Id = 5;
pub const EXPIRATION_EVICT_ID: Id = 6;

pub trait MailboxId {
    fn get_id(&self) -> Id;

    fn set_id(&mut self, id: Id);
}

// Implement the trait for Id (u64)
impl MailboxId for Id {
    fn get_id(&self) -> Id {
        *self
    }

    fn set_id(&mut self, id: Id) {
        *self = id;
    }
}

// Implement the trait for &mut Id
impl MailboxId for &mut Id {
    fn get_id(&self) -> Id {
        **self
    }

    fn set_id(&mut self, id: Id) {
        **self = id;
    }
}

// PostOffice负责任务之间的相互通信
//
// 任务开始时，需要注册一个mailbox，得到mailbox guard
// 任务结束时，需要drop mailbox guard，注销mailbox。AOF和SET MASTER任务还需要
// 将aof_outbox和set_master_outbox设置为None
//
// 任何任务都不应当长时间不查看自己的inbox是否有消息。接收inbox消息应当是最高
// 优先级的
//
// 创建普通任务(处理客户端请求)时，按序注册一个ID
// 创建特殊任务时，直接注册一个特殊ID。特殊任务是唯一的
#[derive(Debug)]
pub struct PostOffice {
    pub(super) normal_mailboxs: DashMap<Id, (Outbox, Inbox), nohash::BuildNoHashHasher<u64>>,
    pub(super) special_mailboxs:
        RwLock<HashMap<Id, (Outbox, Inbox), nohash::BuildNoHashHasher<u64>>>,
}

impl PostOffice {
    pub fn new() -> Self {
        Self {
            normal_mailboxs: DashMap::with_hasher(nohash::BuildNoHashHasher::default()),
            special_mailboxs: RwLock::new(HashMap::with_hasher(
                nohash::BuildNoHashHasher::default(),
            )),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.normal_mailboxs.is_empty() && self.special_mailboxs.read().unwrap().is_empty()
    }

    pub fn contains(&self, id: Id) -> bool {
        if SPECIAL_ID_RANGE.contains(&id) {
            self.special_mailboxs.read().unwrap().contains_key(&id)
        } else {
            self.normal_mailboxs.contains_key(&id)
        }
    }

    pub fn remove(&self, id: Id) {
        if SPECIAL_ID_RANGE.contains(&id) {
            self.special_mailboxs.write().unwrap().remove(&id);
        } else {
            self.normal_mailboxs.remove(&id);
        }
    }

    // 如果提供的id已经存在mailbox：
    // 对于SPECIAL_ID_RANGE以外的id会寻找一个未使用的ID，然后注册一个mailbox
    // 对于SPECIAL_ID_RANGE内的id则会直接覆盖
    pub fn register_mailbox(&'static self, mut id: impl MailboxId) -> MailboxGuard {
        let mut id_temp = id.get_id();

        let (outbox, inbox) = flume::unbounded();

        if SPECIAL_ID_RANGE.contains(&id_temp) {
            let mut special_mailboxs = self.special_mailboxs.write().unwrap();

            if id_temp != NULL_ID {
                special_mailboxs.insert(id_temp, (outbox.clone(), inbox.clone()));
            }

            MailboxGuard {
                post_office: self,
                id: id_temp,
                outbox,
                inbox,
            }
        } else {
            // 找到一个未使用的ID
            loop {
                match self.normal_mailboxs.entry(id_temp) {
                    Entry::Vacant(entry) => {
                        entry.insert((outbox.clone(), inbox.clone()));
                        break;
                    }
                    Entry::Occupied(_) => {
                        id_temp += 1;
                    }
                }
            }

            id.set_id(id_temp);

            MailboxGuard {
                post_office: self,
                id: id_temp,
                outbox,
                inbox,
            }
        }
    }

    #[inline]
    pub fn delay_shutdown_token(&'static self) -> MailboxGuard {
        let mut id = fastrand::u64(SPECIAL_ID_RANGE.end..);
        self.register_mailbox(&mut id)
    }

    // #[inline]
    // pub fn get_clients_count(&self) -> usize {
    //     self.inner.len() - USED_SPECIAL_ID_COUNT
    // }

    #[inline]
    pub fn get_mailbox(&self, id: Id) -> Option<(Outbox, Inbox)> {
        if SPECIAL_ID_RANGE.contains(&id) {
            self.special_mailboxs.read().unwrap().get(&id).cloned()
        } else {
            self.normal_mailboxs
                .get(&id)
                .map(|entry| entry.value().clone())
        }
    }

    #[inline]
    pub fn get_outbox(&self, id: Id) -> Option<Outbox> {
        if SPECIAL_ID_RANGE.contains(&id) {
            self.special_mailboxs
                .read()
                .unwrap()
                .get(&id)
                .map(|entry| entry.0.clone())
        } else {
            self.normal_mailboxs.get(&id).map(|entry| entry.0.clone())
        }
    }

    #[inline]
    pub fn get_inbox(&self, id: Id) -> Option<Inbox> {
        if SPECIAL_ID_RANGE.contains(&id) {
            self.special_mailboxs
                .read()
                .unwrap()
                .get(&id)
                .map(|entry| entry.1.clone())
        } else {
            self.normal_mailboxs.get(&id).map(|entry| entry.1.clone())
        }
    }

    #[inline]
    pub fn need_send_wcmd(&self) -> Option<Outbox> {
        if let Some(outbox) = self.get_outbox(AOF_ID) {
            Some(outbox)
        } else {
            self.get_outbox(SET_MASTER_ID)
        }
    }

    pub async fn wait_task_shutdown(&self, id: Id) {
        let mut interval = tokio::time::interval(Duration::from_millis(300));

        while self.contains(id) {
            interval.tick().await;
        }
    }

    #[instrument(level = "debug", skip(self))]
    pub fn send_shutdown(&self, id: Id) {
        if let Some(outbox) = self.get_outbox(id) {
            outbox.send(Letter::Shutdown).ok();
        }
    }

    #[instrument(level = "debug", skip(self))]
    pub fn send_shutdown_server(&self) {
        for entry in self.normal_mailboxs.iter() {
            entry.0.send(Letter::Shutdown).ok();
        }

        for entry in self.special_mailboxs.read().unwrap().iter() {
            entry.1 .0.send(Letter::Shutdown).ok();
        }
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn send_block(&self, id: Id) -> BlockGuard {
        let event = Arc::new(Event::new());
        {
            // 向指定的task发送
            if let Some(outbox) = self.get_outbox(id) {
                listener!(event => listener);

                outbox
                    .send_async(Letter::Block {
                        unblock_event: event.clone(),
                    })
                    .await
                    .ok();

                // 等待task返回响应，证明已经阻塞
                listener.await;
            }
        }

        BlockGuard(event)
    }

    // send_block_all执行之后，Shared不再被访问
    #[instrument(level = "debug", skip(self))]
    pub async fn send_block_all(&self, src_id: Id) -> BlockGuard {
        let event = Arc::new(Event::new());

        // 先阻塞主任务，避免有新的任务加入
        {
            listener!(event => listener);
            if let Some(outbox) = self.get_outbox(MAIN_ID) {
                outbox
                    .send_async(Letter::Block {
                        unblock_event: event.clone(),
                    })
                    .await
                    .ok();
            }
            listener.await;
        }

        let mut count = 0;
        // 向除自己以外的task发送
        for entry in self.normal_mailboxs.iter() {
            let key = *entry.key();
            if key == src_id || key == MAIN_ID {
                continue;
            }

            entry
                .0
                .send_async(Letter::Block {
                    unblock_event: event.clone(),
                })
                .await
                .ok();

            count += 1;
        }

        // 等待所有task返回响应，证明已经阻塞
        for _ in 0..count {
            listener!(event => listener);
            listener.await;
        }

        BlockGuard(event)
    }
}

impl Default for PostOffice {
    fn default() -> Self {
        Self::new()
    }
}

pub struct BlockGuard(pub Arc<Event>);

// WARN: 必须执行该步骤否则阻塞的服务无法正常关闭，因此不能设置panic="abort"
impl Drop for BlockGuard {
    fn drop(&mut self) {
        self.0.notify(usize::MAX);
    }
}

pub enum Letter {
    // 关闭服务。
    Shutdown,
    // // 重置服务。关闭除主任务以外的所有任务，并重置Shared(必须保证没有其它任务
    // // 在使用Shared，即保证只有主任务一个任务在运行)
    // ModifyShared(Box<dyn FnOnce(&mut SharedInner) + Send>),
    Block {
        unblock_event: Arc<Event>,
    },

    // 用于客户端之间重定向
    Resp3(CheapResp3),

    Wcmd(BytesMut),
    Psync {
        handle_replica: Handler<TcpStream>,
        repl_id: ByteString,
        repl_offset: u64,
    },
    // RemoveReplica,

    // Block

    // // do nothing
    // Null,
}

impl Debug for Letter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Letter::Resp3(resp) => write!(f, "Letter::Resp3({:?})", resp),
            Letter::Shutdown => write!(f, "Letter::ShutdownServer"),
            // Letter::ModifyShared(..) => write!(f, "Letter::ModifyShared"),
            Letter::Block {
                unblock_event: event,
            } => write!(f, "Letter::BlockAll({:?})", event),
            Letter::Wcmd(cmd) => write!(f, "Letter::Wcmd({:?})", cmd),
            Letter::Psync {
                repl_id,
                repl_offset,
                ..
            } => write!(f, "Letter::Psync({:?}, {:?})", repl_id, repl_offset),
            // Letter::RemoveReplica => write!(f, "Letter::RemoveReplica"),
        }
    }
}

impl Clone for Letter {
    fn clone(&self) -> Self {
        match self {
            Letter::Resp3(resp) => Letter::Resp3(resp.clone()),
            Letter::Shutdown => Letter::Shutdown,
            Letter::Block {
                unblock_event: event,
            } => Letter::Block {
                unblock_event: event.clone(),
            },
            Letter::Wcmd(cmd) => Letter::Wcmd(cmd.clone()),
            Letter::Psync { .. } /* | Letter::ModifyShared(..) */ => unreachable!(),
            // Letter::RemoveReplica => Letter::RemoveReplica,
        }
    }
}

impl Letter {
    // TODO: 使用过程宏自动生成
    pub fn as_resp3_unchecked(&self) -> &CheapResp3 {
        match self {
            Letter::Resp3(resp) => resp,
            _ => panic!("Letter is not Resp3"),
        }
    }

    pub fn into_resp3_unchecked(self) -> CheapResp3 {
        match self {
            Letter::Resp3(resp) => resp,
            _ => panic!("Letter is not Resp3"),
        }
    }
}

pub type Outbox = Sender<Letter>;
pub type Inbox = Receiver<Letter>;

#[derive(Debug)]
pub struct MailboxGuard {
    post_office: &'static PostOffice,
    pub id: Id,
    pub outbox: Outbox,
    pub inbox: Inbox,
}

impl MailboxGuard {
    pub async fn recv_async(&self) -> Letter {
        self.inbox.recv_async().await.unwrap()
    }

    pub fn recv(&self) -> Letter {
        self.inbox.recv().unwrap()
    }

    pub async fn send_async(&self, letter: Letter) {
        self.outbox.send_async(letter).await.ok();
    }

    pub fn send(&self, letter: Letter) {
        self.outbox.send(letter).ok();
    }
}

impl Drop for MailboxGuard {
    fn drop(&mut self) {
        self.post_office.remove(self.id);
    }
}
