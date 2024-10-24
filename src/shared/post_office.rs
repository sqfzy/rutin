// 目前Inbox存在与三种类型的任务中：Server, Wcmd Propagate, Handler，
//
// Server需要处理ShutdownServer和BlockAllClients消息，
// Wcmd Propagate需要处理Wcmd，AddReplica和RemoveReplica消息，
// Handler需要处理ShutdownServer和BlockAllClients消息。

use core::panic;
use std::{fmt::Debug, sync::Arc};

use crate::{conf::Conf, frame::CheapResp3, server::Handler, shared::SharedInner, Id};
use bytes::{Bytes, BytesMut};
use dashmap::{mapref::one::Ref, DashMap, Entry};
use event_listener::{listener, Event};
use flume::{Receiver, Sender};
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
pub const UPDATE_USED_MEMORY_ID: Id = 7;
pub const UPDATE_LRU_CLOCK_ID: Id = 8;

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
    pub(super) inner: DashMap<Id, (Outbox, Inbox), nohash::BuildNoHashHasher<u64>>,
    // 传播写命令到aof或replica。由于需要频繁使用其OutBox，因此直接保存在字段中。
    // 由于传播写命令是耗时操作，因此需要尽可能减少传播次数。
    //
    // 只会在服务初始时设置一次
    aof_mailbox: Option<(Outbox, Inbox)>,

    // 配置了MasterConf后，会在inner中插入SET_MASTER_ID对应的mailbox，但是暂时不
    // 会设置set_master_outbox，因为一旦设置了set_master_outbox，就会传播wcmd，而
    // 一开始SET MASTER任务并不存在replica，不需要传播wcmd。只有在设置了replica后，
    // 才会通过BlockAll阻止其它任务访问set_master_outbox，然后设置set_master_outbox
    //
    // 只会在服务初始时设置一次
    set_master_mailbox: Option<(Outbox, Inbox)>,
}

impl PostOffice {
    pub fn new(conf: &Conf) -> Self {
        Self {
            inner: DashMap::with_hasher(nohash::BuildNoHashHasher::default()),
            aof_mailbox: if conf.aof.is_some() {
                Some(flume::unbounded())
            } else {
                None
            },
            set_master_mailbox: if conf.master.is_some() {
                Some(flume::unbounded())
            } else {
                None
            },
        }
    }

    pub fn contains(&self, id: Id) -> bool {
        self.inner.contains_key(&id)
    }

    // 从提供的id开始寻找一个未使用的ID，然后注册一个mailbox
    pub fn register_handler_mailbox(&'static self, mut id: Id) -> (Id, MailboxGuard) {
        let (outbox, inbox) = flume::unbounded();

        // 跳过特殊ID
        if SPECIAL_ID_RANGE.contains(&id) {
            id = SPECIAL_ID_RANGE.end;
        }

        // 找到一个未使用的ID
        loop {
            match self.inner.entry(id) {
                Entry::Vacant(entry) => {
                    entry.insert((outbox.clone(), inbox.clone()));
                    break;
                }
                Entry::Occupied(_) => {
                    id += 1;
                }
            }
        }

        (
            id,
            MailboxGuard {
                post_office: self,
                id,
                outbox,
                inbox,
            },
        )
    }

    // 注册一个特殊的mailbox，该mailbox是唯一的
    #[instrument(level = "debug", skip(self))]
    pub fn register_special_mailbox(&'static self, id: Id) -> MailboxGuard {
        assert!(SPECIAL_ID_RANGE.contains(&id));
        // 特殊任务只能注册一次，如果希望重新注册，可以Reset服务
        assert!(!self.inner.contains_key(&id), "id={} already exists", id);

        let (outbox, inbox) = match id {
            AOF_ID => self.aof_mailbox.clone().unwrap(),
            SET_MASTER_ID => self.set_master_mailbox.clone().unwrap(),
            _ => flume::unbounded(),
        };

        if id != NULL_ID {
            self.inner.insert(id, (outbox.clone(), inbox.clone()));
        }

        MailboxGuard {
            post_office: self,
            id,
            outbox,
            inbox,
        }
    }

    #[inline]
    pub fn aof_outbox(&self) -> Option<&Outbox> {
        self.aof_mailbox.as_ref().map(|mailbox| &mailbox.0)
    }

    #[inline]
    pub fn set_master_outbox(&self) -> Option<&Outbox> {
        self.set_master_mailbox.as_ref().map(|mailbox| &mailbox.0)
    }

    #[inline]
    pub fn delay_shutdown_token(&'static self) -> MailboxGuard {
        self.register_handler_mailbox(fastrand::u64(..)).1
    }

    // #[inline]
    // pub fn get_clients_count(&self) -> usize {
    //     self.inner.len() - USED_SPECIAL_ID_COUNT
    // }

    #[inline]
    pub fn get_mailbox(&self, id: Id) -> Option<Ref<'_, Id, (Outbox, Inbox)>> {
        self.inner.get(&id)
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
        self.aof_outbox().is_some() || self.set_master_outbox().is_some()
    }

    pub async fn wait_task_shutdown(&self, id: Id) {
        let mut interval = tokio::time::interval(Duration::from_millis(300));

        while self.inner.contains_key(&id) {
            interval.tick().await;
        }
    }

    #[inline]
    pub async fn send_wcmd(&self, wcmd: BytesMut) {
        // AOF和SET MASTER任务都需要处理写命令，但AOF只需要&wcmd，为了避免clone，
        // 如果存在AOF任务，则由AOF任务在使用完&wcmd后尝试发送给SetMaster任务
        if let Some(outbox) = self.aof_outbox().as_ref() {
            outbox.send_async(Letter::Wcmd(wcmd)).await.ok();
        } else if let Some(outbox) = self.set_master_outbox().as_ref() {
            outbox.send_async(Letter::Wcmd(wcmd)).await.ok();
        }
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn send_shutdown(&self, id: Id) {
        if let Some(outbox) = self.get_outbox(id) {
            outbox.send_async(Letter::Shutdown).await.ok();
        }
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn send_shutdown_server(&self) {
        for entry in self.inner.iter() {
            entry.0.send_async(Letter::Shutdown).await.ok();
        }
    }

    #[instrument(level = "debug", skip(self, f))]
    pub async fn send_reset_server(&self, f: Box<dyn FnOnce(&mut SharedInner) + Send>) {
        self.inner
            .get(&MAIN_ID)
            .unwrap()
            .0
            .send_async(Letter::ModifyShared(f))
            .await
            .ok();

        for entry in self.inner.iter() {
            if *entry.key() == MAIN_ID {
                continue;
            }

            entry.0.send_async(Letter::Shutdown).await.ok();
        }
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn send_block(&self, id: Id) -> BlockGuard {
        let event = Arc::new(Event::new());
        {
            listener!(event => listener);

            // 向指定的task发送
            if let Some(outbox) = self.get_outbox(id) {
                outbox
                    .send_async(Letter::Block {
                        unblock_event: event.clone(),
                    })
                    .await
                    .ok();
            }

            // 等待task返回响应，证明已经阻塞
            listener.await;
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
        for entry in self.inner.iter() {
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
    // 重置服务。关闭除主任务以外的所有任务，并重置Shared(必须保证没有其它任务
    // 在使用Shared，即保证只有主任务一个任务在运行)
    ModifyShared(Box<dyn FnOnce(&mut SharedInner) + Send>),

    Block {
        unblock_event: Arc<Event>,
    },

    // 用于客户端之间重定向
    Resp3(CheapResp3),

    Wcmd(BytesMut),
    Psync {
        handle_replica: Handler<TcpStream>,
        repl_id: Bytes,
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
            Letter::ModifyShared(..) => write!(f, "Letter::ModifyShared"),
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
            Letter::Psync { .. } | Letter::ModifyShared(..) => unreachable!(),
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
        self.post_office.inner.remove(&self.id);
    }
}
