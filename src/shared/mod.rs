pub mod db;
pub mod script;
pub mod signal_manager;

pub use script::*;
pub use signal_manager::*;

use crate::{conf::Conf, server::Handler, shared::db::Db, Id};
use async_shutdown::ShutdownManager;
use bytes::BytesMut;
use flume::{Receiver, Sender};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::TcpStream;

#[derive(Debug, Clone)]
pub struct Shared {
    inner: &'static SharedInner,
}

pub struct SharedInner {
    db: Db,
    conf: &'static Conf,
    script: Script,
    /// 传播写命令到aof或replica。
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
    /// replica的handler，见`Message`
    wcmd_channel: Option<(Sender<Message>, Receiver<Message>)>,
    // back_log:
    offset: AtomicU64,
    signal_manager: ShutdownManager<Id>,
}

impl Shared {
    // 整个程序运行期间只会创建一次
    pub fn new(conf: &'static Conf, signal_manager: ShutdownManager<Id>) -> Self {
        let db = Db::new(conf);
        let script = Script::new();

        // 如果是集群模式，或者开启了aof，则需要传播写命令
        if !conf.server.standalone || conf.aof.is_some() {
            let (tx, rx) = flume::unbounded();
            Self::new_with(db, conf, script, Some((tx, rx)), signal_manager)
        } else {
            Self::new_with(db, conf, script, None, signal_manager)
        }
    }

    pub fn new_with(
        db: Db,
        conf: &'static Conf,
        script: Script,
        wcmd_propagator: Option<(Sender<Message>, Receiver<Message>)>,
        shutdown: ShutdownManager<Id>,
    ) -> Self {
        Self {
            inner: Box::leak(Box::new(SharedInner {
                db,
                conf,
                script,
                wcmd_channel: wcmd_propagator,
                offset: AtomicU64::new(0),
                signal_manager: shutdown,
            })),
        }
    }

    #[inline]
    pub fn db(&self) -> &'static Db {
        &self.inner.db
    }

    #[inline]
    pub fn script(&self) -> &'static Script {
        &self.inner.script
    }

    #[inline]
    pub fn conf(&self) -> &'static Conf {
        self.inner.conf
    }

    #[inline]
    pub fn wcmd_tx(&self) -> Option<&'static Sender<Message>> {
        self.inner.wcmd_channel.as_ref().map(|(tx, _)| tx)
    }

    #[inline]
    pub fn wcmd_rx(&self) -> Option<&'static Receiver<Message>> {
        self.inner.wcmd_channel.as_ref().map(|(_, rx)| rx)
    }

    #[inline]
    pub fn signal_manager(&self) -> &'static ShutdownManager<Id> {
        &self.inner.signal_manager
    }

    #[inline]
    pub fn offset(&self) -> u64 {
        self.inner.offset.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn add_offset(&self, offset: u64) {
        self.inner.offset.fetch_add(offset, Ordering::Relaxed);
    }

    pub fn reset(&self) {
        self.inner.db.clear();
        self.inner.script.clear();
        self.inner.offset.store(0, Ordering::Relaxed);
    }
}

impl std::fmt::Debug for SharedInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedInner")
            .field("db", &self.db)
            .field("conf", &self.conf)
            .field("script", &self.script)
            .finish()
    }
}

pub enum Message {
    Wcmd(BytesMut),
    AddReplica(Handler<TcpStream>),
}
