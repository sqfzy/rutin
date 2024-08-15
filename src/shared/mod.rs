pub mod db;
pub mod post_office;
pub mod script;

pub use post_office::*;
pub use script::*;

use crate::{conf::Conf, server::Handler, shared::db::Db, util::UnsafeLazy};
use bytes::BytesMut;
use std::time::SystemTime;
use tokio::{net::TcpStream, time::Instant};

pub static UNIX_EPOCH: UnsafeLazy<Instant> = UnsafeLazy::new(|| {
    Instant::now()
        - SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
});

#[derive(Debug, Copy, Clone)]
pub struct Shared {
    inner: &'static SharedInner,
}

pub struct SharedInner {
    db: Db,
    conf: Conf,
    script: Script,
    post_office: PostOffice,
    // offset: AtomicU64,
}

impl Shared {
    // 整个程序运行期间只会创建一次
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let conf = Conf::new().unwrap();

        Self::with_conf(conf)
    }

    pub fn with_conf(conf: Conf) -> Self {
        unsafe {
            UNIX_EPOCH.init();
        }

        let db = Db::new(conf.memory.clone());
        let script = Script::new();

        let mailbox = if !conf.server.standalone || conf.aof.is_some() {
            Some(flume::unbounded())
        } else {
            None
        };

        // 如果是集群模式，或者开启了aof，则需要传播写命令
        if let Some((outbox, inbox)) = mailbox {
            let post_office = PostOffice::new(Some(outbox.clone()));

            let shared = Self {
                inner: Box::leak(Box::new(SharedInner {
                    db,
                    conf,
                    script,
                    post_office,
                    // offset: AtomicU64::new(0),
                })),
            };

            shared.post_office().inner.insert(
                WCMD_PROPAGATE_ID,
                (
                    outbox,
                    Inbox {
                        inner: inbox,
                        post_office: shared.post_office(),
                        id: WCMD_PROPAGATE_ID,
                    },
                ),
            );

            shared
        } else {
            let post_office = PostOffice::new(None);

            let shared = Self {
                inner: Box::leak(Box::new(SharedInner {
                    db,
                    conf,
                    script,
                    post_office,
                    // offset: AtomicU64::new(0),
                })),
            };

            shared
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
        &self.inner.conf
    }

    #[inline]
    pub fn post_office(&self) -> &'static PostOffice {
        &self.inner.post_office
    }

    // #[inline]
    // pub fn get_offset(&self) -> u64 {
    //     self.inner.offset.load(Ordering::AcqRel)
    // }
    //
    // #[inline]
    // pub fn add_offset(&self, offset: u64) {
    //     self.inner.offset.fetch_add(offset, Ordering::AcqRel);
    // }
    //
    // #[inline]
    // pub fn set_offset(&self, offset: u64) {
    //     self.inner.offset.store(offset, Ordering::AcqRel);
    // }

    pub fn clear(&self) {
        self.inner.db.clear();
        self.inner.script.clear();
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
