pub mod db;
pub mod post_office;
pub mod script;

pub use post_office::*;
pub use script::*;

use crate::{conf::Conf, server::Handler, shared::db::Db};
use bytes::BytesMut;
use tokio::net::TcpStream;
use tokio_util::task::LocalPoolHandle;

#[derive(Debug, Copy, Clone)]
pub struct Shared {
    pub inner: &'static SharedInner,
}

unsafe impl Send for Shared {}
unsafe impl Sync for Shared {}

impl Shared {
    // 整个程序运行期间只会创建一次
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let conf = Conf::new().unwrap();

        Self::with_conf(conf)
    }

    pub fn with_conf(conf: Conf) -> Self {
        let db = Db::new(conf.memory.clone());
        let script = Script::new();
        let post_office = PostOffice::new();

        let inner = SharedInner {
            pool: LocalPoolHandle::new(num_cpus::get()),
            db,
            conf,
            script,
            post_office,
            // back_log: Mutex::new(BytesMut::new()),
            // offset: AtomicU64::new(0),
        };

        Self {
            inner: Box::leak(Box::new(inner)),
        }
    }

    #[inline]
    pub fn pool(&self) -> &LocalPoolHandle {
        &self.inner.pool
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

    pub fn reset(&self) {
        self.db().clear();
        self.script().clear();
    }
}

pub struct SharedInner {
    pub pool: LocalPoolHandle,
    pub db: Db,
    pub conf: Conf,
    pub script: Script,
    pub post_office: PostOffice,
    // back_log: Mutex<BytesMut>,
    // offset: AtomicU64,
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
