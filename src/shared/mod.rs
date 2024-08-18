pub mod db;
pub mod post_office;
pub mod script;

pub use post_office::*;
pub use script::*;
use tokio_util::task::LocalPoolHandle;

use crate::{conf::Conf, server::Handler, shared::db::Db, util::UnsafeLazy};
use bytes::BytesMut;
use std::{cell::UnsafeCell, time::SystemTime};
use tokio::{net::TcpStream, time::Instant};

pub static UNIX_EPOCH: UnsafeLazy<Instant> = UnsafeLazy::new(|| {
    Instant::now()
        - SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
});

#[derive(Debug, Copy, Clone)]
pub struct Shared {
    pub inner: &'static UnsafeCell<SharedInner>,
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
        unsafe {
            UNIX_EPOCH.init();
        }

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
            inner: Box::leak(Box::new(UnsafeCell::new(inner))),
        }
    }

    #[inline]
    pub fn pool(&self) -> &LocalPoolHandle {
        unsafe { &(*self.inner.get()).pool }
    }

    #[inline]
    pub fn db(&self) -> &'static Db {
        unsafe { &(*self.inner.get()).db }
    }

    #[inline]
    pub fn script(&self) -> &'static Script {
        unsafe { &(*self.inner.get()).script }
    }

    #[inline]
    pub fn conf(&self) -> &'static Conf {
        unsafe { &(*self.inner.get()).conf }
    }

    #[inline]
    pub fn post_office(&self) -> &'static PostOffice {
        unsafe { &(*self.inner.get()).post_office }
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
