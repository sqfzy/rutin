pub mod db;
pub mod propagator;
pub mod script;

pub use script::*;

use crate::{
    conf::Conf,
    shared::{db::Db, propagator::Propagator},
    Id,
};
use async_shutdown::ShutdownManager;

#[derive(Debug, Clone)]
pub struct Shared {
    inner: &'static SharedInner,
}

pub struct SharedInner {
    db: Db,
    conf: &'static Conf,
    script: Script,
    wcmd_propagator: Propagator,
    signal_manager: ShutdownManager<Id>,
}

impl Shared {
    pub fn new(conf: &'static Conf, shutdown: ShutdownManager<Id>) -> Self {
        let db = Db::new(conf);
        let wcmd_propagator = Propagator::new(conf.aof.is_some(), conf.replica.max_replica);
        let script = Script::new();
        Self {
            inner: Box::leak(Box::new(SharedInner {
                db,
                conf,
                script,
                wcmd_propagator,
                signal_manager: shutdown,
            })),
        }
    }

    pub fn new_with(
        db: Db,
        conf: &'static Conf,
        script: Script,
        wcmd_propagator: Propagator,
        shutdown: ShutdownManager<Id>,
    ) -> Self {
        Self {
            inner: Box::leak(Box::new(SharedInner {
                db,
                conf,
                script,
                wcmd_propagator,
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
    pub fn wcmd_propagator(&self) -> &'static Propagator {
        &self.inner.wcmd_propagator
    }

    #[inline]
    pub fn signal_manager(&self) -> &'static ShutdownManager<Id> {
        &self.inner.signal_manager
    }
}

impl std::fmt::Debug for SharedInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedInner")
            .field("db", &self.db)
            .field("script", &self.script)
            .field("wcmd_propagator", &self.wcmd_propagator)
            .finish()
    }
}
