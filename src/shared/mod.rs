use crate::{
    conf::Conf,
    shared::{db::Db, wcmd_propagator::WCmdPropergator},
};
use async_shutdown::ShutdownManager;
use std::sync::Arc;

pub mod db;
pub mod wcmd_propagator;

#[derive(Clone, Default)]
pub struct Shared {
    inner: Arc<SharedInner>,
}

#[derive(Default)]
pub struct SharedInner {
    db: db::Db,
    wcmd_propagator: WCmdPropergator,
    shutdown: ShutdownManager<()>,
}

impl Shared {
    pub fn new(db: Db, conf: &Arc<Conf>, shutdown: ShutdownManager<()>) -> Self {
        Self {
            inner: Arc::new(SharedInner {
                db,
                wcmd_propagator: WCmdPropergator::new(conf.aof.enable),
                shutdown,
            }),
        }
    }

    #[inline]
    pub fn db(&self) -> &db::Db {
        &self.inner.db
    }

    #[inline]
    pub fn wcmd_propagator(&self) -> &WCmdPropergator {
        &self.inner.wcmd_propagator
    }

    #[inline]
    pub fn shutdown(&self) -> &ShutdownManager<()> {
        &self.inner.shutdown
    }
}
