use crate::{
    conf::Conf,
    shared::{db::Db, propagator::Propagator},
};
use async_shutdown::ShutdownManager;
use std::sync::Arc;

pub mod db;
pub mod propagator;

#[derive(Clone, Default)]
pub struct Shared {
    inner: Arc<SharedInner>,
}

#[derive(Default)]
pub struct SharedInner {
    db: db::Db,
    wcmd_propagator: Propagator,
    shutdown: ShutdownManager<()>,
}

impl Shared {
    pub fn new(db: Db, conf: &Arc<Conf>, shutdown: ShutdownManager<()>) -> Self {
        Self {
            inner: Arc::new(SharedInner {
                db,
                wcmd_propagator: Propagator::new(conf.aof.enable, conf.replica.max_replica),
                shutdown,
            }),
        }
    }

    #[inline]
    pub fn db(&self) -> &db::Db {
        &self.inner.db
    }

    #[inline]
    pub fn wcmd_propagator(&self) -> &Propagator {
        &self.inner.wcmd_propagator
    }

    #[inline]
    pub fn shutdown(&self) -> &ShutdownManager<()> {
        &self.inner.shutdown
    }
}
