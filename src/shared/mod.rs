use crate::{
    conf::Conf,
    shared::{db::Db, wcmd_propagator::WCmdPropergator},
};
use std::sync::Arc;

pub mod db;
pub mod wcmd_propagator;

#[derive(Debug, Clone, Default)]
pub struct Shared {
    inner: Arc<SharedInner>,
}

#[derive(Debug, Default)]
pub struct SharedInner {
    db: db::Db,
    wcmd_propagator: WCmdPropergator,
}

impl Shared {
    pub fn new(db: Db, conf: &Arc<Conf>) -> Self {
        Self {
            inner: Arc::new(SharedInner {
                db,
                wcmd_propagator: WCmdPropergator::new(conf.aof.enable),
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
}
