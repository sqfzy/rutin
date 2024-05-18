use crate::shared::{db::Db, wcmd_propagator::WCmdPropergator};
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
    pub fn new(db: Db, wcmd_propagator: WCmdPropergator) -> Self {
        Self {
            inner: Arc::new(SharedInner {
                db,
                wcmd_propagator,
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
