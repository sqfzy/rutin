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
    wcmd_propagator: wcmd_propagator::WCmdPropergator,
}

impl Shared {
    #[inline]
    pub fn db(&self) -> &db::Db {
        &self.inner.db
    }

    #[inline]
    pub fn wcmd_propagator(&self) -> &wcmd_propagator::WCmdPropergator {
        &self.inner.wcmd_propagator
    }
}
