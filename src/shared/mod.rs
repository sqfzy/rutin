pub mod db;
pub mod propagator;
pub mod script;

pub use script::*;

use crate::{
    conf::Conf,
    shared::{db::Db, propagator::Propagator},
};
use async_shutdown::ShutdownManager;
use std::sync::Arc;

#[derive(Clone)]
pub struct Shared {
    db: Arc<Db>,
    conf: Arc<Conf>,
    script: Arc<Script>,
    wcmd_propagator: Arc<Propagator>,
    shutdown: ShutdownManager<i32>,
}

impl Shared {
    pub fn new(db: Arc<Db>, conf: Arc<Conf>, shutdown: ShutdownManager<i32>) -> Self {
        let db = db;
        let conf = conf;
        let wcmd_propagator = Arc::new(Propagator::new(
            conf.aof.is_some(),
            conf.replica.max_replica,
        ));
        let script = Arc::new(Script::new());
        Self {
            db,
            conf,
            script,
            wcmd_propagator,
            shutdown,
        }
    }

    pub fn new_with(
        db: Arc<Db>,
        conf: Arc<Conf>,
        script: Arc<Script>,
        wcmd_propagator: Arc<Propagator>,
        shutdown: ShutdownManager<i32>,
    ) -> Self {
        Self {
            db,
            conf,
            script,
            wcmd_propagator,
            shutdown,
        }
    }

    pub fn db(&self) -> &Arc<Db> {
        &self.db
    }

    pub fn script(&self) -> &Arc<Script> {
        &self.script
    }

    pub fn conf(&self) -> &Arc<Conf> {
        &self.conf
    }

    pub fn wcmd_propagator(&self) -> &Arc<Propagator> {
        &self.wcmd_propagator
    }

    pub fn shutdown(&self) -> &ShutdownManager<i32> {
        &self.shutdown
    }
}

impl std::fmt::Debug for Shared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedInner")
            .field("db", &self.db)
            .field("script", &self.script)
            .field("conf", &self.conf)
            .field("wcmd_propagator", &self.wcmd_propagator)
            .finish()
    }
}
