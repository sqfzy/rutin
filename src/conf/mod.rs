mod aof;
mod master;
mod memory;
mod rdb;
mod replica;
mod security;
mod server;
mod tls;

pub use aof::*;
pub use master::*;
pub use memory::*;
pub use rdb::*;
pub use replica::*;
pub use security::*;
pub use server::*;
pub use tls::*;

use crate::{
    cli::{merge_cli, Cli},
    conf::db::Events,
    server::{incr_lru_clock, USED_MEMORY},
    shared::{post_office::*, *},
};
use arc_swap::{ArcSwap, ArcSwapOption, AsRaw, Guard};
use bytes::Bytes;
use clap::Parser;
use figment::providers::{Format, Toml};
use futures::pin_mut;
use rand::Rng;
use serde::Deserialize;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use sysinfo::ProcessRefreshKind;
use tokio::time::{interval, Instant};

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct Conf {
    // 修改该配置需要重启 MAIN_ID 任务
    pub(super) server: ArcSwap<ServerConf>,

    // 修改该配置需要重启 MAIN_ID 任务
    pub(super) tls: ArcSwapOption<TlsConf>,

    pub(super) security: ArcSwap<SecurityConf>,

    // 修改该配置需要重启 SET_MASTER_ID 任务
    pub(super) master: ArcSwapOption<MasterConf>,

    // 修改该配置需要重启 SET_REPLICA_ID 任务
    pub(super) replica: ArcSwapOption<ReplicaConf>,

    pub(super) rdb: ArcSwapOption<RdbConf>,

    // 修改该配置需要重启 SET_AOF_ID 任务
    pub(super) aof: ArcSwapOption<AofConf>,

    // 修改该配置需要重启 EXPIRATION_EVICT_ID 任务
    // 更新Db中的mem_conf
    pub(super) memory: ArcSwap<MemoryConf>,
}

impl Conf {
    pub fn new() -> anyhow::Result<Self> {
        let mut conf: Conf = figment::Figment::new()
            // 1. 从默认配置文件中加载配置
            .join(Toml::file("config/default.toml"))
            // 2. 从用户自定义配置文件中加载配置
            .merge(Toml::file("config/custom.toml"))
            .extract()?;

        // 3. 从命令行中加载配置
        let cli = Cli::parse();
        merge_cli(&mut conf, cli);

        Ok(conf)
    }

    pub fn init_server() {}

    pub fn server_conf(&self) -> Guard<Arc<ServerConf>> {
        self.server.load()
    }

    pub fn security_conf(&self) -> Guard<Arc<SecurityConf>> {
        self.security.load()
    }

    pub fn master_conf(&self) -> Guard<Option<Arc<MasterConf>>> {
        self.master.load()
    }

    pub fn replica_conf(&self) -> Guard<Option<Arc<ReplicaConf>>> {
        self.replica.load()
    }

    pub fn rdb_conf(&self) -> Guard<Option<Arc<RdbConf>>> {
        self.rdb.load()
    }

    pub fn aof_conf(&self) -> Guard<Option<Arc<AofConf>>> {
        self.aof.load()
    }

    pub fn memory_conf(&self) -> Guard<Arc<MemoryConf>> {
        self.memory.load()
    }

    pub fn tls_conf(&self) -> Guard<Option<Arc<TlsConf>>> {
        self.tls.load()
    }

    pub fn update_server_conf(&self, f: &mut dyn FnMut(ServerConf) -> ServerConf, shared: Shared) {
        self.server.rcu(|old| {
            let new = Arc::new(f(ServerConf::clone(old)));

            ServerConf::may_update_server_state(&new, old, shared);

            new
        });
    }

    pub fn update_security_conf(
        &self,
        f: &mut dyn FnMut(SecurityConf) -> SecurityConf,
        _shared: Shared,
    ) {
        self.security.rcu(|conf| f(SecurityConf::clone(conf)));
    }

    pub fn update_master_conf(
        &self,
        f: &mut dyn FnMut(Option<MasterConf>) -> Option<MasterConf>,
        shared: Shared,
    ) {
        self.master.rcu(|old| {
            let new = f(old.as_ref().map(|conf| MasterConf::clone(conf))).map(Arc::new);

            MasterConf::may_update_server_state(new.as_ref(), old.as_ref(), shared);

            new
        });
    }

    pub fn update_replica_conf(
        &self,
        f: &mut dyn FnMut(Option<ReplicaConf>) -> Option<ReplicaConf>,
        shared: Shared,
    ) {
        self.replica.rcu(|old| {
            let new = f(old.as_ref().map(|conf| ReplicaConf::clone(conf))).map(Arc::new);

            ReplicaConf::may_update_server_state(new.as_ref(), old.as_ref(), shared);

            new
        });
    }

    pub fn update_rdb_conf(
        &self,
        f: &mut dyn FnMut(Option<RdbConf>) -> Option<RdbConf>,
        _shared: Shared,
    ) {
        self.rdb
            .rcu(|conf| f(conf.as_ref().map(|conf| RdbConf::clone(conf))).map(Arc::new));
    }

    pub fn update_aof_conf(
        &self,
        f: &mut dyn FnMut(Option<AofConf>) -> Option<AofConf>,
        shared: Shared,
    ) {
        self.aof.rcu(|old| {
            let new = f(old.as_ref().map(|conf| AofConf::clone(conf))).map(Arc::new);

            AofConf::may_update_server_state(new.as_ref(), old.as_ref(), shared);

            new
        });
    }

    pub fn update_memory_conf(&self, f: &mut dyn FnMut(MemoryConf) -> MemoryConf, shared: Shared) {
        self.memory.rcu(|old| {
            let new = Arc::new(f(MemoryConf::clone(old)));

            MemoryConf::may_update_server_state(&new, old, shared);

            new
        });
    }

    pub fn update_tls_conf(
        &self,
        f: &mut dyn FnMut(Option<TlsConf>) -> Option<TlsConf>,
        shared: Shared,
    ) {
        self.tls.rcu(|old| {
            let new = f(old.as_ref().map(|conf| TlsConf::clone(conf))).map(Arc::new);

            TlsConf::may_update_server_state(new.as_ref(), old.as_ref(), shared);

            new
        });
    }
}
