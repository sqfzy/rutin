mod aof;
mod memory;
mod rdb;
mod replica;
mod security;
mod server;
mod tls;

pub use aof::*;
use figment::providers::{Format, Toml};
pub use memory::*;
pub use rdb::*;
pub use replica::*;
pub use security::*;
pub use server::*;
use sysinfo::ProcessRefreshKind;
pub use tls::*;

use crate::{
    cli::{merge_cli, Cli},
    persist::{aof::Aof, rdb::Rdb},
    server::Listener,
    shared::db::Lru,
};
use clap::Parser;
use rand::{seq::IteratorRandom, Rng};
use serde::Deserialize;
use std::{
    fs::File,
    io::BufReader,
    sync::atomic::{AtomicU32, AtomicU64, Ordering},
    time::Duration,
};
use tokio::{runtime::Handle, time::Instant};
use tokio_rustls::rustls;
use tracing::{error, info};

// 内存使用的粗略统计，每一秒更新为准确值
pub static USED_MEMORY: AtomicU64 = AtomicU64::new(0);

static LRU_CLOCK: AtomicU32 = AtomicU32::new(0);

#[inline]
pub fn incr_lru_clock() {
    LRU_CLOCK.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn get_lru_clock() -> u32 {
    LRU_CLOCK.load(Ordering::Relaxed) % Lru::LRU_CLOCK_MAX
}

#[derive(Debug, Deserialize)]
pub struct Conf {
    pub server: ServerConf,
    pub security: SecurityConf,
    pub replica: ReplicaConf,
    pub rdb: Option<RdbConf>,
    pub aof: Option<AofConf>,
    pub memory: Option<MemoryConf>,
    pub tls: Option<TLSConf>,
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

        // 4. 运行时配置
        let run_id: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(40)
            .map(char::from)
            .collect();

        conf.server.run_id = run_id;

        // 5. 初始化系统内存
        if conf.memory.is_some() {
            let mut system = sysinfo::System::new();
            system.refresh_processes();
            let pid = sysinfo::get_current_pid().unwrap();
            let process = system.process(pid).unwrap();
            USED_MEMORY.store(process.memory(), Ordering::Relaxed);
        }

        Ok(conf)
    }

    pub async fn prepare(listener: &mut Listener) -> anyhow::Result<()> {
        let shared = &listener.shared;
        let conf = shared.conf();

        /*********************/
        /* 是否开启RDB持久化 */
        /*********************/
        if let (true, Some(rdb)) = (conf.aof.is_none(), conf.rdb.as_ref()) {
            let mut rdb = Rdb::new(shared, rdb.file_path.clone(), rdb.enable_checksum);

            let start = std::time::Instant::now();
            info!("Loading RDB file...");
            if let Err(e) = rdb.load().await {
                error!("Failed to load RDB file: {:?}", e);
            } else {
                info!("RDB file loaded. Time elapsed: {:?}", start.elapsed());
            }
        }

        /*********************/
        /* 是否开启AOF持久化 */
        /*********************/
        if let Some(aof) = conf.aof.as_ref() {
            let mut aof = Aof::new(shared.clone(), conf.clone(), aof.file_path.clone()).await?;

            let start = std::time::Instant::now();
            info!("Loading AOF file...");
            if let Err(e) = aof.load().await {
                error!("Failed to load AOF file: {:?}", e);
            } else {
                info!("AOF file loaded. Time elapsed: {:?}", start.elapsed());
            }

            let handle = Handle::current();
            std::thread::spawn(move || {
                handle.block_on(async move {
                    if let Err(e) = aof.save().await {
                        error!("Failed to save AOF file: {:?}", e);
                    }
                });
            });
        }

        /**********************/
        /* 开启过期键定时检查 */
        /**********************/
        let period = Duration::from_secs(conf.server.expire_check_interval_secs);
        tokio::spawn({
            let shared = shared.clone();

            async move {
                let mut interval = tokio::time::interval(period);

                loop {
                    interval.tick().await;

                    for _ in 0..10 {
                        if let Some(record) = {
                            let mut rng = rand::thread_rng();

                            shared.db().entry_expire_records().iter().choose(&mut rng)
                        } {
                            if record.0 <= Instant::now() {
                                tracing::trace!("key {:?} is expired", record.1);

                                let _ = shared.db().remove_object(record.1.clone()).await;
                            }
                        }
                    }
                }
            }
        });

        if conf.memory.is_some() {
            // 定时更新内存使用情况
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(300));

                let mut system = sysinfo::System::new();
                let pid = sysinfo::get_current_pid().unwrap();
                let kind = ProcessRefreshKind::new().with_memory();
                loop {
                    interval.tick().await;
                    system.refresh_process_specifics(pid, kind);
                    let new_used_mem = system.process(pid).unwrap().memory();
                    USED_MEMORY.store(new_used_mem, Ordering::Relaxed);
                }
            });
        };

        // 定时(每分钟)更新LRU_CLOCK
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                incr_lru_clock();
            }
        });

        Ok(())
    }

    pub fn get_tls_config(&self) -> Option<rustls::ServerConfig> {
        let tls = self.tls.as_ref()?;

        let cert = rustls_pemfile::certs(&mut BufReader::new(File::open(&tls.cert_file).unwrap()))
            .map(|cert| cert.unwrap())
            .collect();

        let key = rustls_pemfile::pkcs8_private_keys(&mut BufReader::new(
            File::open(&tls.key_file).unwrap(),
        ))
        .next()
        .unwrap()
        .unwrap();

        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert, rustls::pki_types::PrivateKeyDer::Pkcs8(key))
            .unwrap();

        Some(config)
    }
}
