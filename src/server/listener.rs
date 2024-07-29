use super::Handler;
use crate::{
    persist::{aof::Aof, rdb::Rdb},
    shared::{db::Lru, Shared},
    util::get_test_shared,
};
use async_shutdown::DelayShutdownToken;
use backon::Retryable;
use rand::seq::IteratorRandom;
use std::{
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use sysinfo::ProcessRefreshKind;
use tokio::{io, net::TcpListener, sync::Semaphore, task::LocalSet, time::Instant};
use tokio_rustls::TlsAcceptor;
use tracing::{error, info};

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

pub struct Listener {
    pub shared: Shared,
    pub listener: TcpListener,
    pub tls_acceptor: Option<TlsAcceptor>,
    pub limit_connections: Arc<Semaphore>,
    pub delay_token: DelayShutdownToken<()>,
    pub local_set: LocalSet,
}

impl Listener {
    #[inline]
    pub async fn run(&mut self) -> Result<(), io::Error> {
        tracing::info!(
            "server is running on {}:{}...",
            &self.shared.conf().server.addr,
            self.shared.conf().server.port
        );

        init(self).await.unwrap();

        #[cfg(feature = "debug")]
        println!("debug mode is enabled");

        loop {
            #[cfg(not(feature = "debug"))]
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            let (stream, _) = (|| async { self.listener.accept().await })
                .retry(&backon::ExponentialBuilder::default())
                .await?;

            let shared = self.shared.clone();

            // 对于每个连接都创建一个delay_token，只有当所有连接都正常退出时，才关闭服务
            let delay_token = self.delay_token.clone();
            match &self.tls_acceptor {
                None => {
                    let mut handler = Handler::new(shared, stream);

                    tokio::spawn(async move {
                        // 开始处理连接
                        if let Err(err) = handler.run().await {
                            error!(cause = ?err, "connection error");
                        }

                        // handler.run()不应该block，这会导致delay_token无法释放
                        drop(delay_token);
                        #[cfg(not(feature = "debug"))]
                        drop(permit);
                    });
                }
                // 如果开启了TLS，则使用TlsStream
                Some(tls_acceptor) => {
                    let mut handler = Handler::new(shared, tls_acceptor.accept(stream).await?);

                    tokio::spawn(async move {
                        // 开始处理连接
                        if let Err(err) = handler.run().await {
                            error!(cause = ?err, "connection error");
                        }

                        drop(delay_token);
                        #[cfg(not(feature = "debug"))]
                        drop(permit);
                    });
                }
            };
        }
    }

    pub async fn clean(&mut self) {
        let conf = self.shared.conf();
        if let (true, Some(rdb)) = (conf.aof.is_none(), conf.rdb.as_ref()) {
            let mut rdb = Rdb::new(&self.shared, rdb.file_path.clone(), rdb.enable_checksum);
            let start = tokio::time::Instant::now();
            rdb.save().await.ok();
            tracing::info!("RDB file saved. Time elapsed: {:?}", start.elapsed());
        }
    }
}

pub async fn init(listener: &mut Listener) -> anyhow::Result<()> {
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

        tokio::spawn(async move {
            if let Err(e) = aof.save().await {
                error!("Failed to save AOF file: {:?}", e);
            }
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
