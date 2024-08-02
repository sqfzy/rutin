mod bg_task_channel;
mod handler;
mod listener;

pub use bg_task_channel::*;
use bytes::BytesMut;
pub use handler::*;
pub use listener::*;
use rand::seq::IteratorRandom;
use sysinfo::{ProcessRefreshKind, System};

use crate::{
    conf::Conf,
    persist::{aof::Aof, rdb::Rdb},
    shared::Shared,
    Id,
};
use async_shutdown::ShutdownManager;
use crossbeam::atomic::AtomicCell;
use std::{cell::RefCell, str::FromStr, sync::atomic::Ordering, time::Duration};
use tokio::{task_local, time::Instant};
use tracing::{debug, error, info};

/* 使用保留ID作为特殊信号，其它ID用于指定关闭该ID的连接 */
// 关闭服务。
pub const SHUTDOWN_SIGNAL: Id = 1;
// 重启服务。断开所有连接，释放所有资源，重新初始化。
pub const REBOOT_SIGNAL: Id = 2;
pub const RESET_SIGNAL: Id = 3;

pub const RESERVE_MAX_ID: Id = 20;
pub static RESERVE_ID: AtomicCell<Id> = AtomicCell::new(0);
// 该值作为新连接的客户端的ID。已连接的客户端的ID会被记录在`Shared`中，在设置ID时
// 需要检查是否已经存在相同的ID。保留前20个ID，专用于事务处理
pub static CLIENT_ID_COUNT: AtomicCell<Id> = AtomicCell::new(RESERVE_MAX_ID + 1);

task_local! { pub static ID: Id; }

thread_local! {
    pub static SYSTEM: RefCell<System>  = RefCell::new(System::new_all());
    pub static LOCAL_BUF: RefCell<BytesMut>  = RefCell::new(BytesMut::with_capacity(4096));
}

#[inline]
pub fn using_local_buf(f: impl FnOnce(&mut BytesMut)) -> BytesMut {
    LOCAL_BUF.with_borrow_mut(|buf| {
        if buf.capacity() < 256 {
            buf.reserve(4096);
        }
        f(buf);

        buf.split()
    })
}

#[inline]
pub async fn run() {
    let signal_manager = ShutdownManager::new();

    tokio::spawn({
        let shutdown = signal_manager.clone();
        async move {
            if let Err(e) = tokio::signal::ctrl_c().await {
                eprintln!("Failed to wait for CTRL+C: {}", e);
                std::process::exit(1);
            } else {
                eprintln!("\nShutting down server...");
                shutdown.trigger_shutdown(SHUTDOWN_SIGNAL).ok();
            }
        }
    });

    'reboot: loop {
        // 读取配置
        let conf = Box::leak(Box::new(Conf::new().unwrap()));

        if let Ok(level) = tracing::Level::from_str(conf.server.log_level.as_ref()) {
            tracing_subscriber::fmt()
                .pretty()
                .with_max_level(level)
                .init();
        }

        let shared = Shared::new(conf, signal_manager.clone());

        init(shared.clone()).await.unwrap();

        loop {
            let mut server = Server::new(shared.clone()).await;

            // 运行服务，阻塞主线程。当shutdown触发时，解除主线程的阻塞
            match signal_manager.wrap_cancel(server.run()).await {
                // 服务初始化出错，关闭服务
                Ok(Err(err)) => {
                    error!(cause = %err, "server run error");
                    signal_manager.trigger_shutdown(SHUTDOWN_SIGNAL).ok();
                }
                // 如果是特殊信号，则退出循环，否则继续运行服务
                Err(signal) => match signal {
                    SHUTDOWN_SIGNAL => {
                        debug!("shutdown signal received");
                        break 'reboot;
                    }
                    REBOOT_SIGNAL => {
                        debug!("reboot signal received");
                        break;
                    }
                    RESET_SIGNAL => {
                        debug!("reset signal received");
                        shared.reset();
                    }
                    // 忽略其它信号
                    _ => {}
                },
                // 服务正常运行时，永不返回
                _ => unreachable!(),
            }

            server.clean().await;
            drop(server.delay_token);

            // 等待所有DelayShutdownToken被释放
            debug!("waiting for shutdown complete");
            signal_manager.wait_shutdown_complete().await;
        }
    }
}

pub async fn init(shared: Shared) -> anyhow::Result<()> {
    let conf = shared.conf();

    /*********************/
    /* 是否开启RDB持久化 */
    /*********************/
    if let (true, Some(rdb)) = (conf.aof.is_none(), conf.rdb.as_ref()) {
        let mut rdb = Rdb::new(&shared, rdb.file_path.clone(), rdb.enable_checksum);

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
        let mut aof = Aof::new(shared.clone(), aof.file_path.clone()).await?;

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
