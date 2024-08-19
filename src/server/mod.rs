mod connection;
mod handler;
mod listener;

pub use connection::*;
use event_listener::listener;
pub use handler::*;
pub use listener::*;

use bytes::BytesMut;
use rand::seq::IteratorRandom;
use sysinfo::{ProcessRefreshKind, System};

use crate::{
    persist::{aof::Aof, rdb::Rdb},
    shared::{post_office::Letter, Shared, MAIN_ID},
    util::{set_server_to_master, set_server_to_replica},
    Id,
};
use std::{cell::RefCell, str::FromStr, sync::atomic::Ordering, time::Duration};
use tokio::{task_local, time::Instant};
use tracing::{error, info};

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
    let shared = Shared::new();
    let post_office = shared.post_office();

    init(shared).await.unwrap();

    let inbox = post_office.new_mailbox_with_special_id(MAIN_ID).1;

    let mut server = Listener::new(shared).await;

    loop {
        tokio::select! {
            letter = inbox.recv_async() => match letter {
                Letter::ShutdownServer => {
                    break;
                }
                Letter::Reset => {
                    shared.reset();

                    drop(server);
                    server = Listener::new(shared).await;
                }
                Letter::BlockAll { unblock_event } => {
                    listener!(unblock_event => listener);
                    listener.await;
                }
                Letter::Resp3(_) | Letter::Wcmd(_) | Letter::Psync {..} => {}
            },
            res = server.run() => {
                if let Err(err) = res {
                    error!(cause = %err, "server run error");
                    post_office.send_shutdown_server().await;
                }
            }
        }
    }

    drop(inbox);

    post_office.wait_shutdown_complete().await;
}

pub async fn init(shared: Shared) -> anyhow::Result<()> {
    static ONCE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

    let post_office = shared.post_office();
    let conf = shared.conf();

    if ONCE.load(Ordering::SeqCst) {
        shared.pool().spawn_pinned(|| async {
            if let Err(e) = tokio::signal::ctrl_c().await {
                eprintln!("Failed to wait for CTRL+C: {}", e);
                std::process::exit(1);
            } else {
                eprintln!("\nShutting down server...");
                post_office.send_shutdown_server().await;
            }
        });

        if let Ok(level) = tracing::Level::from_str(shared.conf().server.log_level.as_ref()) {
            tracing_subscriber::fmt()
                .pretty()
                .with_max_level(level)
                .init();
        }

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
            let mut aof = Aof::new(shared, aof.file_path.clone()).await.unwrap();

            let start = std::time::Instant::now();
            info!("Loading AOF file...");
            if let Err(e) = aof.load().await {
                error!("Failed to load AOF file: {:?}", e);
            } else {
                info!("AOF file loaded. Time elapsed: {:?}", start.elapsed());
            }
        }

        /**********************/
        /* 开启过期键定时检查 */
        /**********************/
        let period = Duration::from_secs(conf.server.expire_check_interval_secs);
        shared.pool().spawn_pinned(move || async move {
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
        });

        if conf.memory.is_some() {
            // 定时更新内存使用情况
            shared.pool().spawn_pinned(move || async move {
                let mut interval = tokio::time::interval(Duration::from_millis(300));

                let mut system = sysinfo::System::new();
                let pid = sysinfo::get_current_pid().unwrap();
                let kind = ProcessRefreshKind::new().with_memory();
                loop {
                    interval.tick().await;
                    system.refresh_processes_specifics(
                        sysinfo::ProcessesToUpdate::Some(&[pid]),
                        kind,
                    );
                    let new_used_mem = system.process(pid).unwrap().memory();
                    USED_MEMORY.store(new_used_mem, Ordering::Relaxed);
                }
            });
        };

        // 定时(每分钟)更新LRU_CLOCK
        shared.pool().spawn_pinned(move || async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                incr_lru_clock();
            }
        });

        ONCE.store(false, Ordering::SeqCst);
    }

    /*********************/
    /* 是否开启AOF持久化 */
    /*********************/
    if let Some(aof) = conf.aof.as_ref() {
        let mut aof = Aof::new(shared, aof.file_path.clone()).await?;

        shared.pool().spawn_pinned(move || async move {
            if let Err(e) = aof.save(shared).await {
                error!("Failed to save AOF file: {}", e);
            }
        });
    }

    let ms_info = { conf.replica.master_info.lock().await.clone() };
    if let Some(ms_info) = ms_info {
        /**********************/
        /* 是否开启了主从复制 */
        /**********************/

        set_server_to_replica(shared, ms_info.host, ms_info.port).await?;
    } else if let Some(master_conf) = conf.master.clone() {
        /**********************/
        /* 是否作为主节点启动 */
        /**********************/

        shared.pool().spawn_pinned(move || async move {
            if let Err(e) = set_server_to_master(shared, master_conf).await {
                error!("{}", e);
            }
        });
    }

    Ok(())
}
