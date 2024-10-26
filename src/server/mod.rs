mod connection;
mod handler;
mod listener;

pub use connection::*;
use event_listener::{listener, IntoNotification};
pub use handler::*;
pub use listener::*;

use bytes::BytesMut;
use sysinfo::{ProcessRefreshKind, System};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, Layer};

use crate::{
    conf::{spawn_expiration_evict, ServerConf, TlsConf},
    persist::{
        aof::{load_aof, spawn_save_aof},
        rdb::load_rdb,
    },
    shared::{db::Atc, post_office::Letter, Shared, CTRL_C_ID, MAIN_ID},
    util::{spawn_set_server_to_master, spawn_set_server_to_replica, UnsafeLazy},
    Id,
};
use std::{
    cell::RefCell,
    str::FromStr,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};
use tokio::{task_local, time::Instant};
use tracing::{error, info, level_filters::LevelFilter};

// 程序运行前或测试前需要进行初始化
pub fn preface() {
    unsafe {
        UNIX_EPOCH.init();
        NEVER_EXPIRE.init();
    }
}

pub static UNIX_EPOCH: UnsafeLazy<Instant> = UnsafeLazy::new(|| {
    Instant::now()
        - SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
});

pub static NEVER_EXPIRE: UnsafeLazy<Instant> =
    UnsafeLazy::new(|| Instant::now() + Duration::from_secs(3600 * 24 * 365));

pub static USED_MEMORY: AtomicU64 = AtomicU64::new(0);

static LRU_CLOCK: AtomicU32 = AtomicU32::new(0);

#[inline]
pub fn incr_lru_clock() {
    LRU_CLOCK.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn get_lru_clock() -> u32 {
    LRU_CLOCK.load(Ordering::Relaxed) % Atc::LRU_CLOCK_MAX
}

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
pub fn spawn_run_server(
    server_conf: Arc<ServerConf>,
    tls_conf: Option<Arc<TlsConf>>,
    shared: Shared,
) {
    shared.pool().spawn_pinned(move || async move {
        init_log(shared);

        let post_office = shared.post_office();

        let mailbox = post_office.register_mailbox(MAIN_ID);
        let mut server = Listener::new(server_conf, tls_conf, shared).await;

        loop {
            tokio::select! {
                letter = mailbox.recv_async() => {
                    match letter {
                        Letter::Shutdown => {
                            break;
                        }
                        Letter::Block { unblock_event } => {
                            unblock_event.notify(1.additional());
                            listener!(unblock_event => listener);
                            listener.await;
                        }
                        _ => {}
                    }
                }
                res = server.run() => {
                    if let Err(err) = res {
                        error!(cause = %err, "server run error");
                        post_office.send_shutdown_server();
                    }
                }
            }
        }
    });
}

// TODO: 可修改
pub fn init_log(shared: Shared) {
    let level = tracing::Level::from_str(shared.conf().server_conf().log_level.as_ref())
        .expect("invalid log level");

    let logfile = tracing_appender::rolling::hourly("logs", "rutin.log");
    let file_layer = fmt::layer()
        .with_writer(logfile)
        .with_ansi(false) // 文件日志不使用 ANSI
        .with_filter(LevelFilter::from_level(level));

    let stdout_layer = fmt::layer()
        .pretty()
        .with_writer(std::io::stdout)
        .with_thread_ids(true)
        .with_filter(LevelFilter::from_level(level));

    tracing_subscriber::registry()
        .with(stdout_layer)
        .with(file_layer)
        .init();
}

pub async fn init_server(shared: Shared) -> anyhow::Result<()> {
    let conf = shared.conf();

    /*******************/
    /* 是否加载rdb文件 */
    /*******************/
    if let (true, Some(rdb_conf)) = (conf.aof_conf().is_none(), conf.rdb_conf().as_ref()) {
        load_rdb(shared, rdb_conf).await.ok();
    }

    /*******************/
    /* 是否加载aof文件 */
    /*******************/
    if let Some(aof_conf) = conf.aof_conf().as_ref() {
        load_aof(shared, aof_conf).await.ok();
    }

    // 定时更新内存使用情况
    shared.pool().spawn_pinned(move || async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        let mut system = sysinfo::System::new();
        let pid = sysinfo::get_current_pid().unwrap();
        let kind = ProcessRefreshKind::new().with_memory();

        loop {
            interval.tick().await;

            system.refresh_processes_specifics(
                sysinfo::ProcessesToUpdate::Some(&[pid]),
                false,
                kind,
            );
            let new_used_mem = system.process(pid).unwrap().memory();
            USED_MEMORY.store(new_used_mem, Ordering::Relaxed);
        }
    });

    // 定时(每分钟)更新LRU_CLOCK
    shared.pool().spawn_pinned(|| async {
        let mut interval = tokio::time::interval(Duration::from_secs(60));

        loop {
            interval.tick().await;
            incr_lru_clock();
        }
    });

    // MAIN_ID 任务
    spawn_run_server(conf.server_conf().clone(), conf.tls_conf().clone(), shared);

    // AOF_ID 任务
    if let Some(aof_conf) = conf.aof_conf().clone() {
        spawn_save_aof(shared, aof_conf);
    }

    // SET_MASTER_ID 任务
    if let Some(master_conf) = conf.master_conf().clone() {
        spawn_set_server_to_master(shared, master_conf);
    }

    // SET_REPLICA_ID 任务
    if let Some(replica_conf) = conf.replica_conf().clone() {
        spawn_set_server_to_replica(shared, replica_conf);
    }

    // EXPIRATION_EVICT_ID 任务
    spawn_expiration_evict(shared, conf.memory_conf().clone());

    Ok(())
}

pub async fn waitting_shutdown(shared: Shared) {
    let post_office = shared.post_office();
    let mailbox = post_office.register_mailbox(CTRL_C_ID);

    tokio::select! {
        _ = mailbox.recv_async() => {}
        res = tokio::signal::ctrl_c() => match res {
            Ok(_) => {},
            Err(e) => {
                error!("failed to wait for CTRL+C: {}", e);
                std::process::exit(1);
            }
        }
    }

    info!("shutting down server...");
    post_office.send_shutdown_server();

    shared.wait_shutdown_complete().await;
}
