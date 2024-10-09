mod connection;
mod handler;
mod listener;

pub use connection::*;
use event_listener::{listener, IntoNotification};
use futures::pin_mut;
pub use handler::*;
pub use listener::*;

use bytes::BytesMut;
use sysinfo::{ProcessRefreshKind, System};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, Layer};

use crate::{
    persist::{aof::load_aof, rdb::load_rdb},
    shared::{
        db::{Atc, Events},
        post_office::Letter,
        Shared, CTRL_C_ID, EXPIRATION_EVICT_ID, MAIN_ID, UPDATE_LRU_CLOCK_ID,
        UPDATE_USED_MEMORY_ID,
    },
    util::{set_server_to_master, set_server_to_replica, UnsafeLazy},
    Id,
};
use std::{
    cell::RefCell,
    str::FromStr,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        LazyLock,
    },
    time::{Duration, SystemTime},
};
use tokio::{
    task_local,
    time::{interval, Instant},
};
use tracing::{error, info, level_filters::LevelFilter};

#[cfg(not(feature = "test_util"))]
pub static SHARED: LazyLock<Shared> = LazyLock::new(Shared::new);
#[cfg(feature = "test_util")]
pub static SHARED: LazyLock<Shared> = LazyLock::new(crate::util::gen_test_shared);

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
pub async fn run() {
    let shared = *SHARED;

    init_log(shared);

    let post_office = shared.post_office();

    let mut mailbox = post_office.register_special_mailbox(MAIN_ID);
    let mut server = Listener::new(shared).await;

    init_server(&mut server).await.unwrap();

    loop {
        tokio::select! {
            letter = mailbox.recv_async() => {
                match letter {
                    Letter::Shutdown => {
                        break;
                    }
                    Letter::ModifyShared(f) => {
                        drop(mailbox);
                        drop(server);
                        shared.wait_shutdown_complete().await;

                        f(unsafe { shared.inner_mut() });

                        mailbox = post_office.register_special_mailbox(MAIN_ID);
                        // drop旧server，保存rdb，新建server
                        server = Listener::new(shared).await;

                        init_server(&mut server).await.unwrap();
                    }
                    Letter::Block { unblock_event } => {
                        unblock_event.notify(1.additional());
                        listener!(unblock_event => listener);
                        listener.await;
                    }
                    Letter::Resp3(_) | Letter::Wcmd(_) | Letter::Psync {..} => {}
                }
            }
            res = server.run() => {
                if let Err(err) = res {
                    error!(cause = %err, "server run error");
                    post_office.send_shutdown_server().await;
                }
            }
        }
    }

    drop(mailbox);

    shared.wait_shutdown_complete().await;
}

pub fn init_log(shared: Shared) {
    let level = tracing::Level::from_str(shared.conf().server.log_level.as_ref())
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

pub async fn init_server(server: &mut Listener) -> anyhow::Result<()> {
    let shared = server.shared;
    let post_office = shared.post_office();
    let conf = shared.conf();

    shared.pool().spawn_pinned(|| async {
        let mailbox = post_office.register_special_mailbox(CTRL_C_ID);

        tokio::select! {
            _ = mailbox.recv_async() => {}
            res = tokio::signal::ctrl_c() => {
                if let Err(e) = res {
                    error!("failed to wait for CTRL+C: {}", e);
                    std::process::exit(1);
                } else {
                    info!("shutting down server...");
                    post_office.send_shutdown_server().await;
                }
            }
        }
    });

    /*******************/
    /* 是否加载rdb文件 */
    /*******************/
    if let (true, Some(rdb_conf)) = (conf.aof.is_none(), conf.rdb.as_ref()) {
        load_rdb(shared, rdb_conf).await.ok();
    }

    /*******************/
    /* 是否加载aof文件 */
    /*******************/
    if let Some(aof_conf) = conf.aof.as_ref() {
        load_aof(shared, aof_conf).await.ok();
    }

    /**********************/
    /* 开启过期键定时检查 */
    /**********************/
    shared.pool().spawn_pinned(move || async move {
        let samples_count = conf.memory.expiration_evict.samples_count;
        let map = &shared.db().entries;

        // 运行的间隔时间，受**内存使用情况**和**过期键比例**影响
        let mut millis = 1000;
        let mut interval = interval(Duration::from_millis(millis)); // 100ms ~ 1000ms

        let mut expiration_proportion = 0_u64; // 过期键比例
        let mut new_value_influence = 70_u64; // 新expiration_proportion的影响占比。50% ~ 70%
        let mut old_value_influence = 100_u64 - new_value_influence; // 旧expiration_proportion的影响占比。30% ~ 50%

        fn adjust_millis(expiration_proportion: u64) -> u64 {
            1000 - 10 * expiration_proportion
        }

        fn adjust_new_value_influence(millis: u64) -> u64 {
            millis / 54 + 430 / 9
        }

        let mailbox = post_office.register_special_mailbox(EXPIRATION_EVICT_ID);
        let fut = mailbox.recv_async();
        pin_mut!(fut);
        loop {
            tokio::select! {
                letter = &mut fut => {
                    if let Letter::Shutdown = letter {
                        break;
                    }
                }
                _ = interval.tick() => {}
            };

            let now = Instant::now();

            // 随机选出samples_count个数据，保留过期的数据
            let mut samples = fastrand::choose_multiple(map.iter(), samples_count as usize);
            samples.retain(|data| data.expire < now);
            let expired_samples = samples;

            let expired_samples_count = expired_samples.len();

            // 移除过期数据，并触发事件
            for sample in expired_samples.into_iter() {
                if let Some((_, mut object)) = map.remove(sample.key()) {
                    Events::try_trigger_read_and_write_event(&mut object);
                }
            }

            let new_expiration_proportion = (expired_samples_count as u64 / samples_count) * 100;

            expiration_proportion = (expiration_proportion * old_value_influence
                + new_expiration_proportion * new_value_influence)
                / 100;

            millis = adjust_millis(expiration_proportion);
            interval.reset_after(Duration::from_millis(millis));
            new_value_influence = adjust_new_value_influence(millis);
            old_value_influence = 100 - new_value_influence;
        }
    });

    // 定时更新内存使用情况
    shared.pool().spawn_pinned(move || async move {
        // TODO: configable
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        let mut system = sysinfo::System::new();
        let pid = sysinfo::get_current_pid().unwrap();
        let kind = ProcessRefreshKind::new().with_memory();

        let mailbox = post_office.register_special_mailbox(UPDATE_USED_MEMORY_ID);
        let fut = mailbox.recv_async();
        pin_mut!(fut);
        loop {
            tokio::select! {
                letter = &mut fut => {
                    if let Letter::Shutdown = letter {
                        break;
                    }
                }
                _ = interval.tick() => {}
            };

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
    shared.pool().spawn_pinned(move || async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));

        let mailbox = post_office.register_special_mailbox(UPDATE_LRU_CLOCK_ID);
        let fut = mailbox.recv_async();
        pin_mut!(fut);
        loop {
            tokio::select! {
                letter = &mut fut => {
                    if let Letter::Shutdown = letter {
                        break;
                    }
                }
                _ = interval.tick() => {}
            };
            incr_lru_clock();
        }
    });

    let ms_info = { conf.replica.master_info.lock().unwrap().clone() };
    if let Some(master_info) = ms_info {
        /**********************/
        /* 是否开启了主从复制 */
        /**********************/

        shared.pool().spawn_pinned(move || async move {
            set_server_to_replica(shared, master_info).await.ok();
        });
    } else if let Some(master_conf) = conf.master.clone() {
        /**********************/
        /* 是否作为主节点启动 */
        /**********************/

        shared.pool().spawn_pinned(move || async move {
            set_server_to_master(shared, master_conf).await.ok();
        });
    }

    Ok(())
}
