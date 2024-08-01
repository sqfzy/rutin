mod bg_task_channel;
mod handler;
mod listener;

pub use bg_task_channel::*;
use bytes::BytesMut;
pub use handler::*;
pub use listener::*;
use sysinfo::System;

use crate::{conf::Conf, Id};
use async_shutdown::ShutdownManager;
use crossbeam::atomic::AtomicCell;
use std::{cell::RefCell, str::FromStr};
use tokio::task_local;
use tracing::{debug, error};

/* 使用保留ID作为特殊信号，其它ID用于指定关闭该ID的连接 */
// 关闭服务。
pub const SHUTDOWN_SIGNAL: Id = 1;
// 重启服务。断开所有连接，释放所有资源，重新初始化。
pub const REBOOT_SIGNAL: Id = 2;
pub const REBOOT_RESERVE_CONF_SIGNAL: Id = 3;

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

        if let Ok(level) = tracing::Level::from_str(conf.server.log_level.as_str()) {
            tracing_subscriber::fmt()
                .pretty()
                .with_max_level(level)
                .init();
        }

        let mut server = Server::new(conf, signal_manager.clone()).await;

        loop {
            // 运行服务，阻塞主线程。当shutdown触发时，解除主线程的阻塞
            match signal_manager.wrap_cancel(server.run()).await {
                // 服务初始化出错，关闭服务
                Ok(Err(err)) => {
                    error!(cause = %err, "failed to accept");
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
                    REBOOT_RESERVE_CONF_SIGNAL => {
                        debug!("set to replica signal received");
                        // 重置Server，保留配置
                        server = Server::new(conf, signal_manager.clone()).await;
                    }
                    // 忽略其它信号
                    _ => {}
                },
                // 服务正常运行时，永不返回
                _ => unreachable!(),
            }
        }

        server.clean().await;
        drop(server.delay_token);

        // 等待所有DelayShutdownToken被释放
        debug!("waiting for shutdown complete");
        signal_manager.wait_shutdown_complete().await;
    }
}
