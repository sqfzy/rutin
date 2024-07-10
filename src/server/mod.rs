mod bg_task_channel;
mod handler;
mod listener;

pub use bg_task_channel::*;
pub use handler::*;
pub use listener::*;
use sysinfo::System;

use crate::{
    conf::Conf,
    shared::{db::Db, Shared},
    Id,
};
use async_shutdown::ShutdownManager;
use crossbeam::atomic::AtomicCell;
use std::{cell::RefCell, sync::Arc};
use tokio::{net::TcpListener, sync::Semaphore, task_local};
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error};

pub const RESERVE_MAX_ID: Id = 20;
pub static RESERVE_ID: AtomicCell<Id> = AtomicCell::new(0);
// 该值作为新连接的客户端的ID。已连接的客户端的ID会被记录在`Shared`中，在设置ID时
// 需要检查是否已经存在相同的ID。保留前20个ID，专用于事务处理
pub static CLIENT_ID_COUNT: AtomicCell<Id> = AtomicCell::new(RESERVE_MAX_ID + 1);

task_local! { pub static ID: Id; }

thread_local! {
    pub static SYSTEM: RefCell<System>  = RefCell::new(System::new_all());
}

#[inline]
pub async fn run(listener: TcpListener, conf: Conf) {
    let shutdown_manager = ShutdownManager::new();

    tokio::spawn({
        let shutdown = shutdown_manager.clone();
        async move {
            if let Err(e) = tokio::signal::ctrl_c().await {
                eprintln!("Failed to wait for CTRL+C: {}", e);
                std::process::exit(1);
            } else {
                eprintln!("\nShutting down server...");
                shutdown.trigger_shutdown(()).ok();
            }
        }
    });

    // 如果配置文件中开启了TLS，则创建TlsAcceptor
    let tls_acceptor = if let Some(tls_conf) = conf.get_tls_config() {
        let tls_acceptor = TlsAcceptor::from(Arc::new(tls_conf));
        Some(tls_acceptor)
    } else {
        None
    };

    let limit_connections = Arc::new(Semaphore::new(conf.server.max_connections));
    let conf = Arc::new(conf);
    let mut server = Listener {
        shared: Shared::new(
            Arc::new(Db::new(conf.clone())),
            conf,
            shutdown_manager.clone(),
        ),
        listener,
        tls_acceptor,
        limit_connections,
        delay_token: shutdown_manager.delay_shutdown_token().unwrap(),
    };

    // 运行服务，阻塞主线程。当shutdown触发时，解除主线程的阻塞
    if let Ok(Err(err)) = shutdown_manager.wrap_cancel(server.run()).await {
        error!(cause = %err, "failed to accept");
        shutdown_manager.trigger_shutdown(()).ok();
    }
    debug!("shutdown complete");

    server.clean().await;
    drop(server.delay_token);

    // 等待所有DelayShutdownToken被释放
    debug!("waiting for shutdown complete");
    shutdown_manager.wait_shutdown_complete().await;
}
