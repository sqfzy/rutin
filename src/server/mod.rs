mod bg_task_channel;
mod error;
mod handler;
mod listener;

pub use bg_task_channel::*;
pub use error::*;
pub use handler::*;
pub use listener::*;

use crate::{
    conf::Conf,
    shared::{db::Db, Shared},
};
use async_shutdown::ShutdownManager;
use crossbeam::atomic::AtomicCell;
use std::sync::Arc;
use tokio::{net::TcpListener, sync::Semaphore};
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error};

// 该值作为新连接的客户端的ID。已连接的客户端的ID会被记录在`Shared`中，在设置ID时
// 需要检查是否已经存在相同的ID
pub static CLIENT_ID_COUNT: AtomicCell<u128> = AtomicCell::new(1);

#[inline]
pub async fn run(listener: TcpListener, conf: Arc<Conf>) {
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

    let mut server = Listener {
        shared: Shared::new(Db::default(), &conf, shutdown_manager.clone()),
        listener,
        tls_acceptor,
        limit_connections: Arc::new(Semaphore::new(conf.server.max_connections)),
        delay_token: shutdown_manager.delay_shutdown_token().unwrap(),
        conf,
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
