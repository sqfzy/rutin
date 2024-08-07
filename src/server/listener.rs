use super::Handler;
use crate::{
    persist::rdb::Rdb,
    shared::{db::Lru, Shared},
    Id,
};
use async_shutdown::DelayShutdownToken;
use backon::Retryable;
use std::sync::{
    atomic::{AtomicU32, AtomicU64, Ordering},
    Arc,
};
use tokio::{io, net::TcpListener, sync::Semaphore};
use tokio_rustls::TlsAcceptor;
use tracing::error;

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

pub struct Server {
    pub shared: Shared,
    pub listener: TcpListener,
    pub tls_acceptor: Option<TlsAcceptor>,
    pub limit_connections: Arc<Semaphore>,
    pub delay_token: DelayShutdownToken<Id>,
}

impl Server {
    pub async fn new(shared: Shared) -> Self {
        let conf = shared.conf();

        // 开始监听
        let listener =
            tokio::net::TcpListener::bind(format!("{}:{}", conf.server.addr, conf.server.port))
                .await
                .unwrap();

        // 如果配置文件中开启了TLS，则创建TlsAcceptor
        let tls_acceptor = if let Some(tls_conf) = conf.get_tls_config() {
            let tls_acceptor = TlsAcceptor::from(Arc::new(tls_conf));
            Some(tls_acceptor)
        } else {
            None
        };

        let limit_connections = Arc::new(Semaphore::new(conf.server.max_connections));

        let delay_token = shared.signal_manager().delay_shutdown_token().unwrap();

        Server {
            shared,
            listener,
            tls_acceptor,
            limit_connections,
            delay_token,
        }
    }

    pub async fn run(&mut self) -> Result<(), io::Error> {
        tracing::info!(
            "server is running on {}:{}...",
            &self.shared.conf().server.addr,
            self.shared.conf().server.port
        );

        #[cfg(feature = "debug")]
        println!("debug mode is enabled.\n{:?}", self.shared.conf());

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
