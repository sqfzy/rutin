use super::Handler;
use crate::{
    persist::rdb::Rdb,
    server::HandlerContext,
    shared::{db::Lru, Shared, SPECIAL_ID_RANGE},
};
use backon::Retryable;
use std::sync::{
    atomic::{AtomicU32, AtomicU64, Ordering},
    Arc,
};
use tokio::{net::TcpListener, sync::Semaphore};
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

pub struct Listener {
    pub shared: Shared,
    pub listener: TcpListener,
    pub tls_acceptor: Option<TlsAcceptor>,
    pub limit_connections: Arc<Semaphore>,
    pub next_client_id: u64,
}

impl Listener {
    pub async fn new(shared: Shared) -> Self {
        let conf = shared.conf();

        // 开始监听
        let listener =
            tokio::net::TcpListener::bind(format!("{}:{}", conf.server.host, conf.server.port))
                .await
                .unwrap();

        // 如果配置文件中开启了TLS，则创建TlsAcceptor
        let tls_acceptor = if let Some(tls_conf) = conf.get_tls_config() {
            let tls_acceptor = TlsAcceptor::from(Arc::new(tls_conf));
            Some(tls_acceptor)
        } else {
            None
        };

        Listener {
            shared,
            listener,
            tls_acceptor,
            limit_connections: Arc::new(Semaphore::new(conf.server.max_connections)),
            next_client_id: SPECIAL_ID_RANGE.end,
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        tracing::info!(
            "server is running on {}:{}...",
            &self.shared.conf().server.host,
            self.shared.conf().server.port
        );

        #[cfg(feature = "debug")]
        println!("debug mode is enabled.\n{:?}", self.shared.conf());

        let post_office = self.shared.post_office();
        loop {
            #[cfg(not(feature = "debug"))]
            let permit = self.limit_connections.clone().acquire_owned().await?;

            let (stream, _) = (|| async { self.listener.accept().await })
                .retry(&backon::ExponentialBuilder::default())
                .await?;

            let (id, outbox, inbox) = post_office.new_mailbox(self.next_client_id);
            self.next_client_id = id + 1;

            let context = HandlerContext::new(self.shared, id, outbox, inbox);
            match &self.tls_acceptor {
                None => {
                    let handler = Handler::new(self.shared, stream, context);

                    self.shared.pool().spawn_pinned(|| async move {
                        // 开始处理连接
                        if let Err(err) = handler.run().await {
                            error!(cause = ?err, "connection error");
                        }

                        #[cfg(not(feature = "debug"))]
                        drop(permit);
                    });
                }
                // 如果开启了TLS，则使用TlsStream
                Some(tls_acceptor) => {
                    let handler =
                        Handler::new(self.shared, tls_acceptor.accept(stream).await?, context);

                    self.shared.pool().spawn_pinned(|| async move {
                        // 开始处理连接
                        if let Err(err) = handler.run().await {
                            error!(cause = ?err, "connection error");
                        }

                        #[cfg(not(feature = "debug"))]
                        drop(permit);
                    });
                }
            };
        }
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        let conf = self.shared.conf();
        if let (true, Some(rdb)) = (conf.aof.is_none(), conf.rdb.as_ref()) {
            let mut rdb = Rdb::new(self.shared, rdb.file_path.clone(), rdb.enable_checksum);

            let _delay_token = self.shared.post_office().delay_token();
            self.shared.pool().spawn_pinned(move || async move {
                rdb.save().await.ok();

                drop(_delay_token);
            });
        }
    }
}
