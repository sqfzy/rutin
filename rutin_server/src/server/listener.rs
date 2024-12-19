use super::Handler;
use crate::{
    conf::{ServerConf, TlsConf},
    persist::rdb::save_rdb,
    server::HandlerContext,
    shared::{Shared, SPECIAL_ID_RANGE},
};
use backon::Retryable;
use std::sync::Arc;
use tokio::{net::TcpListener, sync::Semaphore};
use tokio_rustls::TlsAcceptor;

pub struct Listener {
    pub shared: Shared,
    pub server_conf: Arc<ServerConf>,
    pub listener: TcpListener,
    pub tls_acceptor: Option<TlsAcceptor>,
    pub limit_connections: Arc<Semaphore>,
    pub next_client_id: u64,
}

impl Listener {
    pub async fn new(
        server_conf: Arc<ServerConf>,
        tls_conf: Option<Arc<TlsConf>>,
        shared: Shared,
    ) -> Self {
        use std::{fs::File, io::BufReader};
        use tokio_rustls::rustls;

        // 开始监听
        let listener =
            tokio::net::TcpListener::bind(format!("{}:{}", server_conf.host, server_conf.port))
                .await
                .unwrap();

        // 如果配置文件中开启了TLS，则创建TlsAcceptor
        let tls_acceptor = if let Some(tls_conf) = tls_conf.clone() {
            let cert = rustls_pemfile::certs(&mut BufReader::new(
                File::open(&tls_conf.cert_file).unwrap(),
            ))
            .map(|cert| cert.unwrap())
            .collect();

            let key = rustls_pemfile::pkcs8_private_keys(&mut BufReader::new(
                File::open(&tls_conf.key_file).unwrap(),
            ))
            .next()
            .unwrap()
            .unwrap();

            let config = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(cert, rustls::pki_types::PrivateKeyDer::Pkcs8(key))
                .unwrap();

            Some(TlsAcceptor::from(Arc::new(config)))
        } else {
            None
        };

        Listener {
            shared,
            limit_connections: Arc::new(Semaphore::new(server_conf.max_connections)),
            server_conf,
            listener,
            tls_acceptor,
            next_client_id: SPECIAL_ID_RANGE.end,
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        let shared = self.shared;
        let server_conf = &self.server_conf;
        tracing::info!(
            "server is running on {}:{}...",
            server_conf.host,
            server_conf.port
        );

        #[cfg(feature = "debug")]
        println!("debug mode is enabled.\n{:?}", self.shared.conf());

        let post_office = shared.post_office();
        loop {
            #[cfg(not(feature = "debug"))]
            let permit = self.limit_connections.clone().acquire_owned().await?;

            let (stream, _) = (|| async { self.listener.accept().await })
                .retry(backon::ExponentialBuilder::default())
                .await?;

            // 如果next_client_id在特殊范围内，则跳过
            if SPECIAL_ID_RANGE.contains(&self.next_client_id) {
                self.next_client_id = SPECIAL_ID_RANGE.end;
            }

            let mut id = self.next_client_id;
            self.next_client_id = id + 1;
            let mailbox = post_office.register_mailbox(&mut id);

            let context = HandlerContext::new(shared, id, mailbox);

            match &self.tls_acceptor {
                None => {
                    shared.pool().spawn_pinned(move || async move {
                        let handler = Handler::new(shared, stream, context);

                        // 开始处理连接
                        handler.run().await.ok();

                        #[cfg(not(feature = "debug"))]
                        drop(permit);
                    });
                }
                // 如果开启了TLS，则使用TlsStream
                Some(tls_acceptor) => {
                    let stream = tls_acceptor.accept(stream).await?;
                    shared.pool().spawn_pinned(move || async move {
                        let handler = Handler::new(shared, stream, context);

                        // 开始处理连接
                        handler.run().await.ok();

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
        let shared = self.shared;
        let conf = shared.conf();

        if let Some(rdb_conf) = conf.rdb_conf().clone() {
            let handle = tokio::runtime::Handle::current();
            let handle = std::thread::spawn(move || {
                handle.block_on(async {
                    save_rdb(shared, &rdb_conf).await.ok();
                });
            });
            handle.join().ok();
        }
    }
}
