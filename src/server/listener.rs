use super::Handler;

use crate::{conf::Conf, persist::rdb::RDB, shared::Shared};
use async_shutdown::DelayShutdownToken;
use backon::Retryable;
use std::sync::Arc;
use tokio::{io, net::TcpListener, sync::Semaphore};
use tokio_rustls::TlsAcceptor;
use tracing::error;

pub struct Listener {
    pub shared: Shared,
    pub listener: TcpListener,
    pub tls_acceptor: Option<TlsAcceptor>,
    pub limit_connections: Arc<Semaphore>,
    pub delay_token: DelayShutdownToken<()>,
    pub conf: Arc<Conf>,
}

impl Listener {
    #[inline]
    pub async fn run(&mut self) -> Result<(), io::Error> {
        println!(
            "server is running on {}:{}...",
            &self.conf.server.addr, self.conf.server.port
        );

        Conf::prepare(self).await.unwrap();

        #[cfg(feature = "debug")]
        println!("debug mode is enabled");

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
            let conf = self.conf.clone();

            // 对于每个连接都创建一个delay_token，只有当所有连接都正常退出时，才关闭服务
            let delay_token = self.delay_token.clone();
            match &self.tls_acceptor {
                None => {
                    let mut handler = Handler::new(shared, stream, conf);

                    tokio::spawn(async move {
                        // 开始处理连接
                        if let Err(err) = handler.run().await {
                            error!(cause = ?err, "connection error");
                        }

                        // move delay_token 并且在handler结束时释放，对于run()函数中可能会长时间阻塞本协程的，
                        // 应该考虑使用wrap_cancel()；对于持续阻塞的操作，必须使用wrap_cancel()，否则无法正常
                        // 关闭服务
                        drop(delay_token);
                        #[cfg(not(feature = "debug"))]
                        drop(permit);
                    });
                }
                // 如果开启了TLS，则使用TlsStream
                Some(tls_acceptor) => {
                    let mut handler =
                        Handler::new(shared, tls_acceptor.accept(stream).await?, conf);

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
        let mut rdb = RDB::new(
            self.shared.clone(),
            self.conf.rdb.file_path.clone(),
            self.conf.rdb.enable_checksum,
            self.shared.shutdown().clone(),
        );
        let start = tokio::time::Instant::now();
        rdb.save().await.ok();
        println!("RDB file saved. Time elapsed: {:?}", start.elapsed());
    }
}
