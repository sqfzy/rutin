use crate::{
    cmd::*,
    conf::Conf,
    connection::Connection,
    frame::Frame,
    persist::{rdb::RDB, Persist},
    shared::{db::Db, wcmd_propagator::WCmdPropergator, Shared},
    Id, Key,
};
use async_shutdown::{DelayShutdownToken, ShutdownManager};
use backon::{ExponentialBuilder, Retryable};
use crossbeam::atomic::AtomicCell;
use flume::{Receiver, Sender};
use std::sync::Arc;
use tokio::{io, net::TcpListener, sync::Semaphore};
use tracing::{debug, error, instrument};

// 该值作为新连接的客户端的ID。已连接的客户端的ID会被记录在`Shared`中，在设置ID时
// 需要检查是否已经存在相同的ID
pub static CLIENT_ID_COUNT: AtomicCell<u128> = AtomicCell::new(0);

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

    let mut server = Listener {
        shared: Shared::new(Db::default(), WCmdPropergator::new(conf.aof.enable)),
        listener,
        limit_connections: Arc::new(Semaphore::new(conf.server.max_connections)),
        shutdown_manager: shutdown_manager.clone(),
        delay_token: shutdown_manager.delay_shutdown_token().unwrap(),
        conf,
    };

    // 当shutdown触发时，解除主线程的阻塞
    if let Ok(Err(err)) = shutdown_manager.wrap_cancel(server.run()).await {
        error!(cause = %err, "failed to accept");
        shutdown_manager.trigger_shutdown(()).ok();
    }

    server.clean().await;
    drop(server.delay_token);

    // 等待所有DelayShutdownToken被释放
    shutdown_manager.wait_shutdown_complete().await;
}

pub struct Listener {
    pub shared: Shared,
    pub listener: TcpListener,
    pub limit_connections: Arc<Semaphore>,
    pub shutdown_manager: ShutdownManager<()>,
    pub delay_token: DelayShutdownToken<()>,
    pub conf: Arc<Conf>,
}

impl Listener {
    async fn run(&mut self) -> Result<(), io::Error> {
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

            let (socket, peer_addr) = (|| async { self.listener.accept().await })
                .retry(&ExponentialBuilder::default().with_jitter())
                .await?;

            let shared = self.shared.clone();
            let bg_task_channel = BgTaskChannel::default();

            // 获取一个有效的客户端ID
            let id_may_occupied = CLIENT_ID_COUNT.fetch_add(1);
            let client_id = shared
                .db()
                .record_client_id(id_may_occupied, bg_task_channel.new_sender());
            if id_may_occupied != client_id {
                CLIENT_ID_COUNT.store(client_id);
            }

            let mut handler = Handler {
                shared,
                conn: Connection::new(socket),
                shutdown_manager: self.shutdown_manager.clone(),
                bg_task_channel,
                conf: self.conf.clone(),
                context: HandlerContext::new(CLIENT_ID_COUNT.fetch_add(1)),
            };

            let delay_token = self.delay_token.clone();
            tokio::spawn(async move {
                if let Err(err) = handler.run(&format!("client {peer_addr}")).await {
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
    }

    pub async fn clean(&mut self) {
        let mut rdb = RDB::new(
            self.shared.clone(),
            self.conf.rdb.file_path.clone(),
            self.conf.rdb.enable_checksum,
            self.shutdown_manager.clone(),
        );
        let start = tokio::time::Instant::now();
        rdb.save().await.ok();
        println!("RDB file saved. Time elapsed: {:?}", start.elapsed());
    }
}

pub struct Handler {
    pub shared: Shared,
    pub conn: Connection,
    pub shutdown_manager: ShutdownManager<()>,
    pub bg_task_channel: BgTaskChannel,
    pub conf: Arc<Conf>,
    pub context: HandlerContext,
}

impl Handler {
    #[instrument(level = "debug", skip(self), fields(client_id = %self.context.client_id), err)]
    pub async fn run(&mut self, peer: &str) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                // 等待shutdown信号
                _signal = self.shutdown_manager.wait_shutdown_triggered() => {
                    return Ok(());
                }
                // 等待客户端请求
                frames =  self.conn.read_frames() => {
                    if let Some(frames) = frames? {
                        for f in frames.into_iter() {
                            dispatch(f, self).await?;
                        }
                    }else {
                        return Ok(());
                    }
                },
                // 从后台任务接收数据，并发送给客户端。只要拥有对应的BgTaskSender，
                // 任何其它连接 都可以向当前连接的客户端发送消息
                frame = self.bg_task_channel.recv_from_bg_task() => {
                    debug!("recv from background task: {:?}", frame);
                    self.conn.write_frame(&frame).await?;
                },
            };
        }
    }
}

pub type BgTaskSender = Sender<Frame<'static>>;

#[derive(Debug, Clone)]
pub struct BgTaskChannel {
    tx: BgTaskSender,
    rx: Receiver<Frame<'static>>,
}

impl BgTaskChannel {
    pub fn new_sender(&self) -> BgTaskSender {
        self.tx.clone()
    }

    pub fn get_sender(&self) -> &BgTaskSender {
        &self.tx
    }

    pub async fn recv_from_bg_task(&self) -> Frame<'static> {
        self.rx.recv_async().await.unwrap()
    }
}

impl Default for BgTaskChannel {
    fn default() -> Self {
        let (tx, rx) = flume::bounded(1024);
        Self { tx, rx }
    }
}

pub struct HandlerContext {
    pub client_id: Id,
    // 客户端订阅的频道
    pub subscribed_channels: Option<Vec<Key>>,
    // 是否开启缓存追踪
    pub client_track: Option<BgTaskSender>,
}

impl HandlerContext {
    pub fn new(client_id: Id) -> Self {
        Self {
            client_id,
            subscribed_channels: None,
            client_track: None,
        }
    }
}
