use std::time::Duration;

/// 1. 首先配置结构体从默认值开始构造
/// 2. 如果提供了配置文件，读取配置文件并更新
/// 3. 如果命令行参数有 --server.addr 之类的配置项，merge 该配置
/// 4. 如果环境变量有 SERVER_ADDR 之类配置，进行 merge
use crate::{
    cli::Cli,
    cmd::dispatch,
    persist::{
        aof::{AppendFSync, AOF},
        rdb::RDB,
        AsyncPersist, Persist,
    },
    server::{BgTaskChannel, Handler, HandlerContext, Listener},
    Connection,
};
use clap::Parser;
use crossbeam::atomic::AtomicCell;
use rand::Rng;
use serde::Deserialize;
use tokio::time::Instant;

#[derive(Debug, Deserialize)]
pub struct Conf {
    pub server: ServerConf,
    pub security: SecurityConf,
    pub replica: ReplicaConf,
    pub rdb: RDBConf,
    pub aof: AOFConf,
    pub memory: MemoryConf,
}

#[derive(Debug, Deserialize)]
#[serde(rename = "server")]
pub struct ServerConf {
    pub addr: String,
    pub port: u16,
    #[serde(skip)]
    pub run_id: String, // 服务器的运行ID。由40个随机字符组成
    pub expire_check_interval_secs: u64, // 检查过期键的周期
    pub log_level: String,
    pub max_connections: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename = "security")]
pub struct SecurityConf {
    pub requirepass: Option<String>, // 访问密码
    // TODO:
    #[serde(skip)]
    pub forbaiden_commands: Vec<bool>,
    // TODO:
    #[serde(skip)]
    pub rename_commands: Vec<Option<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename = "replication")]
pub struct ReplicaConf {
    pub replicaof: Option<String>, // 主服务器的地址
    /// 最多允许多少个从服务器连接到当前服务器
    pub max_replicate: u64,
    /// 用于记录当前服务器的复制偏移量。当从服务器发送 PSYNC
    /// 命令给主服务器时，比较从服务器和主服务器的ACK_OFFSET，从而判断主从是否一致。
    #[serde(skip)]
    pub offset: AtomicCell<u64>,
    #[serde(skip)]
    // pub repli_backlog: RepliBackLog, // 复制积压缓冲区大小
    pub masterauth: Option<String>, // 主服务器密码，设置该值之后，当从服务器连接到主服务器时会发送该值
}

#[derive(Debug, Deserialize)]
#[serde(rename = "rdb")]
pub struct RDBConf {
    pub enable: bool,               // 是否启用RDB持久化
    pub file_path: String,          // RDB文件路径
    pub interval: Option<Interval>, // RDB持久化间隔。格式为"seconds changes"，seconds表示间隔时间，changes表示键的变化次数
    pub version: u32,               // RDB版本号
    pub enable_checksum: bool,      // 是否启用RDB校验和
}

#[derive(Debug, Deserialize)]
pub struct Interval {
    pub seconds: u64,
    pub changes: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename = "aof")]
pub struct AOFConf {
    pub enable: bool, // 是否启用AOF持久化
    pub use_rdb_preamble: bool,
    pub file_path: String,
    pub append_fsync: AppendFSync,
}

#[derive(Debug, Deserialize)]
#[serde(rename = "memory")]
pub struct MemoryConf {
    // pub max_memory: u64,
    // pub max_memory_policy: String,
    // pub max_memory_samples: u64,
}

impl Default for Conf {
    fn default() -> Self {
        let run_id: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(40)
            .map(char::from)
            .collect();
        Self {
            server: ServerConf {
                addr: "127.0.0.1".to_string(),
                port: 6379,
                run_id,
                expire_check_interval_secs: 1,
                log_level: "info".to_string(),
                max_connections: 1024,
            },
            security: SecurityConf {
                requirepass: None,
                forbaiden_commands: vec![false; 128],
                rename_commands: vec![None; 128],
            },
            replica: ReplicaConf {
                max_replicate: 3,
                offset: AtomicCell::new(0),
                // repli_backlog: RepliBackLog::new(1024),
                replicaof: None,
                masterauth: None,
            },
            rdb: RDBConf {
                enable: false,
                file_path: "dump.rdb".to_string(),
                interval: None,
                version: 6,
                enable_checksum: false,
            },
            aof: AOFConf {
                enable: false,
                use_rdb_preamble: false,
                file_path: "appendonly.aof".to_string(),
                append_fsync: AppendFSync::EverySec,
            },
            memory: MemoryConf {
                // max_memory: 0,
                // max_memory_policy: "noeviction".to_string(),
                // max_memory_samples: 5,
            },
        }
    }
}

impl Conf {
    pub fn new() -> anyhow::Result<Self> {
        // 1. 从默认配置文件中加载配置
        let config_builder = config::Config::builder().add_source(config::File::new(
            "config/default.toml",
            config::FileFormat::Toml,
        ));

        // 2. 从用户自定义配置文件中加载配置
        let config_builder = config_builder.add_source(config::File::new(
            "config/custom.toml",
            config::FileFormat::Toml,
        ));

        // 3. 从命令行中加载配置
        let cli = Cli::parse();
        let config_builder = config_builder
            .set_override_option("replication.replicaof", cli.replicaof)?
            .set_override_option("server.port", cli.port)?
            .set_override_option("rdb.file_path", cli.rdb_path)?;

        let mut config: Conf = config_builder.build()?.try_deserialize()?;

        // 4. 运行时配置
        let run_id: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(40)
            .map(char::from)
            .collect();

        config.server.run_id = run_id;
        // 由于AtomicCell<u64>默认值为0，所以不需要设置。repli_backlog同理

        Ok(config)
    }

    pub async fn prepare(listener: &mut Listener) -> anyhow::Result<()> {
        let Listener {
            shared,
            listener,
            shutdown_manager,
            conf,
            ..
        } = listener;

        /*********************/
        /* 是否开启RDB持久化 */
        /*********************/
        if conf.rdb.enable && !conf.aof.enable {
            let mut rdb = RDB::new(
                shared.clone(),
                conf.rdb.file_path.clone(),
                conf.rdb.enable_checksum,
                shutdown_manager.clone(),
            );

            let start = std::time::Instant::now();
            println!("Loading RDB file...");
            if let Err(e) = rdb.load() {
                println!("Failed to load RDB file: {:?}", e);
            } else {
                println!("RDB file loaded. Time elapsed: {:?}", start.elapsed());
            }
        }

        /*********************/
        /* 是否开启AOF持久化 */
        /*********************/
        if conf.aof.enable {
            let mut aof = AOF::new(
                conf.aof.file_path.as_str(),
                conf.aof.append_fsync,
                format!("{}:{}", conf.server.addr.clone(), conf.server.port),
                shared.clone(),
                shutdown_manager.clone(),
            )
            .await?;

            // 用于通知AOF load结束，可以开始AOF save
            let (tx, rx) = tokio::sync::oneshot::channel();

            let start = std::time::Instant::now();
            println!("Loading AOF file...");
            tokio::spawn(async move {
                if let Err(e) = aof.load_async().await {
                    println!("Failed to load AOF file: {:?}", e);
                } else {
                    println!("AOF file loaded. Time elapsed: {:?}", start.elapsed());
                }

                rx.await.unwrap(); // 等待AOF load结束

                if let Err(e) = aof.save_async().await {
                    println!("Failed to save AOF file: {:?}", e);
                }
            });
            // AOF load: 1. 等待AOF client连接
            let (socket, _) = listener.accept().await?;

            let mut handler = Handler {
                shared: shared.clone(),
                conn: Connection::new(socket),
                shutdown_manager: shutdown_manager.clone(),
                bg_task_channel: BgTaskChannel::default(),
                conf: conf.clone(),
                context: HandlerContext::new(0),
            };

            loop {
                // AOF load: 5. 处理AOF client请求，返回响应
                let frames = handler.conn.read_frames().await?;
                if let Some(frames) = frames {
                    for f in frames.into_iter() {
                        dispatch(f, &mut handler).await?;
                    }
                } else {
                    // AOF load: 7. 收到FIN，结束处理AOF client的请求
                    break;
                }
            }
            // AOF load: 8. 通知AOF client关闭连接
            handler.conn.shutdown().await?;

            // AOF load: 9. 此时AOF load执行完毕，可以开始AOF持久化
            tx.send(()).unwrap();
        }

        /**********************/
        /* 开启过期键定时检查 */
        /**********************/
        let period = Duration::from_secs(conf.server.expire_check_interval_secs);
        tokio::spawn({
            let shared = shared.clone();

            async move {
                let mut interval = tokio::time::interval(period);
                loop {
                    interval.tick().await;
                    let shared = shared.clone();

                    let now = Instant::now();
                    let expired_keys: Vec<_> = shared
                        .db()
                        .entry_expire_records()
                        .iter()
                        .filter(|entry| entry.key().0 <= now)
                        .map(|entry| entry.key().1.clone())
                        .collect();

                    for key in expired_keys {
                        tracing::trace!("key {:?} is expired", key);
                        // 删除过期键，该过程会自动删除对应的expire_record
                        // WARN: 执行remove_object时，不应该持有entry_expire_records元素的引用，否则会导致死锁
                        shared.db().remove_object(&key);
                    }
                }
            }
        });

        Ok(())
    }
}
