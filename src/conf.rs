/// 1. 首先配置结构体从默认值开始构造
/// 2. 如果提供了配置文件，读取配置文件并更新
/// 3. 如果命令行参数有 --server.addr 之类的配置项，merge 该配置
/// 4. 如果环境变量有 SERVER_ADDR 之类配置，进行 merge
use crate::{
    cli::Cli,
    persist::{
        aof::{AppendFSync, AOF},
        rdb::RDB,
    },
    server::Listener,
    shared::Shared,
};
use clap::Parser;
use crossbeam::atomic::AtomicCell;
use rand::Rng;
use serde::Deserialize;
use std::{fs::File, io::BufReader, sync::Arc, time::Duration};
use tokio::{runtime::Handle, time::Instant};
use tokio_rustls::rustls;

#[derive(Debug, Deserialize)]
pub struct Conf {
    pub server: ServerConf,
    pub security: SecurityConf,
    pub replica: ReplicaConf,
    pub rdb: RDBConf,
    pub aof: AOFConf,
    pub memory: MemoryConf,
    pub tls: Option<TLSConf>,
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
    pub max_batch: usize,
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
    pub max_replica: u8,
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
    pub auto_aof_rewrite_min_size: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename = "memory")]
pub struct MemoryConf {
    // pub max_memory: u64,
    // pub max_memory_policy: String,
    // pub max_memory_samples: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename = "tls")]
pub struct TLSConf {
    pub port: u16,
    pub cert_file: String,
    pub key_file: String,
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
                max_batch: 1024,
            },
            security: SecurityConf {
                requirepass: None,
                forbaiden_commands: vec![false; 128],
                rename_commands: vec![None; 128],
            },
            replica: ReplicaConf {
                max_replica: 3,
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
                auto_aof_rewrite_min_size: 64,
            },
            memory: MemoryConf {
                // max_memory: 0,
                // max_memory_policy: "noeviction".to_string(),
                // max_memory_samples: 5,
            },
            tls: None,
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
        let Listener { shared, conf, .. } = listener;

        /*********************/
        /* 是否开启RDB持久化 */
        /*********************/
        if conf.rdb.enable && !conf.aof.enable {
            let mut rdb = RDB::new(
                shared.clone(),
                conf.rdb.file_path.clone(),
                conf.rdb.enable_checksum,
            );

            let start = std::time::Instant::now();
            println!("Loading RDB file...");
            if let Err(e) = rdb.load().await {
                println!("Failed to load RDB file: {:?}", e);
            } else {
                println!("RDB file loaded. Time elapsed: {:?}", start.elapsed());
            }
        }

        /*********************/
        /* 是否开启AOF持久化 */
        /*********************/
        if conf.aof.enable {
            enable_aof(shared.clone(), conf.clone()).await?;
        }

        /**********************/
        /* 开启过期键定时检查 */
        /**********************/
        let period = Duration::from_secs(conf.server.expire_check_interval_secs);
        std::thread::spawn({
            let shared = shared.clone();
            move || {
                std::thread::sleep(period);
                loop {
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

    pub fn get_tls_config(&self) -> Option<rustls::ServerConfig> {
        let tls = self.tls.as_ref()?;

        let cert = rustls_pemfile::certs(&mut BufReader::new(File::open(&tls.cert_file).unwrap()))
            .map(|cert| cert.unwrap())
            .collect();

        let key = rustls_pemfile::pkcs8_private_keys(&mut BufReader::new(
            File::open(&tls.key_file).unwrap(),
        ))
        .next()
        .unwrap()
        .unwrap();

        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert, rustls::pki_types::PrivateKeyDer::Pkcs8(key))
            .unwrap();

        Some(config)
    }
}

async fn enable_aof(shared: Shared, conf: Arc<Conf>) -> anyhow::Result<()> {
    let mut aof = AOF::new(shared.clone(), conf.clone()).await?;

    let (tx, rx) = tokio::sync::oneshot::channel();
    let handle = Handle::current();
    std::thread::spawn(move || {
        handle.block_on(async move {
            let start = std::time::Instant::now();
            println!("Loading AOF file...");
            if let Err(e) = aof.load().await {
                println!("Failed to load AOF file: {:?}", e);
            } else {
                println!("AOF file loaded. Time elapsed: {:?}", start.elapsed());
            }

            tx.send(()).unwrap();

            if let Err(e) = aof.save().await {
                println!("Failed to save AOF file: {:?}", e);
            }
        });
    });

    // 等待AOF文件加载完成
    rx.await?;

    Ok(())
}

#[cfg(test)]
mod conf_tests {
    use crate::{cmd::dispatch, frame::RESP3, server::Handler, shared::db::Db, util::test_init};
    use std::io::Write;

    use super::*;

    #[tokio::test]
    async fn aof_test() {
        test_init();

        const INIT_CONTENT: &[u8; 315] = b"*3\r\n$3\r\nSET\r\n$16\r\nkey:000000000015\r\n$3\r\nVXK\r\n*3\r\n$3\r\nSET\r\n$16\r\nkey:000000000042\r\n$3\r\nVXK\r\n*3\r\n$3\r\nSET\r\n$16\r\nkey:000000000003\r\n$3\r\nVXK\r\n*3\r\n$3\r\nSET\r\n$16\r\nkey:000000000025\r\n$3\r\nVXK\r\n*3\r\n$3\r\nSET\r\n$16\r\nkey:000000000010\r\n$3\r\nVXK\r\n*3\r\n$3\r\nSET\r\n$16\r\nkey:000000000015\r\n$3\r\nVXK\r\n*3\r\n$3\r\nSET\r\n$16\r\nkey:000000000004\r\n$3\r\nVXK\r\n";

        // 1. 测试写传播以及AOF save
        // 2. 测试AOF load

        let test_file_path = "tests/appendonly/test.aof";

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(test_file_path)
            .unwrap_or_else(|e| {
                eprintln!("Failed to open file: {}", e);
                std::process::exit(1);
            });
        file.write_all(INIT_CONTENT).unwrap();
        drop(file);

        let conf = Conf {
            aof: AOFConf {
                enable: true,
                use_rdb_preamble: false,
                file_path: test_file_path.to_string(),
                append_fsync: AppendFSync::Always,
                auto_aof_rewrite_min_size: 64,
            },
            ..Default::default()
        };

        let conf = Arc::new(conf);
        let shutdown = async_shutdown::ShutdownManager::new();
        let shared = Shared::new(Db::default(), &conf, shutdown.clone());
        // 启用AOF，开始AOF save，将AOF文件中的命令加载到内存中
        enable_aof(shared.clone(), conf.clone()).await.unwrap();

        let db = shared.db();
        // 断言AOF文件中的内容已经加载到内存中
        assert_eq!(
            db.get_object_entry(&"key:000000000015".into())
                .unwrap()
                .on_str()
                .unwrap()
                .unwrap()
                .to_vec(),
            b"VXK"
        );
        assert_eq!(
            db.get_object_entry(&"key:000000000003".into())
                .unwrap()
                .on_str()
                .unwrap()
                .unwrap()
                .to_vec(),
            b"VXK"
        );
        assert_eq!(
            db.get_object_entry(&"key:000000000025".into())
                .unwrap()
                .on_str()
                .unwrap()
                .unwrap()
                .to_vec(),
            b"VXK"
        );

        let (mut handler, _) = Handler::new_fake_with(shared.clone(), conf.clone(), None);

        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(test_file_path)
            .await
            .unwrap();
        file.set_len(0).await.unwrap(); // 清空AOF文件
        drop(file);

        let frames = vec![
            RESP3::Array(vec![
                RESP3::Bulk("SET".into()),
                RESP3::Bulk("key:000000000015".into()),
                RESP3::Bulk("VXK".into()),
            ]),
            RESP3::Array(vec![
                RESP3::Bulk("SET".into()),
                RESP3::Bulk("key:000000000003".into()),
                RESP3::Bulk("VXK".into()),
            ]),
            RESP3::Array(vec![
                RESP3::Bulk("SET".into()),
                RESP3::Bulk("key:000000000025".into()),
                RESP3::Bulk("VXK".into()),
            ]),
        ];

        // 执行SET命令, handler会将命令写入AOF文件
        for f in frames {
            dispatch(f, &mut handler).await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(300)).await;
        shutdown.trigger_shutdown(()).unwrap();
    }
}
