mod aof;
mod memory;
mod rdb;
mod replica;
mod security;
mod server;
mod tls;

pub use aof::*;
pub use memory::*;
pub use rdb::*;
pub use replica::*;
pub use security::*;
pub use server::*;
pub use tls::*;

use crate::{
    cli::Cli,
    persist::{aof::Aof, rdb::Rdb},
    server::Listener,
    shared::Shared,
};
use clap::Parser;
use rand::Rng;
use serde::Deserialize;
use std::{fs::File, io::BufReader, sync::Arc, time::Duration};
use tokio::{runtime::Handle, time::Instant};
use tokio_rustls::rustls;
use tracing::{error, info};

#[derive(Debug, Deserialize)]
pub struct Conf {
    pub server: ServerConf,
    pub security: SecurityConf,
    pub replica: ReplicaConf,
    pub rdb: Option<RdbConf>,
    pub aof: Option<AofConf>,
    #[serde(default)]
    pub memory: MemoryConf,
    pub tls: Option<TLSConf>,
}

impl Default for Conf {
    fn default() -> Self {
        Self {
            server: ServerConf::default(),
            security: SecurityConf::default(),
            replica: ReplicaConf::default(),
            rdb: Some(RdbConf::default()),
            aof: Some(AofConf::default()),
            memory: MemoryConf::default(),
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

        Ok(config)
    }

    pub async fn prepare(listener: &mut Listener) -> anyhow::Result<()> {
        let shared = &listener.shared;
        let conf = shared.conf();

        /*********************/
        /* 是否开启RDB持久化 */
        /*********************/
        if let (true, Some(rdb)) = (conf.aof.is_none(), conf.rdb.as_ref()) {
            let mut rdb = Rdb::new(shared, rdb.file_path.clone(), rdb.enable_checksum);

            let start = std::time::Instant::now();
            info!("Loading RDB file...");
            if let Err(e) = rdb.load().await {
                error!("Failed to load RDB file: {:?}", e);
            } else {
                info!("RDB file loaded. Time elapsed: {:?}", start.elapsed());
            }
        }

        /*********************/
        /* 是否开启AOF持久化 */
        /*********************/
        if let Some(aof) = conf.aof.as_ref() {
            enable_aof(shared.clone(), conf.clone(), aof.file_path.clone()).await?;
        }

        /**********************/
        /* 开启过期键定时检查 */
        /**********************/
        let period = Duration::from_secs(conf.server.expire_check_interval_secs);
        let handle = Handle::current();
        std::thread::spawn({
            let shared = shared.clone();
            move || {
                std::thread::sleep(period);
                loop {
                    // TODO: 采用自适应算法，根据过期键的数量动态调整检查周期
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
                        handle.block_on(shared.db().remove_object(key));
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

async fn enable_aof(
    shared: Shared,
    conf: Arc<Conf>,
    file_path: impl AsRef<std::path::Path>,
) -> anyhow::Result<()> {
    let mut aof = Aof::new(shared.clone(), conf.clone(), file_path).await?;

    let (tx, rx) = tokio::sync::oneshot::channel();
    let handle = Handle::current();
    std::thread::spawn(move || {
        handle.block_on(async move {
            let start = std::time::Instant::now();
            info!("Loading AOF file...");
            if let Err(e) = aof.load().await {
                error!("Failed to load AOF file: {:?}", e);
            } else {
                info!("AOF file loaded. Time elapsed: {:?}", start.elapsed());
            }

            tx.send(()).unwrap();

            if let Err(e) = aof.save().await {
                error!("Failed to save AOF file: {:?}", e);
            }
        });
    });

    // 等待AOF文件加载完成
    rx.await?;

    Ok(())
}

#[cfg(test)]
mod conf_tests {
    use crate::{cmd::dispatch, frame::Resp3, server::Handler, shared::db::Db, util::test_init};
    use std::io::Write;

    use super::*;

    #[tokio::test]
    async fn aof_test() {
        test_init();
        use crate::persist::aof::AppendFSync;

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
            aof: Some(AofConf {
                use_rdb_preamble: false,
                file_path: test_file_path.to_string(),
                append_fsync: AppendFSync::Always,
                auto_aof_rewrite_min_size: 64,
            }),
            ..Default::default()
        };

        let shutdown = async_shutdown::ShutdownManager::new();
        let shared = Shared::new(Arc::new(Db::default()), Arc::new(conf), shutdown.clone());
        // 启用AOF，开始AOF save，将AOF文件中的命令加载到内存中
        enable_aof(shared.clone(), shared.conf().clone(), test_file_path)
            .await
            .unwrap();

        let db = shared.db();
        // 断言AOF文件中的内容已经加载到内存中
        assert_eq!(
            db.get(&"key:000000000015".into())
                .await
                .unwrap()
                .on_str()
                .unwrap()
                .unwrap()
                .to_vec(),
            b"VXK"
        );
        assert_eq!(
            db.get(&"key:000000000003".into())
                .await
                .unwrap()
                .on_str()
                .unwrap()
                .unwrap()
                .to_vec(),
            b"VXK"
        );
        assert_eq!(
            db.get(&"key:000000000025".into())
                .await
                .unwrap()
                .on_str()
                .unwrap()
                .unwrap()
                .to_vec(),
            b"VXK"
        );

        let (mut handler, _) = Handler::new_fake_with(shared.clone(), None, None);

        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(test_file_path)
            .await
            .unwrap();
        file.set_len(0).await.unwrap(); // 清空AOF文件
        drop(file);

        let frames = vec![
            Resp3::new_array(vec![
                Resp3::new_blob_string("SET".into()),
                Resp3::new_blob_string("key:000000000015".into()),
                Resp3::new_blob_string("VXK".into()),
            ]),
            Resp3::new_array(vec![
                Resp3::new_blob_string("SET".into()),
                Resp3::new_blob_string("key:000000000003".into()),
                Resp3::new_blob_string("VXK".into()),
            ]),
            Resp3::new_array(vec![
                Resp3::new_blob_string("SET".into()),
                Resp3::new_blob_string("key:000000000025".into()),
                Resp3::new_blob_string("VXK".into()),
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
