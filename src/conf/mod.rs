mod aof;
mod master;
mod memory;
mod rdb;
mod replica;
mod security;
mod server;
mod tls;

pub use aof::*;
pub use master::*;
pub use memory::*;
pub use rdb::*;
pub use replica::*;
pub use security::*;
pub use server::*;
pub use tls::*;

use crate::cli::{merge_cli, Cli};
use bytes::Bytes;
use clap::Parser;
use figment::providers::{Format, Toml};
use rand::Rng;
use serde::Deserialize;
use std::{fs::File, io::BufReader};
use tokio_rustls::rustls;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct Conf {
    pub server: ServerConf,
    pub security: SecurityConf,
    pub master: Option<MasterConf>,
    pub replica: ReplicaConf,
    pub rdb: Option<RdbConf>,
    pub aof: Option<AofConf>,
    pub memory: MemoryConf,
    pub tls: Option<TLSConf>,
}

impl Conf {
    pub fn new() -> anyhow::Result<Self> {
        let mut conf: Conf = figment::Figment::new()
            // 1. 从默认配置文件中加载配置
            .join(Toml::file("config/default.toml"))
            // 2. 从用户自定义配置文件中加载配置
            .merge(Toml::file("config/custom.toml"))
            .extract()?;

        // 3. 从命令行中加载配置
        let cli = Cli::parse();

        merge_cli(&mut conf, cli);

        // 4. 运行时配置
        conf.server.run_id = gen_run_id();

        Ok(conf)
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

pub fn gen_run_id() -> Bytes {
    Bytes::from_iter(
        rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(40),
    )
}
