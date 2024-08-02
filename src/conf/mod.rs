mod aof;
mod memory;
mod rdb;
mod replica;
mod security;
mod server;
mod tls;

pub use aof::*;
use bytestring::ByteString;
use figment::providers::{Format, Toml};
pub use memory::*;
pub use rdb::*;
pub use replica::*;
pub use security::*;
pub use server::*;
pub use tls::*;

use crate::cli::{merge_cli, Cli};
use clap::Parser;
use rand::Rng;
use serde::Deserialize;
use std::{fs::File, io::BufReader};
use tokio_rustls::rustls;

#[derive(Debug, Deserialize)]
pub struct Conf {
    #[serde(rename = "server")]
    pub server: ServerConf,
    #[serde(rename = "security")]
    pub security: SecurityConf,
    #[serde(rename = "replication")]
    pub replica: ReplicaConf,
    #[serde(rename = "rdb")]
    pub rdb: Option<RdbConf>,
    #[serde(rename = "aof")]
    pub aof: Option<AofConf>,
    #[serde(rename = "memory")]
    pub memory: Option<MemoryConf>,
    #[serde(rename = "tls")]
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

    // pub fn as_static(&self) -> &'static Self {
    //     unsafe { transmute(self) }
    // }

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

fn gen_run_id() -> ByteString {
    rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(40)
        .map(char::from)
        .collect::<String>()
        .into()
}
