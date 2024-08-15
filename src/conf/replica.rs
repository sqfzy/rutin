use crate::conf::gen_run_id;
use bytes::Bytes;
use bytestring::ByteString;
use serde::{Deserialize, Deserializer};
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct ReplicaConf {
    pub repl_ping_replica_period: u64,
    pub repl_timeout: u64,
    pub repl_backlog_size: u64,

    pub master_info: Mutex<Option<MasterInfo>>,
    /// 最多允许多少个从服务器连接到当前服务器
    pub max_replica: usize,
    pub read_only: bool,
    pub master_auth: Option<String>, // 主服务器密码，设置该值之后，当从服务器连接到主服务器时会发送该值
}

impl<'de> Deserialize<'de> for ReplicaConf {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct TempReplicaConf {
            repl_ping_replica_period: u64,
            repl_timeout: u64,
            repl_backlog_size: u64,
            master_info: Option<MasterInfo>,
            max_replica: usize,
            read_only: bool,
            master_auth: Option<String>,
        }

        let temp = TempReplicaConf::deserialize(deserializer)?;

        Ok(ReplicaConf {
            repl_ping_replica_period: temp.repl_ping_replica_period,
            repl_timeout: temp.repl_timeout,
            repl_backlog_size: temp.repl_backlog_size << 10,
            master_info: Mutex::new(temp.master_info),
            max_replica: temp.max_replica,
            read_only: temp.read_only,
            master_auth: temp.master_auth,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MasterInfo {
    pub host: ByteString,
    pub port: u16,
    pub run_id: Bytes,
}

impl MasterInfo {
    pub fn new(host: ByteString, port: u16) -> Self {
        Self {
            host,
            port,
            run_id: gen_run_id(),
        }
    }
}
