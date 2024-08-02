use crate::conf::gen_run_id;
use bytestring::ByteString;
use serde::{Deserialize, Deserializer};
use std::sync::Mutex;

#[derive(Debug)]
pub struct ReplicaConf {
    pub master_info: Mutex<Option<MasterInfo>>,
    /// 最多允许多少个从服务器连接到当前服务器
    pub max_replica: usize,
    /// 用于记录当前服务器的复制偏移量。当从服务器发送 PSYNC
    /// 命令给主服务器时，比较从服务器和主服务器的ACK_OFFSET，从而判断主从是否一致。
    // #[serde(skip)]
    // pub offset: AtomicCell<u64>,
    // pub back_log:
    // #[serde(skip)]
    // pub repli_backlog: RepliBackLog, // 复制积压缓冲区大小
    pub master_auth: Option<String>, // 主服务器密码，设置该值之后，当从服务器连接到主服务器时会发送该值
}

impl<'de> Deserialize<'de> for ReplicaConf {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let master_info = Option::<MasterInfo>::deserialize(deserializer)?;
        Ok(ReplicaConf {
            master_info: Mutex::new(master_info),
            max_replica: 0,
            master_auth: None,
        })
    }
}

#[derive(Debug, Deserialize)]
pub struct MasterInfo {
    pub host: ByteString,
    pub port: u16,
    pub run_id: ByteString,
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
