use bytes::Bytes;
use bytestring::ByteString;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ServerConf {
    pub host: ByteString,
    pub port: u16,
    #[serde(skip)]
    pub run_id: Bytes, // 服务器的运行ID。由40个随机字符组成
    pub expire_check_interval_secs: u64, // 检查过期键的周期
    pub log_level: ByteString,
    pub max_connections: usize,
    pub max_batch: usize,

    // 是否为单机模式。如果为false，则总是会传播写命令，无论是否有从节点。这会导致性能有些许下降；
    // 如果为true，则不会传播写命令，且无法添加从节点。
    pub standalone: bool,
}
