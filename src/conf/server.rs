use bytes::Bytes;
use bytestring::ByteString;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ServerConf {
    pub host: ByteString,
    pub port: u16,
    #[serde(skip)]
    pub run_id: Bytes, // 服务器的运行ID。由40个随机字符组成
    pub log_level: ByteString,
    pub max_connections: usize,
    pub max_batch: usize,
}
