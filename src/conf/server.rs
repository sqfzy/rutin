use serde::Deserialize;

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
