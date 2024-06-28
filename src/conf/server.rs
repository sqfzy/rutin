use rand::Rng;
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

impl Default for ServerConf {
    fn default() -> Self {
        let run_id: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(40)
            .map(char::from)
            .collect();

        Self {
            addr: "127.0.0.1".to_string(),
            port: 6379,
            run_id,
            expire_check_interval_secs: 1,
            log_level: "info".to_string(),
            max_connections: 1024,
            max_batch: 1024,
        }
    }
}
