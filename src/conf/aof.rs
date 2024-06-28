use crate::persist::aof::AppendFSync;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename = "aof")]
pub struct AofConf {
    pub use_rdb_preamble: bool,
    pub file_path: String,
    pub append_fsync: AppendFSync,
    pub auto_aof_rewrite_min_size: usize,
}

impl Default for AofConf {
    fn default() -> Self {
        Self {
            use_rdb_preamble: true,
            file_path: "appendonly.aof".to_string(),
            append_fsync: AppendFSync::EverySec,
            auto_aof_rewrite_min_size: 128,
        }
    }
}
