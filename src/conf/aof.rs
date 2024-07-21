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
