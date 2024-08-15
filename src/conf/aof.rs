use crate::persist::aof::AppendFSync;
use serde::{Deserialize, Deserializer};

#[derive(Debug)]
pub struct AofConf {
    pub use_rdb_preamble: bool,
    pub file_path: String,
    pub append_fsync: AppendFSync,
    pub auto_aof_rewrite_min_size: u128,
}

impl<'de> Deserialize<'de> for AofConf {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct TempAofConf {
            pub use_rdb_preamble: bool,
            pub file_path: String,
            pub append_fsync: AppendFSync,
            pub auto_aof_rewrite_min_size: u128,
        }

        let temp = TempAofConf::deserialize(deserializer)?;

        Ok(AofConf {
            use_rdb_preamble: temp.use_rdb_preamble,
            file_path: temp.file_path,
            append_fsync: temp.append_fsync,
            auto_aof_rewrite_min_size: temp.auto_aof_rewrite_min_size << 20,
        })
    }
}
