use crate::{
    conf::{Shared, AOF_ID},
    persist::aof::spawn_save_aof,
};
use serde::{Deserialize, Deserializer};
use std::sync::Arc;

#[derive(Debug, PartialEq, Clone, Deserialize)]
pub struct AofConf {
    pub use_rdb_preamble: bool,
    pub file_path: String,
    pub append_fsync: AppendFSync,
    pub auto_aof_rewrite_min_size: u128, // 单位为MB
}

impl Default for AofConf {
    fn default() -> Self {
        AofConf {
            use_rdb_preamble: true,
            file_path: "appendonly.aof".to_string(),
            append_fsync: AppendFSync::EverySec,
            auto_aof_rewrite_min_size: 64,
        }
    }
}

impl AofConf {
    pub fn may_update_server_state(
        new: Option<&Arc<Self>>,
        old: Option<&Arc<Self>>,
        shared: Shared,
    ) {
        if let Some(new) = new {
            if let Some(old) = old
                && new == old
            {
                return;
            }

            shared.post_office().send_shutdown(AOF_ID);
            spawn_save_aof(shared, new.clone());
        } else {
            shared.post_office().send_shutdown(AOF_ID);
        }
    }

    pub fn update_server_state(new: Option<&Arc<Self>>, shared: Shared) {
        shared.post_office().send_shutdown(AOF_ID);

        if let Some(new) = new {
            spawn_save_aof(shared, new.clone());
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase", rename = "append_fsync")]
pub enum AppendFSync {
    Always,
    #[default]
    EverySec,
    No,
}
