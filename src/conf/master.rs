use super::*;
use crate::util::spawn_set_server_to_master;

#[derive(Debug, PartialEq, Clone, Deserialize)]
pub struct MasterConf {
    pub max_replica: usize,
    pub backlog_size: u64,
    pub ping_replica_period_ms: u64, // 向Replica发送PING的周期。单位：毫秒
    pub timeout_ms: u64,             // 接收Replica的REPLCONF ACK的超时时间。单位：毫秒
}

impl MasterConf {
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

            shared.post_office().send_shutdown(SET_MASTER_ID);
            spawn_set_server_to_master(shared, new.clone());
        } else {
            shared.post_office().send_shutdown(SET_MASTER_ID);
        }
    }

    pub fn update_server_state(new: Option<&Arc<Self>>, shared: Shared) {
        shared.post_office().send_shutdown(SET_MASTER_ID);

        if let Some(new) = new {
            spawn_set_server_to_master(shared, new.clone());
        }
    }
}

impl Default for MasterConf {
    fn default() -> Self {
        MasterConf {
            max_replica: 10,
            backlog_size: 1000,
            ping_replica_period_ms: 1000,
            timeout_ms: 2000,
        }
    }
}
