use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct MasterConf {
    pub max_replica: usize,
    pub backlog_size: u64,
    pub ping_replica_period: u64,
    pub timeout: u64,
}
