use crossbeam::atomic::AtomicCell;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename = "replication")]
pub struct ReplicaConf {
    pub replicaof: Option<String>, // 主服务器的地址
    /// 最多允许多少个从服务器连接到当前服务器
    pub max_replica: u8,
    /// 用于记录当前服务器的复制偏移量。当从服务器发送 PSYNC
    /// 命令给主服务器时，比较从服务器和主服务器的ACK_OFFSET，从而判断主从是否一致。
    #[serde(skip)]
    pub offset: AtomicCell<u64>,
    #[serde(skip)]
    // pub repli_backlog: RepliBackLog, // 复制积压缓冲区大小
    pub masterauth: Option<String>, // 主服务器密码，设置该值之后，当从服务器连接到主服务器时会发送该值
}
