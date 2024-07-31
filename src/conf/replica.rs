use arc_swap::ArcSwapOption;
use bytestring::ByteString;
use crossbeam::atomic::AtomicCell;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
#[serde(rename = "replication")]
pub struct ReplicaConf {
    pub master_addr: ArcSwapOption<(ByteString, u16)>, // 主服务器的地址
    /// 最多允许多少个从服务器连接到当前服务器
    pub max_replica: usize,
    /// 用于记录当前服务器的复制偏移量。当从服务器发送 PSYNC
    /// 命令给主服务器时，比较从服务器和主服务器的ACK_OFFSET，从而判断主从是否一致。
    #[serde(skip)]
    pub offset: AtomicCell<u64>,
    // pub back_log:
    #[serde(skip)]
    // pub repli_backlog: RepliBackLog, // 复制积压缓冲区大小
    pub master_auth: Option<String>, // 主服务器密码，设置该值之后，当从服务器连接到主服务器时会发送该值
}

impl ReplicaConf {
    pub fn reset(&self, master_addr: (ByteString, u16)) {
        self.master_addr.store(Some(Arc::new(master_addr)));
    }
}
