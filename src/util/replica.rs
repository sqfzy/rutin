use crate::shared::Shared;
use bytestring::ByteString;
use std::sync::Arc;

pub fn set_server_to_master(shared: &Shared) {
    let conf = shared.conf();

    if conf.replica.master_addr.load().is_none() {
        return;
    }

    conf.replica.master_addr.store(None);
}

pub fn set_server_to_replica(shared: &Shared, master_addr: (ByteString, u16)) {
    let conf = shared.conf();

    conf.replica.reset(master_addr);

    // 只允许读命令
    // 设置ac

    // 重置db
    todo!()

    //

    // 断开所有连接
    // clean ReplicationBacklog
}
