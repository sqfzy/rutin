use crate::{conf::AccessControl, server::REBOOT_RESERVE_CONF_SIGNAL, shared::Shared};
use bytestring::ByteString;

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

    // 所有ac都只允许读命令
    if let Some(acl) = &conf.security.acl {
        for mut ac in acl.iter_mut() {
            ac.set_only_read();
        }
    }
    conf.security.default_ac.rcu(|ac| {
        let mut ac = AccessControl::clone(ac);
        ac.set_only_read();
        ac
    });

    // 重启服务(释放所有连接和资源)
    shared
        .signal_manager()
        .trigger_shutdown(REBOOT_RESERVE_CONF_SIGNAL)
        .ok();
}
