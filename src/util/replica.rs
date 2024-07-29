use crate::shared::Shared;

pub fn set_server_to_master(shared: &Shared) {
    let conf = shared.conf();

    if conf.replica.master_addr.load().is_none() {
        return;
    }

    conf.replica.master_addr.store(None);
}
