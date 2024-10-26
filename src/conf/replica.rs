use super::*;
use crate::util::spawn_set_server_to_replica;
use bytestring::ByteString;
use std::sync::{atomic::AtomicU64, Mutex};

#[derive(Debug, Deserialize)]
pub struct ReplicaConf {
    pub master_host: ByteString,
    pub master_port: u16,
    pub master_auth: Option<String>,
    pub read_only: bool,

    pub offset: AtomicU64,
    pub master_run_id: ByteString,
}

impl ReplicaConf {
    pub fn new(master_host: ByteString, master_port: u16) -> Self {
        Self {
            master_host,
            master_port,
            master_auth: None,
            read_only: true,

            offset: AtomicU64::new(0),
            master_run_id: ByteString::from("?"),
        }
    }

    pub fn may_update_server_state(
        new: Option<&Arc<Self>>,
        old: Option<&Arc<Self>>,
        shared: Shared,
    ) {
        if let Some(new) = new {
            if let Some(old) = old
                && new.master_host == old.master_host
                && new.master_port == old.master_port
                && new.master_auth == old.master_auth
                && new.read_only == old.read_only
            {
                return;
            }

            shared.post_office().send_shutdown(SET_REPLICA_ID);
            spawn_set_server_to_replica(shared, new.clone());
        } else {
            shared.post_office().send_shutdown(SET_REPLICA_ID);
        }
    }

    pub fn update_server_state(new: Option<&Arc<Self>>, shared: Shared) {
        shared.post_office().send_shutdown(SET_REPLICA_ID);
        if let Some(new) = new {
            spawn_set_server_to_replica(shared, new.clone());
        }
    }
}

impl Clone for ReplicaConf {
    fn clone(&self) -> Self {
        Self {
            master_host: self.master_host.clone(),
            master_port: self.master_port,
            master_auth: self.master_auth.clone(),
            read_only: self.read_only,
            offset: AtomicU64::new(self.offset.load(Ordering::SeqCst)),
            master_run_id: self.master_run_id.clone(),
        }
    }
}
