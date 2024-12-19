use super::*;
use crate::server::spawn_run_server;

#[derive(Debug, PartialEq, Clone, Deserialize)]
pub struct TlsConf {
    pub port: u16,
    pub cert_file: String,
    pub key_file: String,
}

impl TlsConf {
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

            shared.post_office().send_shutdown(MAIN_ID);
            spawn_run_server(
                shared.conf().server_conf().clone(),
                Some(new.clone()),
                shared,
            );
        } else {
            shared.post_office().send_shutdown(MAIN_ID);
            spawn_run_server(shared.conf().server_conf().clone(), None, shared);
        }
    }

    pub fn update_server_state(new: Option<&Arc<Self>>, shared: Shared) {
        shared.post_office().send_shutdown(MAIN_ID);

        spawn_run_server(shared.conf().server_conf().clone(), new.cloned(), shared);
    }
}

impl Default for TlsConf {
    fn default() -> Self {
        TlsConf {
            port: 6379,
            cert_file: "ca/rutin.crt".to_string(),
            key_file: "ca/rutin.key".to_string(),
        }
    }
}
