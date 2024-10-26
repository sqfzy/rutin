use super::*;
use crate::server::spawn_run_server;
use bytestring::ByteString;
use fastrand;

#[derive(Debug, PartialEq, Clone, Deserialize)]
pub struct ServerConf {
    pub host: ByteString,
    pub port: u16,
    pub log_level: ByteString,
    pub max_connections: usize,
    pub max_batch: usize,

    #[serde(skip, default = "gen_run_id")]
    pub run_id: ByteString, // 服务器的运行ID。由40个随机字符组成
}

impl ServerConf {
    pub fn may_update_server_state(new: &Arc<Self>, old: &Arc<Self>, shared: Shared) {
        if new.host != old.host
            || new.port != old.port
            || new.log_level != old.log_level
            || new.max_connections != old.max_connections
            || new.max_batch != old.max_batch
        {
            shared.post_office().send_shutdown(MAIN_ID);
            spawn_run_server(new.clone(), shared.conf().tls_conf().clone(), shared);
        }
    }

    pub fn update_server_state(new: &Arc<Self>, shared: Shared) {
        shared.post_office().send_shutdown(MAIN_ID);
        spawn_run_server(new.clone(), shared.conf().tls_conf().clone(), shared);
    }
}

impl Default for ServerConf {
    fn default() -> Self {
        ServerConf {
            host: "localhost".into(),
            port: 6379,
            log_level: "info".into(),
            max_connections: 256,
            max_batch: 512,
            run_id: gen_run_id(),
        }
    }
}

pub fn gen_run_id() -> ByteString {
    ByteString::from(
        (0..40)
            .into_iter()
            .map(|_| fastrand::alphanumeric())
            .collect::<String>(),
    )
}
