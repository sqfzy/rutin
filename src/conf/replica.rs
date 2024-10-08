use bytes::Bytes;
use bytestring::ByteString;
use serde::Deserialize;
use std::sync::Mutex;

#[derive(Debug, Deserialize)]
pub struct ReplicaConf {
    pub master_info: Mutex<Option<MasterInfo>>,
    pub master_auth: Option<String>,
    pub read_only: bool,
}

impl Clone for ReplicaConf {
    fn clone(&self) -> Self {
        Self {
            master_info: Mutex::new(self.master_info.lock().unwrap().clone()),
            master_auth: self.master_auth.clone(),
            read_only: self.read_only,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MasterInfo {
    pub host: ByteString,
    pub port: u16,
    pub run_id: Bytes,
}

impl MasterInfo {
    pub fn new(host: ByteString, port: u16) -> Self {
        Self {
            host,
            port,
            run_id: "?".into(),
        }
    }
}
