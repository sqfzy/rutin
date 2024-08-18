use crate::conf::gen_run_id;
use bytes::Bytes;
use bytestring::ByteString;
use serde::{Deserialize, Deserializer};
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct ReplicaConf {
    pub master_info: Mutex<Option<MasterInfo>>,
    pub master_auth: Option<String>,
    pub read_only: bool,
}

impl<'de> Deserialize<'de> for ReplicaConf {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct TempReplicaConf {
            master_info: Option<MasterInfo>,
            read_only: bool,
            master_auth: Option<String>,
        }

        let temp = TempReplicaConf::deserialize(deserializer)?;

        Ok(ReplicaConf {
            master_info: Mutex::new(temp.master_info),
            read_only: temp.read_only,
            master_auth: temp.master_auth,
        })
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
            run_id: gen_run_id(),
        }
    }
}
