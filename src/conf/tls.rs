use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct TLSConf {
    pub port: u16,
    pub cert_file: String,
    pub key_file: String,
}
