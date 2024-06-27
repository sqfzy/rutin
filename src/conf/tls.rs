use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename = "tls")]
pub struct TLSConf {
    pub port: u16,
    pub cert_file: String,
    pub key_file: String,
}
