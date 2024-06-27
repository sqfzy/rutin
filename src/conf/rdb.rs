use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename = "rdb")]
pub struct RDBConf {
    pub file_path: String,     // RDB文件路径
    pub save: Option<Save>, // RDB持久化间隔。格式为"seconds changes"，seconds表示间隔时间，changes表示键的变化次数
    pub version: u32,       // RDB版本号
    pub enable_checksum: bool, // 是否启用RDB校验和
}

#[derive(Debug, Deserialize)]
pub struct Save {
    pub seconds: u64,
    pub changes: u64,
}
