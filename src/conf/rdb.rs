use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct RdbConf {
    pub file_path: String,  // RDB文件路径
    pub save: Option<Save>, // RDB持久化间隔。格式为"seconds changes"，seconds表示间隔时间，changes表示键的变化次数
    // pub version: u32,       // RDB版本号
    pub enable_checksum: bool, // 是否启用RDB校验和
}

impl Default for RdbConf {
    fn default() -> Self {
        Self {
            file_path: "dump.rdb".to_string(),
            save: None,
            // version: 9,
            enable_checksum: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Save {
    pub seconds: u64,
    pub changes: u64,
}
