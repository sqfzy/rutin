use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename = "memory")]
pub struct MemoryConf {
    pub max_memory: u64,
    // pub max_memory_policy: String,
    // pub max_memory_samples: u64,
}

impl Default for MemoryConf {
    fn default() -> Self {
        Self {
            max_memory: 1024 * 1024 * 4,
            // max_memory_policy: "noeviction".to_string(),
            // max_memory_samples: 5,
        }
    }
}
