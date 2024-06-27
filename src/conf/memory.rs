use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename = "memory")]
pub struct MemoryConf {
    // pub max_memory: u64,
    // pub max_memory_policy: String,
    // pub max_memory_samples: u64,
}
