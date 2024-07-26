use crate::{
    error::{RutinError, RutinResult},
    server::USED_MEMORY,
    shared::db::Db,
};
use rand::seq::IteratorRandom;
use serde::Deserialize;
use std::sync::atomic::Ordering;
use tracing::error;

#[derive(Debug, Deserialize)]
#[serde(rename = "memory")]
pub struct MemoryConf {
    pub maxmemory: u64,
    pub maxmemory_policy: Policy,
    pub maxmemory_samples: usize,
}

#[derive(Debug, Clone, Copy, Deserialize, Default)]
pub enum Policy {
    #[serde(rename = "volatile_lru")]
    VolatileLRU,
    #[serde(rename = "allkeys_lru")]
    AllKeysLRU,
    #[serde(rename = "volatile_lfu")]
    VolatileLFU,
    #[serde(rename = "allkeys_lfu")]
    AllKeysLFU,
    #[serde(rename = "volatile_random")]
    VolatileRandom,
    #[serde(rename = "allkeys_random")]
    AllKeysRandom,
    #[serde(rename = "volatile_ttl")]
    VolatileTTL,
    #[default]
    #[serde(rename = "noeviction")]
    NoEviction,
}

impl MemoryConf {
    #[inline]
    pub async fn try_evict(&self, db: &Db) -> RutinResult<()> {
        let mut loop_limit = 1000;

        // 一直淘汰直到有空余的内存或者达到循环次数限制
        loop {
            let used_mem = USED_MEMORY.load(Ordering::Relaxed);

            if self.maxmemory > used_mem {
                // 仍有可用内存
                return Ok(());
            }

            // PERF:

            // 如果策略是不淘汰，或者采样数设为0，直接返回内存不足错误
            if matches!(self.maxmemory_policy, Policy::NoEviction) || self.maxmemory_samples == 0 {
                error!(
                    "OOM used memory: {}, maxmemory: {}",
                    used_mem, self.maxmemory
                );
                return Err(RutinError::new_oom(used_mem, self.maxmemory));
            }

            debug_assert!(self.maxmemory_samples > 0);

            match self.maxmemory_policy {
                Policy::AllKeysLRU | Policy::AllKeysLFU | Policy::AllKeysRandom => {
                    // 随机抽取一个样本
                    let (mut sample, mut atc) = {
                        let mut rng = rand::thread_rng();

                        let entry = db
                            .entries()
                            .iter()
                            .choose(&mut rng)
                            .ok_or_else(|| RutinError::new_oom(used_mem, self.maxmemory))?;

                        (entry.key().clone(), entry.atc())
                    };

                    // 如果是AllKeysRandom策略，直接删除样本对象
                    if matches!(self.maxmemory_policy, Policy::AllKeysRandom) {
                        let _ = db.remove_object(sample).await;
                        return Ok(());
                    }

                    // 继续抽取样本，找到最适合删除的对象
                    for _ in 1..self.maxmemory_samples {
                        let mut rng = rand::thread_rng();

                        let entry = db
                            .entries()
                            .iter()
                            .choose(&mut rng)
                            .ok_or_else(|| RutinError::new_oom(used_mem, self.maxmemory))?;

                        match self.maxmemory_policy {
                            Policy::AllKeysLRU => {
                                let new_atc = entry.atc();
                                if new_atc.access_time() < atc.access_time() {
                                    atc = new_atc;
                                    sample = entry.key().clone();
                                }
                            }
                            Policy::AllKeysLFU => {
                                let new_atc = entry.atc();
                                if new_atc.access_count() < atc.access_count() {
                                    atc = new_atc;
                                    sample = entry.key().clone();
                                }
                            }
                            _ => unreachable!(),
                        }
                    }

                    // 删除样本对象
                    let _ = db.remove_object(sample).await;
                }
                Policy::VolatileLRU
                | Policy::VolatileLFU
                | Policy::VolatileRandom
                | Policy::VolatileTTL => {
                    // 随机抽取一个样本
                    let (mut sample, mut atc, mut ex) = loop {
                        let record = {
                            let mut rng = rand::thread_rng();

                            db.entry_expire_records()
                                .iter()
                                .choose(&mut rng)
                                .ok_or_else(|| RutinError::new_oom(used_mem, self.maxmemory))?
                        };
                        if let Ok(entry) = db.get(&record.1).await {
                            break (record.1.clone(), entry.atc(), record.0);
                        }
                    };

                    // 如果是VolatileRandom策略，直接删除样本对象
                    if matches!(self.maxmemory_policy, Policy::VolatileRandom) {
                        let _ = db.remove_object(sample).await;
                        return Ok(());
                    }

                    // 继续抽取样本，找到最适合删除的对象
                    for _ in 1..self.maxmemory_samples {
                        let (new_sample, new_atc, new_ex) = loop {
                            let record = {
                                let mut rng = rand::thread_rng();

                                db.entry_expire_records()
                                    .iter()
                                    .choose(&mut rng)
                                    .ok_or_else(|| RutinError::new_oom(used_mem, self.maxmemory))?
                            };
                            if let Ok(entry) = db.get(&record.1).await {
                                break (record.1.clone(), entry.atc(), record.0);
                            }
                        };

                        match self.maxmemory_policy {
                            Policy::VolatileLRU => {
                                if new_atc.access_time() < atc.access_time() {
                                    atc = new_atc;
                                    sample = new_sample;
                                }
                            }
                            Policy::VolatileLFU => {
                                if new_atc.access_count() < atc.access_count() {
                                    atc = new_atc;
                                    sample = new_sample;
                                }
                            }
                            Policy::VolatileTTL => {
                                if new_ex < ex {
                                    ex = new_ex;
                                    sample = new_sample;
                                }
                            }
                            _ => unreachable!(),
                        }
                    }

                    // 删除样本对象
                    let _ = db.remove_object(sample).await;
                }
                Policy::NoEviction => unreachable!(),
            }

            // 防止无限循环
            loop_limit -= 1;
            if loop_limit == 0 {
                return Err(RutinError::new_server_error("evict loop limit reached"));
            }
        }
    }
}
