use super::*;
use crate::{
    conf::db::{Atc, Db},
    error::{RutinError, RutinResult},
};

// 不可变配置，因为Db持有其副本，应当保持一致
#[derive(Debug, PartialEq, Clone, Deserialize)]
pub struct MemoryConf {
    pub oom: Option<OomConf>,
    pub expiration_evict: ExpirationEvict,
}

impl MemoryConf {
    pub fn may_update_server_state(self: &Arc<Self>, old: &Arc<Self>, shared: Shared) {
        if self.expiration_evict != old.expiration_evict {
            shared.post_office().send_shutdown(EXPIRATION_EVICT_ID);
            spawn_expiration_evict(shared, self.clone());
        }

        if self.oom != old.oom {
            shared.db().oom_conf.rcu(|_| self.oom.clone().map(Arc::new));
        }
    }

    pub fn update_server_state(self: &Arc<Self>, shared: Shared) {
        shared.post_office().send_shutdown(EXPIRATION_EVICT_ID);
        spawn_expiration_evict(shared, self.clone());

        shared.db().oom_conf.rcu(|_| self.oom.clone().map(Arc::new));
    }
}

impl Default for MemoryConf {
    fn default() -> Self {
        MemoryConf {
            oom: None,
            expiration_evict: Default::default(),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
pub struct OomConf {
    pub maxmemory: u64, // 单位：MB
    pub maxmemory_policy: Policy,
    pub maxmemory_samples_count: usize,
}

impl Default for OomConf {
    fn default() -> Self {
        OomConf {
            maxmemory: u64::MAX,
            maxmemory_policy: Default::default(),
            maxmemory_samples_count: 5,
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy, Deserialize, Default)]
pub enum Policy {
    // #[serde(rename = "volatile_lru")]
    // VolatileLRU,
    #[serde(rename = "allkeys_lru")]
    AllKeysLRU,
    // #[serde(rename = "volatile_lfu")]
    // VolatileLFU,
    #[serde(rename = "allkeys_lfu")]
    AllKeysLFU,
    // #[serde(rename = "volatile_random")]
    // VolatileRandom,
    #[serde(rename = "allkeys_random")]
    AllKeysRandom,
    // #[serde(rename = "volatile_ttl")]
    // VolatileTTL,
    #[default]
    #[serde(rename = "noeviction")]
    NoEviction,
}

impl OomConf {
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

            // 如果策略是不淘汰，或者采样数设为0，直接返回内存不足错误
            if matches!(self.maxmemory_policy, Policy::NoEviction)
                || self.maxmemory_samples_count == 0
            {
                return Err(RutinError::new_oom(used_mem, self.maxmemory));
            }

            debug_assert!(self.maxmemory_samples_count > 0);

            let len = db.entries.len();
            let mut map_iter = db.entries.iter();
            let mut rng = fastrand::Rng::new();

            match self.maxmemory_policy {
                Policy::AllKeysLRU | Policy::AllKeysLFU | Policy::AllKeysRandom => {
                    // 随机抽取一个样本
                    let (mut sample_key, mut atc) = {
                        let sample_entry = map_iter
                            .nth(rng.usize(0..len))
                            .ok_or_else(|| RutinError::new_oom(used_mem, self.maxmemory))?;

                        (sample_entry.key().clone(), sample_entry.atc.get())
                    };

                    // 如果是AllKeysRandom策略，直接删除样本对象
                    if matches!(self.maxmemory_policy, Policy::AllKeysRandom) {
                        db.remove_object(&sample_key).await;
                        return Ok(());
                    }

                    // 继续抽取样本，找到最适合删除的对象
                    for _ in 1..self.maxmemory_samples_count {
                        let sample_entry = map_iter
                            .nth(rng.usize(0..len))
                            .ok_or_else(|| RutinError::new_oom(used_mem, self.maxmemory))?;

                        match self.maxmemory_policy {
                            Policy::AllKeysLRU => {
                                let new_atc = sample_entry.atc.get();
                                if Atc::access_time3(new_atc) < Atc::access_time3(atc) {
                                    atc = new_atc;
                                    sample_key = sample_entry.key().clone();
                                }
                            }
                            Policy::AllKeysLFU => {
                                let new_atc = sample_entry.atc.get();
                                if Atc::access_count3(new_atc) < Atc::access_count3(atc) {
                                    atc = new_atc;
                                    sample_key = sample_entry.key().clone();
                                }
                            }
                            _ => unreachable!(),
                        }
                    }

                    // 删除样本对象
                    db.remove_object(&sample_key).await;
                } // Policy::VolatileLRU
                // | Policy::VolatileLFU
                // | Policy::VolatileRandom
                // | Policy::VolatileTTL => {
                //     // 随机抽取一个样本
                //     let (mut sample, mut atc, mut ex) = loop {
                //         let record = {
                //             let mut rng = rand::thread_rng();
                //
                //             db.entry_expire_records()
                //                 .iter()
                //                 .choose(&mut rng)
                //                 .ok_or_else(|| RutinError::new_oom(used_mem, self.maxmemory))?
                //         };
                //         if let Ok(entry) = db.get(&record.1).await {
                //             break (record.1.clone(), entry.atc.get(), record.0);
                //         }
                //     };
                //
                //     // 如果是VolatileRandom策略，直接删除样本对象
                //     if matches!(self.maxmemory_policy, Policy::VolatileRandom) {
                //         let _ = db.remove_object(sample).await;
                //         return Ok(());
                //     }
                //
                //     // 继续抽取样本，找到最适合删除的对象
                //     for _ in 1..self.maxmemory_samples {
                //         let (new_sample, new_atc, new_ex) = loop {
                //             let sample_entry = map_iter
                //                 .nth(rng.usize(0..len))
                //                 .ok_or_else(|| RutinError::new_oom(used_mem, self.maxmemory))?;
                //
                //             if let Ok(entry) = db.get(&record.1).await {
                //                 break (record.1.clone(), entry.atc(), record.0);
                //             }
                //         };
                //
                //         match self.maxmemory_policy {
                //             Policy::VolatileLRU => {
                //                 if new_atc.access_time() < atc.access_time() {
                //                     atc = new_atc;
                //                     sample = new_sample;
                //                 }
                //             }
                //             Policy::VolatileLFU => {
                //                 if new_atc.access_count() < atc.access_count() {
                //                     atc = new_atc;
                //                     sample = new_sample;
                //                 }
                //             }
                //             Policy::VolatileTTL => {
                //                 if new_ex < ex {
                //                     ex = new_ex;
                //                     sample = new_sample;
                //                 }
                //             }
                //             _ => unreachable!(),
                //         }
                //     }
                //
                //     // 删除样本对象
                //     let _ = db.remove_object1(&sample).await;
                // }
                Policy::NoEviction => return Err(RutinError::new_oom(used_mem, self.maxmemory)),
            }

            // 防止无限循环
            loop_limit -= 1;
            if loop_limit == 0 {
                return Err(RutinError::new_server_error("evict loop limit reached"));
            }
        }
    }
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
pub struct ExpirationEvict {
    pub samples_count: u64,
}

impl Default for ExpirationEvict {
    fn default() -> Self {
        ExpirationEvict { samples_count: 800 }
    }
}

// 开启过期键定时检查
pub fn spawn_expiration_evict(shared: Shared, memory_conf: Arc<MemoryConf>) {
    let samples_count = memory_conf.expiration_evict.samples_count;
    let post_office = shared.post_office();

    shared.pool().spawn_pinned(move || async move {
        let map = &shared.db().entries;

        // 运行的间隔时间，受**内存使用情况**和**过期键比例**影响
        let mut millis = 1000;
        let mut interval = interval(Duration::from_millis(millis)); // 100ms ~ 1000ms

        let mut expiration_proportion = 0_u64; // 过期键比例
        let mut new_value_influence = 70_u64; // 新expiration_proportion的影响占比。50% ~ 70%
        let mut old_value_influence = 100_u64 - new_value_influence; // 旧expiration_proportion的影响占比。30% ~ 50%

        fn adjust_millis(expiration_proportion: u64) -> u64 {
            1000 - 10 * expiration_proportion
        }

        fn adjust_new_value_influence(millis: u64) -> u64 {
            millis / 54 + 430 / 9
        }

        let mailbox = post_office.register_mailbox(EXPIRATION_EVICT_ID);
        let fut = mailbox.recv_async();
        pin_mut!(fut);
        loop {
            tokio::select! {
                letter = &mut fut => {
                    if let Letter::Shutdown = letter {
                        break;
                    }
                }
                _ = interval.tick() => {}
            };

            let now = Instant::now();

            // 随机选出samples_count个数据，保留过期的数据
            let mut samples = fastrand::choose_multiple(map.iter(), samples_count as usize);
            samples.retain(|data| data.expire < now);
            let expired_samples = samples;

            let expired_samples_count = expired_samples.len();

            // 移除过期数据，并触发事件
            for sample in expired_samples.into_iter() {
                if let Some((_, mut object)) = map.remove(sample.key()) {
                    Events::try_trigger_read_and_write_event(&mut object);
                }
            }

            let new_expiration_proportion = (expired_samples_count as u64 / samples_count) * 100;

            expiration_proportion = (expiration_proportion * old_value_influence
                + new_expiration_proportion * new_value_influence)
                / 100;

            millis = adjust_millis(expiration_proportion);
            interval.reset_after(Duration::from_millis(millis));
            new_value_influence = adjust_new_value_influence(millis);
            old_value_influence = 100 - new_value_influence;
        }
    });
}
