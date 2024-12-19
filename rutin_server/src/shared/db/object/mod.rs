mod event;
mod hash;
mod list;
mod set;
mod str;
mod zset;

pub use event::*;
pub use hash::*;
pub use list::*;
pub use set::*;
pub use str::*;
pub use zset::*;

use crate::{
    error::{RutinError, RutinResult},
    server::{NEVER_EXPIRE, get_lru_clock},
};
use std::sync::atomic::{
    AtomicU32,
    Ordering::{self, Relaxed},
};
use strum::{EnumDiscriminants, IntoStaticStr};
use tokio::time::Instant;

#[derive(Debug)]
pub struct Object {
    pub value: ObjectValue,
    pub expire: Instant,
    /// access time and count，高位[`LRU_BITS`]表示access time，其余表示access count
    /// 在读取时也需要更新atc，由于不可变引用无法更新，因此需要使用原子操作
    pub atc: Atc,
    pub events: Events,
}

impl Object {
    #[inline]
    fn new(value: impl Into<ObjectValue>) -> Self {
        Object::with_expire(value, *NEVER_EXPIRE)
    }

    #[inline]
    pub fn with_expire(value: impl Into<ObjectValue>, expire: Instant) -> Self {
        Object {
            value: value.into(),
            expire,
            atc: Atc::default(),
            events: Default::default(),
        }
    }

    #[inline]
    pub fn is_expired(&self) -> bool {
        self.expire <= Instant::now()
    }

    #[inline]
    pub fn is_never_expired(&self) -> bool {
        self.expire == *NEVER_EXPIRE
    }

    // pub fn on_str(&self) -> RutinResult<&Str> {
    //     if let ObjectValue::Str(ref s) = self.value {
    //         Ok(s)
    //     } else {
    //         Err(RutinError::TypeErr {
    //             expected: ObjectValueType::Str.into(),
    //             found: self.type_str(),
    //         })
    //     }
    // }
    //
    // pub fn on_list(&self) -> RutinResult<&List> {
    //     if let ObjectValue::List(ref l) = self.value {
    //         Ok(l)
    //     } else {
    //         Err(RutinError::TypeErr {
    //             expected: ObjectValueType::List.into(),
    //             found: self.type_str(),
    //         })
    //     }
    // }
    //
    // pub fn on_set(&self) -> RutinResult<&Set> {
    //     if let ObjectValue::Set(ref s) = self.value {
    //         Ok(s)
    //     } else {
    //         Err(RutinError::TypeErr {
    //             expected: ObjectValueType::Set.into(),
    //             found: self.type_str(),
    //         })
    //     }
    // }
    //
    // pub fn on_hash(&self) -> RutinResult<&Hash> {
    //     if let ObjectValue::Hash(ref h) = self.value {
    //         Ok(h)
    //     } else {
    //         Err(RutinError::TypeErr {
    //             expected: ObjectValueType::Hash.into(),
    //             found: self.type_str(),
    //         })
    //     }
    // }
    //
    // pub fn on_zset(&self) -> RutinResult<&ZSet> {
    //     if let ObjectValue::ZSet(z) = &self.value {
    //         Ok(z)
    //     } else {
    //         Err(RutinError::TypeErr {
    //             expected: ObjectValueType::ZSet.into(),
    //             found: self.type_str(),
    //         })
    //     }
    // }
    //
    // pub fn on_str_mut(&mut self) -> RutinResult<&mut Str> {
    //     if let ObjectValue::Str(&mut s) = self.value {
    //         Ok(s)
    //     } else {
    //         Err(RutinError::TypeErr {
    //             expected: ObjectValueType::Str.into(),
    //             found: self.type_str(),
    //         })
    //     }
    // }
    //
    // pub fn on_list_mut(&mut self) -> RutinResult<&mut List> {
    //     if let ObjectValue::List(&mut l) = self.value {
    //         Ok(l)
    //     } else {
    //         Err(RutinError::TypeErr {
    //             expected: ObjectValueType::List.into(),
    //             found: self.type_str(),
    //         })
    //     }
    // }
    //
    // pub fn on_set_mut(&mut self) -> RutinResult<&mut Set> {
    //     if let ObjectValue::Set(&mut s) = self.value {
    //         Ok(s)
    //     } else {
    //         Err(RutinError::TypeErr {
    //             expected: ObjectValueType::Set.into(),
    //             found: self.type_str(),
    //         })
    //     }
    // }
    //
    // pub fn on_hash_mut(&mut self) -> RutinResult<&mut Hash> {
    //     if let ObjectValue::Hash(&mut h) = self.value {
    //         Ok(h)
    //     } else {
    //         Err(RutinError::TypeErr {
    //             expected: ObjectValueType::Hash.into(),
    //             found: self.type_str(),
    //         })
    //     }
    // }
    //
    // pub fn on_zset_mut(&mut self) -> RutinResult<&mut ZSet> {
    //     if let ObjectValue::ZSet(&mut z) = self.value {
    //         Ok(z)
    //     } else {
    //         Err(RutinError::TypeErr {
    //             expected: ObjectValueType::ZSet.into(),
    //             found: self.type_str(),
    //         })
    //     }
    // }

    ///  access time更新为lru_clock，access count加1
    #[inline]
    pub fn update_atc1(&self) {
        let atc = self.atc.0.load(Relaxed);

        let count = atc & Atc::LFU_MASK;

        let new_count = if count == Atc::LFU_FREQUENCY_MAX {
            count
        } else {
            // 每次有1 / (count / 2)的概率更新count
            let prob = fastrand::u32(..=count / 2);
            if prob == 0 { count + 1 } else { count }
        };

        let new_atc = (get_lru_clock() << Atc::LFU_BITS) | new_count;

        // 有可能有多个线程在同时更新lru，这会覆盖掉其他线程的更新，但这是可以接受的
        self.atc.0.store(new_atc, Relaxed);
    }

    #[inline]
    pub fn update_atc2(&mut self) {
        let atc = self.atc.0.get_mut();

        let count = *atc & Atc::LFU_MASK;

        let new_count = if count == Atc::LFU_FREQUENCY_MAX {
            count
        } else {
            // 每次有1 / (count / 2)的概率更新count
            let prob = fastrand::u32(..=count / 2);
            if prob == 0 { count + 1 } else { count }
        };

        let new_atc = (get_lru_clock() << Atc::LFU_BITS) | new_count;

        *atc = new_atc;
    }

    pub fn typ(&self) -> ObjectValueType {
        match &self.value {
            ObjectValue::Str(_) => ObjectValueType::Str,
            ObjectValue::List(_) => ObjectValueType::List,
            ObjectValue::Set(_) => ObjectValueType::Set,
            ObjectValue::Hash(_) => ObjectValueType::Hash,
            ObjectValue::ZSet(_) => ObjectValueType::ZSet,
        }
    }

    pub fn type_str(&self) -> &'static str {
        match &self.value {
            ObjectValue::Str(_) => ObjectValueType::Str.into(),
            ObjectValue::List(_) => ObjectValueType::List.into(),
            ObjectValue::Set(_) => ObjectValueType::Set.into(),
            ObjectValue::Hash(_) => ObjectValueType::Hash.into(),
            ObjectValue::ZSet(_) => ObjectValueType::ZSet.into(),
        }
    }
}

impl From<ObjectValue> for Object {
    fn from(value: ObjectValue) -> Self {
        Object::new(value)
    }
}

impl From<Str> for Object {
    fn from(s: Str) -> Self {
        Object::new(ObjectValue::Str(s))
    }
}

impl From<List> for Object {
    fn from(l: List) -> Self {
        Object::new(ObjectValue::List(l))
    }
}

impl From<Set> for Object {
    fn from(s: Set) -> Self {
        Object::new(ObjectValue::Set(s))
    }
}

impl From<Hash> for Object {
    fn from(h: Hash) -> Self {
        Object::new(ObjectValue::Hash(h))
    }
}

impl From<ZSet> for Object {
    fn from(z: ZSet) -> Self {
        Object::new(ObjectValue::ZSet(z))
    }
}

impl Clone for Object {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            expire: self.expire,
            atc: Atc::default(),
            events: Default::default(),
        }
    }
}

impl PartialEq for Object {
    fn eq(&self, other: &Self) -> bool {
        let ex_is_eq = if self.expire > other.expire {
            self.expire.duration_since(other.expire).as_secs() < 1
        } else {
            other.expire.duration_since(self.expire).as_secs() < 1
        };

        ex_is_eq && self.value == other.value
    }
}

#[derive(Debug)]
pub struct Atc(AtomicU32);

impl Atc {
    pub const LRU_BITS: u8 = 20;
    pub const LFU_BITS: u8 = 12;

    pub const LRU_CLOCK_MAX: u32 = (1 << Self::LRU_BITS) - 1;
    pub const LFU_FREQUENCY_MAX: u32 = (1 << Self::LRU_BITS) - 1;

    pub const LFU_MASK: u32 = (1 << Self::LFU_BITS) - 1;
    pub const LRU_MASK: u32 = u32::MAX ^ Self::LFU_MASK;

    pub const DEFAULT_ATC: u32 = Self::LRU_MASK; // 默认access time为最大，access_count为0

    #[inline]
    pub fn get(&self) -> u32 {
        self.0.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn get_mut(&mut self) -> &mut u32 {
        self.0.get_mut()
    }

    #[inline]
    pub fn access_time1(&self) -> u32 {
        self.0.load(Ordering::Relaxed) >> Self::LFU_BITS
    }

    #[inline]
    pub fn access_time2(&mut self) -> u32 {
        *self.0.get_mut() >> Self::LFU_BITS
    }

    #[inline]
    pub fn access_time3(atc: u32) -> u32 {
        atc >> Self::LFU_BITS
    }

    #[inline]
    pub fn access_count1(&self) -> u32 {
        self.0.load(Ordering::Relaxed) & Self::LFU_MASK
    }

    #[inline]
    pub fn access_count2(&mut self) -> u32 {
        *self.0.get_mut() & Self::LFU_MASK
    }

    #[inline]
    pub fn access_count3(atc: u32) -> u32 {
        atc & Self::LFU_MASK
    }
}

impl Default for Atc {
    fn default() -> Self {
        Atc(AtomicU32::new(Atc::DEFAULT_ATC))
    }
}

impl From<u32> for Atc {
    fn from(atc: u32) -> Self {
        Atc(AtomicU32::new(atc))
    }
}

#[derive(EnumDiscriminants, IntoStaticStr)]
#[strum_discriminants(vis(pub), name(ObjectValueType), derive(IntoStaticStr))]
#[derive(Debug, Clone, PartialEq)]
pub enum ObjectValue {
    #[strum_discriminants(strum(serialize = "string"))]
    Str(Str),
    #[strum_discriminants(strum(serialize = "list"))]
    List(List),
    #[strum_discriminants(strum(serialize = "set"))]
    Set(Set),
    #[strum_discriminants(strum(serialize = "hash"))]
    Hash(Hash),
    #[strum_discriminants(strum(serialize = "zset"))]
    ZSet(ZSet),
}

impl ObjectValue {
    pub fn type_str(&self) -> &'static str {
        match self {
            ObjectValue::Str(_) => ObjectValueType::Str.into(),
            ObjectValue::List(_) => ObjectValueType::List.into(),
            ObjectValue::Set(_) => ObjectValueType::Set.into(),
            ObjectValue::Hash(_) => ObjectValueType::Hash.into(),
            ObjectValue::ZSet(_) => ObjectValueType::ZSet.into(),
        }
    }

    pub fn typ(&self) -> ObjectValueType {
        match self {
            ObjectValue::Str(_) => ObjectValueType::Str,
            ObjectValue::List(_) => ObjectValueType::List,
            ObjectValue::Set(_) => ObjectValueType::Set,
            ObjectValue::Hash(_) => ObjectValueType::Hash,
            ObjectValue::ZSet(_) => ObjectValueType::ZSet,
        }
    }

    pub fn on_str(&self) -> RutinResult<&Str> {
        if let ObjectValue::Str(s) = self {
            Ok(s)
        } else {
            Err(RutinError::TypeErr {
                expected: ObjectValueType::Str.into(),
                found: self.type_str(),
            })
        }
    }

    pub fn on_list(&self) -> RutinResult<&List> {
        if let ObjectValue::List(l) = self {
            Ok(l)
        } else {
            Err(RutinError::TypeErr {
                expected: ObjectValueType::List.into(),
                found: self.type_str(),
            })
        }
    }

    pub fn on_set(&self) -> RutinResult<&Set> {
        if let ObjectValue::Set(s) = self {
            Ok(s)
        } else {
            Err(RutinError::TypeErr {
                expected: ObjectValueType::Set.into(),
                found: self.type_str(),
            })
        }
    }

    pub fn on_hash(&self) -> RutinResult<&Hash> {
        if let ObjectValue::Hash(h) = self {
            Ok(h)
        } else {
            Err(RutinError::TypeErr {
                expected: ObjectValueType::Hash.into(),
                found: self.type_str(),
            })
        }
    }

    pub fn on_zset(&self) -> RutinResult<&ZSet> {
        if let ObjectValue::ZSet(z) = self {
            Ok(z)
        } else {
            Err(RutinError::TypeErr {
                expected: ObjectValueType::ZSet.into(),
                found: self.type_str(),
            })
        }
    }

    pub fn on_str_mut(&mut self) -> RutinResult<&mut Str> {
        if let ObjectValue::Str(s) = self {
            Ok(s)
        } else {
            Err(RutinError::TypeErr {
                expected: ObjectValueType::Str.into(),
                found: self.type_str(),
            })
        }
    }

    pub fn on_list_mut(&mut self) -> RutinResult<&mut List> {
        if let ObjectValue::List(l) = self {
            Ok(l)
        } else {
            Err(RutinError::TypeErr {
                expected: ObjectValueType::List.into(),
                found: self.type_str(),
            })
        }
    }

    pub fn on_set_mut(&mut self) -> RutinResult<&mut Set> {
        if let ObjectValue::Set(s) = self {
            Ok(s)
        } else {
            Err(RutinError::TypeErr {
                expected: ObjectValueType::Set.into(),
                found: self.type_str(),
            })
        }
    }

    pub fn on_hash_mut(&mut self) -> RutinResult<&mut Hash> {
        if let ObjectValue::Hash(h) = self {
            Ok(h)
        } else {
            Err(RutinError::TypeErr {
                expected: ObjectValueType::Hash.into(),
                found: self.type_str(),
            })
        }
    }

    pub fn on_zset_mut(&mut self) -> RutinResult<&mut ZSet> {
        if let ObjectValue::ZSet(z) = self {
            Ok(z)
        } else {
            Err(RutinError::TypeErr {
                expected: ObjectValueType::ZSet.into(),
                found: self.type_str(),
            })
        }
    }
}

impl std::fmt::Display for ObjectValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.into())
    }
}

impl From<Str> for ObjectValue {
    fn from(s: Str) -> Self {
        Self::Str(s)
    }
}

impl From<List> for ObjectValue {
    fn from(l: List) -> Self {
        Self::List(l)
    }
}

impl From<Set> for ObjectValue {
    fn from(s: Set) -> Self {
        Self::Set(s)
    }
}

impl From<Hash> for ObjectValue {
    fn from(h: Hash) -> Self {
        Self::Hash(h)
    }
}

impl From<ZSet> for ObjectValue {
    fn from(z: ZSet) -> Self {
        Self::ZSet(z)
    }
}

// #[cfg(test)]
// mod object_tests {
//     use crate::{
//         server::NEVER_EXPIRE,
//         util::{get_test_db, test_init},
//     };
//
//     use super::*;
//     use std::sync::atomic::{AtomicUsize, Ordering};
//
//     #[test]
//     fn may_update_test() {
//         test_init();
//
//         let mut obj = Object::new_str("".into(), *NEVER_EXPIRE);
//
//         let (tx, rx) = flume::unbounded();
//
//         obj.add_may_update_event(tx);
//
//         obj.trigger_may_update_event(&"key".into());
//
//         let key = rx.recv().unwrap();
//         assert_eq!(&key.to_bytes(), "key");
//     }
//
//     #[test]
//     fn track_test() {
//         test_init();
//
//         let mut obj = Object::new_str("".into(), *NEVER_EXPIRE);
//
//         let (tx, rx) = flume::unbounded();
//
//         obj.add_track_event(tx);
//
//         obj.trigger_track_event(&"key".into());
//
//         let resp = rx.recv().unwrap();
//
//         assert_eq!(
//             resp,
//             Resp3::new_push(vec![
//                 Resp3::new_blob_string("invalidate".into()),
//                 Resp3::new_array(vec![Resp3::new_blob_string("key".into())]),
//             ])
//         );
//     }
//
//     #[tokio::test]
//     async fn intention_lock_test() {
//         test_init();
//
//         let flag = Arc::new(AtomicUsize::new(0));
//         let db = Arc::new(get_test_db());
//
//         let notifiy = Arc::new(Notify::const_new());
//
//         db.insert_object("".into(), ObjectInner::new_str("", *NEVER_EXPIRE))
//             .await
//             .unwrap();
//         db.add_lock_event("".into(), 0).await.unwrap();
//
//         // 目标ID不符，会被阻塞
//         let handle1 = tokio::spawn({
//             let db = db.clone();
//             let flag = flag.clone();
//             async move {
//                 ID.scope(1, Object::trigger_lock_event(&db, "".into()))
//                     .await;
//                 // 该语句应该被最先执行
//                 assert_eq!(flag.fetch_add(1, Ordering::SeqCst), 1);
//             }
//         });
//
//         // 目标ID不符，会被阻塞
//         let handle2 = tokio::spawn({
//             let db = db.clone();
//             let flag = flag.clone();
//             async move {
//                 ID.scope(2, Object::trigger_lock_event(&db, "".into()))
//                     .await;
//                 // 该语句应该第二执行
//                 assert_eq!(flag.fetch_add(1, Ordering::SeqCst), 2);
//             }
//         });
//
//         // 目标ID不符，会被阻塞
//         let handle3 = tokio::spawn({
//             let db = db.clone();
//             let flag = flag.clone();
//             async move {
//                 // 重入IntentionLock事件
//                 ID.scope(3, db.add_lock_event("".into(), 3)).await.unwrap();
//                 // 该语句应该第三执行
//                 assert_eq!(flag.fetch_add(1, Ordering::SeqCst), 3);
//             }
//         });
//
//         // 目标ID不符，会被阻塞
//         let handle4 = tokio::spawn({
//             let db = db.clone();
//             let flag = flag.clone();
//             async move {
//                 ID.scope(4, Object::trigger_lock_event(&db, "".into()))
//                     .await;
//                 // 该语句应该最后执行
//                 assert_eq!(flag.fetch_add(1, Ordering::SeqCst), 4);
//             }
//         });
//
//         // 符合目标ID，不会阻塞
//         ID.scope(0, Object::trigger_lock_event(&db, "".into()))
//             .await;
//         flag.fetch_add(1, Ordering::SeqCst);
//
//         // 通知等待的任务，意向锁已经可以释放了
//         notifiy.notify_one();
//
//         handle1.await.unwrap();
//         handle2.await.unwrap();
//         handle3.await.unwrap();
//         handle4.await.unwrap();
//
//         let e = db.get(&"".into()).await.unwrap();
//         assert!(e.events.inner.is_empty())
//     }
// }
