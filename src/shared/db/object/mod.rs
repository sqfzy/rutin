mod hash;
mod list;
mod set;
mod str;
mod zset;

pub use hash::*;
pub use list::*;
pub use set::*;
pub use str::*;
pub use zset::*;

use crate::{
    error::{RutinError, RutinResult},
    frame::Resp3,
    server::{get_lru_clock, ID},
    shared::db::{
        object_entry::{IntentionLock, ObjectEntry},
        Db, NEVER_EXPIRE,
    },
    Id, Key,
};
use dashmap::mapref::entry::Entry;
use flume::Sender;
use rand::Rng;
use std::{
    ops::Deref,
    sync::{
        atomic::{AtomicU32, Ordering::Relaxed},
        Arc,
    },
};
use strum::{EnumDiscriminants, EnumProperty, IntoStaticStr};
use tokio::{sync::Notify, time::Instant};
use tracing::instrument;

// 对象可以为空对象(不存储对象值)，只存储事件
#[derive(Debug, Default)]
pub struct Object {
    inner: Option<ObjectInner>,
    events: Events,
}

impl Clone for Object {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            ..Default::default()
        }
    }
}

impl Object {
    pub(super) fn new(object: ObjectInner) -> Self {
        Object {
            inner: Some(object),
            events: Default::default(),
        }
    }

    #[inline]
    pub fn inner(&self) -> Option<&ObjectInner> {
        self.inner.as_ref()
    }

    #[inline]
    pub fn inner_mut(&mut self) -> Option<&mut ObjectInner> {
        self.inner.as_mut()
    }

    #[inline]
    pub fn inner_unchecked(&self) -> &ObjectInner {
        self.inner.as_ref().unwrap()
    }

    #[inline]
    pub fn into_inner(self) -> Option<ObjectInner> {
        self.inner
    }

    #[inline]
    pub fn into_inner_unchecked(self) -> ObjectInner {
        self.inner.unwrap()
    }

    #[inline]
    pub fn expire_unchecked(&self) -> Instant {
        if let Some(inner) = &self.inner {
            inner.expire_unchecked()
        } else {
            *NEVER_EXPIRE
        }
    }

    #[inline]
    pub fn try_expire(&self) -> Result<Instant, &'static str> {
        if let Some(inner) = &self.inner {
            inner.try_expire()
        } else {
            Ok(*NEVER_EXPIRE)
        }
    }

    #[inline]
    pub fn expire_expect_never(&self) -> Option<Instant> {
        if let Some(inner) = &self.inner {
            inner.expire_expect_never()
        } else {
            None
        }
    }

    #[inline]
    pub fn atc(&self) -> Lru {
        self.inner
            .as_ref()
            .map_or(Lru::default(), |inner| inner.lru())
    }

    pub fn new_str(s: Str, expire: Instant) -> Self {
        Object::new(ObjectInner::new_str(s, expire))
    }

    pub fn new_list(l: List, expire: Instant) -> Self {
        Object::new(ObjectInner::new_list(l, expire))
    }

    pub fn new_set(s: Set, expire: Instant) -> Self {
        Object::new(ObjectInner::new_set(s, expire))
    }

    pub fn new_hash(h: Hash, expire: Instant) -> Self {
        Object::new(ObjectInner::new_hash(h, expire))
    }

    pub fn new_zset(z: ZSet, expire: Instant) -> Self {
        Object::new(ObjectInner::new_zset(z, expire))
    }

    pub fn new_null() -> Self {
        Object {
            inner: None,
            events: Default::default(),
        }
    }

    pub fn on_str(&self) -> Option<RutinResult<&Str>> {
        let inner = if let Some(inner) = &self.inner {
            inner
        } else {
            return None;
        };

        if let ObjValue::Str(s) = &inner.value {
            Some(Ok(s))
        } else {
            Some(Err(RutinError::TypeErr {
                expected: ObjValueType::Str.into(),
                found: inner.type_str(),
            }))
        }
    }

    pub fn on_list(&self) -> Option<RutinResult<&List>> {
        let inner = if let Some(inner) = &self.inner {
            inner
        } else {
            return None;
        };

        if let ObjValue::List(l) = &inner.value {
            Some(Ok(l))
        } else {
            Some(Err(RutinError::TypeErr {
                expected: ObjValueType::List.into(),
                found: inner.type_str(),
            }))
        }
    }

    pub fn on_set(&self) -> Option<RutinResult<&Set>> {
        let inner = if let Some(inner) = &self.inner {
            inner
        } else {
            return None;
        };

        if let ObjValue::Set(s) = &inner.value {
            Some(Ok(s))
        } else {
            Some(Err(RutinError::TypeErr {
                expected: ObjValueType::Set.into(),
                found: inner.type_str(),
            }))
        }
    }

    pub fn on_hash(&self) -> Option<RutinResult<&Hash>> {
        let inner = if let Some(inner) = &self.inner {
            inner
        } else {
            return None;
        };

        if let ObjValue::Hash(h) = &inner.value {
            Some(Ok(h))
        } else {
            Some(Err(RutinError::TypeErr {
                expected: ObjValueType::Hash.into(),
                found: inner.type_str(),
            }))
        }
    }

    pub fn on_zset(&self) -> Option<RutinResult<&ZSet>> {
        let inner = if let Some(inner) = &self.inner {
            inner
        } else {
            return None;
        };

        if let ObjValue::ZSet(z) = &inner.value {
            Some(Ok(z))
        } else {
            Some(Err(RutinError::TypeErr {
                expected: ObjValueType::ZSet.into(),
                found: inner.type_str(),
            }))
        }
    }

    pub fn on_str_mut(&mut self) -> Option<RutinResult<&mut Str>> {
        let inner = if let Some(inner) = &mut self.inner {
            inner
        } else {
            return None;
        };

        let typ = inner.type_str();
        if let ObjValue::Str(s) = &mut inner.value {
            Some(Ok(s))
        } else {
            Some(Err(RutinError::TypeErr {
                expected: ObjValueType::Str.into(),
                found: typ,
            }))
        }
    }

    pub fn on_list_mut(&mut self) -> Option<RutinResult<&mut List>> {
        let inner = if let Some(inner) = &mut self.inner {
            inner
        } else {
            return None;
        };

        let typ = inner.type_str();
        if let ObjValue::List(l) = &mut inner.value {
            Some(Ok(l))
        } else {
            Some(Err(RutinError::TypeErr {
                expected: ObjValueType::List.into(),
                found: typ,
            }))
        }
    }

    pub fn on_set_mut(&mut self) -> Option<RutinResult<&mut Set>> {
        let inner = if let Some(inner) = &mut self.inner {
            inner
        } else {
            return None;
        };

        let typ = inner.type_str();
        if let ObjValue::Set(s) = &mut inner.value {
            Some(Ok(s))
        } else {
            Some(Err(RutinError::TypeErr {
                expected: ObjValueType::Set.into(),
                found: typ,
            }))
        }
    }

    pub fn on_hash_mut(&mut self) -> Option<RutinResult<&mut Hash>> {
        let inner = if let Some(inner) = &mut self.inner {
            inner
        } else {
            return None;
        };

        let typ = inner.type_str();
        if let ObjValue::Hash(h) = &mut inner.value {
            Some(Ok(h))
        } else {
            Some(Err(RutinError::TypeErr {
                expected: ObjValueType::Hash.into(),
                found: typ,
            }))
        }
    }

    pub fn on_zset_mut(&mut self) -> Option<RutinResult<&mut ZSet>> {
        let inner = if let Some(inner) = &mut self.inner {
            inner
        } else {
            return None;
        };

        let typ = inner.type_str();
        if let ObjValue::ZSet(z) = &mut inner.value {
            Some(Ok(z))
        } else {
            Some(Err(RutinError::TypeErr {
                expected: ObjValueType::ZSet.into(),
                found: typ,
            }))
        }
    }

    #[inline]
    pub fn set_flag(&mut self, flag: u8) {
        self.events.flags |= flag;
    }

    #[inline]
    pub fn remove_flag(&mut self, flag: u8) {
        self.events.flags &= !flag;

        if self.events.flags == 0 {
            self.events.inner.clear();
        }
    }

    pub(super) fn add_lock_event(&mut self, target_id: Id) -> IntentionLock {
        let id = target_id;
        if self.events.contains(INTENTION_LOCK_FLAG) {
            for e in self.events.inner.iter_mut() {
                if let Event::IntentionLock {
                    target_id,
                    intention_lock: notify_unlock,
                    ..
                } = e
                {
                    *target_id = id;
                    return notify_unlock.clone();
                }
            }

            unreachable!()
        }

        let notify_unlock = IntentionLock::new(Arc::new(Notify::const_new()));
        let event = Event::IntentionLock {
            target_id: id,
            intention_lock: notify_unlock.clone(),
            count: 0,
        };

        self.set_flag(event.flag());
        self.events.inner.push(event);

        notify_unlock
    }

    #[inline]
    pub(super) fn add_may_update_event(&mut self, sender: Sender<Key>) {
        let event = Event::MayUpdate(sender);
        self.set_flag(event.flag());
        self.events.inner.push(event);
    }

    #[inline]
    pub(super) fn add_track_event(&mut self, sender: Sender<Resp3>) {
        let event = Event::Track(sender);
        self.set_flag(event.flag());
        self.events.inner.push(event);
    }

    #[inline]
    pub(super) fn remove_event(&mut self, index: usize, flag: u8) {
        if !self.events.contains(flag) {
            return;
        }

        self.events.inner.swap_remove(index);
    }

    #[instrument(level = "debug", skip(db))]
    pub(super) async fn trigger_lock_event(db: &Db, key: Key) -> ObjectEntry {
        let mut entry = db.entries.entry(key);

        match &mut entry {
            Entry::Occupied(e) => {
                // 如果没有意向锁事件，则直接返回。
                if !e.get().events.contains(INTENTION_LOCK_FLAG) {
                    return ObjectEntry::new(entry, db, None);
                }

                let events = &mut e.get_mut().events;
                for (i, e) in events.inner.iter_mut().enumerate() {
                    if let Event::IntentionLock {
                        target_id,
                        intention_lock,
                        count,
                    } = e
                    {
                        // 如果正在执行的不是带有目标ID的task，则释放读写锁后等待。
                        if ID.get() != *target_id {
                            let intention_lock = intention_lock.clone();

                            // 等待者数目加1
                            *count += 1;
                            // 当前等待者的序号，用于判断是否是最后一个等待者
                            let seq = *count;

                            // 释放写锁
                            let key = entry.into_key();

                            // 多个任务有序等待获取写锁的许可。
                            let _ = intention_lock.lock().await;

                            // 重新获取写锁
                            let mut new_entry = db.entries.entry(key.clone());

                            if let Entry::Occupied(e) = &mut new_entry {
                                let obj = e.get_mut();

                                if let Event::IntentionLock { count, .. } = obj
                                    .events
                                    .inner
                                    .get_mut(i)
                                    .expect("must exist since no one removed it")
                                {
                                    // 如果当前任务是最后一个获取写锁的任务，则由该任务负责移除IntentionLock事件
                                    if seq == *count {
                                        obj.remove_event(i, INTENTION_LOCK_FLAG);
                                        obj.remove_flag(INTENTION_LOCK_FLAG);
                                    }
                                } else {
                                    unreachable!(
                                        "get the same result that must be IntentionLock event"
                                    )
                                }
                            }

                            // 获取写锁并带上notify，当写锁释放时通知下一个等待的任务。在使用写锁时，
                            // 可能会有新的任务想要获取写锁，因此不能简单地通知唤醒所有等待的任务（这
                            // 还会导致写锁争用，从而引起阻塞），而是应该一个接一个的唤醒
                            return ObjectEntry::new(new_entry, db, Some(intention_lock));
                        }

                        return ObjectEntry::new(entry, db, None);
                    }
                }

                unreachable!()
            }
            Entry::Vacant(_) => ObjectEntry::new(entry, db, None),
        }
    }

    #[inline]
    #[instrument(level = "debug", skip(self))]
    pub(super) fn trigger_may_update_event(&mut self, key: &Key) {
        if !self.events.contains(MAY_UPDATE_FLAG) {
            return;
        }

        // 触发并移除所有的MayUpdate事件
        self.events.inner.retain(|e| {
            if let Event::MayUpdate(sender) = e {
                let _ = sender.send(key.clone());
                false
            } else {
                true
            }
        });

        self.remove_flag(MAY_UPDATE_FLAG);
    }

    #[inline]
    pub(super) fn trigger_track_event(&mut self, key: &Key) {
        if !self.events.contains(TRACK_FLAG) {
            return;
        }

        let mut should_remov_flag = true;

        self.events.inner.retain(|e| {
            if let Event::Track(sender) = e {
                let res = sender.send(Resp3::new_push(vec![
                    Resp3::new_blob_string("invalidate".into()),
                    Resp3::new_array(vec![Resp3::new_blob_string(key.to_bytes())]),
                ]));

                // 只要有一个发送失败，则不移除该事件的flag
                should_remov_flag &= res.is_ok();

                // 如果发送失败，则表明客户端已经断开连接，移除该事件
                res.is_ok()
            } else {
                true
            }
        });

        if should_remov_flag {
            self.remove_flag(TRACK_FLAG);
        }
    }
}

impl From<ObjectInner> for Object {
    fn from(value: ObjectInner) -> Self {
        Object {
            inner: Some(value),
            events: Default::default(),
        }
    }
}

pub const INTENTION_LOCK_FLAG: u8 = 1 << 0;
pub const TRACK_FLAG: u8 = 1 << 1;
pub const MAY_UPDATE_FLAG: u8 = 1 << 2;

#[derive(Debug, Default)]
struct Events {
    // TODO: tinyvec
    inner: Vec<Event>,
    // 事件类型标志位，用于快速判断是否包含某种事件
    flags: u8,
}

impl Events {
    // pub const fn new(inner: Vec<Event>) -> Self {
    //     Self { inner, flags: 0 }
    // }

    #[inline]
    pub fn contains(&self, flag: u8) -> bool {
        self.flags & flag != 0
    }
}

#[derive(EnumDiscriminants, EnumProperty)]
#[strum_discriminants(vis(pub))]
#[strum_discriminants(name(EventType))]
#[derive(Debug)]
pub enum Event {
    /// 意向锁事件，代表该键值对在未来某个时刻会被某个[`Handler`]上锁，在此期间
    /// 除了id为target_id的[`Handler`]外其余[`Handler`]只能**访问**，而不能修改该键
    /// 值对。
    ///
    /// 每个[`Handler`]在执行时都有一个唯一的[`Id`]，target_id代表设置该意向锁的
    /// [`Handler`]的id，当其它[`Handler`]获取写锁时，如果发现存在IntentionLock则
    /// 马上释放写锁，并等待允许再次获取写锁的通知。
    ///
    /// [`NotifyUnlock`]是有序的，因此命令会按照原来的顺序执行。
    ///
    /// # Example:
    ///
    /// 在执行一个事务时，需要对多个键值对进行操作，为了保证事务的一致性，在事务
    /// 的执行期间，这些键值对只能被该事务的[`Handler`]访问，此时就可以使用意向
    /// 锁。使用意向锁之后，其余[`Handler`]在获取写锁后马上释放，并await直到被允
    /// 许再次获取写锁。当事务执行完毕后，会通知最先等待的[`Handler`]，允许其获取
    /// 写锁。该[`Handler`]使用完写锁后，会通知下一个等待的[`Handler`]，以此类推。
    /// 直到最后一个等待的[`Handler`]获取写锁后，负责移除该事件。在此过程中，某个
    /// [`Handler`]可以重新设置意向锁，此时修改target_id为新的[`Handler`]的id。
    ///
    IntentionLock {
        // 设置意向锁的handler的ID，只有该ID的handler可以访问该键值对
        target_id: Id,
        // 用于通知等待的handler，可以获取写锁了。通知是one by one的
        intention_lock: IntentionLock,
        // 用于记录等待的handler数目，最后一个获取写锁的handler，负责移除该事件
        count: usize,
    },

    Track(Sender<Resp3>),

    /// 触发该事件代表对象的值(不包括expire)可能被修改了
    MayUpdate(Sender<Key>),
    // Remove,
}

impl Event {
    pub const fn flag(&self) -> u8 {
        match self {
            Event::IntentionLock { .. } => INTENTION_LOCK_FLAG,
            Event::Track(_) => TRACK_FLAG,
            Event::MayUpdate(_) => MAY_UPDATE_FLAG,
            // Event::Insert => {}
            // Event::Remove => {}
        }
    }
}

#[derive(Debug)]
pub struct ObjectInner {
    value: ObjValue,
    expire: Instant,
    /// access time and count，高位[`LRU_BITS`]表示access time，其余表示access count
    /// 在读取时也需要更新atc，由于不可变引用无法更新，因此需要使用原子操作
    pub(super) lru: AtomicU32,
}

impl ObjectInner {
    ///  access time更新为lru_clock，access count加1
    #[inline]
    pub fn update_lru(&self) {
        // PERF:
        let atc = self.lru.load(Relaxed);

        let count = atc & Lru::LFU_MASK;

        // TODO:
        let new_count = count.wrapping_add(1);
        // if count == Lru::LFU_FREQUENCY_MAX {
        //     count
        // } else {
        //     // 每次有1 / (count / 2)的概率更新count
        //     // let prob = rand::thread_rng().gen_range(0..=count / 2);
        //     // if prob == 0 {
        //     //     count + 1
        //     // } else {
        //     //     count
        //     // }
        //
        // };

        // PERF:
        let new_atc = (get_lru_clock() << Lru::LFU_BITS) | new_count;

        // PERF:
        // WARN: 有可能有多个线程在同时更新lru，这会覆盖掉其他线程的更新
        // 或许这是可以接受的？
        self.lru.store(new_atc, Relaxed);
    }

    #[inline]
    pub fn lru(&self) -> Lru {
        Lru(self.lru.load(Relaxed))
    }

    #[inline]
    pub fn new_str(s: impl Into<Str>, expire: Instant) -> Self {
        ObjectInner {
            value: ObjValue::Str(s.into()),
            expire,
            lru: Default::default(),
        }
    }

    #[inline]
    pub fn new_list(l: impl Into<List>, expire: Instant) -> Self {
        ObjectInner {
            value: ObjValue::List(l.into()),
            expire,
            lru: Default::default(),
        }
    }

    #[inline]
    pub fn new_set(s: impl Into<Set>, expire: Instant) -> Self {
        ObjectInner {
            value: ObjValue::Set(s.into()),
            expire,
            lru: Default::default(),
        }
    }

    #[inline]
    pub fn new_hash(h: impl Into<Hash>, expire: Instant) -> Self {
        ObjectInner {
            value: ObjValue::Hash(h.into()),
            expire,
            lru: Default::default(),
        }
    }

    #[inline]
    pub fn new_zset(z: impl Into<ZSet>, expire: Instant) -> Self {
        ObjectInner {
            value: ObjValue::ZSet(z.into()),
            expire,
            lru: Default::default(),
        }
    }

    #[inline]
    pub fn is_expired(&self) -> bool {
        self.expire <= Instant::now()
    }

    pub fn typ(&self) -> ObjValueType {
        match &self.value {
            ObjValue::Str(_) => ObjValueType::Str,
            ObjValue::List(_) => ObjValueType::List,
            ObjValue::Set(_) => ObjValueType::Set,
            ObjValue::Hash(_) => ObjValueType::Hash,
            ObjValue::ZSet(_) => ObjValueType::ZSet,
        }
    }

    pub fn type_str(&self) -> &'static str {
        match &self.value {
            ObjValue::Str(_) => ObjValueType::Str.into(),
            ObjValue::List(_) => ObjValueType::List.into(),
            ObjValue::Set(_) => ObjValueType::Set.into(),
            ObjValue::Hash(_) => ObjValueType::Hash.into(),
            ObjValue::ZSet(_) => ObjValueType::ZSet.into(),
        }
    }

    #[inline]
    pub fn value(&self) -> &ObjValue {
        &self.value
    }

    #[inline]
    pub fn expire_unchecked(&self) -> Instant {
        self.expire
    }

    #[inline]
    pub fn try_expire(&self) -> Result<Instant, &'static str> {
        if self.expire <= Instant::now() {
            Ok(self.expire)
        } else {
            Err("object has expired")
        }
    }

    #[inline]
    pub fn expire_expect_never(&self) -> Option<Instant> {
        if self.expire == *NEVER_EXPIRE {
            None
        } else {
            Some(self.expire)
        }
    }

    #[inline]
    pub fn set_expire_unchecked(&mut self, new_ex: Instant) -> Result<Instant, &'static str> {
        if new_ex <= Instant::now() {
            return Err("invalid expire time");
        }

        Ok(std::mem::replace(&mut self.expire, new_ex))
    }

    pub fn set_expire(&mut self, new_ex: Instant) -> Result<Instant, &'static str> {
        if new_ex <= Instant::now() {
            return Err("invalid expire time");
        }

        Ok(std::mem::replace(&mut self.expire, new_ex))
    }

    pub fn on_str(&self) -> RutinResult<&Str> {
        if let ObjValue::Str(s) = &self.value {
            Ok(s)
        } else {
            Err(RutinError::TypeErr {
                expected: ObjValueType::Str.into(),
                found: self.type_str(),
            })
        }
    }

    pub fn on_list(&self) -> RutinResult<&List> {
        if let ObjValue::List(l) = &self.value {
            Ok(l)
        } else {
            Err(RutinError::TypeErr {
                expected: ObjValueType::List.into(),
                found: self.type_str(),
            })
        }
    }

    pub fn on_set(&self) -> RutinResult<&Set> {
        if let ObjValue::Set(s) = &self.value {
            Ok(s)
        } else {
            Err(RutinError::TypeErr {
                expected: ObjValueType::Set.into(),
                found: self.type_str(),
            })
        }
    }

    pub fn on_hash(&self) -> RutinResult<&Hash> {
        if let ObjValue::Hash(h) = &self.value {
            Ok(h)
        } else {
            Err(RutinError::TypeErr {
                expected: ObjValueType::Hash.into(),
                found: self.type_str(),
            })
        }
    }

    pub fn on_zset(&self) -> RutinResult<&ZSet> {
        if let ObjValue::ZSet(z) = &self.value {
            Ok(z)
        } else {
            Err(RutinError::TypeErr {
                expected: ObjValueType::ZSet.into(),
                found: self.type_str(),
            })
        }
    }

    pub fn on_str_mut(&mut self) -> RutinResult<&mut Str> {
        let typ = self.type_str();
        if let ObjValue::Str(s) = &mut self.value {
            Ok(s)
        } else {
            Err(RutinError::TypeErr {
                expected: ObjValueType::Str.into(),
                found: typ,
            })
        }
    }

    pub fn on_list_mut(&mut self) -> RutinResult<&mut List> {
        let typ = self.type_str();
        if let ObjValue::List(l) = &mut self.value {
            Ok(l)
        } else {
            Err(RutinError::TypeErr {
                expected: ObjValueType::List.into(),
                found: typ,
            })
        }
    }

    pub fn on_set_mut(&mut self) -> RutinResult<&mut Set> {
        let typ = self.type_str();
        if let ObjValue::Set(s) = &mut self.value {
            Ok(s)
        } else {
            Err(RutinError::TypeErr {
                expected: ObjValueType::Set.into(),
                found: typ,
            })
        }
    }

    pub fn on_hash_mut(&mut self) -> RutinResult<&mut Hash> {
        let typ = self.type_str();
        if let ObjValue::Hash(h) = &mut self.value {
            Ok(h)
        } else {
            Err(RutinError::TypeErr {
                expected: ObjValueType::Hash.into(),
                found: typ,
            })
        }
    }

    pub fn on_zset_mut(&mut self) -> RutinResult<&mut ZSet> {
        let typ = self.type_str();
        if let ObjValue::ZSet(z) = &mut self.value {
            Ok(z)
        } else {
            Err(RutinError::TypeErr {
                expected: ObjValueType::ZSet.into(),
                found: typ,
            })
        }
    }
}

#[derive(Debug, Clone)]
pub struct Lru(u32);

impl Lru {
    pub const LRU_BITS: u8 = 20;
    pub const LFU_BITS: u8 = 12;

    pub const LRU_CLOCK_MAX: u32 = (1 << Self::LRU_BITS) - 1;
    pub const LFU_FREQUENCY_MAX: u32 = (1 << Self::LRU_BITS) - 1;

    pub const LFU_MASK: u32 = (1 << Self::LFU_BITS) - 1;
    pub const LRU_MASK: u32 = u32::MAX ^ Self::LFU_MASK;

    pub const DEFAULT_ATC: u32 = Self::LRU_MASK; // 默认access time为最大，access_count为0

    #[inline]
    pub fn access_time(&self) -> u32 {
        self.0 >> Self::LFU_BITS
    }

    #[inline]
    pub fn access_count(&self) -> u32 {
        self.0 & Self::LFU_MASK
    }
}

impl Default for Lru {
    fn default() -> Self {
        Lru(Self::LRU_MASK)
    }
}

impl Deref for Lru {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<u32> for Lru {
    fn from(atc: u32) -> Self {
        Lru(atc)
    }
}

impl Clone for ObjectInner {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            expire: self.expire,
            lru: Default::default(),
        }
    }
}

impl PartialEq for ObjectInner {
    fn eq(&self, other: &Self) -> bool {
        let ex_is_eq = if self.expire > other.expire {
            self.expire.duration_since(other.expire).as_secs() < 1
        } else {
            other.expire.duration_since(self.expire).as_secs() < 1
        };

        ex_is_eq && self.value == other.value
    }
}

#[derive(EnumDiscriminants, IntoStaticStr)]
#[strum_discriminants(vis(pub))]
#[strum_discriminants(name(ObjValueType))]
#[strum_discriminants(derive(IntoStaticStr))]
#[derive(Debug, Clone, PartialEq)]
pub enum ObjValue {
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

impl std::fmt::Display for ObjValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.into())
    }
}

impl From<Str> for ObjValue {
    fn from(s: Str) -> Self {
        Self::Str(s)
    }
}

impl From<List> for ObjValue {
    fn from(l: List) -> Self {
        Self::List(l)
    }
}

impl From<Set> for ObjValue {
    fn from(s: Set) -> Self {
        Self::Set(s)
    }
}

impl From<Hash> for ObjValue {
    fn from(h: Hash) -> Self {
        Self::Hash(h)
    }
}

impl From<ZSet> for ObjValue {
    fn from(z: ZSet) -> Self {
        Self::ZSet(z)
    }
}

#[cfg(test)]
mod object_tests {
    use crate::util::{get_test_db, test_init};

    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn may_update_test() {
        test_init();

        let mut obj = Object::new_str("".into(), *NEVER_EXPIRE);

        let (tx, rx) = flume::unbounded();

        obj.add_may_update_event(tx);

        obj.trigger_may_update_event(&"key".into());

        let key = rx.recv().unwrap();
        assert_eq!(&key.to_bytes(), "key");
    }

    #[test]
    fn track_test() {
        test_init();

        let mut obj = Object::new_str("".into(), *NEVER_EXPIRE);

        let (tx, rx) = flume::unbounded();

        obj.add_track_event(tx);

        obj.trigger_track_event(&"key".into());

        let resp = rx.recv().unwrap();

        assert_eq!(
            resp,
            Resp3::new_push(vec![
                Resp3::new_blob_string("invalidate".into()),
                Resp3::new_array(vec![Resp3::new_blob_string("key".into())]),
            ])
        );
    }

    #[tokio::test]
    async fn intention_lock_test() {
        test_init();

        let flag = Arc::new(AtomicUsize::new(0));
        let db = Arc::new(get_test_db());

        let notifiy = Arc::new(Notify::const_new());

        db.insert_object("".into(), ObjectInner::new_str("", *NEVER_EXPIRE))
            .await
            .unwrap();
        db.add_lock_event("".into(), 0).await.unwrap();

        // 目标ID不符，会被阻塞
        let handle1 = tokio::spawn({
            let db = db.clone();
            let flag = flag.clone();
            async move {
                ID.scope(1, Object::trigger_lock_event(&db, "".into()))
                    .await;
                // 该语句应该被最先执行
                assert_eq!(flag.fetch_add(1, Ordering::SeqCst), 1);
            }
        });

        // 目标ID不符，会被阻塞
        let handle2 = tokio::spawn({
            let db = db.clone();
            let flag = flag.clone();
            async move {
                ID.scope(2, Object::trigger_lock_event(&db, "".into()))
                    .await;
                // 该语句应该第二执行
                assert_eq!(flag.fetch_add(1, Ordering::SeqCst), 2);
            }
        });

        // 目标ID不符，会被阻塞
        let handle3 = tokio::spawn({
            let db = db.clone();
            let flag = flag.clone();
            async move {
                // 重入IntentionLock事件
                ID.scope(3, db.add_lock_event("".into(), 3)).await.unwrap();
                // 该语句应该第三执行
                assert_eq!(flag.fetch_add(1, Ordering::SeqCst), 3);
            }
        });

        // 目标ID不符，会被阻塞
        let handle4 = tokio::spawn({
            let db = db.clone();
            let flag = flag.clone();
            async move {
                ID.scope(4, Object::trigger_lock_event(&db, "".into()))
                    .await;
                // 该语句应该最后执行
                assert_eq!(flag.fetch_add(1, Ordering::SeqCst), 4);
            }
        });

        // 符合目标ID，不会阻塞
        ID.scope(0, Object::trigger_lock_event(&db, "".into()))
            .await;
        flag.fetch_add(1, Ordering::SeqCst);

        // 通知等待的任务，意向锁已经可以释放了
        notifiy.notify_one();

        handle1.await.unwrap();
        handle2.await.unwrap();
        handle3.await.unwrap();
        handle4.await.unwrap();

        let e = db.get(&"".into()).await.unwrap();
        assert!(e.events.inner.is_empty())
    }
}
