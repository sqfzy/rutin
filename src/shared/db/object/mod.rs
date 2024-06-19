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
    frame::Resp3,
    server::ID,
    shared::db::{
        object_entry::{NotifyUnlock, ObjectEntryMut},
        Db, DbError,
    },
    Id, Key,
};
use bytes::Bytes;
use dashmap::mapref::entry::Entry;
use flume::Sender;
use std::sync::Arc;
use strum::{EnumDiscriminants, EnumProperty};
use tokio::{sync::Notify, time::Instant};
use tracing::instrument;

// 对象可以为空对象(不存储对象值)，只存储事件
// TODO: LRU
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

    pub fn new_str(s: Str, expire: Option<Instant>) -> Self {
        Object::new(ObjectInner::new_str(s, expire))
    }

    pub fn new_list(l: List, expire: Option<Instant>) -> Self {
        Object::new(ObjectInner::new_list(l, expire))
    }

    pub fn new_set(s: Set, expire: Option<Instant>) -> Self {
        Object::new(ObjectInner::new_set(s, expire))
    }

    pub fn new_hash(h: Hash, expire: Option<Instant>) -> Self {
        Object::new(ObjectInner::new_hash(h, expire))
    }

    pub fn new_zset(z: ZSet, expire: Option<Instant>) -> Self {
        Object::new(ObjectInner::new_zset(z, expire))
    }

    pub fn new_null() -> Self {
        Object {
            inner: None,
            events: Default::default(),
        }
    }

    pub fn on_str(&self) -> Option<Result<&Str, DbError>> {
        let inner = if let Some(inner) = &self.inner {
            inner
        } else {
            return None;
        };

        if let ObjValue::Str(s) = &inner.value {
            Some(Ok(s))
        } else {
            Some(Err(DbError::TypeErr {
                expected: "string",
                found: inner.type_str(),
            }))
        }
    }

    pub fn on_list(&self) -> Option<Result<&List, DbError>> {
        let inner = if let Some(inner) = &self.inner {
            inner
        } else {
            return None;
        };

        if let ObjValue::List(l) = &inner.value {
            Some(Ok(l))
        } else {
            Some(Err(DbError::TypeErr {
                expected: "list",
                found: inner.type_str(),
            }))
        }
    }

    pub fn on_set(&self) -> Option<Result<&Set, DbError>> {
        let inner = if let Some(inner) = &self.inner {
            inner
        } else {
            return None;
        };

        if let ObjValue::Set(s) = &inner.value {
            Some(Ok(s))
        } else {
            Some(Err(DbError::TypeErr {
                expected: "set",
                found: inner.type_str(),
            }))
        }
    }

    pub fn on_hash(&self) -> Option<Result<&Hash, DbError>> {
        let inner = if let Some(inner) = &self.inner {
            inner
        } else {
            return None;
        };

        if let ObjValue::Hash(h) = &inner.value {
            Some(Ok(h))
        } else {
            Some(Err(DbError::TypeErr {
                expected: "hash",
                found: inner.type_str(),
            }))
        }
    }

    pub fn on_zset(&self) -> Option<Result<&ZSet, DbError>> {
        let inner = if let Some(inner) = &self.inner {
            inner
        } else {
            return None;
        };

        if let ObjValue::ZSet(z) = &inner.value {
            Some(Ok(z))
        } else {
            Some(Err(DbError::TypeErr {
                expected: "zset",
                found: inner.type_str(),
            }))
        }
    }

    pub fn on_str_mut(&mut self) -> Option<Result<&mut Str, DbError>> {
        let inner = if let Some(inner) = &mut self.inner {
            inner
        } else {
            return None;
        };

        let typ = inner.type_str();
        if let ObjValue::Str(s) = &mut inner.value {
            Some(Ok(s))
        } else {
            Some(Err(DbError::TypeErr {
                expected: "string",
                found: typ,
            }))
        }
    }

    pub fn on_list_mut(&mut self) -> Option<Result<&mut List, DbError>> {
        let inner = if let Some(inner) = &mut self.inner {
            inner
        } else {
            return None;
        };

        let typ = inner.type_str();
        if let ObjValue::List(l) = &mut inner.value {
            Some(Ok(l))
        } else {
            Some(Err(DbError::TypeErr {
                expected: "list",
                found: typ,
            }))
        }
    }

    pub fn on_set_mut(&mut self) -> Option<Result<&mut Set, DbError>> {
        let inner = if let Some(inner) = &mut self.inner {
            inner
        } else {
            return None;
        };

        let typ = inner.type_str();
        if let ObjValue::Set(s) = &mut inner.value {
            Some(Ok(s))
        } else {
            Some(Err(DbError::TypeErr {
                expected: "set",
                found: typ,
            }))
        }
    }

    pub fn on_hash_mut(&mut self) -> Option<Result<&mut Hash, DbError>> {
        let inner = if let Some(inner) = &mut self.inner {
            inner
        } else {
            return None;
        };

        let typ = inner.type_str();
        if let ObjValue::Hash(h) = &mut inner.value {
            Some(Ok(h))
        } else {
            Some(Err(DbError::TypeErr {
                expected: "hash",
                found: typ,
            }))
        }
    }

    pub fn on_zset_mut(&mut self) -> Option<Result<&mut ZSet, DbError>> {
        let inner = if let Some(inner) = &mut self.inner {
            inner
        } else {
            return None;
        };

        let typ = inner.type_str();
        if let ObjValue::ZSet(z) = &mut inner.value {
            Some(Ok(z))
        } else {
            Some(Err(DbError::TypeErr {
                expected: "zset",
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
    }

    pub(super) fn add_lock_event(&mut self, target_id: Id) -> NotifyUnlock {
        let id = target_id;
        if self.events.contains(INTENTION_LOCK_FLAG) {
            for e in self.events.inner.iter_mut() {
                if let Event::IntentionLock {
                    target_id,
                    notify_unlock,
                    ..
                } = e
                {
                    *target_id = id;
                    return notify_unlock.clone();
                }
            }

            unreachable!()
        }

        let notify_unlock = NotifyUnlock::new(Arc::new(Notify::const_new()));
        let event = Event::IntentionLock {
            target_id: id,
            notify_unlock: notify_unlock.clone(),
            count: 0,
        };

        self.set_flag(event.flag());
        self.events.inner.push(event);

        notify_unlock
    }

    pub(super) fn add_may_update_event(&mut self, sender: Sender<Bytes>) {
        let event = Event::MayUpdate(sender);
        self.set_flag(event.flag());
        self.events.inner.push(event);
    }

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

    #[inline]
    #[instrument(level = "debug", skip(db))]
    pub(super) async fn trigger_lock_event(db: &Db, key: Key) -> ObjectEntryMut {
        let mut entry = db.entries.entry(key);

        match &mut entry {
            Entry::Occupied(e) => {
                // 如果没有意向锁事件，则直接返回
                if !e.get().events.contains(INTENTION_LOCK_FLAG) {
                    return ObjectEntryMut::new(entry, db, None);
                }

                let events = &mut e.get_mut().events;
                let mut i = 0;
                while let Some(e) = events.inner.get_mut(i) {
                    match e {
                        // 找到IntentionLock事件
                        Event::IntentionLock {
                            target_id,
                            notify_unlock,
                            count,
                        } => {
                            // 如果正在执行的不是带有目标ID的task，则释放读写锁后等待意向锁释放
                            if ID.get() != *target_id {
                                let notify_unlock = notify_unlock.clone();

                                // 等待者数目加1
                                *count += 1;
                                let seq = *count;

                                // 释放写锁
                                let key = entry.into_key();

                                // 多个任务有序等待获取写锁的许可
                                let _ = notify_unlock.wait().await;

                                // 重新获取写锁
                                let mut new_entry = db.entries.entry(key.clone());

                                // 如果当前任务是最后一个获取写锁的任务，则由该任务负责移除IntentionLock事件
                                if let Entry::Occupied(e) = &mut new_entry {
                                    let obj = e.get_mut();

                                    if let Event::IntentionLock { count, .. } =
                                        obj.events.inner.get_mut(i).unwrap()
                                    {
                                        if seq == *count {
                                            obj.remove_event(i, INTENTION_LOCK_FLAG);
                                            obj.remove_flag(INTENTION_LOCK_FLAG);
                                        }
                                    } else {
                                        unreachable!()
                                    }
                                }

                                // 获取写锁并带上notify，当写锁释放时通知下一个等待的任务。在使用写锁时，
                                // 可能会有新的任务想要获取写锁，因此不能简单地通知唤醒所有等待的任务（这
                                // 还会导致写锁争用，从而引起阻塞），而是应该一个接一个的唤醒
                                return ObjectEntryMut::new(new_entry, db, Some(notify_unlock));
                            }

                            return ObjectEntryMut::new(entry, db, None);
                        }
                        _ => i += 1,
                    }
                }

                unreachable!()
            }
            Entry::Vacant(_) => ObjectEntryMut::new(entry, db, None),
        }
    }

    #[inline]
    #[instrument(level = "debug", skip(self))]
    pub(super) fn trigger_may_update_event(&mut self, key: &Bytes) {
        if !self.events.contains(MAY_UPDATE_FLAG) {
            return;
        }

        let mut i = 0;
        while let Some(e) = self.events.inner.get(i) {
            match e {
                Event::MayUpdate(sender) => {
                    let _ = sender.send(key.clone());

                    self.remove_event(i, MAY_UPDATE_FLAG);
                    self.remove_flag(MAY_UPDATE_FLAG);
                }
                _ => i += 1,
            }
        }
    }

    #[inline]
    pub(super) fn trigger_track_event(&mut self, _key: &Key) {
        // if !self.events.contains(TRACK_FLAG) {
        //     return;
        // }

        // TODO:
        // todo!()
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
    inner: Vec<Event>,
    // 事件类型标志位，用于快速判断是否包含某种事件
    flags: u8,
}

impl Events {
    pub const fn new(inner: Vec<Event>) -> Self {
        Self { inner, flags: 0 }
    }

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
    /// 意向锁事件，代表该键值对在未来某个时刻会被某个['Handler']上锁，在此期间
    /// 除了id为target_id的[`Handler`]外其余[`Handler`]只能访问，而不能修改该键
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
    /// 锁。使用意向锁之后，其余['Handler']在获取写锁后马上释放，并await直到被允
    /// 许再次获取写锁。当事务执行完毕后，会通知最先等待的['Handler']，允许其获取
    /// 写锁。该[`Handler`]使用完写锁后，会通知下一个等待的['Handler']，以此类推。
    /// 直到最后一个等待的['Handler']获取写锁后，负责移除该事件。在此过程中，某个
    /// [`Handler`]可以重新设置意向锁，此时修改target_id为新的[`Handler`]的id。
    ///
    IntentionLock {
        // 设置意向锁的handler的ID，只有该ID的handler可以访问该键值对
        target_id: Id,
        // 用于通知等待的handler，可以获取写锁了。通知是one by one的
        notify_unlock: NotifyUnlock,
        // 用于记录等待的handler数目，最后一个获取写锁的handler，负责移除该事件
        count: usize,
    },

    Track(Sender<Resp3>),

    /// 触发该事件代表对象的值(不包括expire)可能被修改了
    MayUpdate(Sender<Bytes>),
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

#[derive(Debug, Clone)]
pub struct ObjectInner {
    value: ObjValue,
    expire: Option<Instant>, // None代表永不过期
}

impl ObjectInner {
    pub fn new_str(s: impl Into<Str>, expire: Option<Instant>) -> Self {
        ObjectInner {
            value: ObjValue::Str(s.into()),
            expire,
        }
    }

    pub fn new_list(l: impl Into<List>, expire: Option<Instant>) -> Self {
        ObjectInner {
            value: ObjValue::List(l.into()),
            expire,
        }
    }

    pub fn new_set(s: impl Into<Set>, expire: Option<Instant>) -> Self {
        ObjectInner {
            value: ObjValue::Set(s.into()),
            expire,
        }
    }

    pub fn new_hash(h: impl Into<Hash>, expire: Option<Instant>) -> Self {
        ObjectInner {
            value: ObjValue::Hash(h.into()),
            expire,
        }
    }

    pub fn new_zset(z: impl Into<ZSet>, expire: Option<Instant>) -> Self {
        ObjectInner {
            value: ObjValue::ZSet(z.into()),
            expire,
        }
    }

    pub fn is_expired(&self) -> bool {
        if let Some(ex) = self.expire {
            if ex <= Instant::now() {
                return true;
            }
        }
        false
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
            ObjValue::Str(_) => "string",
            ObjValue::List(_) => "list",
            ObjValue::Set(_) => "set",
            ObjValue::Hash(_) => "hash",
            ObjValue::ZSet(_) => "zset",
        }
    }

    #[inline]
    pub fn value(&self) -> &ObjValue {
        &self.value
    }

    #[inline]
    pub fn expire(&self) -> Option<Instant> {
        self.expire
    }

    pub fn set_expire(&mut self, new_ex: Option<Instant>) -> Result<Option<Instant>, &'static str> {
        if let Some(ex) = new_ex {
            if ex <= Instant::now() {
                return Err("invalid expire time");
            }
        }
        Ok(std::mem::replace(&mut self.expire, new_ex))
    }

    pub fn on_str(&self) -> Result<&Str, DbError> {
        if let ObjValue::Str(s) = &self.value {
            Ok(s)
        } else {
            Err(DbError::TypeErr {
                expected: "string",
                found: self.type_str(),
            })
        }
    }

    pub fn on_list(&self) -> Result<&List, DbError> {
        if let ObjValue::List(l) = &self.value {
            Ok(l)
        } else {
            Err(DbError::TypeErr {
                expected: "list",
                found: self.type_str(),
            })
        }
    }

    pub fn on_set(&self) -> Result<&Set, DbError> {
        if let ObjValue::Set(s) = &self.value {
            Ok(s)
        } else {
            Err(DbError::TypeErr {
                expected: "set",
                found: self.type_str(),
            })
        }
    }

    pub fn on_hash(&self) -> Result<&Hash, DbError> {
        if let ObjValue::Hash(h) = &self.value {
            Ok(h)
        } else {
            Err(DbError::TypeErr {
                expected: "hash",
                found: self.type_str(),
            })
        }
    }

    pub fn on_zset(&self) -> Result<&ZSet, DbError> {
        if let ObjValue::ZSet(z) = &self.value {
            Ok(z)
        } else {
            Err(DbError::TypeErr {
                expected: "zset",
                found: self.type_str(),
            })
        }
    }

    pub fn on_str_mut(&mut self) -> Result<&mut Str, DbError> {
        let typ = self.type_str();
        if let ObjValue::Str(s) = &mut self.value {
            Ok(s)
        } else {
            Err(DbError::TypeErr {
                expected: "string",
                found: typ,
            })
        }
    }

    pub fn on_list_mut(&mut self) -> Result<&mut List, DbError> {
        let typ = self.type_str();
        if let ObjValue::List(l) = &mut self.value {
            Ok(l)
        } else {
            Err(DbError::TypeErr {
                expected: "list",
                found: typ,
            })
        }
    }

    pub fn on_set_mut(&mut self) -> Result<&mut Set, DbError> {
        let typ = self.type_str();
        if let ObjValue::Set(s) = &mut self.value {
            Ok(s)
        } else {
            Err(DbError::TypeErr {
                expected: "set",
                found: typ,
            })
        }
    }

    pub fn on_hash_mut(&mut self) -> Result<&mut Hash, DbError> {
        let typ = self.type_str();
        if let ObjValue::Hash(h) = &mut self.value {
            Ok(h)
        } else {
            Err(DbError::TypeErr {
                expected: "hash",
                found: typ,
            })
        }
    }

    pub fn on_zset_mut(&mut self) -> Result<&mut ZSet, DbError> {
        let typ = self.type_str();
        if let ObjValue::ZSet(z) = &mut self.value {
            Ok(z)
        } else {
            Err(DbError::TypeErr {
                expected: "zset",
                found: typ,
            })
        }
    }
}

impl PartialEq for ObjectInner {
    fn eq(&self, other: &Self) -> bool {
        let ex_is_eq = if let (Some(ex1), Some(ex2)) = (self.expire, other.expire) {
            if ex1 > ex2 {
                ex1.duration_since(ex2).as_secs() < 1
            } else {
                ex2.duration_since(ex1).as_secs() < 1
            }
        } else {
            self.expire.is_none() && other.expire.is_none()
        };

        self.value == other.value && ex_is_eq
    }
}

#[derive(EnumDiscriminants)]
#[strum_discriminants(vis(pub))]
#[strum_discriminants(name(ObjValueType))]
#[derive(Debug, Clone, PartialEq)]
pub enum ObjValue {
    Str(Str),
    List(List),
    Set(Set),
    Hash(Hash),
    ZSet(ZSet),
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
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn may_update_test() {
        let mut obj = Object::new_str("".into(), None);

        let (tx, rx) = flume::unbounded();

        obj.add_may_update_event(tx);

        obj.trigger_may_update_event(&"key".into());

        let key = rx.recv().unwrap();
        assert_eq!(&key, "key");
    }

    #[tokio::test]
    async fn intention_lock_test() {
        let flag = Arc::new(AtomicUsize::new(0));
        let db = Arc::new(Db::default());

        let notifiy = Arc::new(Notify::const_new());

        db.insert_object("".into(), ObjectInner::new_str("", None))
            .await;
        db.add_lock_event("".into(), 0).await;

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
                ID.scope(3, db.add_lock_event("".into(), 3)).await;
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

        let e = db.get_object_entry(&"".into()).await.unwrap();
        assert!(e.events.inner.is_empty())
    }
}
