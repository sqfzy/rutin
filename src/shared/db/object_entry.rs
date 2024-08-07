use super::*;
use crate::{
    error::{RutinError, RutinResult},
    Key,
};
use dashmap::mapref::entry::{self, Entry};
use std::sync::{atomic::Ordering, Arc};
use tokio::{sync::Notify, time::Instant};
use tracing::instrument;

pub struct ObjectEntry<'a> {
    pub(super) entry: Entry<'a, Str, Object>,
    db: &'a Db,
    intention_lock: Option<IntentionLock>,
}

impl<'a> ObjectEntry<'a> {
    pub fn new(
        entry: Entry<'a, Str, Object>,
        db: &'a Db,
        intention_lock: Option<IntentionLock>,
    ) -> Self {
        Self {
            entry,
            db,
            intention_lock,
        }
    }
}

impl ObjectEntry<'_> {
    pub fn is_object_existed(&self) -> bool {
        if let Entry::Occupied(e) = &self.entry {
            return e.get().inner().is_some();
        }

        false
    }

    #[inline]
    pub fn key(&self) -> &Key {
        self.entry.key()
    }

    #[inline]
    pub fn value(&self) -> Option<&ObjectInner> {
        if let Entry::Occupied(e) = &self.entry {
            // 对象不为空
            if let Some(inner) = e.get().inner() {
                return Some(inner);
            }
        }

        None
    }

    #[inline]
    pub fn set_notify_unlock(&mut self, notify: Arc<Notify>) {
        self.intention_lock = Some(IntentionLock(notify));
    }

    /// # Desc:
    ///
    /// 创建一个新对象，如果对象已存在则不会创建新对象。
    ///
    /// # Return:
    ///
    /// 返回一个新的[`ObjectEntryMut`]以便继续其它操作。
    #[inline]
    #[instrument(level = "debug", skip(self))]
    pub fn create_object(self, obj_type: ObjValueType) -> Self {
        match self.entry {
            Entry::Occupied(_) => self,
            Entry::Vacant(e) => {
                let db = self.db;

                let new_obj = match obj_type {
                    ObjValueType::Str => Object::new_str(Str::default(), *NEVER_EXPIRE),
                    ObjValueType::List => Object::new_list(List::default(), *NEVER_EXPIRE),
                    ObjValueType::Set => Object::new_set(Set::default(), *NEVER_EXPIRE),
                    ObjValueType::Hash => Object::new_hash(Hash::default(), *NEVER_EXPIRE),
                    ObjValueType::ZSet => Object::new_zset(ZSet::default(), *NEVER_EXPIRE),
                };

                let new_entry = e.insert_entry(new_obj);
                Self {
                    entry: entry::Entry::Occupied(new_entry),
                    db,
                    intention_lock: None,
                }
            }
        }
    }

    /// # Desc:
    ///
    /// 插入对象。如果存在旧对象，则会触发旧对象中的**MayUpdate**和**Track**事件。
    ///
    /// # Return:
    ///
    /// 返回一个旧对象和[`ObjectEntryMut`]以便重复操作。
    #[instrument(level = "debug", skip(self))]
    pub fn insert_object(mut self, object: ObjectInner) -> (Self, Option<ObjectInner>) {
        let new_ex = object.expire_unchecked();

        let db = self.db;
        match self.entry {
            Entry::Occupied(ref mut e) => {
                if let Some(inner) = e.get().inner() {
                    object
                        .lru
                        .store(inner.lru.load(Ordering::Relaxed), Ordering::Relaxed);
                }
                let mut old_obj = e.insert(object.into());
                let key = e.key();

                old_obj.trigger_may_update_event(key);
                old_obj.trigger_track_event(key);

                if let Some(old_obj_inner) = old_obj.inner() {
                    // 旧对象为有效对象
                    db.update_expire_records(key, new_ex, old_obj_inner.expire_unchecked());
                } else {
                    // 旧对象中为空对象，则old_expire为NEVER_EXPIRE
                    db.update_expire_records(key, new_ex, *NEVER_EXPIRE);
                }
                (self, old_obj.into_inner())
            }
            Entry::Vacant(e) => {
                // 不存在旧对象，则old_expire为NEVER_EXPIRE
                db.update_expire_records(e.key(), new_ex, *NEVER_EXPIRE);

                let new_entry = e.insert_entry(object.into());

                (
                    Self {
                        entry: entry::Entry::Occupied(new_entry),
                        db: self.db,
                        intention_lock: self.intention_lock,
                    },
                    None,
                )
            }
        }
    }

    /// # Desc:
    ///
    /// 移除对象。如果存在旧对象，则会触发旧对象中的**Track**事件
    #[inline]
    #[instrument(level = "debug", skip(self), ret)]
    pub fn remove_object(self) -> Option<(Key, Object)> {
        match self.entry {
            Entry::Occupied(e) => {
                let (key, mut obj) = e.remove_entry();

                if let Some(obj_inner) = obj.inner() {
                    self.db.update_expire_records(
                        &key,
                        *NEVER_EXPIRE,
                        obj_inner.expire_unchecked(),
                    );
                }

                obj.trigger_track_event(&key);

                Some((key, obj))
            }
            Entry::Vacant(_) => None,
        }
    }

    /// # Desc:
    ///
    /// 更新对象的过期时间。如果对象不存在，则返回错误。
    #[inline]
    #[instrument(level = "debug", skip(self), err)]
    pub fn update_object_expire(&mut self, new_ex: Instant) -> RutinResult<Instant> {
        if let Entry::Occupied(e) = &mut self.entry {
            if e.get_mut().inner().is_some() {
                let obj_inner = e.get_mut().inner_mut().unwrap();
                let old_ex = obj_inner.set_expire_unchecked(new_ex)?;

                self.db.update_expire_records(e.key(), new_ex, old_ex);
                return Ok(old_ex);
            }
        }

        Err(RutinError::Null)
    }

    /// # Desc:
    ///
    /// 尝试更新对象的值，在回调函数中可以通过on_str_mut()，on_list_mut()等接口获
    /// 取可变的对象值的引用进行更新。会触发对象中的**MayUpdate**和**Track**事件。
    ///
    /// # Error:
    ///
    /// 如果对象不存在，对象为空或者对象已过期则返回RutinError::Null
    #[inline]
    #[instrument(level = "debug", skip(self, f), err)]
    pub fn update_object_value(
        &mut self,
        f: impl FnOnce(&mut ObjectInner) -> RutinResult<()>,
    ) -> RutinResult<()> {
        if let Entry::Occupied(e) = &mut self.entry {
            if let Some(obj_inner) = e.get_mut().inner_mut() {
                f(obj_inner)?;

                let key = e.key().clone();
                let obj = e.get_mut();

                obj.trigger_may_update_event(&key);
                obj.trigger_track_event(&key);

                return Ok(());
            }
        }

        Err(RutinError::Null)
    }

    /// # Desc:
    ///
    /// 尝试更新对象的值，如果对象不存在或者对象为空，则创建一个新对象后再进行更新。
    /// 会触发对象中的**MayUpdate**和**Track**事件。
    ///
    /// # Error:
    ///
    /// 与[`update_object_value()`]不同的是，该函数完全不关心Db中是否存有键值对。除非在
    /// 回调函数`f`中发生错误，否则该函数一定成功
    #[inline]
    #[instrument(level = "debug", skip(self, f), err)]
    pub fn update_or_create_object(
        mut self,
        obj_type: ObjValueType,
        f: impl FnOnce(&mut ObjectInner) -> RutinResult<()>,
    ) -> RutinResult<Self> {
        match self.entry {
            Entry::Occupied(ref mut e) => match e.get_mut().inner_mut() {
                Some(obj_inner) => {
                    f(obj_inner)?;

                    let key = e.key().clone();
                    let obj = e.get_mut();

                    obj.trigger_may_update_event(&key);
                    obj.trigger_track_event(&key);

                    Ok(self)
                }
                None => {
                    // 创建新对象，新对象执行回调函数后插入到Db，触发旧对象中的事件
                    let mut new_obj = match obj_type {
                        ObjValueType::Str => Object::new_str(Str::default(), *NEVER_EXPIRE),
                        ObjValueType::List => Object::new_list(List::default(), *NEVER_EXPIRE),
                        ObjValueType::Set => Object::new_set(Set::default(), *NEVER_EXPIRE),
                        ObjValueType::Hash => Object::new_hash(Hash::default(), *NEVER_EXPIRE),
                        ObjValueType::ZSet => Object::new_zset(ZSet::default(), *NEVER_EXPIRE),
                    };
                    f(new_obj.inner_mut().unwrap())?;

                    let mut old_obj = e.insert(new_obj);

                    old_obj.trigger_may_update_event(e.key());
                    old_obj.trigger_track_event(e.key());

                    Ok(self)
                }
            },
            Entry::Vacant(e) => {
                let mut new_obj = match obj_type {
                    ObjValueType::Str => Object::new_str(Str::default(), *NEVER_EXPIRE),
                    ObjValueType::List => Object::new_list(List::default(), *NEVER_EXPIRE),
                    ObjValueType::Set => Object::new_set(Set::default(), *NEVER_EXPIRE),
                    ObjValueType::Hash => Object::new_hash(Hash::default(), *NEVER_EXPIRE),
                    ObjValueType::ZSet => Object::new_zset(ZSet::default(), *NEVER_EXPIRE),
                };
                f(new_obj.inner_mut().unwrap())?;

                let new_entry = e.insert_entry(new_obj);
                Ok(Self {
                    entry: entry::Entry::Occupied(new_entry),
                    db: self.db,
                    intention_lock: self.intention_lock,
                })
            }
        }
    }

    /// # Desc:
    ///
    /// 如果对象存在且不为空，则添加监听事件
    #[inline]
    #[instrument(level = "debug", skip(self))]
    pub fn add_lock_event(mut self, target_id: Id) -> (Self, Option<IntentionLock>) {
        match self.entry {
            Entry::Occupied(ref mut e) => {
                let obj = e.get_mut();
                let noti = obj.add_lock_event(target_id);

                (self, Some(noti))
            }
            Entry::Vacant(_) => (self, None),
        }
    }

    /// # Desc:
    ///
    /// 向对象添加监听事件，如果对象不存在，则创建一个空对象，再添加监听事件。
    ///
    /// # Warn:
    ///
    /// 如果创建了一个空对象，但监听事件长时间（或永远）不会被触发，则会浪费
    /// 内存
    #[inline]
    #[instrument(level = "debug", skip(self))]
    pub fn add_may_update_event(mut self, sender: Sender<Key>) -> Self {
        match self.entry {
            Entry::Occupied(ref mut e) => {
                let obj = e.get_mut();
                obj.add_may_update_event(sender);

                self
            }
            Entry::Vacant(e) => {
                // 不存在对象，创建一个空对象
                let mut obj = Object::new_null();
                obj.add_may_update_event(sender);
                let new_entry = e.insert_entry(obj);

                Self {
                    entry: entry::Entry::Occupied(new_entry),
                    db: self.db,
                    intention_lock: self.intention_lock,
                }
            }
        }
    }

    /// # Desc:
    ///
    /// 向对象添加监听事件，如果对象不存在，则创建一个空对象，再添加监听事件。
    ///
    /// # Warn:
    ///
    /// 如果创建了一个空对象，但监听事件长时间（或永远）不会被触发，则会浪费
    /// 内存
    #[inline]
    #[instrument(level = "debug", skip(self))]
    pub fn add_track_event(mut self, sender: Sender<Resp3>) -> Self {
        match self.entry {
            Entry::Occupied(ref mut e) => {
                let obj = e.get_mut();
                obj.add_track_event(sender);

                self
            }
            Entry::Vacant(e) => {
                // 不存在对象，创建一个空对象
                let mut obj = Object::new_null();
                obj.add_track_event(sender);
                let new_entry = e.insert_entry(obj);

                Self {
                    entry: entry::Entry::Occupied(new_entry),
                    db: self.db,
                    intention_lock: self.intention_lock,
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct IntentionLock(Arc<Notify>);

impl IntentionLock {
    pub fn new(notify: Arc<Notify>) -> Self {
        Self(notify)
    }

    pub fn unlock(&self) {
        self.0.notify_one();
    }

    pub async fn lock(&self) {
        self.0.notified().await;
    }
}

impl Drop for IntentionLock {
    #[inline]
    fn drop(&mut self) {
        self.0.notify_one();
    }
}
