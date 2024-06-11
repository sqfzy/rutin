use super::*;
use crate::{cmd::CmdResult, Key};
use bytes::Bytes;
use dashmap::mapref::entry::{self, Entry};
use tokio::time::Instant;
use tracing::instrument;

// 该结构体持有写锁
// 取得该结构体则证明对象存在，但对象可能为空对象
pub struct ObjectEntryMut<'a> {
    entry: Entry<'a, Bytes, Object, ahash::RandomState>,
    db: &'a Db,
}

impl<'a> ObjectEntryMut<'a> {
    pub fn new(entry: Entry<'a, Bytes, Object, ahash::RandomState>, db: &'a Db) -> Self {
        Self { entry, db }
    }
}

impl ObjectEntryMut<'_> {
    pub async fn is_object_existed(&self) -> bool {
        match &self.entry {
            Entry::Occupied(e) => {
                let inner = e.get().inner().await;

                if let Some(inner) = inner {
                    !inner.is_expired()
                } else {
                    false
                }
            }
            Entry::Vacant(_) => false,
        }
    }

    #[inline]
    pub fn key(&self) -> &Key {
        self.entry.key()
    }

    /// # Warn:
    ///
    ///
    #[inline]
    pub async fn value(&self) -> Option<&ObjectInner> {
        if let Entry::Occupied(e) = &self.entry {
            // 对象不为空
            if let Some(inner) = e.get().inner().await {
                // 对象未过期
                if !inner.is_expired() {
                    return Some(inner);
                }
            }
        }

        None
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
                    ObjValueType::Str => Object::new_str(Str::default(), None),
                    ObjValueType::List => Object::new_list(List::default(), None),
                    ObjValueType::Set => Object::new_set(Set::default(), None),
                    ObjValueType::Hash => Object::new_hash(Hash::default(), None),
                    ObjValueType::ZSet => Object::new_zset(ZSet::default(), None),
                };

                let new_entry = e.insert_entry(new_obj);
                Self {
                    entry: entry::Entry::Occupied(new_entry),
                    db,
                }
            }
        }
    }

    // /// # Desc:
    // ///
    // /// 一次性插入对象，返回一个旧对象。如果存在旧对象，则会触发旧对象中的**MayUpdate**和**Track**事件。
    // #[inline]
    // #[instrument(level = "debug", skip(self), ret)]
    // pub fn insert_object_once(self, object: ObjectInner) -> Option<ObjectInner> {
    //     let key = self.entry.key().clone();
    //     let new_ex = object.expire();
    //
    //     let old_obj = match self.entry {
    //         Entry::Occupied(mut e) => Some(e.insert(object.into())),
    //         Entry::Vacant(e) => {
    //             e.insert(object.into());
    //             None
    //         }
    //     }; // 释放entry中的锁
    //
    //     let db = self.db;
    //     match old_obj {
    //         Some(mut old_obj) => {
    //             // 触发对象中的事件
    //             old_obj
    //                 .event
    //                 .trigger_events(&key, &[EventType::MayUpdate, EventType::Track]);
    //
    //             if let Some(old_obj_inner) = &old_obj.inner {
    //                 // 旧对象为有效对象
    //                 db.update_expire_records(&key, new_ex, old_obj_inner.expire());
    //             } else {
    //                 // 旧对象中为空对象，则old_expire为None
    //                 db.update_expire_records(&key, new_ex, None);
    //             }
    //             old_obj.inner
    //         }
    //         None => {
    //             // 不存在旧对象，则old_expire为None
    //             db.update_expire_records(&key, new_ex, None);
    //             None
    //         }
    //     }
    // }

    /// # Desc:
    ///
    /// 插入对象。如果存在旧对象，则会触发旧对象中的**MayUpdate**和**Track**事件。
    ///
    /// # Return:
    ///
    /// 返回一个旧对象和[`ObjectEntryMut`]以便重复操作。
    #[instrument(level = "debug", skip(self))]
    pub async fn insert_object(mut self, object: ObjectInner) -> (Self, Option<ObjectInner>) {
        let key = self.entry.key().clone();
        let new_ex = object.expire();

        let db = self.db;
        match self.entry {
            Entry::Occupied(ref mut e) => {
                let mut old_obj = e.insert(object.into());

                old_obj
                    .event_mut()
                    .await
                    .trigger_events(&key, &[EventType::MayUpdate, EventType::Track]);

                if let Some(old_obj_inner) = old_obj.inner().await {
                    // 旧对象为有效对象
                    db.update_expire_records(&key, new_ex, old_obj_inner.expire());
                } else {
                    // 旧对象中为空对象，则old_expire为None
                    db.update_expire_records(&key, new_ex, None);
                }
                (self, old_obj.to_inner().await)
            }
            Entry::Vacant(e) => {
                let new_entry = e.insert_entry(object.into());

                // 不存在旧对象，则old_expire为None
                db.update_expire_records(&key, new_ex, None);

                (
                    Self {
                        entry: entry::Entry::Occupied(new_entry),
                        db: self.db,
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
    pub async fn remove_object(self) -> Option<(Key, Object)> {
        match self.entry {
            Entry::Occupied(e) => {
                let (key, mut obj) = e.remove_entry();

                if let Some(obj_inner) = obj.inner().await {
                    self.db
                        .update_expire_records(&key, None, obj_inner.expire());
                }

                obj.event_mut()
                    .await
                    .trigger_events(&key, &[EventType::Track]);

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
    pub async fn update_object_expire(
        &mut self,
        new_ex: Option<Instant>,
    ) -> CmdResult<Option<Instant>> {
        if let Entry::Occupied(e) = &mut self.entry {
            if let Some(obj_inner) = e.get_mut().inner_mut().await {
                let old_ex = obj_inner.set_expire(new_ex)?;

                self.db.update_expire_records(e.key(), new_ex, old_ex);
                return Ok(old_ex);
            }
        }

        Err(DbError::KeyNotFound.into())
    }

    /// # Desc:
    ///
    /// 尝试更新对象的值，在回调函数中可以通过on_str_mut()，on_list_mut()等接口获
    /// 取可变的对象值的引用进行更新。会触发对象中的**MayUpdate**和**Track**事件。
    ///
    /// # Error:
    ///
    /// 如果对象不存在，对象为空或者对象已过期则返回CmdError::from(DbError::KeyNotFound)
    #[inline]
    #[instrument(level = "debug", skip(self, f), err)]
    pub async fn update_object_value(
        &mut self,
        f: impl FnOnce(&mut ObjectInner) -> CmdResult<()>,
    ) -> CmdResult<()> {
        if let Entry::Occupied(e) = &mut self.entry {
            if let Some(obj_inner) = e.get_mut().inner_mut().await {
                if obj_inner.is_expired() {
                    return Err(DbError::KeyNotFound.into());
                }

                f(obj_inner)?;

                let key = e.key().clone();
                e.get_mut()
                    .event_mut()
                    .await
                    .trigger_events(&key, &[EventType::MayUpdate, EventType::Track]);

                return Ok(());
            }
        }

        Err(DbError::KeyNotFound.into())
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
    pub async fn update_or_create_object(
        mut self,
        obj_type: ObjValueType,
        f: impl FnOnce(&mut ObjectInner) -> CmdResult<()>,
    ) -> CmdResult<Self> {
        match self.entry {
            Entry::Occupied(ref mut e) => match e.get_mut().inner_mut().await {
                Some(obj_inner) => {
                    f(obj_inner)?;

                    let key = e.key().clone();
                    e.get_mut()
                        .event_mut()
                        .await
                        .trigger_events(&key, &[EventType::MayUpdate, EventType::Track]);

                    Ok(self)
                }
                None => {
                    // 创建新对象，新对象执行回调函数后插入到Db，触发旧对象中的事件
                    let mut new_obj = match obj_type {
                        ObjValueType::Str => Object::new_str(Str::default(), None),
                        ObjValueType::List => Object::new_list(List::default(), None),
                        ObjValueType::Set => Object::new_set(Set::default(), None),
                        ObjValueType::Hash => Object::new_hash(Hash::default(), None),
                        ObjValueType::ZSet => Object::new_zset(ZSet::default(), None),
                    };
                    f(new_obj.inner_mut().await.unwrap())?;

                    let mut old_obj = e.insert(new_obj);
                    old_obj
                        .event_mut()
                        .await
                        .trigger_events(e.key(), &[EventType::MayUpdate, EventType::Track]);

                    Ok(self)
                }
            },
            Entry::Vacant(e) => {
                let mut new_obj = match obj_type {
                    ObjValueType::Str => Object::new_str(Str::default(), None),
                    ObjValueType::List => Object::new_list(List::default(), None),
                    ObjValueType::Set => Object::new_set(Set::default(), None),
                    ObjValueType::Hash => Object::new_hash(Hash::default(), None),
                    ObjValueType::ZSet => Object::new_zset(ZSet::default(), None),
                };
                f(new_obj.inner_mut().await.unwrap())?;

                let new_entry = e.insert_entry(new_obj);
                Ok(Self {
                    entry: entry::Entry::Occupied(new_entry),
                    db: self.db,
                })
            }
        }
    }

    /// # Desc:
    ///
    /// 尝试向对象添加监听事件，如果对象不存在，则创建一个空对象，再添加监听事件。
    #[instrument(level = "debug", skip(self, sender))]
    pub async fn add_event(mut self, sender: Sender<RESP3>, event: EventType) -> Self {
        match self.entry {
            Entry::Occupied(ref mut e) => {
                let obj = e.get_mut();
                obj.event_mut().await.add_event(sender, event);

                self
            }
            Entry::Vacant(e) => {
                // 不存在对象，创建一个空对象
                let mut obj = Object::new(None, Default::default());
                obj.event_mut().await.add_event(sender, event);
                let new_entry = e.insert_entry(obj);

                Self {
                    entry: entry::Entry::Occupied(new_entry),
                    db: self.db,
                }
            }
        }
    }
}
