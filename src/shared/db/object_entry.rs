use std::convert::From;

use super::*;
use crate::{
    error::{RutinError, RutinResult},
    Key,
};
use dashmap::mapref::{entry::OccupiedEntry, entry_ref};

pub type StaticEntryRef<'a, 'b, Q> = entry_ref::EntryRef<'a, 'b, Key, Q, Object>;
pub type StaticOccupiedEntryRef<'a> = entry_ref::OccupiedEntryRef<'a, Key, Object>;
pub type StaticVacantEntryRef<'a, Q> = entry_ref::VacantEntryRef<'a, 'static, Key, Q, Object>;

// ObjectEntry的对象在一定时间内是合法的，应当尽快使用
//
// 当用户持有ObjectEntry时，它满足：
// 1. 如果是Vacant，则有可能变为Occupied
// 2 .如果是Occupied，则ObjectInner一定存在，但可能是过期的(用户插入了一个过期的ObjectInner)
pub struct ObjectEntry<'a, 'b, Q: ?Sized> {
    pub(super) inner: StaticEntryRef<'a, 'b, Q>,
}

// impl<'a> ObjectEntry<'a> {
//     pub fn new(entry: Entry<'a, Key, Object>, db: &'a Db) -> Self {
//         Self { inner: entry, db }
//     }
// }

impl<'a, 'b, Q> ObjectEntry<'a, 'b, Q> {
    #[inline]
    pub fn is_occupied(&self) -> bool {
        matches!(self.inner, StaticEntryRef::Occupied(_))
    }

    #[inline]
    pub fn key(&self) -> &[u8]
    where
        Q: AsRef<[u8]>,
    {
        self.inner.key_as_ref::<[u8]>()
    }

    // #[inline]
    // pub fn into_key(self) -> Key {
    //     self.inner.into_key()
    // }

    #[inline]
    pub fn expire(&self) -> Option<Instant> {
        if let StaticEntryRef::Occupied(e) = &self.inner {
            let obj = e.get();
            Events::try_trigger_read_event(&obj);

            return Some(obj.expire);
        }

        None
    }

    pub fn and_modify(
        mut self,
        f: impl FnOnce(&mut Object) -> RutinResult<()>,
    ) -> RutinResult<Self> {
        if let StaticEntryRef::Occupied(e) = &mut self.inner {
            let obj = e.get_mut();
            f(obj)?;

            Events::try_trigger_read_and_write_event(obj);

            return Ok(self);
        }

        Ok(self)
    }

    pub fn visit(self, f: impl FnOnce(&Object) -> RutinResult<()>) -> RutinResult<Self> {
        if let StaticEntryRef::Occupied(e) = &self.inner {
            let obj = e.get();
            f(obj)?;

            Events::try_trigger_read_event(obj);

            return Ok(self);
        }

        Err(RutinError::Null)
    }

    /// # Desc:
    ///
    ///
    /// # Error:
    ///
    /// 如果对象不存在，对象为空或者对象已过期则返回RutinError::Null
    #[inline]
    #[instrument(level = "debug", skip(self, f), err)]
    pub fn update1(mut self, f: impl FnOnce(&mut Object) -> RutinResult<()>) -> RutinResult<Self> {
        if let StaticEntryRef::Occupied(e) = &mut self.inner {
            let obj = e.get_mut();
            f(obj)?;

            Events::try_trigger_read_and_write_event(obj);

            return Ok(self);
        }

        Err(RutinError::Null)
    }

    #[inline]
    #[instrument(level = "debug", skip(self, f), err)]
    pub fn update2<F: FnOnce(&mut Object) -> RutinResult<()>>(
        mut self,
        f: F,
    ) -> RutinResult<(Self, Option<F>)> {
        if let StaticEntryRef::Occupied(e) = &mut self.inner {
            let obj = e.get_mut();
            f(obj)?;

            Events::try_trigger_read_and_write_event(obj);

            Ok((self, None))
        } else {
            Ok((self, Some(f)))
        }
    }

    /// # Desc:
    ///
    /// 创建一个新对象，如果对象已存在则不会创建新对象。
    ///
    /// # Return:
    ///
    /// 返回一个新的[`ObjectEntryMut`]以便继续其它操作。
    #[instrument(level = "debug", skip(self))]
    pub fn or_insert(self, value: Object) -> Self
    where
        Key: From<&'b Q>,
    {
        match self.inner {
            StaticEntryRef::Occupied(_) => self,
            StaticEntryRef::Vacant(e) => {
                let new_entry = e.insert_entry(value.into());

                StaticEntryRef::Occupied(new_entry).into()
            }
        }
    }

    #[instrument(level = "debug", skip(self, value))]
    pub fn or_insert_with(self, value: impl FnOnce() -> Object) -> Self
    where
        Key: From<&'b Q>,
    {
        match self.inner {
            StaticEntryRef::Occupied(_) => self,
            StaticEntryRef::Vacant(e) => {
                let new_entry = e.insert_entry(value().into());

                StaticEntryRef::Occupied(new_entry).into()
            }
        }
    }

    #[instrument(level = "debug", skip(self, value))]
    pub fn or_try_insert_with(
        self,
        value: impl FnOnce() -> RutinResult<Object>,
    ) -> RutinResult<Self>
    where
        Key: From<&'b Q>,
    {
        match self.inner {
            StaticEntryRef::Occupied(_) => Ok(self),
            StaticEntryRef::Vacant(e) => {
                let new_entry = e.insert_entry(value()?.into());

                Ok(StaticEntryRef::Occupied(new_entry).into())
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
    pub fn insert1(mut self, object: Object) -> Self
    where
        Key: From<&'b Q>,
    {
        match self.inner {
            StaticEntryRef::Occupied(ref mut e) => {
                let mut old_obj = e.insert(object);

                // 存在旧对象，触发旧对象中的写事件
                Events::try_trigger_read_and_write_event(&mut old_obj);

                self
            }
            StaticEntryRef::Vacant(e) => {
                let new_entry = e.insert_entry(object);

                StaticEntryRef::Occupied(new_entry).into()
            }
        }
    }

    #[instrument(level = "debug", skip(self))]
    pub fn insert2(mut self, object: Object) -> (Self, Option<Object>)
    where
        Key: From<&'b Q>,
    {
        match self.inner {
            StaticEntryRef::Occupied(ref mut e) => {
                let mut old_obj = e.insert(object);

                // 存在旧对象，触发旧对象中的写事件
                Events::try_trigger_read_and_write_event(&mut old_obj);

                (self, Some(old_obj))
            }
            StaticEntryRef::Vacant(e) => {
                let new_entry = e.insert_entry(object);

                (StaticEntryRef::Occupied(new_entry).into(), None)
            }
        }
    }

    /// # Desc:
    ///
    /// 移除对象。如果存在旧对象，则会触发旧对象中的**Track**事件
    #[inline]
    #[instrument(level = "debug", skip(self), ret)]
    pub fn remove(self) -> Option<(Key, Object)> {
        match self.inner {
            StaticEntryRef::Occupied(e) => {
                let (key, mut obj) = e.remove_entry();

                Events::try_trigger_read_and_write_event(&mut obj);

                Some((key, obj))
            }
            StaticEntryRef::Vacant(_) => None,
        }
    }

    // /// # Desc:
    // ///
    // /// 更新对象的过期时间。如果对象不存在，则返回错误。
    // #[inline]
    // #[instrument(level = "debug", skip(self), err)]
    // pub fn update_expire(&mut self, new_ex: Instant) -> RutinResult<Instant> {
    //     if let Entry::Occupied(e) = &mut self.inner {
    //         if e.get_mut().inner().is_some() {
    //             let obj_inner = e.get_mut().inner_mut().unwrap();
    //             let old_ex = obj_inner.set_expire_unchecked(new_ex)?;
    //
    //             self.db.update_expire_records(e.key(), new_ex, old_ex);
    //             return Ok(old_ex);
    //         }
    //     }
    //
    //     Err(RutinError::Null)
    // }

    /// # Desc:
    ///
    /// 如果对象存在且不为空，则添加监听事件
    #[inline]
    #[instrument(level = "debug", skip(self))]
    pub fn add_lock_event(mut self) -> Self {
        match &mut self.inner {
            StaticEntryRef::Occupied(e) => {
                e.get_mut().events.add_lock_event();

                self
            }
            StaticEntryRef::Vacant(_) => self,
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
    #[instrument(level = "debug", skip(self, event))]
    pub fn add_read_event(mut self, event: ReadEvent) -> Self {
        match self.inner {
            StaticEntryRef::Occupied(ref mut e) => {
                e.get_mut().events.add_read_event(event);

                self
            }
            // 不存在对象，则什么都不做
            StaticEntryRef::Vacant(_) => self,
        }
    }

    #[inline]
    #[instrument(level = "debug", skip(self, event))]
    pub fn add_write_event(mut self, event: WriteEvent) -> Self {
        match self.inner {
            StaticEntryRef::Occupied(ref mut e) => {
                e.get_mut().events.add_write_event(event);

                self
            }
            // 不存在对象，则什么都不做
            StaticEntryRef::Vacant(_) => self,
        }
    }

    #[inline]
    #[instrument(level = "debug", skip(self, event))]
    pub fn add_write_event_force(mut self, obj_type: ObjectValueType, event: WriteEvent) -> Self
    where
        Key: From<&'b Q>,
    {
        match self.inner {
            StaticEntryRef::Occupied(ref mut e) => {
                e.get_mut().events.add_write_event(event);

                self
            }
            // 不存在对象，则创建一个空对象
            StaticEntryRef::Vacant(e) => match obj_type {
                ObjectValueType::Str => {
                    let mut new_entry = e.insert_entry(Str::default().into());
                    new_entry.get_mut().events.add_write_event(event);
                    StaticEntryRef::Occupied(new_entry).into()
                }
                ObjectValueType::List => {
                    let mut new_entry = e.insert_entry(List::default().into());
                    new_entry.get_mut().events.add_write_event(event);
                    StaticEntryRef::Occupied(new_entry).into()
                }
                ObjectValueType::Set => {
                    let mut new_entry = e.insert_entry(Set::default().into());
                    new_entry.get_mut().events.add_write_event(event);
                    StaticEntryRef::Occupied(new_entry).into()
                }
                ObjectValueType::Hash => {
                    let mut new_entry = e.insert_entry(Hash::default().into());
                    new_entry.get_mut().events.add_write_event(event);
                    StaticEntryRef::Occupied(new_entry).into()
                }
                ObjectValueType::ZSet => {
                    let mut new_entry = e.insert_entry(ZSet::default().into());
                    new_entry.get_mut().events.add_write_event(event);
                    StaticEntryRef::Occupied(new_entry).into()
                }
            },
        }
    }
}

impl<'a, 'b, Q: ?Sized> From<StaticEntryRef<'a, 'b, Q>> for ObjectEntry<'a, 'b, Q> {
    fn from(entry: StaticEntryRef<'a, 'b, Q>) -> Self {
        Self { inner: entry }
    }
}
