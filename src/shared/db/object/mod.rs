mod hash;
mod list;
mod set;
mod str;
mod zset;

pub use hash::*;
pub use list::*;
pub use set::*;
pub use str::*;
use strum::EnumDiscriminants;
pub use zset::*;

use crate::shared::db::{event::Event, DbError};
use tokio::time::Instant;

// 对象可以为空对象(不存储对象值)，只存储事件
#[derive(Debug, Clone)]
pub struct Object {
    pub inner: Option<ObjectInner>,
    pub event: Event,
}

// 在db模块以外创建的Entry的inner字段一定不为None
impl Object {
    pub(super) fn new(object: ObjectInner) -> Self {
        Object {
            inner: Some(object),
            event: Event::default(),
        }
    }

    #[inline]
    pub fn inner(&self) -> Option<&ObjectInner> {
        self.inner.as_ref()
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
}

impl From<ObjectInner> for Object {
    fn from(value: ObjectInner) -> Self {
        Object {
            inner: Some(value),
            event: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ObjectInner {
    value: ObjValue,
    expire: Option<Instant>, // None代表永不过期
}

impl ObjectInner {
    pub fn new_str(s: Str, expire: Option<Instant>) -> Self {
        ObjectInner {
            value: ObjValue::Str(s),
            expire,
        }
    }

    pub fn new_list(l: List, expire: Option<Instant>) -> Self {
        ObjectInner {
            value: ObjValue::List(l),
            expire,
        }
    }

    pub fn new_set(s: Set, expire: Option<Instant>) -> Self {
        ObjectInner {
            value: ObjValue::Set(s),
            expire,
        }
    }

    pub fn new_hash(h: Hash, expire: Option<Instant>) -> Self {
        ObjectInner {
            value: ObjValue::Hash(h),
            expire,
        }
    }

    pub fn new_zset(z: ZSet, expire: Option<Instant>) -> Self {
        ObjectInner {
            value: ObjValue::ZSet(z),
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
