use super::Str;
use crate::shared::db::Key;
use ahash::AHashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Hash {
    HashMap(Box<AHashMap<Key, Str>>),
    ZipList,
}

impl Hash {
    pub fn len(&self) -> usize {
        match self {
            Hash::HashMap(map) => map.len(),
            Hash::ZipList => unimplemented!(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Hash::HashMap(map) => map.is_empty(),
            Hash::ZipList => unimplemented!(),
        }
    }

    pub fn insert(&mut self, field: Key, value: impl Into<Str>) -> Option<Str> {
        match self {
            Hash::HashMap(map) => map.insert(field, value.into()),
            Hash::ZipList => unimplemented!(),
        }
    }

    pub fn remove(&mut self, field: &[u8]) -> Option<Str> {
        match self {
            Hash::HashMap(map) => map.remove(field),
            Hash::ZipList => unimplemented!(),
        }
    }

    pub fn get(&self, field: &[u8]) -> Option<Str> {
        match self {
            Hash::HashMap(map) => map.get(field).cloned(),
            Hash::ZipList => unimplemented!(),
        }
    }

    pub fn contains_key(&self, field: &[u8]) -> bool {
        match self {
            Hash::HashMap(map) => map.contains_key(field),
            Hash::ZipList => unimplemented!(),
        }
    }
}

impl Default for Hash {
    fn default() -> Self {
        Self::HashMap(Box::default())
    }
}

impl<M: Into<AHashMap<Key, Str>>> From<M> for Hash {
    fn from(map: M) -> Self {
        Hash::HashMap(Box::new(map.into()))
    }
}
