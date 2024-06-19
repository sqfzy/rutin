use crate::Key;
use ahash::AHashMap;
use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Hash {
    HashMap(AHashMap<Key, Bytes>),
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

    pub fn insert(&mut self, field: Key, value: Bytes) -> Option<Bytes> {
        match self {
            Hash::HashMap(map) => map.insert(field, value),
            Hash::ZipList => unimplemented!(),
        }
    }

    pub fn remove(&mut self, field: &Key) -> Option<Bytes> {
        match self {
            Hash::HashMap(map) => map.remove(field),
            Hash::ZipList => unimplemented!(),
        }
    }

    pub fn get(&self, field: &Key) -> Option<Bytes> {
        match self {
            Hash::HashMap(map) => map.get(field).cloned(),
            Hash::ZipList => unimplemented!(),
        }
    }

    pub fn contains_key(&self, field: &Key) -> bool {
        match self {
            Hash::HashMap(map) => map.contains_key(field),
            Hash::ZipList => unimplemented!(),
        }
    }
}

impl Default for Hash {
    fn default() -> Self {
        Self::HashMap(AHashMap::default())
    }
}

impl<M: Into<AHashMap<Key, Bytes>>> From<M> for Hash {
    fn from(map: M) -> Self {
        Hash::HashMap(map.into())
    }
}
