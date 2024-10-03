use bytes::Bytes;
use equivalent::Equivalent;
use std::ops::Deref;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Key(Bytes);

impl Equivalent<Key> for Bytes {
    fn equivalent(&self, key: &Key) -> bool {
        self == &key.0
    }
}

impl Equivalent<Key> for &[u8] {
    fn equivalent(&self, key: &Key) -> bool {
        self == &key.0
    }
}

impl std::borrow::Borrow<[u8]> for Key {
    fn borrow(&self) -> &[u8] {
        self.0.borrow()
    }
}

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<Bytes> for Key {
    fn from(bytes: Bytes) -> Self {
        Self(bytes)
    }
}

impl From<Key> for Bytes {
    fn from(value: Key) -> Self {
        value.0
    }
}

impl From<&[u8]> for Key {
    fn from(value: &[u8]) -> Self {
        Self(Bytes::copy_from_slice(value))
    }
}

impl From<&'static str> for Key {
    fn from(value: &'static str) -> Self {
        Self(Bytes::from_static(value.as_bytes()))
    }
}

impl From<&Key> for Key {
    fn from(value: &Key) -> Self {
        value.clone()
    }
}

impl PartialEq<&[u8]> for Key {
    fn eq(&self, other: &&[u8]) -> bool {
        self.0.eq(other)
    }
}

impl PartialEq<Bytes> for Key {
    fn eq(&self, other: &Bytes) -> bool {
        self.0.eq(other)
    }
}

impl Deref for Key {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
