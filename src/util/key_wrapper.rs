use equivalent::Equivalent;

use crate::Key;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct KeyWrapper(Key);

impl Equivalent<Key> for KeyWrapper {
    fn equivalent(&self, other: &Key) -> bool {
        self.0 == other
    }
}

impl From<&'static str> for KeyWrapper {
    fn from(s: &'static str) -> Self {
        KeyWrapper(Key::from(s))
    }
}

impl From<&KeyWrapper> for Key {
    fn from(wrapper: &KeyWrapper) -> Self {
        wrapper.0.clone()
    }
}

impl From<KeyWrapper> for Key {
    fn from(wrapper: KeyWrapper) -> Self {
        wrapper.0
    }
}

impl From<Key> for KeyWrapper {
    fn from(key: Key) -> Self {
        KeyWrapper(key)
    }
}

impl From<&[u8]> for KeyWrapper {
    fn from(bytes: &[u8]) -> Self {
        KeyWrapper(Key::copy_from_slice(bytes))
    }
}
