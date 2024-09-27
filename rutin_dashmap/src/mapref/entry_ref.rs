use super::one::RefMut;
use crate::lock::RwLockWriteGuard;
use crate::util::SharedValue;
use crate::HashMap;
use core::hash::Hash;
use core::mem;
use std::borrow::Borrow;

pub enum EntryRef<'a, 'b, K, Q: ?Sized, V> {
    Occupied(OccupiedEntryRef<'a, K, V>),
    Vacant(VacantEntryRef<'a, 'b, K, Q, V>),
}

impl<'a, 'b, K: Eq + Hash, Q: ?Sized, V> EntryRef<'a, 'b, K, Q, V> {
    /// Apply a function to the stored value if it exists.
    pub fn and_modify(self, f: impl FnOnce(&mut V)) -> Self {
        match self {
            EntryRef::Occupied(mut entry) => {
                f(entry.get_mut());

                EntryRef::Occupied(entry)
            }

            EntryRef::Vacant(entry) => EntryRef::Vacant(entry),
        }
    }

    /// Get the key of the entry.
    pub fn key(&self) -> &Q
    where
        K: Borrow<Q>,
    {
        match *self {
            EntryRef::Occupied(ref entry) => entry.key().borrow(),
            EntryRef::Vacant(ref entry) => entry.key(),
        }
    }

    pub fn key_as_ref<T: ?Sized>(&'a self) -> &T
    where
        K: AsRef<T>,
        Q: AsRef<T>,
    {
        match *self {
            EntryRef::Occupied(ref entry) => entry.key().as_ref(),
            EntryRef::Vacant(ref entry) => entry.key().as_ref(),
        }
    }

    // /// Into the key of the entry.
    // pub fn into_key(self) -> K {
    //     match self {
    //         Entry::Occupied(entry) => entry.into_key(),
    //         Entry::Vacant(entry) => entry.into_key(),
    //     }
    // }

    /// Return a mutable reference to the element if it exists,
    /// otherwise insert the default and return a mutable reference to that.
    pub fn or_default(self) -> RefMut<'a, K, V>
    where
        K: Hash + From<&'b Q> + Default,
        V: Default,
    {
        match self {
            EntryRef::Occupied(entry) => entry.into_ref(),
            EntryRef::Vacant(entry) => entry.insert(V::default()),
        }
    }

    /// Return a mutable reference to the element if it exists,
    /// otherwise a provided value and return a mutable reference to that.
    pub fn or_insert(self, value: V) -> RefMut<'a, K, V>
    where
        K: Hash + From<&'b Q>,
    {
        match self {
            EntryRef::Occupied(entry) => entry.into_ref(),
            EntryRef::Vacant(entry) => entry.insert(value),
        }
    }

    /// Return a mutable reference to the element if it exists,
    /// otherwise insert the result of a provided function and return a mutable reference to that.
    pub fn or_insert_with(self, value: impl FnOnce() -> V) -> RefMut<'a, K, V>
    where
        K: Hash + From<&'b Q>,
    {
        match self {
            EntryRef::Occupied(entry) => entry.into_ref(),
            EntryRef::Vacant(entry) => entry.insert(value()),
        }
    }

    pub fn or_try_insert_with<E>(
        self,
        value: impl FnOnce() -> Result<V, E>,
    ) -> Result<RefMut<'a, K, V>, E>
    where
        K: Hash + From<&'b Q>,
    {
        match self {
            EntryRef::Occupied(entry) => Ok(entry.into_ref()),
            EntryRef::Vacant(entry) => Ok(entry.insert(value()?)),
        }
    }

    /// Sets the value of the entry, and returns a reference to the inserted value.
    pub fn insert(self, value: V) -> RefMut<'a, K, V>
    where
        K: Hash + From<&'b Q>,
    {
        match self {
            EntryRef::Occupied(mut entry) => {
                entry.insert(value);
                entry.into_ref()
            }
            EntryRef::Vacant(entry) => entry.insert(value),
        }
    }

    /// Sets the value of the entry, and returns an OccupiedEntry.
    ///
    /// If you are not interested in the occupied entry,
    /// consider [`insert`] as it doesn't need to clone the key.
    ///
    /// [`insert`]: Entry::insert
    pub fn insert_entry(self, value: V) -> OccupiedEntryRef<'a, K, V>
    where
        K: Hash + From<&'b Q>,
    {
        match self {
            EntryRef::Occupied(mut entry) => {
                entry.insert(value);
                entry
            }
            EntryRef::Vacant(entry) => entry.insert_entry(value),
        }
    }
}

pub struct VacantEntryRef<'a, 'b, K, Q: ?Sized, V> {
    shard: RwLockWriteGuard<'a, HashMap<K, V>>,
    key: &'b Q,
    hash: u64,
    slot: hashbrown::raw::InsertSlot,
}

unsafe impl<'a, 'b, K: Eq + Hash + Sync, Q: ?Sized, V: Sync> Send
    for VacantEntryRef<'a, 'b, K, Q, V>
{
}
unsafe impl<'a, 'b, K: Eq + Hash + Sync, Q: ?Sized, V: Sync> Sync
    for VacantEntryRef<'a, 'b, K, Q, V>
{
}

impl<'a, 'b, K: Eq + Hash, Q: ?Sized, V> VacantEntryRef<'a, 'b, K, Q, V> {
    pub(crate) unsafe fn new(
        shard: RwLockWriteGuard<'a, HashMap<K, V>>,
        key: &'b Q,
        hash: u64,
        slot: hashbrown::raw::InsertSlot,
    ) -> Self {
        Self {
            shard,
            key,
            hash,
            slot,
        }
    }

    pub fn hash(&self) -> u64 {
        self.hash
    }

    pub fn insert(mut self, value: V) -> RefMut<'a, K, V>
    where
        K: Hash + From<&'b Q>,
    {
        unsafe {
            let occupied = self.shard.insert_in_slot(
                self.hash,
                self.slot,
                (self.key.into(), SharedValue::new(value)),
            );

            let (k, v) = occupied.as_ref();

            RefMut::new(self.shard, k, v.as_ptr())
        }
    }

    /// Sets the value of the entry with the VacantEntry’s key, and returns an OccupiedEntry.
    pub fn insert_entry(mut self, value: V) -> OccupiedEntryRef<'a, K, V>
    where
        K: Hash + From<&'b Q>,
    {
        unsafe {
            let bucket = self.shard.insert_in_slot(
                self.hash,
                self.slot,
                (self.key.into(), SharedValue::new(value)),
            );

            OccupiedEntryRef::new(self.hash, self.shard, bucket)
        }
    }

    // pub fn into_key(self) -> K {
    //     self.key
    // }

    pub fn key(&self) -> &'b Q {
        self.key
    }
}

pub struct OccupiedEntryRef<'a, K, V> {
    hash: u64,
    shard: RwLockWriteGuard<'a, HashMap<K, V>>,
    bucket: hashbrown::raw::Bucket<(K, SharedValue<V>)>,
}

unsafe impl<'a, K: Eq + Hash + Sync, V: Sync> Send for OccupiedEntryRef<'a, K, V> {}
unsafe impl<'a, K: Eq + Hash + Sync, V: Sync> Sync for OccupiedEntryRef<'a, K, V> {}

impl<'a, K: Eq + Hash, V> OccupiedEntryRef<'a, K, V> {
    pub(crate) unsafe fn new(
        hash: u64,
        shard: RwLockWriteGuard<'a, HashMap<K, V>>,
        bucket: hashbrown::raw::Bucket<(K, SharedValue<V>)>,
    ) -> Self {
        Self {
            hash,
            shard,
            bucket,
        }
    }

    pub fn hash(&self) -> u64 {
        self.hash
    }

    pub fn get(&self) -> &V {
        unsafe { self.bucket.as_ref().1.get() }
    }

    pub fn get_mut(&mut self) -> &mut V {
        unsafe { self.bucket.as_mut().1.get_mut() }
    }

    pub fn insert(&mut self, value: V) -> V {
        mem::replace(self.get_mut(), value)
    }

    pub fn into_ref(self) -> RefMut<'a, K, V> {
        unsafe {
            let (k, v) = self.bucket.as_ref();
            RefMut::new(self.shard, k, v.as_ptr())
        }
    }

    // pub fn into_key(self) -> K {
    //     self.key
    // }

    pub fn key(&self) -> &K {
        unsafe { &self.bucket.as_ref().0 }
    }

    pub fn remove(mut self) -> V {
        let ((_k, v), _) = unsafe { self.shard.remove(self.bucket) };
        v.into_inner()
    }

    pub fn remove_entry(mut self) -> (K, V) {
        let ((k, v), _) = unsafe { self.shard.remove(self.bucket) };
        (k, v.into_inner())
    }

    // pub fn replace_entry(self, value: V) -> (K, V) {
    //     let (k, v) = mem::replace(
    //         unsafe { self.bucket.as_mut() },
    //         (self.key, SharedValue::new(value)),
    //     );
    //     (k, v.into_inner())
    // }
}

#[cfg(test)]
mod tests {
    use crate::DashMap;

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct Q {
        key: u32,
    }

    impl From<&Q> for u32 {
        fn from(q: &Q) -> u32 {
            q.key
        }
    }

    impl equivalent::Equivalent<u32> for Q {
        fn equivalent(&self, key: &u32) -> bool {
            self.key == *key
        }
    }

    #[test]
    fn test_insert_entry_into_vacant() {
        let map: DashMap<u32, u32> = DashMap::new();

        let key = Q { key: 1 };
        let entry = map.entry_ref(&key);

        assert!(matches!(entry, EntryRef::Vacant(_)));

        let entry = entry.insert_entry(2);

        assert_eq!(*entry.get(), 2);

        drop(entry);

        assert_eq!(*map.get(&1).unwrap(), 2);
    }

    #[test]
    fn test_insert_entry_into_occupied() {
        let map: DashMap<u32, u32> = DashMap::new();

        map.insert(1, 1000);

        let key = Q { key: 1 };
        let entry = map.entry_ref(&key);

        assert!(matches!(&entry, EntryRef::Occupied(entry) if *entry.get() == 1000));

        let entry = entry.insert_entry(2);

        assert_eq!(*entry.get(), 2);

        drop(entry);

        assert_eq!(*map.get(&1).unwrap(), 2);
    }
}
