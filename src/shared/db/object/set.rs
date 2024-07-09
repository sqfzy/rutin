use ahash::AHashSet;
use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Set {
    HashSet(Box<AHashSet<Bytes>>),
    IntSet,
}

impl Set {
    pub fn len(&self) -> usize {
        match self {
            Set::HashSet(set) => set.len(),
            Set::IntSet => unimplemented!(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Set::HashSet(set) => set.is_empty(),
            Set::IntSet => unimplemented!(),
        }
    }

    pub fn insert(&mut self, elem: Bytes) -> bool {
        match self {
            Set::HashSet(set) => set.insert(elem),
            Set::IntSet => unimplemented!(),
        }
    }

    pub fn remove(&mut self, elem: &Bytes) -> bool {
        match self {
            Set::HashSet(set) => set.remove(elem),
            Set::IntSet => unimplemented!(),
        }
    }

    pub fn contains(&self, elem: &Bytes) -> bool {
        match self {
            Set::HashSet(set) => set.contains(elem),
            Set::IntSet => unimplemented!(),
        }
    }
}

impl Default for Set {
    fn default() -> Self {
        Self::HashSet(Box::default())
    }
}

impl<S: Into<AHashSet<Bytes>>> From<S> for Set {
    fn from(set: S) -> Self {
        Set::HashSet(Box::new(set.into()))
    }
}
