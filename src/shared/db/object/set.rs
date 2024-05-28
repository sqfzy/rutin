use ahash::AHashSet;
use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Set {
    HashSet(AHashSet<Bytes>),
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

impl From<AHashSet<Bytes>> for Set {
    fn from(set: AHashSet<Bytes>) -> Self {
        Set::HashSet(set)
    }
}

impl From<Vec<&'static str>> for Set {
    fn from(value: Vec<&'static str>) -> Self {
        let mut set = AHashSet::with_capacity(value.len());
        for v in value {
            set.insert(Bytes::from(v));
        }
        Set::HashSet(set)
    }
}

impl Default for Set {
    fn default() -> Self {
        Self::HashSet(AHashSet::default())
    }
}
