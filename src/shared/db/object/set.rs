use super::Str;
use ahash::AHashSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Set {
    HashSet(Box<AHashSet<Str>>),
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

    pub fn insert(&mut self, elem: impl Into<Str>) -> bool {
        match self {
            Set::HashSet(set) => set.insert(elem.into()),
            Set::IntSet => unimplemented!(),
        }
    }

    pub fn remove<'a>(&mut self, elem: impl Into<&'a Str>) -> bool {
        match self {
            Set::HashSet(set) => set.remove(elem.into()),
            Set::IntSet => unimplemented!(),
        }
    }

    pub fn contains(&self, elem: &Str) -> bool {
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

impl<S: Into<AHashSet<Str>>> From<S> for Set {
    fn from(set: S) -> Self {
        Set::HashSet(Box::new(set.into()))
    }
}
