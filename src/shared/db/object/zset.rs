use bytes::Bytes;
use skiplist::OrderedSkipList;

#[derive(Debug, PartialEq)]
pub enum ZSet {
    SkipList(Box<OrderedSkipList<ZSetElem>>),
    ZipSet,
}

impl ZSet {
    pub fn len(&self) -> usize {
        match self {
            ZSet::SkipList(sl) => sl.len(),
            ZSet::ZipSet => unimplemented!(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            ZSet::SkipList(sl) => sl.is_empty(),
            ZSet::ZipSet => unimplemented!(),
        }
    }

    pub fn insert(&mut self, elem: ZSetElem) {
        match self {
            ZSet::SkipList(sl) => sl.insert(elem),
            ZSet::ZipSet => unimplemented!(),
        }
    }

    pub fn remove(&mut self, elem: &ZSetElem) -> Option<ZSetElem> {
        match self {
            ZSet::SkipList(sl) => sl.remove(elem),
            ZSet::ZipSet => unimplemented!(),
        }
    }
}

impl Clone for ZSet {
    fn clone(&self) -> Self {
        match self {
            ZSet::SkipList(sl) => {
                let mut new_sl = OrderedSkipList::with_capacity(sl.len());
                new_sl.extend(sl.iter().cloned());
                ZSet::SkipList(Box::new(new_sl))
            }
            ZSet::ZipSet => unimplemented!(),
        }
    }
}

impl Default for ZSet {
    fn default() -> Self {
        ZSet::SkipList(Box::default())
    }
}

impl From<OrderedSkipList<ZSetElem>> for ZSet {
    fn from(sl: OrderedSkipList<ZSetElem>) -> Self {
        ZSet::SkipList(Box::new(sl))
    }
}

impl From<Vec<(f64, Bytes)>> for ZSet {
    fn from(vec: Vec<(f64, Bytes)>) -> Self {
        let mut sl = OrderedSkipList::with_capacity(vec.len());
        for (score, member) in vec {
            sl.insert(ZSetElem(score, member));
        }
        ZSet::SkipList(Box::new(sl))
    }
}

impl<B: Into<Bytes>, const N: usize> From<[(f64, B); N]> for ZSet {
    fn from(value: [(f64, B); N]) -> Self {
        let mut sl = OrderedSkipList::with_capacity(N);
        for (score, member) in value {
            sl.insert(ZSetElem(score, member.into()));
        }
        ZSet::SkipList(Box::new(sl))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ZSetElem(pub f64, pub Bytes); // (score, member)

impl ZSetElem {
    pub fn new(score: f64, member: Bytes) -> Self {
        Self(score, member)
    }

    pub fn score(&self) -> f64 {
        self.0
    }

    pub fn member(&self) -> &Bytes {
        &self.1
    }
}

impl PartialOrd for ZSetElem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl<B: Into<Bytes>> From<(f64, B)> for ZSetElem {
    fn from((score, member): (f64, B)) -> Self {
        Self(score, member.into())
    }
}
