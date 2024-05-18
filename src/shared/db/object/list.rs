use bytes::Bytes;
use std::{collections::VecDeque, ops::Index};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum List {
    LinkedList(VecDeque<Bytes>),
    ZipList,
}

impl List {
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            List::LinkedList(list) => list.len(),
            List::ZipList => unimplemented!(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        match self {
            List::LinkedList(list) => list.is_empty(),
            List::ZipList => unimplemented!(),
        }
    }

    #[inline]
    pub fn push_back(&mut self, elem: Bytes) {
        match self {
            List::LinkedList(list) => list.push_back(elem),
            List::ZipList => unimplemented!(),
        }
    }

    #[inline]
    pub fn pop_back(&mut self) -> Option<Bytes> {
        match self {
            List::LinkedList(list) => list.pop_back(),
            List::ZipList => unimplemented!(),
        }
    }

    #[inline]
    pub fn push_front(&mut self, elem: Bytes) {
        match self {
            List::LinkedList(list) => list.push_front(elem),
            List::ZipList => unimplemented!(),
        }
    }

    #[inline]
    pub fn pop_front(&mut self) -> Option<Bytes> {
        match self {
            List::LinkedList(list) => list.pop_front(),
            List::ZipList => unimplemented!(),
        }
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<Bytes> {
        match self {
            List::LinkedList(list) => list.get(index).cloned(),
            List::ZipList => unimplemented!(),
        }
    }

    #[inline]
    pub fn replace(&mut self, index: usize, elem: Bytes) -> Option<Bytes> {
        match self {
            List::LinkedList(list) => list.get_mut(index).map(|old| std::mem::replace(old, elem)),
            List::ZipList => unimplemented!(),
        }
    }

    #[inline]
    pub fn insert(&mut self, index: usize, elem: Bytes) {
        match self {
            List::LinkedList(list) => list.insert(index, elem),
            List::ZipList => unimplemented!(),
        }
    }

    #[inline]
    pub fn remove(&mut self, index: usize) -> Option<Bytes> {
        match self {
            List::LinkedList(list) => list.remove(index),
            List::ZipList => unimplemented!(),
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        match self {
            List::LinkedList(list) => list.clear(),
            List::ZipList => unimplemented!(),
        }
    }
}

impl<'a> Iterator for &'a List {
    type Item = &'a Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            List::LinkedList(list) => list.iter().next(),
            List::ZipList => unimplemented!(),
        }
    }
}

impl Index<usize> for List {
    type Output = Bytes;

    fn index(&self, index: usize) -> &Self::Output {
        match self {
            List::LinkedList(list) => &list[index],
            List::ZipList => unimplemented!(),
        }
    }
}

impl From<Vec<Bytes>> for List {
    fn from(mut list: Vec<Bytes>) -> Self {
        list.reverse();
        List::LinkedList(list.into())
    }
}

impl From<Vec<&'static str>> for List {
    fn from(mut list: Vec<&'static str>) -> Self {
        list.reverse();
        List::LinkedList(list.into_iter().map(|s| s.into()).collect())
    }
}

impl Default for List {
    fn default() -> Self {
        List::LinkedList(VecDeque::new())
    }
}
