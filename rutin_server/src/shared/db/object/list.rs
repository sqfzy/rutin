use super::Str;
use std::{collections::VecDeque, ops::Index};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum List {
    LinkedList(VecDeque<Str>),
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
    pub fn push_back(&mut self, elem: impl Into<Str>) {
        match self {
            List::LinkedList(list) => list.push_back(elem.into()),
            List::ZipList => unimplemented!(),
        }
    }

    #[inline]
    pub fn pop_back(&mut self) -> Option<Str> {
        match self {
            List::LinkedList(list) => list.pop_back(),
            List::ZipList => unimplemented!(),
        }
    }

    #[inline]
    pub fn push_front(&mut self, elem: impl Into<Str>) {
        match self {
            List::LinkedList(list) => list.push_front(elem.into()),
            List::ZipList => unimplemented!(),
        }
    }

    #[inline]
    pub fn pop_front(&mut self) -> Option<Str> {
        match self {
            List::LinkedList(list) => list.pop_front(),
            List::ZipList => unimplemented!(),
        }
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<&Str> {
        match self {
            List::LinkedList(list) => list.get(index),
            List::ZipList => unimplemented!(),
        }
    }

    #[inline]
    pub fn replace(&mut self, index: usize, elem: impl Into<Str>) -> Option<Str> {
        match self {
            List::LinkedList(list) => list
                .get_mut(index)
                .map(|old| std::mem::replace(old, elem.into())),
            List::ZipList => unimplemented!(),
        }
    }

    #[inline]
    pub fn insert(&mut self, index: usize, elem: impl Into<Str>) {
        match self {
            List::LinkedList(list) => list.insert(index, elem.into()),
            List::ZipList => unimplemented!(),
        }
    }

    #[inline]
    pub fn remove(&mut self, index: usize) -> Option<Str> {
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
    type Item = &'a Str;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            List::LinkedList(list) => list.iter().next(),
            List::ZipList => unimplemented!(),
        }
    }
}

impl Index<usize> for List {
    type Output = Str;

    fn index(&self, index: usize) -> &Self::Output {
        match self {
            List::LinkedList(list) => &list[index],
            List::ZipList => unimplemented!(),
        }
    }
}

impl Default for List {
    fn default() -> Self {
        List::LinkedList(VecDeque::new())
    }
}

impl<L: Into<VecDeque<Str>>> From<L> for List {
    fn from(list: L) -> Self {
        List::LinkedList(list.into())
    }
}
