use bytes::{Bytes, BytesMut};

use crate::util::StaticBytes;
use std::ops::Deref;

pub trait IntoUppercase: Sized {
    type Mut: AsRef<[u8]> = &'static [u8];

    fn into_uppercase<const L: usize>(self) -> Uppercase<L, Self::Mut>;
}

// impl<B: AsMut<[u8]>> IntoUppercase for B {
//     type Mut = B;
//
//     fn into_uppercase<const L: usize>(self) -> Uppercase<L, Self::Mut> {
//         Uppercase::from_mut(self)
//     }
// }

impl IntoUppercase for Bytes {
    fn into_uppercase<const L: usize>(self) -> Uppercase<L, Self::Mut> {
        Uppercase::from_const(self.as_ref())
    }
}

impl IntoUppercase for StaticBytes {
    type Mut = &'static mut [u8];

    fn into_uppercase<const L: usize>(self) -> Uppercase<L, &'static mut [u8]> {
        match self {
            StaticBytes::Const(s) => Uppercase::from_const(s),
            StaticBytes::Mut(s) => Uppercase::from_mut(s),
        }
    }
}

impl IntoUppercase for BytesMut {
    type Mut = BytesMut;

    fn into_uppercase<const L: usize>(self) -> Uppercase<L, BytesMut> {
        Uppercase::from_mut(self)
    }
}

// 结构体中存储的数据必须是大写的
#[derive(Debug)]
pub enum Uppercase<const L: usize, M> {
    Const { data: [u8; L], len: usize },
    Mut(M),
}

impl<const L: usize, M> Uppercase<L, M> {
    pub fn from_const(data: &[u8]) -> Self {
        let len = data.len();

        assert!(len <= L, "buf too small");

        let mut buf = [0; L];
        buf[..len].copy_from_slice(data);
        buf.make_ascii_uppercase();

        Self::Const { data: buf, len }
    }
}

impl<const L: usize, M: AsMut<[u8]>> Uppercase<L, M> {
    pub fn from_mut(mut s: M) -> Self {
        s.as_mut().make_ascii_uppercase();
        Self::Mut(s)
    }
}

impl<const L: usize, M: AsRef<[u8]>> Deref for Uppercase<L, M> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<const L: usize, M: AsRef<[u8]>> AsRef<[u8]> for Uppercase<L, M> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Const { data, len } => &data[..*len],
            Self::Mut(s) => s.as_ref(),
        }
    }
}

impl<const L: usize, M: AsRef<[u8]>> PartialEq<[u8]> for Uppercase<L, M> {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_ref() == other
    }
}
//
// impl<const L: usize> From<&[u8]> for Uppercase<L, &mut [u8]> {
//     fn from(s: &[u8]) -> Self {
//         Uppercase::from_const(s)
//     }
// }
//
// impl<const L: usize> From<StaticBytes> for Uppercase<L, &'static mut [u8]> {
//     fn from(m: StaticBytes) -> Self {
//         match m {
//             StaticBytes::Const(s) => Uppercase::from_const(s),
//             StaticBytes::Mut(s) => Uppercase::from_mut(s),
//         }
//     }
// }
//
// impl<const L: usize, M: AsMut<[u8]>> From<M> for Uppercase<L, M> {
//     fn from(m: M) -> Self {
//         Uppercase::from_mut(m)
//     }
// }
