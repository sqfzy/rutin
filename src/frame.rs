use std::hash::Hash;

use crate::{
    cmd::CmdArg,
    shared::db::{Key, Str},
    util::{IntoUppercase, StaticBytes, StaticStr, Uppercase},
};
use bytes::{Bytes, BytesMut};
use bytestring::ByteString;
use equivalent::Equivalent;
use mlua::{BorrowedBytes, BorrowedStr};
pub use rutin_resp3::resp3::Resp3;

pub type CheapResp3 = Resp3<Key, ByteString>;
pub type ExpensiveResp3 = Resp3<BytesMut, String>;
/// Safty: 通过extend lifetime 实现zero copy的帧解析，在执行reader_buf.clear()之后即失效，不应当再被使用。
/// **因此所有期望其在clear之后仍然有效的操作都必须复制一份新的数据，并且在使用StaticBytes时不允许执行drop
/// 释放其中的数据**
pub type StaticResp3 = Resp3<StaticBytes, StaticStr>;

#[derive(Debug)]
pub struct BorrowedBytesWrapper<'a>(BorrowedBytes<'a>);

impl<'a> From<BorrowedBytes<'a>> for BorrowedBytesWrapper<'a> {
    fn from(value: BorrowedBytes<'a>) -> Self {
        Self(value)
    }
}

impl IntoUppercase for BorrowedBytesWrapper<'_> {
    fn into_uppercase<const L: usize>(self) -> Uppercase<L, Self::Mut> {
        Uppercase::from_const(self.0.as_ref())
    }

    fn to_uppercase<const L: usize>(&mut self) -> Uppercase<L, &mut [u8]> {
        Uppercase::from_const(self.0.as_ref())
    }
}

impl Equivalent<Key> for BorrowedBytesWrapper<'_> {
    fn equivalent(&self, other: &Key) -> bool {
        self.0.as_ref() == other.as_ref()
    }
}

impl AsRef<[u8]> for BorrowedBytesWrapper<'_> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Into<Str> for BorrowedBytesWrapper<'_> {
    fn into(self) -> Str {
        Str::from(self.0.as_ref())
    }
}

impl Into<Key> for BorrowedBytesWrapper<'_> {
    fn into(self) -> Key {
        Key::from(self.0.as_ref())
    }
}

impl Hash for BorrowedBytesWrapper<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.as_ref().hash(state)
    }
}

// impl<'a, 'b> From<&'a BorrowedBytesWrapper<'b>> for Key {
//     fn from(value: &'a BorrowedBytesWrapper<'b>) -> Self {
//         Key::from(value.0.as_ref())
//     }
// }

fn test_cmd_arg_trait(bar: BorrowedBytesWrapper<'_>) {
    fn foo(cmd_arg: impl crate::cmd::CmdArg) {}

    foo(bar);
}
