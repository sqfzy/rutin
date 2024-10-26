use crate::{
    shared::db::Key,
    util::{StaticBytes, StaticStr},
};
use bytes::BytesMut;
use bytestring::ByteString;
pub use rutin_resp3::resp3::Resp3;

pub type CheapResp3 = Resp3<Key, ByteString>;
pub type ExpensiveResp3 = Resp3<BytesMut, String>;
/// Safty: 通过extend lifetime 实现zero copy的帧解析，在执行reader_buf.clear()之后即失效，不应当再被使用。
/// **因此所有期望其在clear之后仍然有效的操作都必须复制一份新的数据，并且在使用StaticBytes时不允许执行drop
/// 释放其中的数据**
pub type StaticResp3 = Resp3<StaticBytes, StaticStr>;
