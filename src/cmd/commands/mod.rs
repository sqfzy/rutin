mod acl;
mod hash;
mod key;
mod list;
mod other;
mod pub_sub;
mod replica;
mod script;
mod set;
mod str;

pub use acl::*;
pub use hash::*;
pub use key::*;
pub use list::*;
pub use other::*;
pub use pub_sub::*;
pub use script::*;
pub use str::*;

use crate::CmdFlag;

pub const ALL_CMD_FLAG: CmdFlag = CmdFlag::MAX;
pub const NO_CMD_FLAG: CmdFlag = CmdFlag::MIN | AUTH_FLAG; // 允许AUTH命令

pub(super) const ECHO_FLAG: CmdFlag = 1;
pub(super) const PING_FLAG: CmdFlag = 1 << 1;
pub(super) const CLIENT_TRACKING_FLAG: CmdFlag = 1 << 2;

pub(super) const DEL_FLAG: CmdFlag = 1 << 3;
pub(super) const EXISTS_FLAG: CmdFlag = 1 << 4;
pub(super) const EXPIRE_FLAG: CmdFlag = 1 << 5;
pub(super) const EXPIREAT_FLAG: CmdFlag = 1 << 6;
pub(super) const EXPIRETIME_FLAG: CmdFlag = 1 << 7;
pub(super) const KEYS_FLAG: CmdFlag = 1 << 8;
pub(super) const PERSIST_FLAG: CmdFlag = 1 << 9;
pub(super) const PTTL_FLAG: CmdFlag = 1 << 10;
pub(super) const TTL_FLAG: CmdFlag = 1 << 11;
pub(super) const TYPE_FLAG: CmdFlag = 1 << 12;

pub(super) const APPEND_FLAG: CmdFlag = 1 << 13;
pub(super) const DECR_FLAG: CmdFlag = 1 << 14;
pub(super) const DECRBY_FLAG: CmdFlag = 1 << 15;
pub(super) const GET_FLAG: CmdFlag = 1 << 16;
pub(super) const GETRANGE_FLAG: CmdFlag = 1 << 17;
pub(super) const GETSET_FLAG: CmdFlag = 1 << 18;
pub(super) const INCR_FLAG: CmdFlag = 1 << 19;
pub(super) const INCRBY_FLAG: CmdFlag = 1 << 20;
pub(super) const MGET_FLAG: CmdFlag = 1 << 21;
pub(super) const MSET_FLAG: CmdFlag = 1 << 22;
pub(super) const MSETNX_FLAG: CmdFlag = 1 << 23;
pub(super) const SET_FLAG: CmdFlag = 1 << 24;
pub(super) const SETEX_FLAG: CmdFlag = 1 << 25;
pub(super) const SETNX_FLAG: CmdFlag = 1 << 26;
pub(super) const STRLEN_FLAG: CmdFlag = 1 << 27;

pub(super) const LLEN_FLAG: CmdFlag = 1 << 28;
pub(super) const LPUSH_FLAG: CmdFlag = 1 << 29;
pub(super) const LPOP_FLAG: CmdFlag = 1 << 30;
pub(super) const BLPOP_FLAG: CmdFlag = 1 << 31;
pub(super) const NBLPOP_FLAG: CmdFlag = 1 << 32;
pub(super) const BLMOVE_FLAG: CmdFlag = 1 << 33;

pub(super) const HDEL_FLAG: CmdFlag = 1 << 34;
pub(super) const HEXISTS_FLAG: CmdFlag = 1 << 35;
pub(super) const HGET_FLAG: CmdFlag = 1 << 36;
pub(super) const HSET_FLAG: CmdFlag = 1 << 37;

pub(super) const PUBLISH_FLAG: CmdFlag = 1 << 38;
pub(super) const SUBSCRIBE_FLAG: CmdFlag = 1 << 39;
pub(super) const UNSUBSCRIBE_FLAG: CmdFlag = 1 << 40;

pub(super) const EVAL_FLAG: CmdFlag = 1 << 41;
pub(super) const EVALNAME_FLAG: CmdFlag = 1 << 42;
pub(super) const SCRIPT_EXISTS_FLAG: CmdFlag = 1 << 43;
pub(super) const SCRIPT_FLUSH_FLAG: CmdFlag = 1 << 44;
pub(super) const SCRIPT_REGISTER_FLAG: CmdFlag = 1 << 45;

pub(super) const DUMP_FLAG: CmdFlag = 1 << 46;
pub(super) const NBKEYS_FLAG: CmdFlag = 1 << 47;
pub(super) const LPOS_FLAG: CmdFlag = 1 << 48;
pub(super) const BGSAVE_FLAG: CmdFlag = 1 << 49;
pub(super) const AUTH_FLAG: CmdFlag = 1 << 50;
pub(super) const ACLCAT_FLAG: CmdFlag = 1 << 51;
pub(super) const ACLDELUSER_FLAG: CmdFlag = 1 << 52;
pub(super) const ACLSETUSER_FLAG: CmdFlag = 1 << 53;
pub(super) const ACLWHOAMI_FLAG: CmdFlag = 1 << 54;
pub(super) const ACLUSERS_FLAG: CmdFlag = 1 << 55;

pub(super) const PSYNC_FLAG: CmdFlag = 1 << 56;
pub(super) const REPLCONF_FLAG: CmdFlag = 1 << 57;
pub(super) const REPLICAOF_FLAG: CmdFlag = 1 << 58;
