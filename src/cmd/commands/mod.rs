mod hash;
mod key;
mod list;
mod other;
mod pub_sub;
mod script;
mod set;
mod str;

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

pub const ECHO_FLAG: CmdFlag = 1;
pub const PING_FLAG: CmdFlag = 1 << 1;
pub const CLIENT_TRACKING_FLAG: CmdFlag = 1 << 2;

pub const DEL_FLAG: CmdFlag = 1 << 3;
pub const EXISTS_FLAG: CmdFlag = 1 << 4;
pub const EXPIRE_FLAG: CmdFlag = 1 << 5;
pub const EXPIREAT_FLAG: CmdFlag = 1 << 6;
pub const EXPIRETIME_FLAG: CmdFlag = 1 << 7;
pub const KEYS_FLAG: CmdFlag = 1 << 8;
pub const PERSIST_FLAG: CmdFlag = 1 << 9;
pub const PTTL_FLAG: CmdFlag = 1 << 10;
pub const TTL_FLAG: CmdFlag = 1 << 11;
pub const TYPE_FLAG: CmdFlag = 1 << 12;

pub const APPEND_FLAG: CmdFlag = 1 << 13;
pub const DECR_FLAG: CmdFlag = 1 << 14;
pub const DECRBY_FLAG: CmdFlag = 1 << 15;
pub const GET_FLAG: CmdFlag = 1 << 16;
pub const GETRANGE_FLAG: CmdFlag = 1 << 17;
pub const GETSET_FLAG: CmdFlag = 1 << 18;
pub const INCR_FLAG: CmdFlag = 1 << 19;
pub const INCRBY_FLAG: CmdFlag = 1 << 20;
pub const MGET_FLAG: CmdFlag = 1 << 21;
pub const MSET_FLAG: CmdFlag = 1 << 22;
pub const MSETNX_FLAG: CmdFlag = 1 << 23;
pub const SET_FLAG: CmdFlag = 1 << 24;
pub const SETEX_FLAG: CmdFlag = 1 << 25;
pub const SETNX_FLAG: CmdFlag = 1 << 26;
pub const STRLEN_FLAG: CmdFlag = 1 << 27;

pub const LLEN_FLAG: CmdFlag = 1 << 28;
pub const LPUSH_FLAG: CmdFlag = 1 << 29;
pub const LPOP_FLAG: CmdFlag = 1 << 30;
pub const BLPOP_FLAG: CmdFlag = 1 << 31;
pub const NBLPOP_FLAG: CmdFlag = 1 << 32;
pub const BLMOVE_FLAG: CmdFlag = 1 << 33;

pub const HDEL_FLAG: CmdFlag = 1 << 34;
pub const HEXISTS_FLAG: CmdFlag = 1 << 35;
pub const HGET_FLAG: CmdFlag = 1 << 36;
pub const HSET_FLAG: CmdFlag = 1 << 37;

pub const PUBLISH_FLAG: CmdFlag = 1 << 38;
pub const SUBSCRIBE_FLAG: CmdFlag = 1 << 39;
pub const UNSUBSCRIBE_FLAG: CmdFlag = 1 << 40;

pub const EVAL_FLAG: CmdFlag = 1 << 41;
pub const EVALNAME_FLAG: CmdFlag = 1 << 42;
pub const SCRIPT_EXISTS_FLAG: CmdFlag = 1 << 43;
pub const SCRIPT_FLUSH_FLAG: CmdFlag = 1 << 44;
pub const SCRIPT_REGISTER_FLAG: CmdFlag = 1 << 45;

pub const DUMP_FLAG: CmdFlag = 1 << 46;
pub const NBKEYS_FLAG: CmdFlag = 1 << 47;
pub const LPOS_FLAG: CmdFlag = 1 << 48;
pub const BGSAVE_FLAG: CmdFlag = 1 << 49;
pub const AUTH_FLAG: CmdFlag = 1 << 50;
