use crate::{frame::CheapResp3, Int};
use bytes::Bytes;
use rutin_resp3::error::Resp3Error;
use snafu::Snafu;
use std::borrow::Cow;
use tracing::error;

pub type RutinResult<T> = Result<T, RutinError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum RutinError {
    #[snafu(display("{msg}"))]
    ServerErr {
        msg: Cow<'static, str>,
    },

    #[snafu(display("connection reset"))]
    ConnectionReset,

    ErrCode {
        code: Int,
    },

    Null,

    #[snafu(display("ERR unknown command"))]
    UnknownCmd,

    #[snafu(display("ERR command is forbaiden"))]
    Forbaiden,

    #[snafu(display("ERR wrong number of arguments"))]
    WrongArgNum,

    #[snafu(display("OOM command not allowed when used memory > 'maxmemory'"))]
    OutOfMemory {
        used_mem: u64,
        maxmemory: u64,
    },

    #[snafu(display("WRONGTYPE expected: {expected} found {found}"))]
    TypeErr {
        expected: &'static str,
        found: &'static str,
    },

    #[snafu(display("ERR value out of range"))]
    Overflow,

    #[snafu(display("ERR value {invalid:?} is not an integer or out of range"))]
    A2IParse {
        invalid: Bytes,
    },

    #[snafu(display("ERR value {invalid:?} is not an floating number or out of range"))]
    A2FParse {
        invalid: Bytes,
    },

    #[snafu(display("ERR syntax error"))]
    Syntax,

    #[snafu(display("NOPERM this user has insufficient permissions"))]
    NoPermission,

    #[snafu(display("ERR unknow command category"))]
    UnknownCmdCategory {
        category: Bytes,
    },

    #[snafu(display("ERR invalid pattern: {source}"))]
    InvalidPattern {
        source: regex::Error,
    },

    #[snafu(transparent)]
    FromUtf8Error {
        source: std::string::FromUtf8Error,
    },

    #[snafu(transparent)]
    Utf8Error {
        source: std::str::Utf8Error,
    },

    #[snafu(display("{msg}"))]
    Other {
        msg: Cow<'static, str>,
    },

    Whatever,
}

impl RutinError {
    #[inline]
    pub fn new_server_error(msg: impl Into<Cow<'static, str>>) -> Self {
        RutinError::ServerErr { msg: msg.into() }
    }

    #[inline]
    pub fn new_oom(used_mem: u64, maxmemory: u64) -> Self {
        let e = RutinError::OutOfMemory {
            used_mem,
            maxmemory,
        };
        error!("{e}. used memory: {used_mem}, maxmemory: {maxmemory}");
        e
    }
}

impl From<Int> for RutinError {
    fn from(value: Int) -> Self {
        RutinError::ErrCode { code: value }
    }
}

impl From<regex::Error> for RutinError {
    fn from(source: regex::Error) -> Self {
        RutinError::InvalidPattern { source }
    }
}

impl From<tokio::io::Error> for RutinError {
    fn from(source: tokio::io::Error) -> Self {
        RutinError::ServerErr {
            msg: source.to_string().into(),
        }
    }
}

impl From<&'static str> for RutinError {
    fn from(value: &'static str) -> Self {
        RutinError::Other { msg: value.into() }
    }
}

impl From<String> for RutinError {
    fn from(value: String) -> Self {
        RutinError::Other { msg: value.into() }
    }
}

impl From<Resp3Error> for RutinError {
    fn from(value: Resp3Error) -> Self {
        Self::ServerErr {
            msg: value.to_string().into(),
        }
    }
}

impl TryFrom<RutinError> for CheapResp3 {
    type Error = RutinError;

    fn try_from(value: RutinError) -> Result<CheapResp3, Self::Error> {
        let frame = match value {
            RutinError::ServerErr { .. } => return Err(value),
            // 命令执行失败，向客户端返回错误码
            RutinError::ErrCode { code } => CheapResp3::new_integer(code),
            // 命令执行失败，向客户端返回空值
            RutinError::Null => CheapResp3::Null,
            // 命令执行失败，向客户端返回错误信息
            e => CheapResp3::new_simple_error(e.to_string()),
        };

        Ok(frame)
    }
}
