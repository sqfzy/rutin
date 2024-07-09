use crate::{frame::Resp3, Int};
use bytes::Bytes;
use snafu::Snafu;
use std::borrow::Cow;

pub type RutinResult<T> = Result<T, RutinError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum RutinError {
    #[snafu(display("{msg}"))]
    ServerErr {
        msg: Cow<'static, str>,
    },

    #[snafu(display("incomplete frame"))]
    Incomplete,

    #[snafu(display("invalid frame format: {}", msg))]
    InvalidFormat {
        msg: Cow<'static, str>,
    },

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
    OutOfMemory,

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
}

impl RutinError {
    #[inline]
    pub fn new_server_error(msg: impl Into<Cow<'static, str>>) -> Self {
        RutinError::ServerErr { msg: msg.into() }
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

impl TryInto<Resp3> for RutinError {
    type Error = RutinError;

    fn try_into(self) -> Result<Resp3, Self::Error> {
        let frame = match self {
            RutinError::ServerErr { .. } => return Err(self),
            // 命令执行失败，向客户端返回错误码
            RutinError::ErrCode { code } => Resp3::new_integer(code),
            // 命令执行失败，向客户端返回空值
            RutinError::Null => Resp3::Null,
            // 命令执行失败，向客户端返回错误信息
            e => Resp3::new_simple_error(e.to_string().into()),
        };

        Ok(frame)
    }
}
