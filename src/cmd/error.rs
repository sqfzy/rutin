pub type CmdResult<T> = Result<T, CmdError>;

use crate::{frame::Resp3, server::ServerError, shared::db::DbError, Int};
use bytestring::ByteString;
use snafu::{Location, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum CmdError {
    ServerErr {
        source: ServerError,
        #[snafu(implicit)]
        loc: Location,
    },
    ErrorCode {
        code: Int,
    },
    Null,
    #[snafu(transparent)]
    Err {
        source: Err,
    },
}

impl From<Int> for CmdError {
    fn from(value: Int) -> Self {
        CmdError::ErrorCode { code: value }
    }
}

impl From<&str> for CmdError {
    fn from(value: &str) -> Self {
        Err::Other {
            message: value.into(),
        }
        .into()
    }
}

impl From<String> for CmdError {
    fn from(value: String) -> Self {
        Err::Other {
            message: value.into(),
        }
        .into()
    }
}

impl From<anyhow::Error> for CmdError {
    fn from(value: anyhow::Error) -> Self {
        Err::Other {
            message: value.to_string().into(),
        }
        .into()
    }
}

impl From<DbError> for CmdError {
    fn from(e: DbError) -> Self {
        match e {
            DbError::KeyNotFound => CmdError::Null,
            DbError::TypeErr { expected, found } => Err::Other {
                message: format!("WRONGTYPE expected: {expected} found {found}").into(),
            }
            .into(),
            DbError::Overflow => Err::Other {
                message: "ERR value out of range".into(),
            }
            .into(),
        }
    }
}

impl TryInto<Resp3> for CmdError {
    type Error = ServerError;

    fn try_into(self) -> Result<Resp3, Self::Error> {
        let frame = match self {
            CmdError::ServerErr { source, loc } => {
                return Err(format!("{}: {}", loc, source).into())
            }
            // 命令执行失败，向客户端返回错误码
            CmdError::ErrorCode { code } => Resp3::new_integer(code),
            // 命令执行失败，向客户端返回空值
            CmdError::Null => Resp3::Null,
            // 命令执行失败，向客户端返回错误信息
            CmdError::Err { source } => Resp3::new_simple_error(source.to_string().into()),
        };

        Ok(frame)
    }
}

#[derive(Debug, Snafu)]
pub enum Err {
    #[snafu(display("ERR unknown command"))]
    UnknownCmd,
    #[snafu(display("ERR command is forbaiden"))]
    Forbaiden,
    #[snafu(display("ERR wrong number of arguments"))]
    WrongArgNum,
    #[snafu(display(
        "ERR value is not an integer or out of range or can't be represented as integer"
    ))]
    A2IParse,
    #[snafu(display("ERR syntax error"))]
    Syntax,
    #[snafu(display("NOPERM this user has insufficient permissions"))]
    NoPermission,
    #[snafu(display("{}", message))]
    Other { message: ByteString },
}
