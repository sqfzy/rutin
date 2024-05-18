pub type CmdResult<T> = Result<T, CmdError>;

use crate::{shared::db::DbError, frame::FrameError, Int};
use snafu::{Location, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum CmdError {
    ServerErr {
        source: tokio::io::Error,
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

impl From<FrameError> for CmdError {
    fn from(_: FrameError) -> Self {
        Err::Syntax.into()
    }
}

impl From<Int> for CmdError {
    fn from(value: Int) -> Self {
        CmdError::ErrorCode { code: value }
    }
}

impl From<&str> for CmdError {
    fn from(value: &str) -> Self {
        Err::Other {
            message: value.to_string(),
        }
        .into()
    }
}

impl From<anyhow::Error> for CmdError {
    fn from(value: anyhow::Error) -> Self {
        Err::Other {
            message: value.to_string(),
        }
        .into()
    }
}

impl From<DbError> for CmdError {
    fn from(e: DbError) -> Self {
        match e {
            DbError::KeyNotFound => CmdError::Null,
            DbError::TypeErr { expected, found } => Err::Other {
                message: format!("WRONGTYPE expected: {expected} found {found}"),
            }
            .into(),
            DbError::Overflow => Err::Other {
                message: "ERR value out of range".to_string(),
            }
            .into(),
        }
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
    #[snafu(display("syntax error"))]
    Syntax,
    #[snafu(whatever, display("{}", message))]
    Other { message: String },
}
