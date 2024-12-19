use std::borrow::Cow;

#[derive(Debug)]
pub enum Resp3Error {
    Io(std::io::Error),
    Incomplete,
    InvalidFormat { msg: Cow<'static, str> },
}

impl std::fmt::Display for Resp3Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Resp3Error::Io(e) => write!(f, "io error: {}", e),
            Resp3Error::Incomplete => write!(f, "incomplete resp3"),
            Resp3Error::InvalidFormat { msg } => write!(f, "invalid resp3 format '{}'", msg),
        }
    }
}

impl std::error::Error for Resp3Error {}

impl From<std::io::Error> for Resp3Error {
    fn from(e: std::io::Error) -> Self {
        Resp3Error::Io(e)
    }
}
