#[derive(Debug)]
pub enum DbError {
    KeyNotFound,
    TypeErr {
        expected: &'static str,
        found: &'static str,
    },
    Overflow,
}

impl std::error::Error for DbError {}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DbError::KeyNotFound => write!(f, "key not found"),
            DbError::TypeErr { expected, found } => {
                write!(f, "type error expected: {expected} found {found}")
            }
            DbError::Overflow => write!(f, "value out of range"),
        }
    }
}
