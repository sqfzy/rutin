#[derive(Debug)]
pub struct ServerError(String);

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "server error: {}", self.0)
    }
}

impl std::error::Error for ServerError {}

impl From<String> for ServerError {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for ServerError {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<tokio::io::Error> for ServerError {
    fn from(e: tokio::io::Error) -> Self {
        Self(e.to_string())
    }
}
