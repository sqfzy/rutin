use std::str::FromStr;
use tracing::Level;

pub fn init(log_level: &str) {
    // console_subscriber::init();
    #[cfg(feature = "debug")]
    tracing_subscriber::fmt()
        .pretty()
        .with_max_level(tracing::Level::TRACE)
        .init();

    let rust_log = std::env::var("RUST_LOG");
    let log_level = if let Ok(l) = rust_log.as_ref() {
        l.as_str()
    } else {
        log_level
    };

    if let Ok(level) = Level::from_str(log_level) {
        tracing_subscriber::fmt()
            .pretty()
            .with_max_level(level)
            .init();
    }
}
