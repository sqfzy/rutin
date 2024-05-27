use tracing::Level;

pub fn test_init() {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .init();
}
