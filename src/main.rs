use rutin::{
    conf,
    server::{REBOOT_SIGNAL, SHUTDOWN_SIGNAL},
};
use std::{str::FromStr, sync::Arc};

#[tokio::main]
async fn main() {
    #[cfg(feature = "debug_server")]
    {
        rutin::util::debug_server().await;
        return;
    }

    #[cfg(feature = "debug_client")]
    {
        rutin::util::debug_client().await;
        return;
    }

    let conf = Arc::new(conf::Conf::new().unwrap());

    if let Ok(level) = tracing::Level::from_str(conf.server.log_level.as_str()) {
        tracing_subscriber::fmt()
            .pretty()
            .with_max_level(level)
            .init();
    }

    loop {
        let listener =
            tokio::net::TcpListener::bind(format!("{}:{}", conf.server.addr, conf.server.port))
                .await
                .unwrap();

        match rutin::server::run(listener, conf.clone()).await {
            SHUTDOWN_SIGNAL => break,
            REBOOT_SIGNAL => continue,
            _ => unreachable!(),
        }
    }
}
