use rutin::conf;

#[tokio::main]
async fn main() {
    #[cfg(feature = "fake_server")]
    {
        rutin::util::fake_server().await;
        return;
    }

    #[cfg(feature = "fake_client")]
    {
        rutin::util::fake_client().await;
        return;
    }

    let conf = std::sync::Arc::new(conf::Conf::new().unwrap());

    rutin::init(conf.server.log_level.as_str());

    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", conf.server.port))
        .await
        .unwrap();

    rutin::run(listener, conf).await;
}
