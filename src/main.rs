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

    let conf = conf::Conf::new().unwrap();

    rutin::init(conf.server.log_level.as_str());

    let listener =
        tokio::net::TcpListener::bind(format!("{}:{}", conf.server.addr, conf.server.port))
            .await
            .unwrap();

    rutin::run(listener, conf).await;
}
