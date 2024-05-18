use rutin::conf;

#[tokio::main]
async fn main() {
    // rutin::util::client_test(
    //     rutin::frame::Frame::new_bulks_from_static(&[b"get", b"a", b"b"])
    //         .to_raw()
    //         .as_ref(),
    // )
    // .await;

    let conf = std::sync::Arc::new(conf::Conf::new().unwrap());

    rutin::init(conf.server.log_level.as_str());

    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", conf.server.port))
        .await
        .unwrap();

    rutin::run(listener, conf).await;
}
