#[tokio::main]
async fn main() {
    use rutin_server::server::*;

    #[cfg(feature = "debug_server")]
    {
        rutin_server::util::debug_server().await;
        return;
    }

    #[cfg(feature = "debug_client")]
    {
        rutin_server::util::debug_client().await;
        return;
    }

    preface();

    let shared = rutin_server::shared::Shared::new();

    init_server(shared).await.unwrap();

    waitting_shutdown(shared).await;
}
