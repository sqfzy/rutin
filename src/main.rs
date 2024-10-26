#[tokio::main]
async fn main() {
    use rutin::server::*;

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

    preface();

    let shared = *SHARED;

    init_server(shared).await.unwrap();

    waitting_shutdown(shared).await;
}
