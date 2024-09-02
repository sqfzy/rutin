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

    rutin::server::preface();

    rutin::server::run().await;
}
