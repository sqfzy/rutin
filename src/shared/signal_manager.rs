use crate::{server::RESERVE_MAX_ID, Id};
use async_shutdown::ShutdownManager;

#[allow(async_fn_in_trait)]
pub trait SignalManager {
    async fn wait_special_signal(&self) -> Id;

    async fn wait_all_signal(&self) -> Id;
}

impl SignalManager for ShutdownManager<Id> {
    async fn wait_special_signal(&self) -> Id {
        loop {
            let signal = self.wait_shutdown_triggered().await;
            if signal <= RESERVE_MAX_ID {
                return signal;
            }
        }
    }

    async fn wait_all_signal(&self) -> Id {
        self.wait_shutdown_triggered().await
    }
}
