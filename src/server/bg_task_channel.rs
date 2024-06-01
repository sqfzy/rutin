use crate::frame::Frame;
use flume::{Receiver, Sender};

pub type BgTaskSender = Sender<Frame>;

#[derive(Debug, Clone)]
pub struct BgTaskChannel {
    tx: BgTaskSender,
    rx: Receiver<Frame>,
}

impl BgTaskChannel {
    pub fn new_sender(&self) -> BgTaskSender {
        self.tx.clone()
    }

    pub fn get_sender(&self) -> &BgTaskSender {
        &self.tx
    }

    pub async fn recv_from_bg_task(&self) -> Frame {
        self.rx.recv_async().await.unwrap()
    }
}

impl Default for BgTaskChannel {
    fn default() -> Self {
        let (tx, rx) = flume::bounded(1024);
        Self { tx, rx }
    }
}
