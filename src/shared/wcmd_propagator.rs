use crate::frame::Frame;
use bytes::Bytes;
use flume::{Receiver, Sender};
use std::sync::atomic::{AtomicBool, Ordering};

pub type WCmdReceiver = Receiver<Bytes>;

#[derive(Debug, Default)]
pub struct WCmdPropergator {
    inner: parking_lot::RwLock<Vec<(Sender<Bytes>, WCmdReceiver)>>,
    exists_receiver: AtomicBool,
}

impl WCmdPropergator {
    pub fn new_receiver(&self) -> WCmdReceiver {
        self.exists_receiver.store(true, Ordering::Relaxed);
        let (tx, rx) = flume::unbounded();
        self.inner.write().push((tx, rx.clone()));
        rx
    }

    #[inline]
    pub fn propergate(&self, wcmd: Frame<'static>) {
        if !self.exists_receiver.load(Ordering::Relaxed) {
            return;
        }

        let wcmd = wcmd.to_raw();
        for (tx, _) in self.inner.read().iter() {
            tx.send(wcmd.clone()).unwrap();
        }
    }
}
