use crate::frame::Frame;
use flume::{Receiver, Sender};
use try_lock::TryLock;

#[derive(Debug, Default)]
pub struct WCmdPropergator {
    pub to_aof: Option<(Sender<Frame<'static>>, Receiver<Frame<'static>>)>,
    // PERF: 也许可以改用`bus`库
    to_replica: TryLock<Vec<(Sender<Frame<'static>>, Receiver<Frame<'static>>)>>,
}

impl WCmdPropergator {
    pub fn new(aof_enable: bool) -> Self {
        let (tx, rx) = flume::unbounded();
        Self {
            to_aof: if aof_enable { Some((tx, rx)) } else { None },
            to_replica: TryLock::new(Vec::new()),
        }
    }

    pub fn new_receiver(&self) -> Receiver<Frame<'static>> {
        let (tx, rx) = flume::unbounded();
        loop {
            if let Some(mut to_replica) = self.to_replica.try_lock() {
                to_replica.push((tx, rx.clone()));
                break rx;
            }
        }
    }

    pub fn delete_receiver(&self, rx: &Receiver<Frame<'static>>) {
        loop {
            if let Some(mut to_replica) = self.to_replica.try_lock() {
                to_replica.retain(|(_, r)| r.same_channel(rx));
                break;
            }
        }
    }

    #[inline]
    pub fn propergate(&self, wcmd: Frame<'static>) {
        loop {
            if let Some(to_replica) = self.to_replica.try_lock() {
                for (tx, _) in to_replica.iter() {
                    tx.send(wcmd.clone()).unwrap();
                }
                break;
            }
        }

        if let Some(to_aof) = &self.to_aof {
            to_aof.0.send(wcmd).unwrap();
        }
    }
}
