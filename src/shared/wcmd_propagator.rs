use bytes::Bytes;
use flume::{Receiver, Sender};
use try_lock::TryLock;

#[derive(Debug, Default)]
pub struct WCmdPropergator {
    pub to_aof: Option<(Sender<Bytes>, Receiver<Bytes>)>,
    // TODO: 也许可以改用`bus`库
    to_replica: TryLock<Vec<(Sender<Bytes>, Receiver<Bytes>)>>,
}

impl WCmdPropergator {
    pub fn new(aof_enable: bool) -> Self {
        let (tx, rx) = flume::unbounded();
        Self {
            to_aof: if aof_enable { Some((tx, rx)) } else { None },
            to_replica: TryLock::new(Vec::new()),
        }
    }

    pub fn new_receiver(&self) -> Receiver<Bytes> {
        let (tx, rx) = flume::unbounded();
        loop {
            if let Some(mut to_replica) = self.to_replica.try_lock() {
                to_replica.push((tx, rx.clone()));
                break rx;
            }
        }
    }

    pub fn delete_receiver(&self, rx: &Receiver<Bytes>) {
        loop {
            if let Some(mut to_replica) = self.to_replica.try_lock() {
                to_replica.retain(|(_, r)| r.same_channel(rx));
                break;
            }
        }
    }

    #[inline]
    pub async fn propergate(&self, wcmd: Bytes) {
        loop {
            if let Some(to_replica) = self.to_replica.try_lock() {
                for (tx, _) in to_replica.iter() {
                    tx.send(wcmd.clone()).unwrap();
                }
                break;
            }
        }

        if let Some(to_aof) = &self.to_aof {
            // PERF: pipline的性能瓶颈, 多线程写单文件问题
            // rps=953600.0 (overall: 906282.9) avg_msec=10.405 (overall: 10.951)
            to_aof.0.send_async(wcmd).await.unwrap();
        }
    }
}
