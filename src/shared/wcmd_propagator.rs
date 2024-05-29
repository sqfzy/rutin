use bytes::Bytes;
use flume::{Receiver, Sender};
use try_lock::TryLock;

use crate::{connection::AsyncStream, frame::Frame, server::Handler};

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
    pub async fn propergate(&self, wcmd: Frame, handler: &mut Handler<impl AsyncStream>) {
        let mut wcmd_buf = None;
        let should_propergate = handler.conn.count() <= 1;

        loop {
            if let Some(to_replica) = self.to_replica.try_lock() {
                if to_replica.is_empty() {
                    break;
                }

                if wcmd_buf.is_none() {
                    wcmd.to_raw_in_buf(&mut handler.context.wcmd_buf);
                    wcmd_buf = Some(handler.context.wcmd_buf.split().freeze());
                }

                if !should_propergate {
                    return;
                }

                for (tx, _) in to_replica.iter() {
                    tx.send(wcmd_buf.as_ref().unwrap().clone()).unwrap();
                }
                break;
            }
        }

        // 是否需要传播，是否需要to_raw
        if let Some(to_aof) = &self.to_aof {
            if wcmd_buf.is_none() {
                wcmd.to_raw_in_buf(&mut handler.context.wcmd_buf);
                wcmd_buf = Some(handler.context.wcmd_buf.split().freeze());
            }

            if !should_propergate {
                return;
            }

            to_aof.0.send_async(wcmd_buf.unwrap()).await.unwrap();
        }
    }
}
