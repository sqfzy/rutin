use bytes::Bytes;
use kanal::{AsyncReceiver, Sender};
use std::sync::atomic::{AtomicU8, Ordering};

use crate::{
    cmd::CmdUnparsed,
    error::{RutinError, RutinResult},
    frame::Resp3,
    server::WCMD_BUF,
};

#[derive(Debug, Default)]
pub struct Propagator {
    pub to_aof: Option<(Sender<Bytes>, AsyncReceiver<Bytes>)>,
    to_replicas: Box<[(Sender<Bytes>, AsyncReceiver<Bytes>)]>,
    existing_replicas: AtomicU8,
}

impl Propagator {
    pub fn new(aof_enable: bool, max_replica: u8) -> Self {
        let (tx, rx) = kanal::unbounded();
        Self {
            to_aof: if aof_enable {
                Some((tx, rx.to_async()))
            } else {
                None
            },
            to_replicas: (0..max_replica)
                .map(|_| {
                    let (tx, rx) = kanal::unbounded();
                    (tx, rx.to_async())
                })
                .collect(),
            existing_replicas: AtomicU8::new(0),
        }
    }

    pub fn new_receiver(&self) -> RutinResult<AsyncReceiver<Bytes>> {
        let prev_len = self.existing_replicas.fetch_add(1, Ordering::Relaxed) as usize;

        if prev_len + 1 > self.to_replicas.len() {
            self.existing_replicas.fetch_sub(1, Ordering::Relaxed);
            return Err(RutinError::from("ERR too many replica"));
        }

        Ok(self.to_replicas[prev_len].1.clone())
    }

    pub fn delete_receiver(&self) -> RutinResult<usize> {
        let curr_len = self.existing_replicas.fetch_sub(1, Ordering::Relaxed);

        Ok(curr_len as usize)
    }

    #[inline]
    pub fn may_propagate(&self, cmd: CmdUnparsed) {
        let existing_replicas = self.existing_replicas.load(Ordering::Relaxed);

        if existing_replicas == 0 && self.to_aof.is_none() {
            return;
        }

        let wcmd = WCMD_BUF.with_borrow_mut(|buf| {
            Resp3::from(cmd).encode_buf(buf);
            buf.split().freeze()
        });

        // 传播到aof
        if let Some((tx, _)) = &self.to_aof {
            tx.send(wcmd.clone()).unwrap();
        }

        // 传播到replica
        for i in 0..existing_replicas {
            let (tx, _) = &self.to_replicas[i as usize];
            tx.send(wcmd.clone()).unwrap();
        }
    }
}
