use crate::error::{RutinError, RutinResult};
use arc_swap::ArcSwap;
use bytes::Bytes;
use kanal::{AsyncReceiver, Sender};
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct Propagator {
    pub to_aof: Option<(Sender<Bytes>, AsyncReceiver<Bytes>)>,
    // to_replicas: Box<[(Sender<Bytes>, AsyncReceiver<Bytes>)]>,
    // existing_replicas: AtomicU8,
    to_replicas: ArcSwap<Vec<Sender<Bytes>>>,
    max_replica: usize,
}

impl Propagator {
    pub fn new(aof_enable: bool, max_replica: usize) -> Self {
        let (tx, rx) = kanal::unbounded();
        Self {
            to_aof: if aof_enable {
                Some((tx, rx.to_async()))
            } else {
                None
            },
            to_replicas: ArcSwap::new(Arc::new(Vec::new())),
            max_replica,
        }
    }

    pub fn should_propagate(&self) -> bool {
        self.to_aof.is_some() || self.to_replicas.load().len() > 0
    }

    pub fn new_receiver(&self) -> RutinResult<AsyncReceiver<Bytes>> {
        let to_replicas = self.to_replicas.load();

        if to_replicas.len() >= self.max_replica {
            return Err(RutinError::from("ERR too many replica"));
        }

        let (tx, rx) = kanal::unbounded();

        self.to_replicas.rcu(|to_replicas| {
            let mut to_replicas = Vec::clone(to_replicas.as_ref());
            to_replicas.push(tx.clone());
            to_replicas
        });

        Ok(rx.to_async())
    }

    pub fn delete_receiver(&self) -> RutinResult<usize> {
        let to_replicas = self.to_replicas.load();

        if to_replicas.len() == 0 {
            return Err(RutinError::from("ERR no replica"));
        }

        self.to_replicas.rcu(|to_replicas| {
            let mut to_replicas = Vec::clone(to_replicas.as_ref());
            to_replicas.pop();
            to_replicas
        });

        Ok(to_replicas.len())
    }

    #[inline]
    pub fn propagate(&self, wcmd: Bytes) {
        // 传播到aof
        if let Some((tx, _)) = &self.to_aof {
            tx.send(wcmd.clone()).unwrap();
        }

        // 传播到replica
        for to_replica in self.to_replicas.load().iter() {
            to_replica
                .send(wcmd.clone())
                .expect("must remove to_replica sender before remove replica");
        }
    }
}
