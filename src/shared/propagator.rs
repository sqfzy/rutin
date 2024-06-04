use bytes::BytesMut;
use kanal::{AsyncReceiver, AsyncSender};
use std::sync::atomic::{AtomicU8, Ordering};

use crate::{
    cmd::{CmdUnparsed, Mutable},
    connection::AsyncStream,
    frame::RESP3,
    server::{Handler, ServerError},
};

#[derive(Debug, Default)]
pub struct Propagator {
    pub to_aof: Option<(AsyncSender<BytesMut>, AsyncReceiver<BytesMut>)>,
    to_replicas: Box<[(AsyncSender<BytesMut>, AsyncReceiver<BytesMut>)]>,
    existing_replicas: AtomicU8,
}

impl Propagator {
    pub fn new(aof_enable: bool, max_replica: u8) -> Self {
        let (tx, rx) = kanal::unbounded_async();
        Self {
            to_aof: if aof_enable { Some((tx, rx)) } else { None },
            to_replicas: (0..max_replica).map(|_| kanal::unbounded_async()).collect(),
            existing_replicas: AtomicU8::new(0),
        }
    }

    pub fn new_receiver(&self) -> Result<AsyncReceiver<BytesMut>, ServerError> {
        let prev_len = self.existing_replicas.fetch_add(1, Ordering::Relaxed) as usize;

        if prev_len + 1 > self.to_replicas.len() {
            self.existing_replicas.fetch_sub(1, Ordering::Relaxed);
            return Err(ServerError::from("too many replica connections"));
        }

        Ok(self.to_replicas[prev_len].1.clone())
    }

    pub fn delete_receiver(&self) -> Result<usize, ServerError> {
        let curr_len = self.existing_replicas.fetch_sub(1, Ordering::Relaxed);

        Ok(curr_len as usize)
    }

    #[inline]
    pub async fn may_propagate(
        &self,
        cmd: CmdUnparsed<Mutable>,
        handler: &mut Handler<impl AsyncStream>,
    ) {
        let existing_replicas = self.existing_replicas.load(Ordering::Relaxed);

        if existing_replicas != 0 || self.to_aof.is_some() {
            RESP3::from(cmd).encode_buf(&mut handler.context.wcmd_buf);
        } else {
            // 不存在replica也没有开启aof则不进行propagate
            return;
        };

        // 如果有多个未处理的命令，则暂时不进行传播，等待处理完毕
        if handler.conn.unhandled_count() > 1 {
            return;
        }

        // 传播到aof
        if let Some((tx, _)) = &self.to_aof {
            tx.send(handler.context.wcmd_buf.split()).await.unwrap();
        }

        // 传播到replica
        for i in 0..existing_replicas {
            let (tx, _) = &self.to_replicas[i as usize];
            tx.send(handler.context.wcmd_buf.split()).await.unwrap();
        }
    }
}
