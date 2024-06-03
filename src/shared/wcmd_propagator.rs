use bytes::Bytes;
use flume::{Receiver, Sender};
use std::sync::RwLock;
use try_lock::TryLock;

use crate::{
    cmd::{CmdUnparsed, Mutable},
    connection::AsyncStream,
    frame::RESP3,
    server::{Handler, ServerError},
};

#[derive(Debug, Default)]
pub struct WCmdPropergator {
    pub to_aof: Option<(Sender<Bytes>, Receiver<Bytes>)>,
    // TODO: 也许可以改用`bus`库
    to_replica: RwLock<Vec<(Sender<Bytes>, Receiver<Bytes>)>>,
}

impl WCmdPropergator {
    pub fn new(aof_enable: bool) -> Self {
        let (tx, rx) = flume::unbounded();
        Self {
            to_aof: if aof_enable { Some((tx, rx)) } else { None },
            to_replica: RwLock::new(Vec::new()),
        }
    }

    pub fn new_receiver(&self) -> Result<Receiver<Bytes>, ServerError> {
        let (tx, rx) = flume::unbounded();

        let mut writer = self
            .to_replica
            .write()
            .map_err(|e| ServerError::from(e.to_string()))?;
        writer.push((tx, rx.clone()));

        Ok(rx)
    }

    pub fn delete_receiver(&self, rx: &Receiver<Bytes>) -> Result<(), ServerError> {
        let mut writer = self
            .to_replica
            .write()
            .map_err(|e| ServerError::from(e.to_string()))?;

        writer.retain(|(_, r)| !r.same_channel(rx));

        Ok(())
    }

    #[inline]
    pub async fn propergate(
        &self,
        cmd: CmdUnparsed<Mutable>,
        handler: &mut Handler<impl AsyncStream>,
    ) {
        // let mut wcmd_buf = None;
        // let should_propergate = handler.conn.count() <= 1;
        //
        // // 是否需要传播，是否需要to_raw
        // if let Some(to_aof) = &self.to_aof {
        //     if wcmd_buf.is_none() {
        //         RESP3::decode_async(, src)
        //         wcmd.to_raw_in_buf(&mut handler.context.wcmd_buf);
        //         wcmd_buf = Some(handler.context.wcmd_buf.split().freeze());
        //     }
        //
        //     if !should_propergate {
        //         return;
        //     }
        //
        //     to_aof.0.send_async(wcmd_buf.unwrap()).await.unwrap();
        // }
        //
        // loop {
        //     if let Some(to_replica) = self.to_replica.try_lock() {
        //         if to_replica.is_empty() {
        //             break;
        //         }
        //
        //         if wcmd_buf.is_none() {
        //             wcmd.to_raw_in_buf(&mut handler.context.wcmd_buf);
        //             wcmd_buf = Some(handler.context.wcmd_buf.split().freeze());
        //         }
        //
        //         if !should_propergate {
        //             return;
        //         }
        //
        //         for (tx, _) in to_replica.iter() {
        //             tx.send(wcmd_buf.as_ref().unwrap().clone()).unwrap();
        //         }
        //         break;
        //     }
        // }
        //
        // TODO:
        todo!()
    }
}
