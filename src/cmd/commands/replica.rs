use std::sync::Arc;

use bytes::Bytes;
use bytestring::ByteString;

use super::*;
use crate::{
    cmd::{CmdExecutor, CmdType, CmdUnparsed},
    conf::AccessControl,
    connection::AsyncStream,
    error::{RutinError, RutinResult},
    frame::Resp3,
    server::Handler,
    util::{self, set_server_to_master},
    CmdFlag,
};

/// # Reply:
///
/// **Simple string reply**: OK.
#[derive(Debug)]
pub struct PSync {
    pub replication_id: Bytes,
    pub offset: u64,
}

impl CmdExecutor for PSync {
    const NAME: &'static str = "PSYNC";
    const FLAG: CmdFlag = PSYNC_FLAG;
    const TYPE: CmdType = CmdType::Other;

    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        todo!()
    }

    fn parse(mut args: CmdUnparsed, _ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        }

        Ok(PSync {
            replication_id: args.next().unwrap(),
            offset: util::atoi(&args.next().unwrap())?,
        })
    }
}

/// # Reply:
///
/// **Simple string reply**: OK.
#[derive(Debug)]
pub struct Replicaof {
    master_host: Bytes,
    master_port: Bytes,
}

impl CmdExecutor for Replicaof {
    const NAME: &'static str = "REPLICAOF";
    const FLAG: CmdFlag = REPLICAOF_FLAG;
    const TYPE: CmdType = CmdType::Other;

    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let replica_conf = &handler.shared.conf().replica;

        if self.master_port == "NO" && self.master_host == "ONE" {
            if replica_conf.master_addr.load().is_none() {
                return Ok(Some(Resp3::new_simple_string("OK".into())));
            }

            set_server_to_master(&handler.shared);
            // let conf = &handler.shared.conf();
            // let replica_conf = &conf.replica;
            // replica_conf.master_addr.store(None);
            // return Ok(Some(Resp3::new_simple_string("OK".into())));
        }

        // 如果当前节点已经是从节点，则返回错误

        // replica_conf
        //     .master_addr
        //     .store(Some(Arc::new((self.master_host.clone(), self.master_port))));

        // 向客户端返回 OK
        handler
            .conn
            .write_frame(&Resp3::<Bytes, ByteString>::new_simple_string("OK".into()))
            .await?;

        todo!()
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        };

        Ok(Replicaof {
            // master_host: args
            //     .next()
            //     .unwrap()
            //     .try_into()
            //     .map_err(|_| RutinError::from("ERR value is not a valid hostname or ip address"))?,
            // master_port: util::atoi(&args.next().unwrap())?,
            master_host: args.next().unwrap(),
            master_port: args.next().unwrap(),
        })
    }
}
