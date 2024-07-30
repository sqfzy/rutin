use std::{sync::Arc, time::Duration};

use backon::Retryable;
use bytes::Bytes;
use bytestring::ByteString;
use tokio::{io::AsyncWriteExt, net::TcpStream};
use tokio_util::codec::Encoder;

use super::*;
use crate::{
    cmd::{CmdExecutor, CmdType, CmdUnparsed},
    conf::AccessControl,
    connection::AsyncStream,
    error::{RutinError, RutinResult},
    frame::{Resp3, Resp3Encoder},
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
    should_set_to_master: bool,
    master_host: ByteString,
    master_port: u16,
}

impl CmdExecutor for Replicaof {
    const NAME: &'static str = "REPLICAOF";
    const FLAG: CmdFlag = REPLICAOF_FLAG;
    const TYPE: CmdType = CmdType::Other;

    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let replica_conf = &handler.shared.conf().replica;

        if self.should_set_to_master {
            set_server_to_master(&handler.shared);

            return Ok(Some(Resp3::new_simple_string("OK".into())));
        }

        // 如果当前节点已经是从节点，则关闭与主节点的连接，清除服务器状态和数据，
        if replica_conf.master_addr.load().is_some() {}

        /* step1: 设置主节点地址 */

        replica_conf
            .master_addr
            .store(Some(Arc::new((self.master_host.clone(), self.master_port))));

        // 向客户端返回 OK
        handler
            .conn
            .write_frame(&Resp3::<Bytes, ByteString>::new_simple_string("OK".into()))
            .await?;

        (|| async {
            /* step2: 连接主节点 */

            let to_master =
                TcpStream::connect((self.master_host.as_ref(), self.master_port)).await?;
            let mut to_master_handler = Handler::new(handler.shared.clone(), to_master);

            to_master_handler
                .conn
                .write_frame_force(&Resp3::<_, String>::new_blob_string(b"PING"))
                .await?;

            if !to_master_handler
                .conn
                .read_frame()
                .await?
                .is_some_and(|frame| {
                    frame
                        .try_simple_string()
                        .is_some_and(|frame| frame == "PONG")
                })
            {
                return Err(RutinError::new_server_error(format!(
                    "ERR master {}:{} is unreachable or busy",
                    self.master_host, self.master_port
                )));
            };

            /* step3: 身份验证(可选) */

            if let Some(password) = handler.shared.conf().replica.master_auth.as_ref() {
                to_master_handler
                    .conn
                    .write_frame_force(&Resp3::<_, String>::new_array(vec![
                        Resp3::new_blob_string(b"AUTH".to_vec()),
                        Resp3::new_blob_string(password.clone().into_bytes()),
                    ]))
                    .await?;

                if to_master_handler
                    .conn
                    .read_frame()
                    .await?
                    .is_some_and(|frame| frame.is_simple_error())
                {
                    return Err(RutinError::new_server_error(
                        "ERR master authentication failed",
                    ));
                }
            }

            /* step4: 发送端口信息 */

            to_master_handler
                .conn
                .write_frame_force(&Resp3::<_, String>::new_array(vec![
                    Resp3::new_blob_string(b"REPLCONF".to_vec()),
                    Resp3::new_blob_string(b"listening-port".to_vec()),
                    Resp3::new_blob_string(
                        handler.shared.conf().server.port.to_string().into_bytes(),
                    ),
                ]))
                .await?;

            /* step5: 发送PSYNC命令 */
            to_master_handler
                .conn
                .write_frame_force(&Resp3::<_, String>::new_array(vec![
                    Resp3::new_blob_string(b"PSYNC".as_ref()),
                    Resp3::new_blob_string(b"?".as_ref()),
                    Resp3::new_blob_string(b"0".as_ref()),
                ]))
                .await?;

            Ok(())
        })
        .retry(&backon::ExponentialBuilder::default())
        .await?;

        todo!()
    }

    fn parse(mut args: CmdUnparsed, _ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        };

        let arg1 = args.next().unwrap();
        let arg2 = args.next().unwrap();

        if arg1 == "NO" && arg2 == "ONE" {
            return Ok(Replicaof {
                should_set_to_master: true,
                master_host: Default::default(),
                master_port: 0,
            });
        }

        Ok(Replicaof {
            should_set_to_master: false,
            master_host: args
                .next()
                .unwrap()
                .try_into()
                .map_err(|_| RutinError::from("ERR value is not a valid hostname or ip address"))?,
            master_port: util::atoi(&args.next().unwrap())?,
        })
    }
}
