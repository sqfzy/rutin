use crate::{
    cmd::commands::{PING_CMD_FLAG, REPLCONF_CMD_FLAG, WRITE_CMDS_FLAG},
    conf::{AccessControl, MasterInfo, DEFAULT_USER},
    error::{RutinError, RutinResult},
    frame::Resp3,
    server::{Handler, HandlerContext},
    shared::{Shared, SET_REPLICA_ID},
};
use bytestring::ByteString;
use std::{future::Future, sync::Arc, time::Duration};
use tokio::{net::TcpStream, sync::Mutex};
use tokio_util::time::FutureExt as _;
use tracing::error;

/// ## set_server_to_replica意味着：
///
/// 1. 服务会清空所有旧数据
/// 2. 不存在SET_MASTER任务
/// 3. post_office中不存在SET_MASTER_ID mailbox以及set_master_outbox
/// 4. 服务需要定期向主节点发送REPLCONF ACK <offset>命令，以便主节点知道从节点的
///    复制进度
/// 5. 服务会定期收到主节点的PING命令，以便主节点知道从节点是否存活
/// 6. 服务会接收主节点传播的写命令，并执行，但不会返回结果
/// 7. 服务只能执行读命令(如果配置了read_only))
//
// WARN: :
// Recursive async functions don't internally implement auto traits
// 因此需要指明返回impl Future<Output = RutinResult<()>> + Send
// issue: https://github.com/rust-lang/rust/issues/123072
#[allow(clippy::manual_async_fn)]
pub fn set_server_to_replica(
    shared: Shared,
    master_host: ByteString,
    master_port: u16,
) -> impl Future<Output = RutinResult<()>> {
    async move {
        // 记录replica的offset。运行run_replica()时，由于发送
        // PSYNC <run_id> <offset>是多线程的，所以需要上锁共享
        // offset。run_replica()会一直持有该锁，直到断开连接时
        // 释放，避免锁的性能影响
        static OFFSET: Mutex<u64> = Mutex::const_new(0);

        let conf = shared.conf();
        let post_office = shared.post_office();

        // 如果已经有一个异步任务在执行该函数(未执行到run_replica())，则直接返回错误
        let mut ms_info =
            conf.replica.master_info.try_lock().map_err(|_| {
                "ERR another replica operation is in progress, please try again later"
            })?;

        /* step2: 与主节点握手建立连接 */

        let to_master = TcpStream::connect((master_host.as_ref(), master_port))
            .timeout(Duration::from_secs(3))
            .await
            .map_err(|_| {
                RutinError::from(format!(
                    "ERR timeout connecting to master {}:{}",
                    master_host, master_port
                ))
            })??;

        let mut handle_master = {
            let ac = get_handle_master_ac();

            let (outbox, inbox) = post_office.new_mailbox_with_special_id(SET_REPLICA_ID);
            let context =
                HandlerContext::with_ac(SET_REPLICA_ID, outbox, inbox, Arc::new(ac), DEFAULT_USER);
            Handler::new(shared, to_master, context)
        };

        // 发送PING测试主节点是否可达
        handle_master
            .conn
            .write_frame(&Resp3::<_, String>::new_array(vec![
                Resp3::new_blob_string(b"PING"),
            ]))
            .await?;

        // 主节点应当返回"+PONG\r\n"
        if let Some(err) = handle_master
            .conn
            .read_frame_force()
            .await?
            .try_simple_error()
        {
            return Err(RutinError::from(err.to_string()));
        }

        // 身份验证(可选)
        if let Some(password) = conf.replica.master_auth.as_ref() {
            handle_master
                .conn
                .write_frame(&Resp3::<_, String>::new_array(vec![
                    Resp3::new_blob_string(b"AUTH".to_vec()),
                    Resp3::new_blob_string(password.clone().into_bytes()),
                ]))
                .await?;

            if handle_master
                .conn
                .read_frame()
                .await?
                .is_some_and(|frame| frame.is_simple_error())
            {
                return Err(RutinError::from("ERR master authentication failed"));
            }
        }

        // 发送端口信息
        handle_master
            .conn
            .write_frame(&Resp3::<_, String>::new_array(vec![
                Resp3::new_blob_string(b"REPLCONF".to_vec()),
                Resp3::new_blob_string(b"listening-port".to_vec()),
                Resp3::new_blob_string(conf.server.port.to_string().into_bytes()),
            ]))
            .await?;

        // 主节点应当返回+OK\r\n
        if let Some(err) = handle_master
            .conn
            .read_frame_force()
            .await?
            .try_simple_error()
        {
            return Err(RutinError::from(err.to_string()));
        }

        // 等待旧的run_replica()任务结束并获取offset
        let mut offset = OFFSET.lock().await;

        // 发送PSYNC命令
        if let Some(ms_info) = ms_info.as_mut() {
            // PSYNC <run_id> <offset>
            let array = vec![
                Resp3::new_blob_string("PSYNC".into()),
                Resp3::new_blob_string(ms_info.run_id.clone()),
                Resp3::new_blob_string((*offset).to_string().into()),
            ];
            handle_master
                .conn
                .write_frame(&Resp3::<_, String>::new_array(array))
                .await?;
        } else {
            // PSYNC ? -1
            handle_master
                .conn
                .write_frame(&Resp3::<_, String>::new_array(vec![
                    Resp3::new_blob_string(b"PSYNC".as_ref()),
                    Resp3::new_blob_string(b"?".as_ref()),
                    Resp3::new_blob_string(b"-1".as_ref()),
                ]))
                .await?;
        };

        shared.pool().spawn_pinned(move || async move {
            // read_only模式下，所有ac都只允许读命令
            if conf.replica.read_only {
                if let Some(acl) = &conf.security.acl {
                    for mut ac in acl.iter_mut() {
                        ac.allow_only_read();
                    }
                }

                conf.security.default_ac.rcu(|ac| {
                    let mut ac = AccessControl::clone(ac);
                    ac.allow_only_read();
                    ac
                });
            }

            /* step3: 进行同步 */

            loop {
                let resp = handle_master.conn.read_frame_force().await.unwrap();
                match resp {
                    // 如果响应为+FULLRESYNC run_id offset，执行全量同步
                    // 如果响应为+CONTINUE，执行增量同步(什么都不做，等之后向主节点发送REPLCONF ACK
                    // <offset>时会进行同步)
                    Resp3::SimpleString { inner, .. } => {
                        let mut splits = inner.split_whitespace().map(|s| s.to_string());

                        if let Some(fullresync) = splits.next()
                            && fullresync == "FULLRESYNC"
                        {
                            let run_id = splits.next().unwrap();
                            let master_offset = splits.next().unwrap().parse::<u64>().unwrap();

                            *ms_info = Some(MasterInfo {
                                host: master_host.clone(),
                                port: master_port,
                                run_id: run_id.into(),
                            });
                            *offset = master_offset;

                            // 重置服务(清除旧数据，关闭所有记录的任务)并执行全量同步
                            post_office.send_reset(SET_REPLICA_ID).await;
                            full_sync(&mut handle_master).await.unwrap();
                        }
                    }
                    // 如果响应为ping命令，则代表同步成功
                    Resp3::Array { inner, .. }
                        if inner.back().is_some_and(|resp3| {
                            resp3.to_string().eq_ignore_ascii_case("PING")
                        }) =>
                    {
                        break;
                    }
                    _ => panic!(
                        "expect +FULLRESYNC <run_id> <offset>, +CONTINUE or ping, but got {:?}",
                        resp
                    ),
                }
            }

            // 同步成功后释放锁，重新允许执行set_server_to_replica函数
            drop(ms_info);

            /* step4: 接收并处理传播的写命令 */
            if let Err(e) = handle_master.run_replica(offset).await {
                error!(cause = %e, "replica run error");
            }
        });

        Ok(())
    }
}

fn get_handle_master_ac() -> AccessControl {
    let mut ac = AccessControl::new_strict();

    ac.allow_cmds(PING_CMD_FLAG | WRITE_CMDS_FLAG | REPLCONF_CMD_FLAG);

    ac
}

async fn full_sync(handler: &mut Handler<TcpStream>) -> RutinResult<()> {
    // 接收RDB文件，格式为(末尾没有\r\n)：
    // $<len>\r\n<rdb data>
    // let mut len = handler.conn.read_line().await?;
    // len.advance(1); // 忽略'$'
    // let len: usize = atoi::<i128>(len.as_ref())? as usize;
    //
    // CheapResp3::need_bytes_async(&mut handler.conn.stream, &mut handler.conn.reader_buf, len)
    //     .await?;
    // let mut rdb = handler.conn.reader_buf.split_to(len);
    //
    // rdb_load(&mut rdb, handler.shared.db(), false)
    //     .await
    //     .map_err(|e| RutinError::new_server_error(e.to_string()))?;
    //
    // if let Some(aof_conf) = &handler.shared.conf().aof {
    //     let mut aof = Aof::new(handler.shared, aof_conf.file_path.clone()).await?;
    //     aof.rewrite()
    //         .await
    //         .map_err(|e| RutinError::new_server_error(e.to_string()))?;
    // }
    // TODO:
    todo!();

    Ok(())
}
