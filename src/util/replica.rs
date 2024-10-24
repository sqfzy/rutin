use crate::{
    cmd::commands::{PING_CMD_FLAG, REPLCONF_CMD_FLAG, WRITE_CMDS_FLAG},
    conf::{AccessControl, MasterInfo, DEFAULT_USER},
    error::{RutinError, RutinResult},
    frame::Resp3,
    persist::rdb::decode_rdb,
    server::{Handler, HandlerContext},
    shared::{Shared, SET_REPLICA_ID},
    util,
};
use bytes::Buf;
use rutin_resp3::codec::decode::need_bytes_async;
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpStream, sync::Mutex};
use tokio_util::time::FutureExt as _;
use tracing::instrument;

/// ## set_server_to_replica意味着：
///
/// 1. 服务会清空所有旧数据
/// 2. 不存在SET_MASTER任务(检查conf)
/// 3. 服务需要定期向主节点发送REPLCONF ACK <offset>命令，以便主节点知道从节点的
///    复制进度
/// 4. 服务会定期收到主节点的PING命令，以便主节点知道从节点是否存活
/// 5. 服务会接收主节点传播的写命令，并执行，但不会返回结果
/// 6. 服务只能执行读命令(如果配置了read_only))
//
// 该函数只会在服务初始化时调用
#[allow(clippy::manual_async_fn)]
#[instrument(level = "debug", skip(shared), err)]
pub async fn _set_server_to_replica(shared: Shared, master_info: MasterInfo) -> RutinResult<()> {
    static OFFSET: Mutex<u64> = Mutex::const_new(0);

    let conf = shared.conf();
    let replica_conf = &conf.replica;
    let mailbox = shared
        .post_office()
        .register_special_mailbox(SET_REPLICA_ID);
    let MasterInfo {
        host: master_host,
        port: master_port,
        run_id,
    } = master_info;

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

        let context = HandlerContext::with_ac(SET_REPLICA_ID, mailbox, Arc::new(ac), DEFAULT_USER);
        Handler::new(shared, to_master, context)
    };

    // 发送PING测试主节点是否可达
    handle_master
        .conn
        .write_frame(&Resp3::<&'static [u8; 4], String>::new_array(vec![
            Resp3::new_blob_string(b"PING"),
        ]))
        .await?;

    // 主节点应当返回"+PONG\r\n"
    if let Some(err) = handle_master
        .conn
        .read_frame_force()
        .await?
        .as_simple_error()
    {
        return Err(RutinError::from(err.to_string()));
    }

    // 身份验证(可选)
    if let Some(password) = conf.replica.master_auth.as_ref() {
        handle_master
            .conn
            .write_frame(&Resp3::<Vec<u8>, String>::new_array(vec![
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
        .write_frame(&Resp3::<Vec<u8>, String>::new_array(vec![
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
        .as_simple_error()
    {
        return Err(RutinError::from(err.to_string()));
    }

    // 获取旧的offset
    let mut offset = OFFSET.lock().await;

    // 发送PSYNC命令
    if run_id == "?" {
        // PSYNC ? -1
        handle_master
            .conn
            .write_frame(&Resp3::<&'static [u8], String>::new_array(vec![
                Resp3::new_blob_string(b"PSYNC".as_ref()),
                Resp3::new_blob_string(b"?".as_ref()),
                Resp3::new_blob_string(b"-1".as_ref()),
            ]))
            .await?;
    } else {
        // PSYNC <run_id> <offset>
        let array = vec![
            Resp3::new_blob_string("PSYNC"),
            Resp3::new_blob_string(run_id.clone()),
            Resp3::new_blob_string(offset.to_string()),
        ];
        handle_master
            .conn
            .write_frame(&Resp3::<Vec<u8>, String>::new_array(array))
            .await?;
    }

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
    *offset = 0;

    let resp = handle_master.conn.read_frame_force().await.unwrap();
    match resp {
        // 如果响应为+FULLRESYNC run_id offset，执行全量同步
        // 如果响应为+CONTINUE，执行增量同步
        Resp3::SimpleString { inner, .. } => {
            let mut splits = inner.split_whitespace().map(|s| s.to_string());

            if let Some(fullresync) = splits.next()
                && fullresync == "FULLRESYNC"
            {
                let run_id = splits.next().unwrap();
                let master_offset = splits.next().unwrap().parse::<u64>().unwrap();

                {
                    let mut master_info = replica_conf.master_info.lock().unwrap();
                    master_info.as_mut().unwrap().run_id = run_id.into();

                    *offset = master_offset;
                }

                full_sync(&mut handle_master).await.unwrap();
            }
        }
        _ => {
            return Err(RutinError::new_server_error(format!(
                "expect +FULLRESYNC <run_id> <offset>, +CONTINUE, but got {:?}",
                resp
            )))
        }
    }

    /* step4: 接收并处理传播的写命令 */
    handle_master.run_replica(offset).await?;

    Ok(())
}

fn get_handle_master_ac() -> AccessControl {
    let mut ac = AccessControl::new_strict();

    ac.allow_cmds(PING_CMD_FLAG | WRITE_CMDS_FLAG | REPLCONF_CMD_FLAG);

    ac
}

async fn full_sync(handler: &mut Handler<TcpStream>) -> RutinResult<()> {
    let Handler { shared, conn, .. } = handler;
    // 接收RDB文件，格式为(末尾没有\r\n)：
    // $<len>\r\n<rdb data>
    let len = conn.read_line().await?;
    // 忽略'$'
    let len: usize = util::atoi::<i128>(&len[1..])? as usize;

    need_bytes_async(&mut conn.stream, &mut conn.reader_buf, len).await?;

    let pos = conn.reader_buf.position();
    conn.reader_buf.get_mut().advance(pos as usize);
    let mut rdb = conn.reader_buf.get_mut().split_to(len);
    conn.finish_read();

    decode_rdb(&mut rdb, shared.db(), false)
        .await
        .map_err(|e| RutinError::new_server_error(e.to_string()))?;

    Ok(())
}
