use super::*;
use crate::{
    cmd::{CmdExecutor, CmdUnparsed},
    conf::AccessControl,
    connection::AsyncStream,
    error::{RutinError, RutinResult},
    frame::Resp3,
    server::Handler,
    util, Id,
};
use bytes::Bytes;
use tracing::instrument;

/// # Reply:
///
/// **Array reply**: a nested list of command details. The order of the commands in the array is random.
// #[derive(Debug)]
// pub struct _Command;
//
// impl CmdExecutor for _Command {
//     const CMD_TYPE: CmdType = CmdType::Other;
//
//     // TODO: 返回命令字典，以便支持客户端的命令补全
// async fn _execute(self, shared: &Shared) -> RutinResult<Option<RESP3>>{
//
//         Ok(None)
//     }
//
//     fn parse(cmd_frame: RESP3) -> RutinResult<Self> {
//
//         Ok(_Command)
//     }
// }

/// # Reply:
///
/// **Simple string reply**: PONG when no argument is provided.
/// **Bulk string reply**: the provided argument.
#[derive(Debug)]
pub struct Ping {
    msg: Option<Bytes>,
}

impl CmdExecutor for Ping {
    const NAME: &'static str = "PING";
    const CATS_FLAG: Flag = PING_CATS_FLAG;
    const CMD_FLAG: Flag = PING_CMD_FLAG;

    #[instrument(level = "debug", skip(_handler), ret, err)]
    async fn execute(self, _handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let res = match self.msg {
            Some(msg) => Resp3::new_simple_string(msg.try_into()?),
            None => Resp3::new_simple_string("PONG".into()),
        };

        Ok(Some(res))
    }

    fn parse(mut args: CmdUnparsed, _ac: &AccessControl) -> RutinResult<Self> {
        if !args.is_empty() && args.len() != 1 {
            return Err(RutinError::WrongArgNum);
        }

        Ok(Ping { msg: args.next() })
    }
}

/// # Reply:
///
/// **Bulk string reply**: the given string.
#[derive(Debug)]
pub struct Echo {
    msg: Bytes,
}

impl CmdExecutor for Echo {
    const NAME: &'static str = "ECHO";
    const CATS_FLAG: Flag = ECHO_CATS_FLAG;
    const CMD_FLAG: Flag = ECHO_CMD_FLAG;

    #[instrument(level = "debug", skip(_handler), ret, err)]
    async fn execute(self, _handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        Ok(Some(Resp3::new_blob_string(self.msg)))
    }

    fn parse(mut args: CmdUnparsed, _ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 1 {
            return Err(RutinError::WrongArgNum);
        }

        Ok(Echo {
            msg: args.next().unwrap(),
        })
    }
}

#[derive(Debug)]
pub struct Auth {
    pub username: Bytes,
    pub password: Bytes,
}

impl CmdExecutor for Auth {
    const NAME: &'static str = "AUTH";
    const CATS_FLAG: Flag = AUTH_CATS_FLAG;
    const CMD_FLAG: Flag = AUTH_CMD_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        if let Some(acl) = handler.shared.conf().security.acl.as_ref() {
            if let Some(ac) = acl.get(&self.username) {
                if !ac.is_pwd_correct(&self.password) {
                    Err("ERR invalid password".into())
                } else {
                    // 设置客户端的权限
                    handler.context.ac = std::sync::Arc::new(ac.clone());
                    Ok(Some(Resp3::new_simple_string("OK".into())))
                }
            } else {
                Err("ERR invalid username".into())
            }
        } else {
            // 没有设置ACL
            Ok(Some(Resp3::new_simple_string("OK".into())))
        }
    }

    fn parse(mut args: CmdUnparsed, _ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 1 && args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        }

        Ok(Auth {
            username: args.next().unwrap(),
            password: args.next().unwrap_or_default(),
        })
    }
}

/// # Desc:
///
/// 执行该命令后，会开启客户端缓存追踪。每次执行读命令时，客户端都可能缓存该键
/// 值对， 因此需要对客户端读过的键值对加入 监听事件ObjectEvent::Write，当该键
/// 值对被修改时服务端需要发送invalidation messages给客户端。取消追踪并不意味
/// 着客户端一定不会收到invalidation messages，它仅仅保证对于新的读命令，不再监
/// 听其键值对
///
/// # Reply:
///
/// **Simple string reply:** OK if the connection was successfully put in tracking
/// mode or if the tracking mode was successfully disabled. Otherwise, an error is
/// returned.
#[derive(Debug)]
pub struct ClientTracking {
    switch_on: bool,
    // 将该客户端缓存失效的消息重定向
    redirect: Option<Id>,
}

impl CmdExecutor for ClientTracking {
    const NAME: &'static str = "TRACKING";
    const CATS_FLAG: Flag = CLIENTTRACKING_CATS_FLAG;
    const CMD_FLAG: Flag = CLIENTTRACKING_CMD_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        if !self.switch_on {
            // 关闭追踪后并不意味着之前的追踪事件会被删除，只是不再添加新的追踪事件
            handler.context.client_track = None;
            return Ok(Some(Resp3::new_simple_string("OK".into())));
        }

        if let Some(redirect) = self.redirect {
            let redirect_bg_sender = handler
                .shared
                .db()
                .get_client_bg_sender(redirect)
                .ok_or("ERR the client ID you want redirect to does not exist")?;
            handler.context.client_track = Some(redirect_bg_sender);
        } else {
            handler.context.client_track = Some(handler.context.bg_task_channel.new_sender());
        }

        Ok(Some(Resp3::new_simple_string("OK".into())))
    }

    fn parse(mut args: CmdUnparsed, _ac: &AccessControl) -> RutinResult<Self> {
        if args.len() > 2 {
            return Err(RutinError::WrongArgNum);
        }

        let mut switch = [0; 3];
        let bulk = args.next().unwrap();
        let len = bulk.len();
        switch[..len].clone_from_slice(bulk.as_ref());
        switch[..len].make_ascii_uppercase();

        let switch = match &switch[..len] {
            b"ON" => true,
            b"OFF" => false,
            _ => return Err("ERR invalid switch is given")?,
        };

        let redirect = if let Some(redirect) = args.next() {
            Some(util::atoi::<Id>(&redirect)?)
        } else {
            None
        };

        Ok(ClientTracking {
            switch_on: switch,
            redirect,
        })
    }
}

#[cfg(test)]
mod cmd_other_tests {
    use super::*;
    use crate::{
        conf::AccessControl,
        util::{get_test_shared, test_init, TEST_AC_CMDS_FLAG, TEST_AC_PASSWORD, TEST_AC_USERNAME},
    };

    #[tokio::test]
    async fn auth_test() {
        test_init();

        let shared = get_test_shared();
        let (mut handler, _) = Handler::new_fake_with(shared, None, None);

        let auth = Auth::parse(
            CmdUnparsed::from([TEST_AC_USERNAME, "1234567"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let res = auth.execute(&mut handler).await;
        assert_eq!(res.unwrap_err().to_string(), "ERR invalid password");

        let auth = Auth::parse(
            CmdUnparsed::from(["admin1", TEST_AC_PASSWORD].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let res = auth.execute(&mut handler).await;
        assert_eq!(res.unwrap_err().to_string(), "ERR invalid username");

        let auth = Auth::parse(
            CmdUnparsed::from([TEST_AC_USERNAME, TEST_AC_PASSWORD].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        auth.execute(&mut handler).await.unwrap();
        assert_eq!(handler.context.ac.cmd_flag(), TEST_AC_CMDS_FLAG);
    }

    #[tokio::test]
    async fn client_tracking_test() {
        test_init();

        let (mut handler, _) = Handler::new_fake();

        let tracking = ClientTracking::parse(
            CmdUnparsed::from(["ON"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        tracking.execute(&mut handler).await.unwrap();
        assert!(handler.context.client_track.is_some());

        let tracking = ClientTracking::parse(
            CmdUnparsed::from(["OFF"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        tracking.execute(&mut handler).await.unwrap();
        assert!(handler.context.client_track.is_none());
    }
}
