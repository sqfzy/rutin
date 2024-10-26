use super::*;
use crate::{
    cmd::{CmdExecutor, CmdUnparsed},
    conf::AccessControl,
    error::{RutinError, RutinResult},
    frame::{CheapResp3, Resp3},
    server::{AsyncStream, ClientTrack, Handler},
    util::{self},
    Id,
};
use bytestring::ByteString;
use std::fmt::Debug;
use tracing::instrument;

// # Reply:
//
// **Array reply**: a nested list of command details. The order of the commands in the array is random.
// #[derive(Debug)]
// pub struct _Command;
//
// impl<A, > CmdExecutor<A, Bytes,ByteString> for _Command
// where
//
// A:CmdArg
// {
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
pub struct Ping<A> {
    msg: Option<A>,
}

impl<A> CmdExecutor<A> for Ping<A>
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(_handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        _handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let res = match self.msg {
            Some(msg) => Resp3::new_blob_string(msg.into_bytes()),
            None => Resp3::new_simple_string(ByteString::from_static("PONG")),
        };

        Ok(Some(res))
    }

    fn parse(mut args: CmdUnparsed<A>, _ac: &AccessControl) -> RutinResult<Self> {
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
pub struct Echo<A> {
    msg: A,
}

impl<A> CmdExecutor<A> for Echo<A>
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(_handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        _handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        Ok(Some(Resp3::new_blob_string(self.msg.into_bytes())))
    }

    fn parse(mut args: CmdUnparsed<A>, _ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 1 {
            return Err(RutinError::WrongArgNum);
        }

        Ok(Echo {
            msg: args.next().unwrap(),
        })
    }
}

#[derive(Debug)]
pub struct Auth<A> {
    pub username: A,
    pub password: Option<A>,
}

impl<A> CmdExecutor<A> for Auth<A>
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        if let Some(acl) = handler.shared.conf().security_conf().acl.as_ref() {
            if let Some(ac) = acl.get(self.username.as_ref()) {
                // if !ac.is_pwd_correct(&self.password) {
                if self.password.is_none() || !ac.check_pwd(self.password.unwrap().as_ref()) {
                    Err("ERR invalid password".into())
                } else {
                    // 设置客户端的权限
                    handler.context.ac = std::sync::Arc::new(ac.clone());
                    Ok(Some(Resp3::new_simple_string("OK")))
                }
            } else {
                Err("ERR invalid username".into())
            }
        } else {
            // 没有设置ACL
            Ok(Some(Resp3::new_simple_string("OK")))
        }
    }

    fn parse(mut args: CmdUnparsed<A>, _ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 1 && args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        }

        Ok(Auth {
            username: args.next().unwrap(),
            password: args.next(),
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

impl<A> CmdExecutor<A> for ClientTracking
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        if !self.switch_on {
            // 关闭追踪后并不意味着之前的追踪事件会被删除，只是不再添加新的追踪事件
            handler.context.client_track = None;
            return Ok(Some(Resp3::new_simple_string("OK")));
        }

        if let Some(redirect) = self.redirect {
            let redirect_outbox = handler
                .shared
                .post_office()
                .get_outbox(redirect)
                .ok_or("ERR the client ID you want redirect to does not exist")?
                .clone();

            handler.context.client_track = Some(ClientTrack::new(redirect_outbox));
        } else {
            handler.context.client_track =
                Some(ClientTrack::new(handler.context.mailbox.outbox.clone()));
        }

        Ok(Some(Resp3::new_simple_string("OK")))
    }

    fn parse(mut args: CmdUnparsed<A>, _ac: &AccessControl) -> RutinResult<Self> {
        if args.len() > 2 {
            return Err(RutinError::WrongArgNum);
        }

        let mut switch = [0; 3];
        let bulk = args.next().unwrap();
        let len = bulk.as_ref().len();
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
        cmd::{Get, Set},
        util::{
            gen_test_handler, gen_test_shared, test_init, TEST_AC_CMDS_FLAG, TEST_AC_PASSWORD,
            TEST_AC_USERNAME,
        },
    };

    #[tokio::test]
    async fn auth_test() {
        test_init();

        let shared = gen_test_shared();
        let (mut handler, _) = Handler::new_fake_with(shared, None, None);

        // Case: 错误的密码
        let auth_res = Auth::test(&[TEST_AC_USERNAME, "1234567"], &mut handler)
            .await
            .unwrap_err();
        assert_eq!(auth_res.to_string(), "ERR invalid password");

        // Case: 错误的用户名
        let auth_res = Auth::test(&["admin1", TEST_AC_PASSWORD], &mut handler)
            .await
            .unwrap_err();
        assert_eq!(auth_res.to_string(), "ERR invalid username");

        // Case: 正确的用户名和密码
        let auth_res = Auth::test(&[TEST_AC_USERNAME, TEST_AC_PASSWORD], &mut handler)
            .await
            .unwrap();
        auth_res.unwrap();
        assert_eq!(handler.context.ac.cmd_flag(), TEST_AC_CMDS_FLAG);
    }

    #[tokio::test]
    async fn client_tracking_test() {
        test_init();

        let mut handler = gen_test_handler();

        // Case: 开启客户端追踪
        let tracking_res = ClientTracking::test(&["ON"], &mut handler).await.unwrap();
        tracking_res.unwrap();
        assert!(handler.context.client_track.is_some());

        // Case: 设置跟踪的键
        let set_res = Set::test(&["track_key", "foo"], &mut handler)
            .await
            .unwrap();
        set_res.unwrap();

        // 追踪track_key
        Get::test(&["track_key"], &mut handler).await.unwrap();

        // 修改track_key
        let set_res = Set::test(&["track_key", "bar"], &mut handler)
            .await
            .unwrap();
        set_res.unwrap();

        // 验证是否发送了无效ation通知
        assert_eq!(
            handler.context.mailbox.recv().into_resp3_unchecked(),
            CheapResp3::new_array(vec![
                CheapResp3::new_blob_string("INVALIDATE"),
                CheapResp3::new_blob_string("track_key"),
            ])
        );

        // Case: 关闭客户端追踪
        let tracking_res = ClientTracking::test(&["OFF"], &mut handler).await.unwrap();
        tracking_res.unwrap();
        assert!(handler.context.client_track.is_none());
    }
}
