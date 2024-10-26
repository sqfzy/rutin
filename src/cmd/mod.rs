pub mod commands;

use crate::{
    conf::AccessControl,
    error::{RutinError, RutinResult},
    frame::{CheapResp3, Resp3},
    server::{AsyncStream, Handler},
    shared::db::{Key, Str},
    util::{IntoUppercase, StaticBytes, Uppercase},
};
use bytes::Bytes;
use commands::*;
use equivalent::Equivalent;
use itertools::Itertools;
use std::{
    collections::VecDeque,
    fmt::Debug,
    hash::Hash,
    intrinsics::{likely, unlikely},
    iter::Iterator,
};
use tracing::instrument;

pub trait CommandFlag {
    const NAME: &'static str;
    const CATS_FLAG: CatFlag;
    const CMD_FLAG: CmdFlag;
}

pub trait CmdArg
where
    Self: IntoUppercase
        + IntoUppercase<Mut: AsRef<[u8]>>
        + Equivalent<Key>
        + Hash
        + Debug
        + AsRef<[u8]>
        + Into<Str>
        + Into<Key>,
    for<'a> Self: Into<Key>,
{
    fn into_bytes(self) -> Bytes {
        let k: Key = self.into();
        k.into()
    }
}

impl<T> CmdArg for T
where
    T: IntoUppercase
        + IntoUppercase<Mut: AsRef<[u8]>>
        + Equivalent<Key>
        + Hash
        + Debug
        + AsRef<[u8]>
        + Into<Str>
        + Into<Key>, // + Send
    // + 'static
    for<'a> Self: Into<Key>,
{
}

impl<A: CmdArg> From<&A> for Key {
    fn from(value: &A) -> Self {
        value.as_ref().into()
    }
}

trait CmdExecutor<A>: CommandFlag + Sized + std::fmt::Debug
where
    A: CmdArg,
{
    #[inline]
    async fn apply(
        mut cmd: CmdUnparsed<A>,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        // 检查是否有权限执行该命令
        if unlikely(handler.context.ac.is_forbidden_cmd(Self::CMD_FLAG)) {
            return Err(RutinError::NoPermission);
        }

        let mut should_track_key = false;

        let wcmd_sender = if cats_contains_cat(Self::CATS_FLAG, WRITE_CAT_FLAG)
            && let Some(sender) = handler.shared.post_office().need_send_wcmd()
        {
            cmd = handler.context.wcmd_buf.buffer_wcmd(cmd);

            Some(sender)
        } else {
            None
        };

        let res: RutinResult<Option<CheapResp3>> = try {
            let cmd = Self::parse(cmd, &handler.context.ac)?;

            // 如果开启了client-cache则可能需要追踪涉及的键
            should_track_key = cmd.may_track(handler).await;

            cmd.execute(handler).await?
        };

        if likely(res.is_ok()) {
            if let Some(wcmd_sender) = wcmd_sender {
                handler.may_send_wmcd_buf(wcmd_sender);
            } else {
                handler.context.wcmd_buf.rollback();
            }

            if should_track_key {
                handler.track().await;
            }
        }

        // TODO:
        // if let Some(rdb) =  handler.shared.conf().rdb.as_ref() {
        // if let Some(save) = rdb.save {
        //
        // }
        // }

        res
    }

    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>>;

    // 需要检查是否有权限操作对应的键
    fn parse(args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self>;

    // 该方法用于实现client-side cache。所有获取当前string类键值对的命令(不一定是Read类
    // 命令)都需要手动实现该方法
    #[inline]
    async fn may_track(&self, _handler: &mut Handler<impl AsyncStream>) -> bool {
        false
    }
}

#[inline]
#[instrument(
    level = "debug",
    skip(handler),
    ret(level = "debug"),
    err(level = "debug")
)]
pub async fn dispatch<B, S>(
    cmd_frame: Resp3<B, S>,
    handler: &mut Handler<impl AsyncStream>,
) -> RutinResult<Option<CheapResp3>>
where
    B: Debug + CmdArg,
    Key: for<'a> From<&'a B>,
    S: Debug,
{
    let dispatch = async {
        let mut cmd = CmdUnparsed::try_from(cmd_frame)?;
        let cmd_name = cmd.cmd_name_uppercase();
        let cmd_name = if let Ok(s) = std::str::from_utf8(cmd_name.as_ref()) {
            s
        } else {
            return Err(RutinError::UnknownCmd);
        };

        match cmd_name {
            /*********/
            /* admin */
            /*********/
            AclCat::<B>::NAME => AclCat::apply(cmd, handler).await,
            AclDelUser::<B>::NAME => AclDelUser::apply(cmd, handler).await,
            AclSetUser::<B>::NAME => AclSetUser::apply(cmd, handler).await,
            AclUsers::NAME => AclUsers::apply(cmd, handler).await,
            AclWhoAmI::NAME => AclWhoAmI::apply(cmd, handler).await,
            // AppendOnly::<B>::NAME => AppendOnly::apply(cmd, handler).await,
            BgSave::NAME => BgSave::apply(cmd, handler).await,
            PSync::<B>::NAME => PSync::apply(cmd, handler).await,
            ReplConf::NAME => ReplConf::apply(cmd, handler).await,
            ReplicaOf::NAME => ReplicaOf::apply(cmd, handler).await,
            Save::NAME => Save::apply(cmd, handler).await,
            /**************/
            /* connection */
            /**************/
            Auth::<B>::NAME => Auth::apply(cmd, handler).await,
            Echo::<B>::NAME => Echo::apply(cmd, handler).await,
            Ping::<B>::NAME => Ping::apply(cmd, handler).await,
            /************/
            /* keyspace */
            /************/
            Del::<B>::NAME => Del::apply(cmd, handler).await,
            Dump::<B>::NAME => Dump::apply(cmd, handler).await,
            Exists::<B>::NAME => Exists::apply(cmd, handler).await,
            Expire::<B>::NAME => Expire::apply(cmd, handler).await,
            ExpireAt::<B>::NAME => ExpireAt::apply(cmd, handler).await,
            ExpireTime::<B>::NAME => ExpireTime::apply(cmd, handler).await,
            Keys::<B>::NAME => Keys::apply(cmd, handler).await,
            // NBKeys::<B>::NAME => NBKeys::apply(cmd, handler).await,
            Persist::<B>::NAME => Persist::apply(cmd, handler).await,
            Pttl::<B>::NAME => Pttl::apply(cmd, handler).await,
            Ttl::<B>::NAME => Ttl::apply(cmd, handler).await,
            Type::<B>::NAME => Type::apply(cmd, handler).await,
            /**********/
            /* string */
            /**********/
            Append::<B>::NAME => Append::apply(cmd, handler).await,
            Decr::<B>::NAME => Decr::apply(cmd, handler).await,
            DecrBy::<B>::NAME => DecrBy::apply(cmd, handler).await,
            Get::<B>::NAME => Get::apply(cmd, handler).await,
            GetRange::<B>::NAME => GetRange::apply(cmd, handler).await,
            GetSet::<B>::NAME => GetSet::apply(cmd, handler).await,
            Incr::<B>::NAME => Incr::apply(cmd, handler).await,
            IncrBy::<B>::NAME => IncrBy::apply(cmd, handler).await,
            MGet::<B>::NAME => MGet::apply(cmd, handler).await,
            MSet::<B>::NAME => MSet::apply(cmd, handler).await,
            MSetNx::<B>::NAME => MSetNx::apply(cmd, handler).await,
            Set::<B>::NAME => Set::apply(cmd, handler).await,
            SetEx::<B>::NAME => SetEx::apply(cmd, handler).await,
            SetNx::<B>::NAME => SetNx::apply(cmd, handler).await,
            StrLen::<B>::NAME => StrLen::apply(cmd, handler).await,
            /********/
            /* list */
            /********/
            BLMove::<B>::NAME => BLMove::apply(cmd, handler).await,
            BLPop::<B>::NAME => BLPop::apply(cmd, handler).await,
            LLen::<B>::NAME => LLen::apply(cmd, handler).await,
            LPop::<B>::NAME => LPop::apply(cmd, handler).await,
            LPos::<B>::NAME => LPos::apply(cmd, handler).await,
            LPush::<B>::NAME => LPush::apply(cmd, handler).await,
            /********/
            /* hash */
            /********/
            HDel::<B>::NAME => HDel::apply(cmd, handler).await,
            HExists::<B>::NAME => HExists::apply(cmd, handler).await,
            HGet::<B>::NAME => HGet::apply(cmd, handler).await,
            HSet::<B>::NAME => HSet::apply(cmd, handler).await,
            /**********/
            /* pubsub */
            /**********/
            Publish::NAME => Publish::apply(cmd, handler).await,
            Subscribe::NAME => Subscribe::apply(cmd, handler).await,
            Unsubscribe::NAME => Unsubscribe::apply(cmd, handler).await,
            /*************/
            /* scripting */
            /*************/
            Eval::<B>::NAME => Eval::apply(cmd, handler).await,
            EvalName::<B>::NAME => EvalName::apply(cmd, handler).await,

            // 命令中包含子命令
            _ => {
                let sub_cmd_name = cmd.next_uppercase::<16>().ok_or(RutinError::Syntax)?;

                let sub_cmd_name = if let Ok(s) = std::str::from_utf8(sub_cmd_name.as_ref()) {
                    s
                } else {
                    return Err(RutinError::UnknownCmd);
                };

                match sub_cmd_name {
                    ClientTracking::NAME => ClientTracking::apply(cmd, handler).await,
                    ScriptExists::<B>::NAME => ScriptExists::apply(cmd, handler).await,
                    ScriptFlush::NAME => ScriptFlush::apply(cmd, handler).await,
                    ScriptRegister::<B>::NAME => ScriptRegister::apply(cmd, handler).await,
                    _ => Err(RutinError::UnknownCmd),
                }
            }
        }
    };

    match dispatch.await {
        Ok(res) => Ok(res.map(Into::into)),
        Err(e) => {
            let frame = CheapResp3::try_from(e)?; // 尝试将错误转换为RESP3
            Ok(Some(frame))
        }
    }
}

#[derive(Debug)]
pub struct CmdUnparsed<B> {
    pub cmd_name: B,
    pub inner: VecDeque<B>,
}

impl<B> CmdUnparsed<B> {
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn cmd_name_uppercase(&mut self) -> Uppercase<16, &mut [u8]>
    where
        B: IntoUppercase,
    {
        self.cmd_name.to_uppercase::<16>()
    }

    #[inline]
    pub fn next_uppercase<const L: usize>(&mut self) -> Option<Uppercase<L, B::Mut>>
    where
        B: IntoUppercase,
    {
        self.next().map(|b| b.into_uppercase())
    }
}

impl<B> Iterator for CmdUnparsed<B> {
    type Item = B;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.pop_front()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.inner.len(), Some(self.inner.len()))
    }

    #[inline]
    fn count(self) -> usize {
        self.inner.len()
    }
}

impl<B> DoubleEndedIterator for CmdUnparsed<B> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.pop_back()
    }
}

impl<B> ExactSizeIterator for CmdUnparsed<B> {}

impl<B: IntoUppercase, S> TryFrom<Resp3<B, S>> for CmdUnparsed<B> {
    type Error = RutinError;

    #[inline]
    fn try_from(value: Resp3<B, S>) -> Result<Self, Self::Error> {
        match value {
            Resp3::Array { inner, .. } => {
                let mut inner = inner
                    .into_iter()
                    .map(|f| {
                        if let Resp3::BlobString { inner, .. } = f {
                            Ok(inner)
                        } else {
                            Err(RutinError::from(
                                "ERR the command frame is not a blob string type",
                            ))
                        }
                    })
                    .collect::<Result<VecDeque<_>, _>>()?;

                let cmd_name = inner.pop_front().ok_or(RutinError::Other {
                    msg: "ERR the command frame is empty".into(),
                })?;

                Ok(CmdUnparsed { cmd_name, inner })
            }
            _ => Err(RutinError::Other {
                msg: "ERR the command frame is not an array type".into(),
            }),
        }
    }
}

impl<B, S> From<CmdUnparsed<B>> for Resp3<B, S> {
    #[inline]
    fn from(mut val: CmdUnparsed<B>) -> Self {
        val.inner.push_front(val.cmd_name);
        Resp3::new_array(
            val.inner
                .into_iter()
                .map(Resp3::new_blob_string)
                .collect_vec(),
        )
    }
}

impl Default for CmdUnparsed<StaticBytes> {
    fn default() -> Self {
        CmdUnparsed {
            cmd_name: StaticBytes::from("".as_bytes()),
            inner: Default::default(),
        }
    }
}

#[allow(dead_code)]
trait CmdTest: CmdExecutor<StaticBytes> {
    async fn test(
        args: &[&str],
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let mut inner = VecDeque::with_capacity(args.len());
        for c in args {
            inner.push_back(c.as_bytes().into());
        }

        let cmd = CmdUnparsed {
            cmd_name: Self::NAME.as_bytes().into(),
            inner,
        };

        Self::apply(cmd, handler).await
    }
}

impl<T> CmdTest for T where T: CmdExecutor<StaticBytes> {}
