pub mod commands;

use crate::{
    conf::AccessControl,
    error::{RutinError, RutinResult},
    frame::{CheapResp3, Resp3, StaticResp3, ARRAY_PREFIX, CRLF},
    server::{using_local_buf, AsyncStream, Handler},
    util::{StaticBytes, Uppercase},
};
use bytes::BufMut;
use commands::*;
use std::{collections::VecDeque, intrinsics::unlikely, iter::Iterator, num::NonZero};
use tracing::instrument;

pub trait CommandFlag {
    const NAME: &'static str;
    const CATS_FLAG: CatFlag;
    const CMD_FLAG: CmdFlag;
}

#[allow(async_fn_in_trait)]
pub trait CmdExecutor: CommandFlag + Sized + std::fmt::Debug {
    #[inline]
    async fn apply(
        args: CmdUnparsed<'_>,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        // 检查是否有权限执行该命令
        if unlikely(handler.context.ac.is_forbidden_cmd(Self::CMD_FLAG)) {
            return Err(RutinError::NoPermission);
        }

        let post_office = handler.shared.post_office();
        let wcmd =
            if cats_contains_cat(Self::CATS_FLAG, WRITE_CAT_FLAG) && post_office.need_send_wcmd() {
                let wcmd = using_local_buf(|buf| {
                    buf.put_u8(ARRAY_PREFIX);
                    buf.put_slice(itoa::Buffer::new().format(args.len() + 1).as_bytes());
                    buf.put_slice(CRLF);

                    let cmd_name_frame: Resp3<&[u8], String> =
                        Resp3::new_blob_string(Self::NAME.as_bytes());
                    cmd_name_frame.encode_buf(buf);

                    for frame in args.inner.iter() {
                        frame.encode_buf(buf);
                    }
                });

                Some(wcmd)
            } else {
                None
            };

        let cmd = Self::parse(args, &handler.context.ac)?;

        let res = cmd.execute(handler).await?;

        // 如果使用了pipline则该批命令视为一个整体命令。当最后一个命令执行完毕后，才发送写命令，
        // 避免性能瓶颈。如果执行过程中程序崩溃，则客户端会报错，用户会视为该批命令没有执行成
        // 功，也不会传播该批命令，符合一致性。
        if let Some(wcmd) = wcmd {
            let wcmd_buf = &mut handler.context.wcmd_buf;

            if handler.conn.unhandled_count() <= 1 {
                if wcmd_buf.is_empty() {
                    post_office.send_wcmd(wcmd).await;
                } else {
                    wcmd_buf.unsplit(wcmd);
                    post_office.send_wcmd(wcmd_buf.split()).await;
                }
            } else {
                wcmd_buf.unsplit(wcmd);
            }
        }

        // TODO:
        // if let Some(rdb) =  handler.shared.conf().rdb.as_ref() {
        // if let Some(save) = rdb.save {
        //
        // }
        // }

        Ok(res)
    }

    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>>;

    // 需要检查是否有权限操作对应的键
    fn parse(args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self>;
}

#[inline]
pub async fn dispatch(
    cmd_frame: &mut StaticResp3,
    handler: &mut Handler<impl AsyncStream>,
) -> RutinResult<Option<CheapResp3>> {
    let mut cmd: CmdUnparsed = cmd_frame.try_into()?;

    let cmd_name = cmd
        .next_uppercase::<16>()
        .ok_or_else(|| RutinError::Syntax)?;
    let cmd_name = if let Ok(s) = std::str::from_utf8(cmd_name.as_ref()) {
        s
    } else {
        return Err(RutinError::UnknownCmd);
    };

    let res = match cmd_name {
        /*********/
        /* admin */
        /*********/
        AclCat::NAME => AclCat::apply(cmd, handler).await,
        AclDelUser::NAME => AclDelUser::apply(cmd, handler).await,
        AclSetUser::NAME => AclSetUser::apply(cmd, handler).await,
        AclUsers::NAME => AclUsers::apply(cmd, handler).await,
        AclWhoAmI::NAME => AclWhoAmI::apply(cmd, handler).await,
        BgSave::NAME => BgSave::apply(cmd, handler).await,
        PSync::NAME => PSync::apply(cmd, handler).await,
        ReplConf::NAME => ReplConf::apply(cmd, handler).await,
        ReplicaOf::NAME => ReplicaOf::apply(cmd, handler).await,
        /**************/
        /* connection */
        /**************/
        Auth::NAME => Auth::apply(cmd, handler).await,
        Echo::NAME => Echo::apply(cmd, handler).await,
        Ping::NAME => Ping::apply(cmd, handler).await,
        // /************/
        // /* keyspace */
        // /************/
        // Del::NAME => Del::apply(cmd, handler).await,
        // Dump::NAME => Dump::apply(cmd, handler).await,
        // Exists::NAME => Exists::apply(cmd, handler).await,
        // Expire::NAME => Expire::apply(cmd, handler).await,
        // ExpireAt::NAME => ExpireAt::apply(cmd, handler).await,
        // ExpireTime::NAME => ExpireTime::apply(cmd, handler).await,
        // Keys::NAME => Keys::apply(cmd, handler).await,
        // NBKeys::NAME => NBKeys::apply(cmd, handler).await,
        // Persist::NAME => Persist::apply(cmd, handler).await,
        // Pttl::NAME => Pttl::apply(cmd, handler).await,
        // Ttl::NAME => Ttl::apply(cmd, handler).await,
        // Type::NAME => Type::apply(cmd, handler).await,
        /**********/
        /* string */
        /**********/
        // Append::NAME => Append::apply(cmd, handler).await,
        // Decr::NAME => Decr::apply(cmd, handler).await,
        // DecrBy::NAME => DecrBy::apply(cmd, handler).await,
        Get::NAME => Get::apply(cmd, handler).await,
        // GetRange::NAME => GetRange::apply(cmd, handler).await,
        // GetSet::NAME => GetSet::apply(cmd, handler).await,
        // Incr::NAME => Incr::apply(cmd, handler).await,
        // IncrBy::NAME => IncrBy::apply(cmd, handler).await,
        // MGet::NAME => MGet::apply(cmd, handler).await,
        // MSet::NAME => MSet::apply(cmd, handler).await,
        // MSetNx::NAME => MSetNx::apply(cmd, handler).await,
        Set::NAME => Set::apply(cmd, handler).await,
        // SetEx::NAME => SetEx::apply(cmd, handler).await,
        // SetNx::NAME => SetNx::apply(cmd, handler).await,
        // StrLen::NAME => StrLen::apply(cmd, handler).await,
        // /********/
        // /* list */
        // /********/
        // BLMove::NAME => BLMove::apply(cmd, handler).await,
        // BLPop::NAME => BLPop::apply(cmd, handler).await,
        // LLen::NAME => LLen::apply(cmd, handler).await,
        // LPop::NAME => LPop::apply(cmd, handler).await,
        // LPos::NAME => LPos::apply(cmd, handler).await,
        // LPush::NAME => LPush::apply(cmd, handler).await,
        // NBLPop::NAME => NBLPop::apply(cmd, handler).await,
        // /********/
        // /* hash */
        // /********/
        // HDel::NAME => HDel::apply(cmd, handler).await,
        // HExists::NAME => HExists::apply(cmd, handler).await,
        // HGet::NAME => HGet::apply(cmd, handler).await,
        // HSet::NAME => HSet::apply(cmd, handler).await,
        // /**********/
        // /* pubsub */
        // /**********/
        // Publish::NAME => Publish::apply(cmd, handler).await,
        // Subscribe::NAME => Subscribe::apply(cmd, handler).await,
        // Unsubscribe::NAME => Unsubscribe::apply(cmd, handler).await,
        // /*************/
        // /* scripting */
        // /*************/
        // Eval::NAME => Eval::apply(cmd, handler).await,
        // EvalName::NAME => EvalName::apply(cmd, handler).await,

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
                // ScriptExists::NAME => ScriptExists::apply(cmd, handler).await,
                // ScriptFlush::NAME => ScriptFlush::apply(cmd, handler).await,
                // ScriptRegister::NAME => ScriptRegister::apply(cmd, handler).await,
                _ => Err(RutinError::UnknownCmd),
            }
        }
    };

    match res {
        Ok(res) => Ok(res),
        Err(e) => {
            let frame = e.try_into()?; // 尝试将错误转换为RESP3
            Ok(Some(frame))
        }
    }
}

#[derive(Debug)]
pub struct CmdUnparsed<'a> {
    inner: &'a mut VecDeque<StaticResp3>,
}

impl CmdUnparsed<'_> {
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    // pub fn get_uppercase<'a>(&mut self, index: usize, buf: &'a mut [u8]) -> Option<&'a [u8]> {
    //     match self.inner.get(index) {
    //         Some(Resp3::BlobString { inner: b, .. }) => {
    //             debug_assert!(b.len() <= buf.len());
    //
    //             Some(util::get_uppercase(b, buf).unwrap())
    //         }
    //         _ => None,
    //     }
    // }

    pub fn next_uppercase<const L: usize>(&mut self) -> Option<Uppercase<L>> {
        self.next().map(|b| b.into_uppercase())
    }
}

impl Iterator for CmdUnparsed<'_> {
    type Item = StaticBytes;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.pop_front() {
            Some(Resp3::BlobString { inner, .. }) => Some(inner),
            _ => None,
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.inner.len(), Some(self.inner.len()))
    }

    #[inline]
    fn count(self) -> usize {
        self.inner.len()
    }

    fn advance_by(&mut self, n: usize) -> Result<(), std::num::NonZero<usize>> {
        let len = self.inner.len();
        let rem = if len < n {
            self.inner.clear();
            n - len
        } else {
            self.inner.drain(..n);
            0
        };
        NonZero::new(rem).map_or(Ok(()), Err)
    }
}

impl DoubleEndedIterator for CmdUnparsed<'_> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.pop_back().map(|r| {
            if let Resp3::BlobString { inner, .. } = r {
                inner
            } else {
                unreachable!()
            }
        })
    }

    fn advance_back_by(&mut self, n: usize) -> Result<(), NonZero<usize>> {
        let len = self.inner.len();
        let rem = if len < n {
            self.inner.clear();
            n - len
        } else {
            self.inner.drain(len - n..);
            0
        };
        NonZero::new(rem).map_or(Ok(()), Err)
    }
}

impl ExactSizeIterator for CmdUnparsed<'_> {}

impl<'a> TryFrom<&'a mut StaticResp3> for CmdUnparsed<'a> {
    type Error = RutinError;

    #[inline]
    fn try_from(value: &'a mut StaticResp3) -> Result<Self, Self::Error> {
        match value {
            Resp3::Array { inner, .. } => Ok(Self { inner }),
            _ => Err(RutinError::Syntax),
        }
    }
}

impl Default for CmdUnparsed<'_> {
    fn default() -> Self {
        CmdUnparsed {
            inner: Box::leak(Box::new(VecDeque::new())),
        }
    }
}

// impl From<CmdUnparsed<'_>> for RefMutResp3<'static> {
//     #[inline]
//     fn from(val: CmdUnparsed) -> Self {
//         Resp3::new_array(val.inner)
//     }
// }

pub fn gen_cmdunparsed_test(cmd: &[&str]) -> CmdUnparsed<'static> {
    let mut inner = VecDeque::with_capacity(cmd.len());
    for c in cmd {
        inner.push_back(Resp3::new_blob_string(c.as_bytes().to_vec().leak().into()));
    }
    CmdUnparsed {
        inner: Box::leak(Box::new(inner)),
    }
}
