pub mod commands;

use std::{collections::VecDeque, num::NonZero};

use crate::{
    conf::AccessControl,
    error::{RutinError, RutinResult},
    frame::Resp3,
    server::{AsyncStream, Handler},
    util,
};
use bytes::Bytes;
use commands::*;
use tracing::instrument;

#[allow(async_fn_in_trait)]
pub trait CmdExecutor: Sized + std::fmt::Debug {
    const NAME: &'static str;
    const CATS_FLAG: CatFlag;
    const CMD_FLAG: CmdFlag;

    #[inline]
    async fn apply(
        mut args: CmdUnparsed,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<Resp3>> {
        // 检查是否有权限执行该命令
        if handler.context.ac.is_forbidden_cmd(Self::CMD_FLAG) {
            return Err(RutinError::NoPermission);
        }

        let post_office = handler.shared.post_office();
        let wcmd =
            if cats_contains_cat(Self::CATS_FLAG, WRITE_CAT_FLAG) && post_office.need_send_wcmd() {
                // 加上命令名
                args.inner
                    .push_front(Resp3::new_blob_string(Self::NAME.into()));
                let resp3 = Resp3::from(args);
                let wcmd = resp3.encode_local_buf();

                args = resp3.try_into()?;
                args.inner.pop_front(); // 去掉命令名
                Some(wcmd)
            }
            // else if let Some(back_log) = &mut handler.context.back_log
            //         && cmds_contains_cmd(WRITE_CAT_CMDS_FLAG, Self::CMD_FLAG)
            //     {
            //         // handle_master执行主节点传来的写命令时需要记录offset，由于从节点永远不会传播
            //         // 写命令给主节点，因此handle_replica即使有back_log也不会执行以下代码
            //         args.inner
            //             .push_front(Resp3::new_blob_string(Self::NAME.into()));
            //         let resp3 = Resp3::from(args);
            //         resp3.encode_buf(&mut back_log.buf);
            //         let wcmd = back_log.buf.split();
            //
            //         back_log.offset += wcmd.len() as u64;
            //
            //         args = resp3.try_into()?;
            //         args.inner.pop_front(); // 去掉命令名
            //         None
            //     }
            else {
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

    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>>;

    // 需要检查是否有权限操作对应的键
    fn parse(args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self>;
}

#[inline]
pub async fn dispatch(
    cmd_frame: Resp3,
    handler: &mut Handler<impl AsyncStream>,
) -> RutinResult<Option<Resp3>> {
    macro_rules! dispatch_command {
        ( $cmd:expr, $handler:expr, $( $cmd_type:ident ),*; $( $cmd_group:expr => $( $cmd_type2:ident ),* );* ) => {
            {
                let mut buf = [0; 32];
                let cmd_name = $cmd.next().ok_or_else(|| RutinError::Syntax)?;

                debug_assert!(cmd_name.len() <= buf.len());
                let len1 = util::uppercase(&cmd_name, &mut buf).unwrap();

                let cmd_name = if let Ok(s) = std::str::from_utf8(&buf[..len1]) {
                    s
                } else {
                    return Err(RutinError::UnknownCmd);
                };

                match cmd_name {
                    $(
                        $cmd_type::NAME => $cmd_type::apply($cmd, $handler).await,
                    )*
                    $(
                        $cmd_group => {
                            let sub_cmd_name = $cmd.next().ok_or(RutinError::Syntax)?;

                            debug_assert!(sub_cmd_name.len() <= buf.len() - len1);
                            let len2 = util::uppercase(&sub_cmd_name, &mut buf[len1..]).unwrap();

                            let cmd_name = if let Ok(s) = std::str::from_utf8(&buf[..len1 + len2]) {
                                s
                            } else {
                                return Err(RutinError::UnknownCmd);
                            };
                            match cmd_name {
                                $(
                                    $cmd_type2::NAME => $cmd_type2::apply($cmd, $handler).await,
                                )*
                                _ => Err(RutinError::UnknownCmd),
                            }
                        }
                    )*
                    _ => Err(RutinError::UnknownCmd),
                }
            }
        };
    }

    let mut cmd: CmdUnparsed = cmd_frame.try_into()?;

    let res = dispatch_command!(
        cmd,
        handler,
        /*********/
        /* admin */
        /*********/
        AclCat,
        AclDelUser,
        AclSetUser,
        AclUsers,
        AclWhoAmI,
        BgSave,
        PSync,
        ReplConf,
        ReplicaOf,
        /**************/
        /* connection */
        /**************/
        Auth,
        Echo,
        Ping,
        /************/
        /* keyspace */
        /************/
        Del,
        Dump,
        Exists,
        Expire,
        ExpireAt,
        ExpireTime,
        Keys,
        NBKeys,
        Persist,
        Pttl,
        Ttl,
        Type,
        /**********/
        /* string */
        /**********/
        Append,
        Decr,
        DecrBy,
        Get,
        GetRange,
        GetSet,
        Incr,
        IncrBy,
        MGet,
        MSet,
        MSetNx,
        Set,
        SetEx,
        SetNx,
        StrLen,
        /********/
        /* list */
        /********/
        BLMove,
        BLPop,
        LLen,
        LPop,
        LPos,
        LPush,
        NBLPop,
        /********/
        /* hash */
        /********/
        HDel,
        HExists,
        HGet,
        HSet,
        /**********/
        /* pubsub */
        /**********/
        Publish,
        Subscribe,
        Unsubscribe,
        /*************/
        /* scripting */
        /*************/
        Eval,
        EvalName;

       "CLIENT" => ClientTracking;

       "SCRIPT" => ScriptExists, ScriptFlush, ScriptRegister
    );

    match res {
        Ok(res) => Ok(res),
        Err(e) => {
            let frame = e.try_into()?; // 尝试将错误转换为RESP3
            Ok(Some(frame))
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct CmdUnparsed {
    inner: VecDeque<Resp3>,
}

impl CmdUnparsed {
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn get_uppercase<'a>(&mut self, index: usize, buf: &'a mut [u8]) -> Option<&'a [u8]> {
        match self.inner.get(index) {
            Some(Resp3::BlobString { inner: b, .. }) => {
                debug_assert!(b.len() <= buf.len());

                Some(util::get_uppercase(b, buf).unwrap())
            }
            _ => None,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &Bytes> {
        self.inner.iter().filter_map(|r| match r {
            Resp3::BlobString { inner, .. } => Some(inner),
            _ => None,
        })
    }
}

impl Iterator for CmdUnparsed {
    type Item = Bytes;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.pop_front().and_then(|r| match r {
            Resp3::BlobString { inner, .. } => Some(inner),
            _ => None,
        })
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

impl DoubleEndedIterator for CmdUnparsed {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.pop_back().and_then(|r| match r {
            Resp3::BlobString { inner, .. } => Some(inner),
            _ => None,
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

impl ExactSizeIterator for CmdUnparsed {}

impl TryFrom<Resp3> for CmdUnparsed {
    type Error = RutinError;

    #[inline]
    fn try_from(value: Resp3) -> Result<Self, Self::Error> {
        match value {
            Resp3::Array { inner, .. } => Ok(Self {
                inner: inner.into(),
            }),
            _ => Err(RutinError::Syntax),
        }
    }
}

impl From<CmdUnparsed> for Resp3 {
    #[inline]
    fn from(val: CmdUnparsed) -> Self {
        Resp3::new_array(val.inner)
    }
}

impl From<&[&str]> for CmdUnparsed {
    fn from(val: &[&str]) -> Self {
        Self {
            inner: val
                .iter()
                .map(|s| Resp3::new_blob_string(Bytes::copy_from_slice(s.as_bytes())))
                .collect(),
        }
    }
}
