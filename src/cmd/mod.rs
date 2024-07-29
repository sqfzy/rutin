pub mod commands;

use std::{collections::VecDeque, num::NonZero};

use crate::{
    conf::AccessControl,
    connection::AsyncStream,
    error::{RutinError, RutinResult},
    frame::Resp3,
    server::Handler,
    util, CmdFlag,
};
use bytes::Bytes;
use commands::*;
use itertools::Itertools;
use tracing::instrument;

#[allow(async_fn_in_trait)]
pub trait CmdExecutor: Sized + std::fmt::Debug {
    const NAME: &'static str;
    const TYPE: CmdType;
    const FLAG: CmdFlag;

    #[inline]
    async fn apply(
        args: CmdUnparsed,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<Resp3>> {
        // 检查是否有权限执行该命令
        if handler.context.ac.is_forbidden_cmd(Self::FLAG) {
            return Err(RutinError::NoPermission);
        }

        let cmd = Self::parse(args, &handler.context.ac)?;

        let res = cmd.execute(handler).await?;

        if Self::TYPE == CmdType::Write {
            // 也许存在replicate需要传播
            // handler.shared.wcmd_propagator().may_propagate(args);
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

    fn parse(args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self>;
}

#[derive(PartialEq)]
pub enum CmdType {
    Read,
    Write,
    Other,
}

#[inline]
#[instrument(level = "debug", skip(handler), err, ret)]
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
        cmd, handler,
        // commands::other
        BgSave, Ping, Echo, Auth,

        // commands::key
        Del, Dump, Exists, Expire, ExpireAt, ExpireTime, Keys, NBKeys, Persist,
        Pttl, Ttl, Type,

        // commands::str
        Append, Decr, DecrBy, Get, GetRange, GetSet, Incr, IncrBy, MGet, MSet, MSetNx, Set, SetEx,
        SetNx,
        StrLen,
        // commands::list
        LLen, LPush, LPop, BLPop, LPos, NBLPop, BLMove,

        // commands::hash
        HDel, HExists, HGet, HSet,

        // commands::pub_sub
        Publish, Subscribe, Unsubscribe,

        // commands::script
        Eval, EvalName;

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

pub fn cmd_name_to_flag(cmd_name: &[u8]) -> RutinResult<CmdFlag> {
    macro_rules! cmd_name_to_flag {
        ( $cmd_name:expr,  $( $cmd_type:ident ),*) => {
            match $cmd_name {
                $(
                    $cmd_type::NAME => Ok($cmd_type::FLAG),
                )*
                _ => Err(RutinError::UnknownCmd),
            }
        };
    }

    let mut buf = [0; 32];
    let cmd_name = util::get_uppercase(cmd_name, &mut buf)?;
    let cmd_name = std::str::from_utf8(cmd_name)?;

    cmd_name_to_flag!(
        cmd_name,
        // commands::other
        BgSave,
        Ping,
        Echo,
        Auth,
        // commands::key
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
        // commands::str
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
        // commands::list
        LLen,
        LPush,
        LPop,
        BLPop,
        LPos,
        NBLPop,
        BLMove,
        // commands::hash
        HDel,
        HExists,
        HGet,
        HSet,
        // commands::pub_sub
        Publish,
        Subscribe,
        Unsubscribe,
        // commands::script
        Eval,
        EvalName,
        //
        ClientTracking,
        //
        ScriptExists,
        ScriptFlush,
        ScriptRegister
    )
}

pub fn flag_to_cmd_names(flag: CmdFlag) -> RutinResult<Vec<&'static str>> {
    let mut names = Vec::new();

    macro_rules! flag_to_cmd_names {
        ( $flag:expr,  $( $cmd_type:ident ),* ) => {
            match $flag {
                $(
                    $cmd_type::FLAG => names.push($cmd_type::NAME),
                )*
                _ => return Err(RutinError::UnknownCmd),
            }
        };
    }

    flag_to_cmd_names!(
        flag,
        // commands::other
        BgSave,
        Ping,
        Echo,
        Auth,
        // commands::key
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
        // commands::str
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
        // commands::list
        LLen,
        LPush,
        LPop,
        BLPop,
        LPos,
        NBLPop,
        BLMove,
        // commands::hash
        HDel,
        HExists,
        HGet,
        HSet,
        // commands::pub_sub
        Publish,
        Subscribe,
        Unsubscribe,
        // commands::script
        Eval,
        EvalName,
        //
        ClientTracking,
        //
        ScriptExists,
        ScriptFlush,
        ScriptRegister
    );

    Ok(names)
}

#[derive(Debug, Default)]
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

impl From<CmdUnparsed> for Resp3 {
    #[inline]
    fn from(val: CmdUnparsed) -> Self {
        Resp3::new_array(val.inner)
    }
}
