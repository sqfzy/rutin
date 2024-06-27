pub mod commands;
pub mod error;

pub use error::*;

use crate::{
    conf::AccessControl,
    connection::AsyncStream,
    frame::Resp3,
    server::{Handler, ServerError},
    CmdFlag,
};
use bytes::Bytes;
use commands::*;
use tracing::instrument;

#[allow(async_fn_in_trait)]
pub trait CmdExecutor: Sized + std::fmt::Debug {
    const NAME: &'static str;
    const TYPE: CmdType;
    const FLAG: CmdFlag;

    #[inline]
    async fn apply(
        mut args: CmdUnparsed,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        // 检查是否有权限执行该命令
        if handler.context.ac.is_forbidden_cmd(Self::FLAG) {
            return Err(Err::NoPermission.into());
        }

        let cmd = Self::parse(&mut args, &handler.context.ac)?;

        let res = cmd.execute(handler).await?;

        if Self::TYPE == CmdType::Write {
            // 也许存在replicate需要传播
            handler
                .shared
                .wcmd_propagator()
                .clone()
                .may_propagate(args, handler)
                .await;

            // TODO:
            // if let Some(rdb) =  handler.shared.conf().rdb.as_ref() {
            // if let Some(save) = rdb.save {
            //
            // }
            // }
        }

        Ok(res)
    }

    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError>;

    fn parse(args: &mut CmdUnparsed, ac: &AccessControl) -> Result<Self, CmdError>;
}

#[derive(PartialEq)]
pub enum CmdType {
    Read,
    Write,
    Other,
}

#[inline]
pub async fn dispatch(
    cmd_frame: Resp3,
    handler: &mut Handler<impl AsyncStream>,
) -> Result<Option<Resp3>, ServerError> {
    match _dispatch(cmd_frame, handler).await {
        Ok(res) => Ok(res),
        Err(e) => {
            let frame = e.try_into()?; // 尝试将错误转换为RESP3
            Ok(Some(frame))
        }
    }
}

#[inline]
#[instrument(level = "debug", skip(handler), err, ret)]
pub async fn _dispatch(
    cmd_frame: Resp3,
    handler: &mut Handler<impl AsyncStream>,
) -> Result<Option<Resp3>, CmdError> {
    macro_rules! dispatch_command {
        ( $cmd:expr, $handler:expr, $( $cmd_type:ident ),*; $( $cmd_group:expr => $( $cmd_type2:ident ),* );* ) => {
            {
                let mut cmd_name_buf = [0; 32];
                let len = $cmd.get_uppercase(0, &mut cmd_name_buf).ok_or(Err::Syntax)?;
                $cmd.advance(1);

                let cmd_name = if let Ok(s) = std::str::from_utf8(&cmd_name_buf[..len]) {
                    s
                } else {
                    return Err(Err::UnknownCmd.into());
                };

                match cmd_name {
                    $(
                        $cmd_type::NAME => $cmd_type::apply($cmd, $handler).await,
                    )*
                    $(
                        $cmd_group => {
                            let len2 = $cmd.get_uppercase(0, &mut cmd_name_buf[len..]).ok_or(Err::Syntax)?;
                            $cmd.advance(1);

                            let cmd_name = if let Ok(s) = std::str::from_utf8(&cmd_name_buf[..(len + len2)]) {
                                s
                            } else {
                                return Err(Err::UnknownCmd.into());
                            };
                            match cmd_name {
                                $(
                                    $cmd_type2::NAME => $cmd_type2::apply($cmd, $handler).await,
                                )*
                                _ => Err(Err::UnknownCmd.into()),
                            }
                        }
                    )*
                    _ => Err(Err::UnknownCmd.into()),
                }
            }
        };
    }

    let mut cmd: CmdUnparsed = cmd_frame.try_into()?;

    dispatch_command!(
        cmd,
        handler,
        // commands::other
        BgSave, Ping, Echo, Auth,

        // commands::key
        Del, Dump, Exists, Expire, ExpireAt, ExpireTime, Keys, NBKeys, Persist,
        Pttl, Ttl, Type,

        // commands::str
        Append, Decr, DecrBy, Get, GetRange, GetSet, Incr, IncrBy, MGet, MSet,
        MSetNx, Set, SetEx, SetNx, StrLen,

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
    )
}

pub fn cmd_name_to_flag(cmd_name: &mut [u8]) -> Result<CmdFlag, &'static str> {
    macro_rules! cmd_name_to_flag {
        ( $cmd_name:expr,  $( $cmd_type:ident ),*) => {
            match $cmd_name {
                $(
                    $cmd_type::NAME => Ok($cmd_type::FLAG),
                )*
                _ => Err("unknown command"),
            }
        };
    }

    cmd_name.make_ascii_uppercase();
    let cmd_name = std::str::from_utf8(cmd_name).map_err(|_| "unknown command")?;

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

#[derive(Debug)]
pub struct CmdUnparsed {
    inner: Vec<Resp3>,
    start: usize,
    end: usize,
}

impl CmdUnparsed {
    #[inline]
    pub fn len(&self) -> usize {
        // self.end - self.start + 1
        self.end.saturating_sub(self.start) + 1
    }

    pub const fn is_empty(&self) -> bool {
        self.start > self.end
    }

    pub fn get_uppercase(&self, index: usize, buf: &mut [u8]) -> Option<usize> {
        match self.inner.get(self.start + index) {
            Some(Resp3::BlobString { inner: b, .. }) => {
                buf[..b.len()].copy_from_slice(b);
                buf.make_ascii_uppercase();
                Some(b.len())
            }
            _ => None,
        }
    }

    pub fn next_back(&mut self) -> Option<Bytes> {
        match self.inner.get(self.end) {
            Some(Resp3::BlobString { inner: b, .. }) => {
                self.end -= 1;
                Some(b.clone())
            }
            _ => None,
        }
    }

    pub fn advance(&mut self, n: usize) {
        self.start += n;
    }
}

impl Default for CmdUnparsed {
    fn default() -> Self {
        Self {
            inner: Vec::new(),
            start: 1,
            end: 0,
        }
    }
}

impl Iterator for CmdUnparsed {
    type Item = Bytes;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.get(self.start) {
            Some(Resp3::BlobString { inner: b, .. }) => {
                self.start += 1;
                Some(b.clone())
            }
            _ => None,
        }
    }
}

impl TryFrom<Resp3> for CmdUnparsed {
    type Error = CmdError;

    #[inline]
    fn try_from(value: Resp3) -> Result<Self, Self::Error> {
        match value {
            Resp3::Array { inner, .. } => Ok(Self {
                start: 0,
                end: inner.len() - 1,
                // 不检查元素是否都为RESP3::Bulk，如果不是RESP3::Bulk，parse时会返回错误给客户端
                inner,
            }),
            _ => Err(Err::Other {
                message: "not an array frame".into(),
            }
            .into()),
        }
    }
}

impl From<&[&str]> for CmdUnparsed {
    fn from(val: &[&str]) -> Self {
        let inner: Vec<_> = val
            .iter()
            .map(|s| Resp3::new_blob_string(Bytes::copy_from_slice(s.as_bytes())))
            .collect();
        if inner.is_empty() {
            Self::default()
        } else {
            Self {
                end: inner.len() - 1,
                inner,
                start: 0,
            }
        }
    }
}

impl From<CmdUnparsed> for Resp3 {
    #[inline]
    fn from(val: CmdUnparsed) -> Self {
        Resp3::new_array(val.inner)
    }
}
