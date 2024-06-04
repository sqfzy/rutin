mod commands;
mod error;

pub use error::*;

use crate::{
    connection::AsyncStream,
    frame::RESP3,
    server::{Handler, ServerError},
    shared::Shared,
};
use bytes::{Bytes, BytesMut};
use commands::*;
use either::Either;
use std::marker::PhantomData;
use tracing::instrument;

#[allow(async_fn_in_trait)]
pub trait CmdExecutor: Sized + std::fmt::Debug {
    const CMD_TYPE: CmdType;

    #[inline]
    async fn apply(
        mut args: CmdUnparsed<Mutable>,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<RESP3>, CmdError> {
        let cmd = Self::parse(&mut args)?;

        let res = cmd.execute(handler).await?;

        if Self::CMD_TYPE == CmdType::Write {
            handler
                .shared
                .clone()
                .wcmd_propagator()
                .may_propagate(args, handler)
                .await;
        }

        Ok(res)
    }

    #[inline]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<RESP3>, CmdError> {
        self._execute(&handler.shared).await
    }

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError>;

    fn parse(args: &mut CmdUnparsed<Mutable>) -> Result<Self, CmdError>;
}

#[derive(PartialEq)]
pub enum CmdType {
    Read,
    Write,
    Other,
}

#[inline]
pub async fn dispatch(
    cmd_frame: RESP3,
    handler: &mut Handler<impl AsyncStream>,
) -> Result<Option<RESP3>, ServerError> {
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
    cmd_frame: RESP3,
    handler: &mut Handler<impl AsyncStream>,
) -> Result<Option<RESP3>, CmdError> {
    let mut cmd: CmdUnparsed<Mutable> = cmd_frame.try_into()?;
    let cmd_name = cmd.next_mut().ok_or(Err::Syntax)?;
    cmd_name.make_ascii_uppercase();

    let res = match cmd_name.as_ref() {
        // commands::other
        // b"COMMAND" => _Command::apply(cmd_frame, handler).await?,
        b"BGSAVE" => BgSave::apply(cmd, handler).await,
        b"ECHO" => Echo::apply(cmd, handler).await,
        b"PING" => Ping::apply(cmd, handler).await,
        b"CLIENT" => {
            let subname = cmd.next_mut().ok_or(Err::Syntax)?;
            subname.make_ascii_uppercase();

            match subname.as_ref() {
                b"TRACKING" => ClientTracking::apply(cmd, handler).await,
                _ => return Err(Err::UnknownCmd.into()),
            }
        }

        // commands::key
        b"DEL" => Del::apply(cmd, handler).await,
        b"EXISTS" => Exists::apply(cmd, handler).await,
        b"EXPIRE" => Expire::apply(cmd, handler).await,
        b"EXPIREAT" => ExpireAt::apply(cmd, handler).await,
        b"EXPIRETIME" => ExpireTime::apply(cmd, handler).await,
        b"KEYS" => Keys::apply(cmd, handler).await,
        b"PERSIST" => Persist::apply(cmd, handler).await,
        b"PTTL" => Pttl::apply(cmd, handler).await,
        b"TTL" => Ttl::apply(cmd, handler).await,
        b"TYPE" => Type::apply(cmd, handler).await,

        // commands::str
        b"APPEND" => Append::apply(cmd, handler).await,
        b"DECR" => Decr::apply(cmd, handler).await,
        b"DECRBY" => DecrBy::apply(cmd, handler).await,
        b"GET" => Get::apply(cmd, handler).await,
        b"GETRANGE" => GetRange::apply(cmd, handler).await,
        b"GETSET" => GetSet::apply(cmd, handler).await,
        b"INCR" => Incr::apply(cmd, handler).await,
        b"INCRBY" => IncrBy::apply(cmd, handler).await,
        b"MGET" => MGet::apply(cmd, handler).await,
        b"MSET" => MSet::apply(cmd, handler).await,
        b"MSETNX" => MSetNx::apply(cmd, handler).await,
        b"SET" => Set::apply(cmd, handler).await,
        b"SETEX" => SetEx::apply(cmd, handler).await,
        b"SETNX" => SetNx::apply(cmd, handler).await,
        b"STRLEN" => StrLen::apply(cmd, handler).await,

        // commands::list
        b"LLEN" => LLen::apply(cmd, handler).await,
        b"LPUSH" => LPush::apply(cmd, handler).await,
        b"LPOP" => LPop::apply(cmd, handler).await,
        b"BLPOP" => BLPop::apply(cmd, handler).await,
        b"NBLPOP" => NBLPop::apply(cmd, handler).await,
        b"BLMOVE" => BLMove::apply(cmd, handler).await,

        // commands::hash
        b"HDEL" => HDel::apply(cmd, handler).await,
        b"HEXISTS" => HExists::apply(cmd, handler).await,
        b"HGET" => HGet::apply(cmd, handler).await,
        b"HSET" => HSet::apply(cmd, handler).await,

        // commands::pub_sub
        b"PUBLISH" => Publish::apply(cmd, handler).await,
        b"SUBSCRIBE" => Subscribe::apply(cmd, handler).await,
        b"UNSUBSCRIBE" => Unsubscribe::apply(cmd, handler).await,
        _ => return Err(Err::UnknownCmd.into()),
    };

    res.map_err(Into::into)
}

pub struct Mutable;
pub struct UnMutable;

#[derive(Debug)]
pub struct CmdUnparsed<S = UnMutable> {
    inner: Vec<RESP3>,
    start: usize,
    end: usize,
    phantom: PhantomData<S>,
}

impl<S> CmdUnparsed<S> {
    #[inline]
    pub fn len(&self) -> usize {
        self.end - self.start + 1
    }

    pub fn is_empty(&self) -> bool {
        self.start > self.end
    }
}

impl CmdUnparsed<Mutable> {
    pub fn freeze(mut self) -> CmdUnparsed<UnMutable> {
        for i in 0..self.inner.len() {
            if let RESP3::Bulk(either) = &mut self.inner[i] {
                if let Either::Right(b) = either {
                    let b = b.split().freeze();
                    *either = Either::Left(b);
                }
            }
        }

        CmdUnparsed {
            inner: self.inner,
            start: self.start,
            end: self.end,
            phantom: PhantomData,
        }
    }

    #[inline]
    pub fn next_mut(&mut self) -> Option<&mut BytesMut> {
        if self.start > self.end {
            return None;
        }

        self.start += 1;
        match &mut self.inner[self.start - 1] {
            RESP3::Bulk(b) => match b {
                Either::Right(b) => Some(b),
                Either::Left(_) => unreachable!(),
            },
            _ => None,
        }
    }

    pub fn next_back_mut(&mut self) -> Option<&mut BytesMut> {
        if self.start > self.end {
            return None;
        }

        self.end -= 1;
        match &mut self.inner[self.end + 1] {
            RESP3::Bulk(b) => match b {
                Either::Right(b) => Some(b),
                Either::Left(_) => unreachable!(),
            },
            _ => None,
        }
    }

    pub fn next_back(&mut self) -> Option<Bytes> {
        if self.start > self.end {
            return None;
        }

        self.end -= 1;
        match &mut self.inner[self.end + 1] {
            RESP3::Bulk(either) => match either {
                Either::Right(b) => {
                    let b = b.split().freeze();
                    *either = Either::Left(b.clone());
                    Some(b)
                }
                Either::Left(_) => unreachable!(),
            },
            _ => None,
        }
    }

    pub fn get(&self, idx: usize) -> Option<&[u8]> {
        if idx > self.end {
            return None;
        }

        match &self.inner[self.start + idx] {
            RESP3::Bulk(either) => match either {
                Either::Left(b) => Some(b),
                Either::Right(b) => Some(b),
            },
            _ => None,
        }
    }
}

impl Iterator for CmdUnparsed<Mutable> {
    type Item = Bytes;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.start > self.end {
            return None;
        }

        self.start += 1;
        match &mut self.inner[self.start - 1] {
            RESP3::Bulk(either) => match either {
                Either::Right(b) => {
                    let b = b.split().freeze();
                    *either = Either::Left(b.clone());
                    Some(b)
                }
                Either::Left(_) => unreachable!(),
            },
            _ => None,
        }
    }
}

impl TryFrom<RESP3> for CmdUnparsed<Mutable> {
    type Error = CmdError;

    #[inline]
    fn try_from(value: RESP3) -> Result<Self, Self::Error> {
        match value {
            RESP3::Array(arr) => Ok(Self {
                start: 0,
                end: arr.len() - 1,
                // 不检查元素是否都为RESP3::Bulk，如果不是RESP3::Bulk，parse时会返回错误给客户端
                inner: arr,
                phantom: PhantomData,
            }),
            _ => Err(Err::Other {
                message: "not an array frame".to_string(),
            }
            .into()),
        }
    }
}

impl From<CmdUnparsed<Mutable>> for RESP3 {
    fn from(value: CmdUnparsed<Mutable>) -> Self {
        RESP3::Array(value.inner)
    }
}

impl From<&[&'static str]> for CmdUnparsed<Mutable> {
    fn from(value: &[&'static str]) -> Self {
        let mut inner = Vec::with_capacity(value.len());
        for s in value {
            inner.push(RESP3::Bulk(Either::Right(BytesMut::from(s.as_bytes()))));
        }

        Self {
            start: 0,
            end: inner.len() - 1,
            inner,
            phantom: PhantomData,
        }
    }
}

impl CmdUnparsed<UnMutable> {
    pub fn next_back(&mut self) -> Option<Bytes> {
        if self.start > self.end {
            return None;
        }

        self.end -= 1;
        match &self.inner[self.end + 1] {
            RESP3::Bulk(b) => match b {
                Either::Left(b) => Some(b.clone()),
                Either::Right(_) => unreachable!(),
            },
            _ => None,
        }
    }
}

impl Iterator for CmdUnparsed<UnMutable> {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start > self.end {
            return None;
        }

        self.start += 1;
        match &self.inner[self.start - 1] {
            RESP3::Bulk(b) => match b {
                Either::Left(b) => Some(b.clone()),
                Either::Right(_) => unreachable!(),
            },

            _ => None,
        }
    }
}

impl From<CmdUnparsed<UnMutable>> for RESP3 {
    fn from(val: CmdUnparsed<UnMutable>) -> Self {
        RESP3::Array(val.inner)
    }
}

impl From<&[&'static str]> for CmdUnparsed<UnMutable> {
    fn from(value: &[&'static str]) -> Self {
        let mut inner = Vec::with_capacity(value.len());
        for s in value {
            inner.push(RESP3::Bulk(Either::Left(Bytes::from_static(s.as_bytes()))));
        }

        Self {
            start: 0,
            end: inner.len() - 1,
            inner,
            phantom: PhantomData,
        }
    }
}
