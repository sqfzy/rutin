mod commands;
mod error;

use either::Either::{self, Left, Right};
pub use error::*;

use crate::{
    connection::AsyncStream,
    frame::RESP3,
    server::{Handler, ServerError},
    shared::Shared,
};
use bytes::Bytes;
use commands::*;
use tracing::instrument;

#[allow(async_fn_in_trait)]
pub trait CmdExecutor: Sized + std::fmt::Debug {
    const CMD_TYPE: CmdType;
    type RESP3 = RESP3<Bytes, String>;

    #[inline]
    async fn apply(
        mut args: CmdUnparsed,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Self::RESP3>, CmdError> {
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
    ) -> Result<Option<Self::RESP3>, CmdError> {
        self._execute(&handler.shared).await
    }

    async fn _execute(self, shared: &Shared) -> Result<Option<Self::RESP3>, CmdError>;

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError>;
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
) -> Result<Either<Option<RESP3>, Option<RESP3<Bytes, &'static str>>>, ServerError> {
    match _dispatch(cmd_frame, handler).await {
        Ok(res) => Ok(res),
        Err(e) => {
            let frame = e.try_into()?; // 尝试将错误转换为RESP3
            Ok(Left(Some(frame)))
        }
    }
}

#[allow(clippy::type_complexity)]
#[inline]
#[instrument(level = "debug", skip(handler), err, ret)]
pub async fn _dispatch(
    cmd_frame: RESP3,
    handler: &mut Handler<impl AsyncStream>,
) -> Result<Either<Option<RESP3>, Option<RESP3<Bytes, &'static str>>>, CmdError> {
    let mut cmd: CmdUnparsed = cmd_frame.try_into()?;
    let mut cmd_name = [0; 16];
    let len = cmd.get_uppercase(0, &mut cmd_name).ok_or(Err::Syntax)?;
    cmd.advance(1);

    let res = match &cmd_name[..len] {
        // commands::other
        // b"COMMAND" => _Command::apply(cmd_frame, handler).await?,
        b"BGSAVE" => Left(BgSave::apply(cmd, handler).await?),
        b"ECHO" => Left(Echo::apply(cmd, handler).await?),
        b"PING" => Right(Ping::apply(cmd, handler).await?),
        b"CLIENT" => {
            let len = cmd.get_uppercase(0, &mut cmd_name).ok_or(Err::Syntax)?;
            cmd.advance(1);

            match &cmd_name[..len] {
                b"TRACKING" => Right(ClientTracking::apply(cmd, handler).await?),
                _ => return Err(Err::UnknownCmd.into()),
            }
        }

        // commands::key
        b"DEL" => Left(Del::apply(cmd, handler).await?),
        b"EXISTS" => Left(Exists::apply(cmd, handler).await?),
        b"EXPIRE" => Left(Expire::apply(cmd, handler).await?),
        b"EXPIREAT" => Left(ExpireAt::apply(cmd, handler).await?),
        b"EXPIRETIME" => Left(ExpireTime::apply(cmd, handler).await?),
        b"KEYS" => Left(Keys::apply(cmd, handler).await?),
        b"PERSIST" => Left(Persist::apply(cmd, handler).await?),
        b"PTTL" => Left(Pttl::apply(cmd, handler).await?),
        b"TTL" => Left(Ttl::apply(cmd, handler).await?),
        b"TYPE" => Left(Type::apply(cmd, handler).await?),

        // commands::str
        b"APPEND" => Left(Append::apply(cmd, handler).await?),
        b"DECR" => Left(Decr::apply(cmd, handler).await?),
        b"DECRBY" => Left(DecrBy::apply(cmd, handler).await?),
        b"GET" => Left(Get::apply(cmd, handler).await?),
        b"GETRANGE" => Left(GetRange::apply(cmd, handler).await?),
        b"GETSET" => Left(GetSet::apply(cmd, handler).await?),
        b"INCR" => Left(Incr::apply(cmd, handler).await?),
        b"INCRBY" => Left(IncrBy::apply(cmd, handler).await?),
        b"MGET" => Left(MGet::apply(cmd, handler).await?),
        b"MSET" => Right(MSet::apply(cmd, handler).await?),
        b"MSETNX" => Left(MSetNx::apply(cmd, handler).await?),
        b"SET" => Right(Set::apply(cmd, handler).await?),
        b"SETEX" => Right(SetEx::apply(cmd, handler).await?),
        b"SETNX" => Left(SetNx::apply(cmd, handler).await?),
        b"STRLEN" => Left(StrLen::apply(cmd, handler).await?),

        // commands::list
        b"LLEN" => Left(LLen::apply(cmd, handler).await?),
        b"LPUSH" => Left(LPush::apply(cmd, handler).await?),
        b"LPOP" => Left(LPop::apply(cmd, handler).await?),
        b"BLPOP" => Left(BLPop::apply(cmd, handler).await?),
        b"NBLPOP" => Left(NBLPop::apply(cmd, handler).await?),
        b"BLMOVE" => Left(BLMove::apply(cmd, handler).await?),

        // commands::hash
        b"HDEL" => Left(HDel::apply(cmd, handler).await?),
        b"HEXISTS" => Left(HExists::apply(cmd, handler).await?),
        b"HGET" => Left(HGet::apply(cmd, handler).await?),
        b"HSET" => Left(HSet::apply(cmd, handler).await?),

        // commands::pub_sub
        b"PUBLISH" => Left(Publish::apply(cmd, handler).await?),
        b"SUBSCRIBE" => Left(Subscribe::apply(cmd, handler).await?),
        b"UNSUBSCRIBE" => Left(Unsubscribe::apply(cmd, handler).await?),
        _ => return Err(Err::UnknownCmd.into()),
    };

    Ok(res)
}

#[derive(Debug)]
pub struct CmdUnparsed {
    inner: Vec<RESP3<Bytes, String>>,
    start: usize,
    end: usize,
}

impl CmdUnparsed {
    #[inline]
    pub fn len(&self) -> usize {
        self.end - self.start + 1
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.start > self.end
    }

    pub fn get_uppercase(&self, index: usize, buf: &mut [u8]) -> Option<usize> {
        match self.inner.get(self.start + index) {
            Some(RESP3::Bulk(b)) => {
                buf[..b.len()].copy_from_slice(b);
                buf.make_ascii_uppercase();
                Some(b.len())
            }
            _ => None,
        }
    }

    pub fn next_back(&mut self) -> Option<Bytes> {
        match self.inner.get(self.end) {
            Some(RESP3::Bulk(b)) => {
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
            Some(RESP3::Bulk(b)) => {
                self.start += 1;
                Some(b.clone())
            }
            _ => None,
        }
    }
}

impl TryFrom<RESP3> for CmdUnparsed {
    type Error = CmdError;

    #[inline]
    fn try_from(value: RESP3) -> Result<Self, Self::Error> {
        match value {
            RESP3::Array(arr) => Ok(Self {
                start: 0,
                end: arr.len() - 1,
                // 不检查元素是否都为RESP3::Bulk，如果不是RESP3::Bulk，parse时会返回错误给客户端
                inner: arr,
            }),
            _ => Err(Err::Other {
                message: "not an array frame".to_string(),
            }
            .into()),
        }
    }
}

impl From<&[&str]> for CmdUnparsed {
    fn from(val: &[&str]) -> Self {
        let inner: Vec<_> = val
            .iter()
            .map(|s| RESP3::Bulk(Bytes::copy_from_slice(s.as_bytes())))
            .collect();
        Self {
            end: inner.len() - 1,
            inner,
            start: 0,
        }
    }
}

impl From<CmdUnparsed> for RESP3 {
    #[inline]
    fn from(val: CmdUnparsed) -> Self {
        RESP3::Array(val.inner)
    }
}
