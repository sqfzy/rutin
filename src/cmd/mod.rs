//! 执行命令时不应该长时间持有某个键的锁，否则会导致其他命令无法执行

mod commands;
mod error;

pub use error::*;

use crate::{
    connection::AsyncStream,
    frame::Resp3,
    server::{Handler, ServerError},
    shared::Shared,
};
use bytes::Bytes;
use commands::*;
use tracing::instrument;

#[allow(async_fn_in_trait)]
pub trait CmdExecutor: Sized + std::fmt::Debug {
    const CMD_TYPE: CmdType;

    #[inline]
    async fn apply(
        mut args: CmdUnparsed,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        let cmd = Self::parse(&mut args)?;

        let res = cmd.execute(handler).await?;

        if Self::CMD_TYPE == CmdType::Write {
            handler
                .shared
                .wcmd_propagator()
                .clone()
                .may_propagate(args, handler)
                .await;
        }

        Ok(res)
    }

    #[inline]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        self._execute(&handler.shared).await
    }

    async fn _execute(self, shared: &Shared) -> Result<Option<Resp3>, CmdError>;

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

#[allow(clippy::type_complexity)]
#[inline]
#[instrument(level = "debug", skip(handler), err, ret)]
pub async fn _dispatch(
    cmd_frame: Resp3,
    handler: &mut Handler<impl AsyncStream>,
) -> Result<Option<Resp3>, CmdError> {
    let mut cmd: CmdUnparsed = cmd_frame.try_into()?;
    let mut cmd_name = [0; 16];
    let len = cmd.get_uppercase(0, &mut cmd_name).ok_or(Err::Syntax)?;
    cmd.advance(1);

    let res = match &cmd_name[..len] {
        // commands::other
        // b"COMMAND" => _Command::apply(cmd_frame, handler).await?,
        b"BGSAVE" => BgSave::apply(cmd, handler).await?,
        b"ECHO" => Echo::apply(cmd, handler).await?,
        b"PING" => Ping::apply(cmd, handler).await?,
        b"CLIENT" => {
            let len = cmd.get_uppercase(0, &mut cmd_name).ok_or(Err::Syntax)?;
            cmd.advance(1);

            match &cmd_name[..len] {
                b"TRACKING" => ClientTracking::apply(cmd, handler).await?,
                _ => return Err(Err::UnknownCmd.into()),
            }
        }

        // commands::key
        b"DEL" => Del::apply(cmd, handler).await?,
        b"EXISTS" => Exists::apply(cmd, handler).await?,
        b"EXPIRE" => Expire::apply(cmd, handler).await?,
        b"EXPIREAT" => ExpireAt::apply(cmd, handler).await?,
        b"EXPIRETIME" => ExpireTime::apply(cmd, handler).await?,
        b"KEYS" => Keys::apply(cmd, handler).await?,
        b"PERSIST" => Persist::apply(cmd, handler).await?,
        b"PTTL" => Pttl::apply(cmd, handler).await?,
        b"TTL" => Ttl::apply(cmd, handler).await?,
        b"TYPE" => Type::apply(cmd, handler).await?,

        // commands::str
        b"APPEND" => Append::apply(cmd, handler).await?,
        b"DECR" => Decr::apply(cmd, handler).await?,
        b"DECRBY" => DecrBy::apply(cmd, handler).await?,
        b"GET" => Get::apply(cmd, handler).await?,
        b"GETRANGE" => GetRange::apply(cmd, handler).await?,
        b"GETSET" => GetSet::apply(cmd, handler).await?,
        b"INCR" => Incr::apply(cmd, handler).await?,
        b"INCRBY" => IncrBy::apply(cmd, handler).await?,
        b"MGET" => MGet::apply(cmd, handler).await?,
        b"MSET" => MSet::apply(cmd, handler).await?,
        b"MSETNX" => MSetNx::apply(cmd, handler).await?,
        b"SET" => Set::apply(cmd, handler).await?,
        b"SETEX" => SetEx::apply(cmd, handler).await?,
        b"SETNX" => SetNx::apply(cmd, handler).await?,
        b"STRLEN" => StrLen::apply(cmd, handler).await?,

        // commands::list
        b"LLEN" => LLen::apply(cmd, handler).await?,
        b"LPUSH" => LPush::apply(cmd, handler).await?,
        b"LPOP" => LPop::apply(cmd, handler).await?,
        b"BLPOP" => BLPop::apply(cmd, handler).await?,
        b"NBLPOP" => NBLPop::apply(cmd, handler).await?,
        b"BLMOVE" => BLMove::apply(cmd, handler).await?,

        // commands::hash
        b"HDEL" => HDel::apply(cmd, handler).await?,
        b"HEXISTS" => HExists::apply(cmd, handler).await?,
        b"HGET" => HGet::apply(cmd, handler).await?,
        b"HSET" => HSet::apply(cmd, handler).await?,

        // commands::pub_sub
        b"PUBLISH" => Publish::apply(cmd, handler).await?,
        b"SUBSCRIBE" => Subscribe::apply(cmd, handler).await?,
        b"UNSUBSCRIBE" => Unsubscribe::apply(cmd, handler).await?,

        // commands::script
        b"EVAL" => Eval::apply(cmd, handler).await?,
        b"EVALNAME" => EvalName::apply(cmd, handler).await?,
        b"SCRIPT" => {
            let len = cmd.get_uppercase(0, &mut cmd_name).ok_or(Err::Syntax)?;
            cmd.advance(1);

            match &cmd_name[..len] {
                b"EXISTS" => ScriptExists::apply(cmd, handler).await?,
                b"FLUSH" => ScriptFlush::apply(cmd, handler).await?,
                b"REGISTER" => ScriptRegister::apply(cmd, handler).await?,
                _ => return Err(Err::UnknownCmd.into()),
            }
        }
        _ => return Err(Err::UnknownCmd.into()),
    };

    Ok(res)
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
        self.end - self.start + 1
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
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
