mod commands;
mod error;

use bytes::{Bytes, BytesMut};
pub use error::*;

use crate::{
    connection::AsyncStream,
    frame::RESP3,
    server::{Handler, ServerError},
    shared::Shared,
};
// use commands::*;
use tracing::instrument;

#[allow(async_fn_in_trait)]
pub trait CmdExecutor<B = Bytes, S = String>: Sized + std::fmt::Debug
where
    B: AsRef<[u8]>,
    S: AsRef<str>,
{
    const CMD_TYPE: CmdType;

    async fn apply(
        mut args: CmdUnparsed,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<RESP3<B, S>>, CmdError> {
        let cmd = Self::parse(&mut args)?;

        let res = cmd.execute(handler).await?;

        if Self::CMD_TYPE == CmdType::Write {
            handler
                .shared
                .clone()
                .wcmd_propagator()
                .propergate(args.into_frame(), handler)
                .await;
        }

        Ok(res)
    }

    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<RESP3<B, S>>, CmdError> {
        self._execute(&handler.shared).await
    }

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3<B, S>>, CmdError>;

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
#[instrument(level = "debug", skip(handler), err)]
pub async fn _dispatch(
    cmd_frame: RESP3,
    handler: &mut Handler<impl AsyncStream>,
) -> Result<Option<RESP3>, CmdError> {
    let mut cmd = cmd_frame.try_into()?;
    let (cmd_name, len) = get_cmd_name_uppercase(&mut cmd)?;

    let res = match &cmd_name[..len] {
        // commands::other
        // b"COMMAND" => _Command::apply(cmd_frame, handler).await?,
        b"BGSAVE" => BgSave::apply(cmd, handler).await,
        b"ECHO" => Echo::apply(cmd, handler).await,
        b"PING" => Ping::apply(cmd, handler).await,
        b"CLIENT" => {
            let (subname, len) = get_cmd_sub_name_uppercase(&mut cmd)?;

            match &subname[..len] {
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

fn get_cmd_name_uppercase(cmd: &mut Bulks) -> Result<([u8; 16], usize), CmdError> {
    let name_bytes = cmd.pop_front().ok_or(CmdError::from(Err::Syntax))?;

    let mut cmd_name = [0u8; 16];
    cmd_name[..name_bytes.len()].copy_from_slice(&name_bytes);
    cmd_name[..name_bytes.len()].make_ascii_uppercase();
    Ok((cmd_name, name_bytes.len()))
}

fn get_cmd_sub_name_uppercase(cmd: &mut Bulks) -> Result<([u8; 16], usize), CmdError> {
    let name_bytes = cmd.pop_front().ok_or(CmdError::from(Err::Syntax))?;

    let mut cmd_name = [0u8; 16];
    cmd_name[..name_bytes.len()].copy_from_slice(&name_bytes);
    cmd_name[..name_bytes.len()].make_ascii_uppercase();
    Ok((cmd_name, name_bytes.len()))
}

#[derive(Debug)]
pub struct CmdUnparsed<B = Bytes>
where
    B: AsRef<[u8]>,
{
    inner: Vec<RESP3<B>>,
    start: usize,
    end: usize,
}

impl<B: AsRef<[u8]>> CmdUnparsed<B> {
    #[inline]
    pub fn len(&self) -> usize {
        self.end - self.start + 1
    }

    pub fn is_empty(&self) -> bool {
        self.start > self.end
    }

    pub fn get(&self, idx: usize) -> Option<&B> {
        if idx >= self.len() {
            return None;
        }

        match &self.inner[self.start + idx] {
            RESP3::Bulk(b) => Some(b),
            _ => None,
        }
    }

    pub fn get_mut(&mut self, idx: usize) -> Option<&mut B> {
        if idx >= self.len() {
            return None;
        }

        match &mut self.inner[self.start + idx] {
            RESP3::Bulk(b) => Some(b),
            _ => None,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &B> {
        self.inner[self.start..=self.end]
            .iter()
            .filter_map(|v| match v {
                RESP3::Bulk(b) => Some(b),
                _ => None,
            })
    }
}

impl CmdUnparsed {
    pub fn next_back(&mut self) -> Option<Bytes> {
        if self.start > self.end {
            return None;
        }

        self.end -= 1;
        match &self.inner[self.end + 1] {
            RESP3::Bulk(b) => Some(b.clone()),
            _ => None,
        }
    }
}

impl Into<RESP3> for CmdUnparsed {
    fn into(self) -> RESP3 {
        RESP3::Array(self.inner)
    }
}

impl TryFrom<RESP3> for CmdUnparsed {
    type Error = CmdError;

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

impl Iterator for CmdUnparsed {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start > self.end {
            return None;
        }

        self.start += 1;
        match &self.inner[self.start - 1] {
            RESP3::Bulk(b) => Some(b.clone()),
            _ => None,
        }
    }
}
