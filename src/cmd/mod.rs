mod commands;
mod error;

pub use error::*;
use snafu::ResultExt;

use crate::{
    frame::{Bulks, Frame},
    server::Handler,
    shared::Shared,
};
use commands::*;
use tracing::instrument;

#[allow(async_fn_in_trait)]
pub trait CmdExecutor: Sized + std::fmt::Debug {
    const CMD_TYPE: CmdType;

    async fn apply(mut args: Bulks, handler: &mut Handler) -> Result<(), CmdError> {
        let cmd = Self::parse(&mut args)?;

        if let Some(res) = cmd.execute(handler).await? {
            // 命令执行成功，返回响应
            handler
                .conn
                .write_frame(&res)
                .await
                .context(ServerErrSnafu)?;
        }

        if Self::CMD_TYPE == CmdType::Write {
            handler.shared.wcmd_propagator().propergate(args.into_frame());
        }

        Ok(())
    }

    async fn execute(self, handler: &mut Handler) -> Result<Option<Frame<'static>>, CmdError> {
        self._execute(&handler.shared).await
    }

    async fn _execute(self, shared: &Shared) -> Result<Option<Frame<'static>>, CmdError>;

    fn parse(args: &mut Bulks) -> Result<Self, CmdError>;
}

#[derive(PartialEq)]
pub enum CmdType {
    Read,
    Write,
    Other,
}

#[inline]
#[instrument(level = "debug", skip(handler), err)]
pub async fn dispatch(cmd_frame: Frame<'static>, handler: &mut Handler) -> anyhow::Result<()> {
    // 处理错误，如果是ServerErr则向上传递，否则返回客户端响应
    if let Err(e) = _dispatch(cmd_frame, handler).await {
        let frame = e.try_into()?; // 尝试将错误转为Frame
        handler.conn.write_frame(&frame).await?;
    }

    Ok(())
}

#[inline]
#[instrument(level = "debug", skip(handler), err)]
pub async fn _dispatch(cmd_frame: Frame<'static>, handler: &mut Handler) -> Result<(), CmdError> {
    let mut cmd = cmd_frame.into_bulks().map_err(|_| Err::Syntax)?;
    let (cmd_name, len) = get_cmd_name_uppercase(&mut cmd)?;

    match &cmd_name[..len] {
        // commands::other
        // b"COMMAND" => _Command::apply(cmd_frame, handler).await?,
        b"BGSAVE" => BgSave::apply(cmd, handler).await?,
        b"ECHO" => Echo::apply(cmd, handler).await?,
        b"PING" => Ping::apply(cmd, handler).await?,
        b"CLIENT" => {
            let (subname, len) = get_cmd_sub_name_uppercase(&mut cmd)?;

            match &subname[..len] {
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
        _ => return Err(Err::UnknownCmd.into()),
    };

    Ok(())
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
