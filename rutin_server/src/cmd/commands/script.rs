use super::*;
use crate::{
    cmd::{CmdArg, CmdExecutor, CmdUnparsed},
    conf::AccessControl,
    error::{RutinError, RutinResult},
    frame::{CheapResp3, Resp3},
    server::{AsyncStream, Handler},
    util::atoi,
};
use std::fmt::Debug;
use tracing::instrument;

#[derive(Debug)]
pub struct Eval<A> {
    script: A,
    keys: Vec<A>,
    args: Vec<A>,
}

impl<A> CmdExecutor<A> for Eval<A>
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let res = handler
            .shared
            .script()
            .lua_script
            .eval(handler, self.script.as_ref(), self.keys, self.args)
            .await?;

        Ok(Some(res))
    }

    fn parse(mut args: CmdUnparsed<A>, _ac: &AccessControl) -> RutinResult<Self> {
        if args.len() < 2 {
            return Err(RutinError::WrongArgNum);
        }

        let script = args.next().unwrap();
        let numkeys = atoi::<usize>(&args.next().unwrap())?;

        if numkeys == 0 {
            return Ok(Eval {
                script,
                keys: vec![],
                args: args.collect(),
            });
        }

        let keys = args.by_ref().take(numkeys).collect();
        let args = args.collect();

        Ok(Eval { script, keys, args })
    }

    fn keys<'a>(&'a self) -> Option<impl Iterator<Item = &'a A>>
    where
        A: 'a,
    {
        Some(self.keys.iter())
    }
}

#[derive(Debug)]
pub struct EvalName<A> {
    name: A,
    keys: Vec<A>,
    args: Vec<A>,
}

impl<A> CmdExecutor<A> for EvalName<A>
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let res = handler
            .shared
            .script()
            .lua_script
            .eval_name(handler, self.name.as_ref(), self.keys, self.args)
            .await?;

        Ok(Some(res))
    }

    fn parse(mut args: CmdUnparsed<A>, _ac: &AccessControl) -> RutinResult<Self> {
        if args.len() < 2 {
            return Err(RutinError::WrongArgNum);
        }

        let name = args.next().unwrap();
        let numkeys = atoi::<usize>(args.next().unwrap().as_ref())?;

        if numkeys == 0 {
            return Ok(EvalName {
                name,
                keys: vec![],
                args: args.collect(),
            });
        }

        let keys = args.by_ref().take(numkeys).collect();
        let args = args.collect();

        Ok(EvalName { name, keys, args })
    }

    fn keys<'a>(&'a self) -> Option<impl Iterator<Item = &'a A>>
    where
        A: 'a,
    {
        Some(self.keys.iter())
    }
}

#[derive(Debug)]
pub struct ScriptExists<A> {
    names: Vec<A>,
}

impl<A> CmdExecutor<A> for ScriptExists<A>
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let res: Vec<_> = self
            .names
            .iter()
            .map(|name| {
                let res = handler.shared.script().lua_script.contain(name.as_ref());
                Resp3::new_boolean(res)
            })
            .collect();

        Ok(Some(Resp3::new_array(res)))
    }

    fn parse(args: CmdUnparsed<A>, _ac: &AccessControl) -> RutinResult<Self> {
        if args.is_empty() {
            return Err(RutinError::WrongArgNum);
        }

        Ok(ScriptExists {
            names: args.collect(),
        })
    }

}

#[derive(Debug)]
pub struct ScriptFlush;

impl<A> CmdExecutor<A> for ScriptFlush
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        handler.shared.script().lua_script.flush();

        Ok(Some(Resp3::new_simple_string("OK")))
    }

    fn parse(args: CmdUnparsed<A>, _ac: &AccessControl) -> RutinResult<Self> {
        if !args.is_empty() {
            return Err(RutinError::WrongArgNum);
        }

        Ok(ScriptFlush {})
    }

}

#[derive(Debug)]
pub struct ScriptRegister<A> {
    name: A,
    script: A,
}

impl<A> CmdExecutor<A> for ScriptRegister<A>
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        handler
            .shared
            .script()
            .lua_script
            .register_script(self.name, self.script)?;

        Ok(Some(Resp3::new_simple_string("OK")))
    }

    fn parse(mut args: CmdUnparsed<A>, _ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        }

        Ok(ScriptRegister {
            name: args.next().unwrap(),
            script: args.next().unwrap(),
        })
    }

}

#[cfg(test)]
mod cmd_script_tests {
    use super::*;
    use crate::util::gen_test_handler;

    #[tokio::test]
    async fn eval_test() {
        let mut handler = gen_test_handler();

        // 执行简单的返回值
        let eval_res = Eval::test(&["return 1", "0"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(eval_res, Resp3::new_integer(1));

        // 执行 redis.call 设置值
        let eval_res = Eval::test(
            &["redis.call('set', KEYS[1], ARGV[1])", "1", "key", "value"],
            &mut handler,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(eval_res, Resp3::new_simple_string("OK"));

        // 执行 redis.call 获取值
        let eval_res = Eval::test(
            &["return redis.call('get', KEYS[1])", "1", "key"],
            &mut handler,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(eval_res, Resp3::new_blob_string("value"));
    }

    #[tokio::test]
    async fn script_test() {
        let mut handler = gen_test_handler();

        // 注册脚本
        let script_register_res = ScriptRegister::test(
            &["test", "redis.call('set', KEYS[1], ARGV[1])"],
            &mut handler,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(script_register_res, Resp3::new_simple_string("OK"));

        // 检查脚本是否存在
        let script_exists_res = ScriptExists::test(&["test", "nothing"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            script_exists_res,
            Resp3::new_array(vec![Resp3::new_boolean(true), Resp3::new_boolean(false)])
        );

        // 通过脚本名执行脚本
        let eval_name_res = EvalName::test(&["test", "1", "key", "value"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(eval_name_res, Resp3::new_simple_string("OK"));

        // 刷新脚本缓存
        let script_flush_res = ScriptFlush::test(Default::default(), &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(script_flush_res, Resp3::new_simple_string("OK"));

        // 再次检查脚本是否存在
        let script_exists_res = ScriptExists::test(&["test"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            script_exists_res,
            Resp3::new_array(vec![Resp3::new_boolean(false)])
        );
    }
}
