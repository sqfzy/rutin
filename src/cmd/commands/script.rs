use crate::{
    cmd::{CmdExecutor, CmdUnparsed},
    conf::AccessControl,
    error::{RutinError, RutinResult},
    frame::{CheapResp3, Resp3},
    server::{AsyncStream, Handler},
    util::{atoi, StaticBytes},
};
use bytes::Bytes;
use tracing::instrument;

#[derive(Debug)]
pub struct Eval {
    script: StaticBytes,
    keys: Vec<StaticBytes>,
    args: Vec<StaticBytes>,
}

impl CmdExecutor for Eval {
    #[instrument(level = "debug", skip(handler), ret, err(level = "warn"))]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let res = handler
            .shared
            .script()
            .lua_script
            .eval(handler, &self.script, self.keys, self.args)
            .await?;

        Ok(Some(res))
    }

    fn parse(mut args: CmdUnparsed, _ac: &AccessControl) -> RutinResult<Self> {
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
}

#[derive(Debug)]
pub struct EvalName {
    name: StaticBytes,
    keys: Vec<StaticBytes>,
    args: Vec<StaticBytes>,
}

impl CmdExecutor for EvalName {
    #[instrument(level = "debug", skip(handler), ret, err(level = "warn"))]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let res = handler
            .shared
            .script()
            .lua_script
            .eval_name(handler, &self.name, self.keys, self.args)
            .await?;

        Ok(Some(res))
    }

    fn parse(mut args: CmdUnparsed, _ac: &AccessControl) -> RutinResult<Self> {
        if args.len() < 2 {
            return Err(RutinError::WrongArgNum);
        }

        let name = args.next().unwrap();
        let numkeys = atoi::<usize>(&args.next().unwrap())?;

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
}

#[derive(Debug)]
pub struct ScriptExists {
    names: Vec<StaticBytes>,
}

impl CmdExecutor for ScriptExists {
    #[instrument(level = "debug", skip(handler), ret, err(level = "warn"))]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let res: Vec<_> = self
            .names
            .iter()
            .map(|name| {
                let res = handler.shared.script().lua_script.contain(name.as_ref());
                Resp3::<Bytes, bytestring::ByteString>::new_boolean(res)
            })
            .collect();

        Ok(Some(Resp3::new_array(res)))
    }

    fn parse(args: CmdUnparsed, _ac: &AccessControl) -> RutinResult<Self> {
        if args.is_empty() {
            return Err(RutinError::WrongArgNum);
        }

        Ok(ScriptExists {
            names: args.collect(),
        })
    }
}

#[derive(Debug)]
pub struct ScriptFlush {}

impl CmdExecutor for ScriptFlush {
    #[instrument(level = "debug", skip(handler), ret, err(level = "warn"))]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        handler.shared.script().lua_script.flush();

        Ok(Some(Resp3::new_simple_string("OK".into())))
    }

    fn parse(args: CmdUnparsed, _ac: &AccessControl) -> RutinResult<Self> {
        if !args.is_empty() {
            return Err(RutinError::WrongArgNum);
        }

        Ok(ScriptFlush {})
    }
}

#[derive(Debug)]
pub struct ScriptRegister {
    name: StaticBytes,
    script: StaticBytes,
}

impl CmdExecutor for ScriptRegister {
    #[instrument(level = "debug", skip(handler), ret, err(level = "warn"))]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        handler
            .shared
            .script()
            .lua_script
            .register_script(&self.name, &self.script)?;

        Ok(Some(Resp3::new_simple_string("OK".into())))
    }

    fn parse(mut args: CmdUnparsed, _ac: &AccessControl) -> RutinResult<Self> {
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
    use crate::{cmd::gen_cmdunparsed_test, util::gen_test_handler};

    use super::*;

    #[tokio::test]
    async fn eval_test() {
        let mut handler = gen_test_handler();

        let eval = Eval::parse(
            gen_cmdunparsed_test(["return 1", "0"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let res = eval.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(res, Resp3::new_integer(1));

        let eval = Eval::parse(
            gen_cmdunparsed_test(
                ["redis.call('set', KEYS[1], ARGV[1])", "1", "key", "value"].as_ref(),
            ),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let res = eval.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(res, Resp3::new_simple_string("OK".into()));

        let eval = Eval::parse(
            gen_cmdunparsed_test(["return redis.call('get', KEYS[1])", "1", "key"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let res = eval.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(res, Resp3::new_blob_string("value".into()));
    }

    #[tokio::test]
    async fn script_test() {
        let mut handler = gen_test_handler();

        let script_register = ScriptRegister::parse(
            gen_cmdunparsed_test(["test", "redis.call('set', KEYS[1], ARGV[1])"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let res = script_register
            .execute(&mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(res, Resp3::new_simple_string("OK".into()));

        let script_exists = ScriptExists::parse(
            gen_cmdunparsed_test(["test", "nothing"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let res = script_exists.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(
            res,
            Resp3::new_array(vec![Resp3::new_boolean(true), Resp3::new_boolean(false)])
        );

        let eval_name = EvalName::parse(
            gen_cmdunparsed_test(["test", "1", "key", "value"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let res = eval_name.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(res, Resp3::new_simple_string("OK".into()));

        let script_flush =
            ScriptFlush::parse(Default::default(), &AccessControl::new_loose()).unwrap();
        let res = script_flush.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(res, Resp3::new_simple_string("OK".into()));

        let script_exists = ScriptExists::parse(
            gen_cmdunparsed_test(["test"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let res = script_exists.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(res, Resp3::new_array(vec![Resp3::new_boolean(false)]));
    }
}
