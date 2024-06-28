use super::*;
use crate::{
    cmd::{CmdError, CmdExecutor, CmdType, CmdUnparsed, Err, ServerErrSnafu},
    conf::AccessControl,
    connection::AsyncStream,
    frame::Resp3,
    server::Handler,
    util::atoi,
};
use bytes::Bytes;
use snafu::ResultExt;
use tracing::instrument;

#[derive(Debug)]
pub struct Eval {
    script: Bytes,
    keys: Vec<Bytes>,
    args: Vec<Bytes>,
}

impl CmdExecutor for Eval {
    const NAME: &'static str = "EVAL";
    const TYPE: CmdType = CmdType::Other;
    const FLAG: CmdFlag = EVAL_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        let res = handler
            .shared
            .script()
            .lua_script
            .eval(handler, self.script, self.keys, self.args)
            .await
            .context(ServerErrSnafu)?;

        Ok(Some(res))
    }

    fn parse(args: &mut CmdUnparsed, _ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() < 2 {
            return Err(Err::WrongArgNum.into());
        }

        let script = args.next().unwrap();
        let numkeys = atoi::<usize>(&args.next().unwrap())
            .map_err(|_| CmdError::from("ERR value is not an integer or out of range"))?;

        let keys = args.take(numkeys).collect();
        let args = args.collect();

        Ok(Eval { script, keys, args })
    }
}

#[derive(Debug)]
pub struct EvalName {
    name: Bytes,
    keys: Vec<Bytes>,
    args: Vec<Bytes>,
}

impl CmdExecutor for EvalName {
    const NAME: &'static str = "EVALNAME";
    const TYPE: CmdType = CmdType::Other;
    const FLAG: CmdFlag = EVALNAME_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        let res = handler
            .shared
            .script()
            .lua_script
            .eval_name(handler, self.name, self.keys, self.args)
            .await?;

        Ok(Some(res))
    }

    fn parse(args: &mut CmdUnparsed, _ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() < 2 {
            return Err(Err::WrongArgNum.into());
        }

        let name = args.next().unwrap();
        let numkeys = atoi::<usize>(&args.next().unwrap())
            .map_err(|_| CmdError::from("ERR value is not an integer or out of range"))?;

        let keys = args.take(numkeys).collect();
        let args = args.collect();

        Ok(EvalName { name, keys, args })
    }
}

#[derive(Debug)]
pub struct ScriptExists {
    names: Vec<Bytes>,
}

impl CmdExecutor for ScriptExists {
    const NAME: &'static str = "SCRIPTEXISTS";
    const TYPE: CmdType = CmdType::Other;
    const FLAG: CmdFlag = SCRIPT_EXISTS_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        let res: Vec<_> = self
            .names
            .iter()
            .map(|name| {
                let res = handler.shared.script().lua_script.contain(name);
                Resp3::<Bytes, bytestring::ByteString>::new_boolean(res)
            })
            .collect();

        Ok(Some(Resp3::new_array(res)))
    }

    fn parse(args: &mut CmdUnparsed, _ac: &AccessControl) -> Result<Self, CmdError> {
        if args.is_empty() {
            return Err(Err::WrongArgNum.into());
        }

        Ok(ScriptExists {
            names: args.collect(),
        })
    }
}

#[derive(Debug)]
pub struct ScriptFlush {}

impl CmdExecutor for ScriptFlush {
    const NAME: &'static str = "SCRIPTFLUSH";
    const TYPE: CmdType = CmdType::Other;
    const FLAG: CmdFlag = SCRIPT_FLUSH_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        handler.shared.script().lua_script.flush();

        Ok(Some(Resp3::new_simple_string("OK".into())))
    }

    fn parse(args: &mut CmdUnparsed, _ac: &AccessControl) -> Result<Self, CmdError> {
        if !args.is_empty() {
            return Err(Err::WrongArgNum.into());
        }

        Ok(ScriptFlush {})
    }
}

#[derive(Debug)]
pub struct ScriptRegister {
    name: Bytes,
    script: Bytes,
}

impl CmdExecutor for ScriptRegister {
    const NAME: &'static str = "SCRIPTREGISTER";
    const TYPE: CmdType = CmdType::Other;
    const FLAG: CmdFlag = SCRIPT_REGISTER_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        handler
            .shared
            .script()
            .lua_script
            .register_script(self.name, self.script)?;

        Ok(Some(Resp3::new_simple_string("OK".into())))
    }

    fn parse(args: &mut CmdUnparsed, _ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() != 2 {
            return Err(Err::WrongArgNum.into());
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

    #[tokio::test]
    async fn eval_test() {
        let (mut handler, _) = Handler::new_fake();

        let eval = Eval::parse(
            &mut ["return 1", "0"].as_ref().into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let res = eval.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(res, Resp3::new_integer(1));

        let eval = Eval::parse(
            &mut ["redis.call('set', KEYS[1], ARGV[1])", "1", "key", "value"]
                .as_ref()
                .into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let res = eval.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(res, Resp3::new_simple_string("OK".into()));

        let eval = Eval::parse(
            &mut ["return redis.call('get', KEYS[1])", "1", "key"]
                .as_ref()
                .into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let res = eval.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(res, Resp3::new_blob_string("value".into()));
    }

    #[tokio::test]
    async fn script_test() {
        let (mut handler, _) = Handler::new_fake();

        let script_register = ScriptRegister::parse(
            &mut ["test", "redis.call('set', KEYS[1], ARGV[1])"]
                .as_ref()
                .into(),
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
            &mut ["test", "nothing"].as_ref().into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let res = script_exists.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(
            res,
            Resp3::new_array(vec![Resp3::new_boolean(true), Resp3::new_boolean(false)])
        );

        let eval_name = EvalName::parse(
            &mut ["test", "1", "key", "value"].as_ref().into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let res = eval_name.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(res, Resp3::new_simple_string("OK".into()));

        let script_flush =
            ScriptFlush::parse(&mut [].as_ref().into(), &AccessControl::new_loose()).unwrap();
        let res = script_flush.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(res, Resp3::new_simple_string("OK".into()));

        let script_exists =
            ScriptExists::parse(&mut ["test"].as_ref().into(), &AccessControl::new_loose())
                .unwrap();
        let res = script_exists.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(res, Resp3::new_array(vec![Resp3::new_boolean(false)]));
    }
}
