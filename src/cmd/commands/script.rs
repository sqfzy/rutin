use crate::{
    cmd::{CmdError, CmdExecutor, CmdType, CmdUnparsed, Err, ServerErrSnafu},
    frame::Resp3,
    shared::Shared,
    util::atoi,
};
use bytes::Bytes;
use snafu::ResultExt;

#[derive(Debug)]
pub struct Eval {
    script: Bytes,
    keys: Vec<Bytes>,
    args: Vec<Bytes>,
}

impl CmdExecutor for Eval {
    const CMD_TYPE: CmdType = CmdType::Other;

    async fn _execute(self, shared: &Shared) -> Result<Option<Resp3>, CmdError> {
        let res = shared
            .script()
            .lua_script
            .eval(shared.clone(), self.script, self.keys, self.args)
            .await
            .context(ServerErrSnafu)?;

        Ok(Some(res))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
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
    const CMD_TYPE: CmdType = CmdType::Other;

    async fn _execute(self, shared: &Shared) -> Result<Option<Resp3>, CmdError> {
        let res = shared
            .script()
            .lua_script
            .eval_name(shared.clone(), self.name, self.keys, self.args)
            .await?;

        Ok(Some(res))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
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
    const CMD_TYPE: CmdType = CmdType::Other;

    async fn _execute(self, shared: &Shared) -> Result<Option<Resp3>, CmdError> {
        let res: Vec<_> = self
            .names
            .iter()
            .map(|name| {
                let res = shared.script().lua_script.contain(name);
                Resp3::<Bytes, bytestring::ByteString>::new_boolean(res)
            })
            .collect();

        Ok(Some(Resp3::new_array(res)))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
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
    const CMD_TYPE: CmdType = CmdType::Other;

    async fn _execute(self, shared: &Shared) -> Result<Option<Resp3>, CmdError> {
        shared.script().lua_script.flush();

        Ok(Some(Resp3::new_simple_string("OK".into())))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
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
    const CMD_TYPE: CmdType = CmdType::Other;

    async fn _execute(self, shared: &Shared) -> Result<Option<Resp3>, CmdError> {
        shared
            .script()
            .lua_script
            .register_script(self.name, self.script)?;

        Ok(Some(Resp3::new_simple_string("OK".into())))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
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
        let shared = Shared::default();

        let eval = Eval::parse(&mut ["return 1", "0"].as_ref().into()).unwrap();
        let res = eval._execute(&shared).await.unwrap().unwrap();
        assert_eq!(res, Resp3::new_integer(1));

        let eval = Eval::parse(
            &mut ["redis.call('set', KEYS[1], ARGV[1])", "1", "key", "value"]
                .as_ref()
                .into(),
        )
        .unwrap();
        let res = eval._execute(&shared).await.unwrap().unwrap();
        assert_eq!(res, Resp3::new_simple_string("OK".into()));

        let eval = Eval::parse(
            &mut ["return redis.call('get', KEYS[1])", "1", "key"]
                .as_ref()
                .into(),
        )
        .unwrap();
        let res = eval._execute(&shared).await.unwrap().unwrap();
        assert_eq!(res, Resp3::new_blob("value".into()));
    }

    #[tokio::test]
    async fn script_test() {
        let shared = Shared::default();

        let script_register = ScriptRegister::parse(
            &mut ["test", "redis.call('set', KEYS[1], ARGV[1])"]
                .as_ref()
                .into(),
        )
        .unwrap();
        let res = script_register._execute(&shared).await.unwrap().unwrap();
        assert_eq!(res, Resp3::new_simple_string("OK".into()));

        let script_exists = ScriptExists::parse(&mut ["test", "nothing"].as_ref().into()).unwrap();
        let res = script_exists._execute(&shared).await.unwrap().unwrap();
        assert_eq!(
            res,
            Resp3::new_array(vec![Resp3::new_boolean(true), Resp3::new_boolean(false)])
        );

        let eval_name =
            EvalName::parse(&mut ["test", "1", "key", "value"].as_ref().into()).unwrap();
        let res = eval_name._execute(&shared).await.unwrap().unwrap();
        assert_eq!(res, Resp3::new_simple_string("OK".into()));

        let script_flush = ScriptFlush::parse(&mut [].as_ref().into()).unwrap();
        let res = script_flush._execute(&shared).await.unwrap().unwrap();
        assert_eq!(res, Resp3::new_simple_string("OK".into()));

        let script_exists = ScriptExists::parse(&mut ["test"].as_ref().into()).unwrap();
        let res = script_exists._execute(&shared).await.unwrap().unwrap();
        assert_eq!(res, Resp3::new_array(vec![Resp3::new_boolean(false)]));
    }
}
