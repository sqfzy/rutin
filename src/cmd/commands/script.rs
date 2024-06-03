use crate::{
    cmd::{CmdError, CmdExecutor, CmdUnparsed, CmdType, Err},
    frame::RESP3,
    shared::Shared,
    util::atoi,
    Int,
};
use bytes::Bytes;

#[derive(Debug)]
pub struct Eval {
    script: Bytes,
    numkeys: usize,
    keys: Vec<Bytes>,
    args: Vec<Bytes>,
}

// impl CmdExecutor for Eval {
//     const CMD_TYPE: CmdType = CmdType::Read;
//
//     async fn _execute(self, shared: &Shared) -> Result<Option<Frame>, CmdError> {
//         let script = shared.get_script(&self.script).ok_or(Err::NoScript)?;
//
//         let mut args = Vec::with_capacity(self.numkeys + self.args.len());
//         args.extend(self.keys);
//         args.extend(self.args);
//
//         let frame = script.run(&args).await?;
//         Ok(Some(frame))
//     }
//
//     fn parse(args: &mut CmdFrame) -> Result<Self, CmdError> {
//         if args.len() < 2 {
//             return Err(Err::WrongArgNum.into());
//         }
//
//         let script = args.pop_front().unwrap();
//         let numkeys = atoi::<usize>(&args.pop_front().unwrap())
//             .map_err(|_| CmdError::from("ERR value is not an integer or out of range"))?;
//
//         let mut keys = Vec::with_capacity(numkeys);
//         for _ in 0..numkeys {
//             keys.push(args.pop_front().unwrap());
//         }
//         let args = args.collect();
//
//         Ok(Eval {
//             script,
//             numkeys,
//             keys,
//             args,
//         })
//     }
// }
