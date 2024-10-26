use crate::{conf::*, util::gen_test_shared};
use bytestring::ByteString;
use clap::Parser;
use std::sync::Mutex;
use std::{str::FromStr, sync::Arc};

#[derive(Parser)]
pub struct Cli {
    #[clap(short, long)]
    pub port: Option<u16>,
    #[clap(long, value_parser = parse_replicaof)]
    pub replicaof: Option<(ByteString, u16)>,
    #[clap(long)]
    pub log_level: Option<String>,
}

pub fn merge_cli(conf: &mut Conf, cli: Cli) {
    let mut server_conf = ServerConf::clone(conf.server_conf().as_ref());
    let mut replica_conf = conf.replica_conf().as_ref().map(|r| ReplicaConf::clone(r));

    if let Some(port) = cli.port {
        let mut sercer_conf = ServerConf::clone(conf.server_conf().as_ref());
        sercer_conf.port = port;
    }

    if let Some((host, port)) = cli.replicaof {
        if let Some(replica_conf) = &mut replica_conf {
            replica_conf.master_host = host;
            replica_conf.master_port = port;
        } else {
            replica_conf = Some(ReplicaConf::new(host, port));
        }
    }

    if let Some(log_level) = cli.log_level {
        server_conf.log_level = log_level.into();
    }

    conf.server.store(Arc::new(server_conf));
    conf.replica.store(replica_conf.map(Arc::new));
}

fn parse_replicaof(s: &str) -> Result<(ByteString, u16), String> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 {
        return Err(format!(
            "'{}' is not in the correct format. Expected 'host:port'",
            s
        ));
    }
    let host = parts[0].into();
    let port = parts[1].parse::<u16>().map_err(|e| e.to_string())?;
    Ok((host, port))
}
