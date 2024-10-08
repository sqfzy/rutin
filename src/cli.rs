use std::str::FromStr;

use crate::conf::MasterInfo;
use bytestring::ByteString;
use clap::Parser;
use std::sync::Mutex;

#[derive(Parser)]
pub struct Cli {
    #[clap(short, long)]
    pub port: Option<u16>,
    #[clap(long, value_parser = parse_replicaof)]
    pub replicaof: Option<(ByteString, u16)>,
    #[clap(long)]
    pub log_level: Option<String>,
}

pub fn merge_cli(conf: &mut crate::conf::Conf, cli: Cli) {
    if let Some(port) = cli.port {
        conf.server.port = port;
    }

    if let Some((host, port)) = cli.replicaof {
        conf.replica.master_info = Mutex::new(Some(MasterInfo::new(host, port)));
    }

    if let Some(log_level) = cli.log_level {
        tracing::Level::from_str(&log_level).unwrap();
        conf.server.log_level = log_level.into();
    }
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
