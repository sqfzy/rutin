use clap::Parser;

#[derive(Parser)]
pub struct Cli {
    #[clap(short, long)]
    pub port: Option<u16>,
    #[clap(long)]
    pub replicaof: Option<String>,
    // #[clap(long)]
    // pub rdb_path: Option<String>,
}

pub fn merge_cli(conf: &mut crate::conf::Conf, cli: Cli) {
    if let Some(port) = cli.port {
        conf.server.port = port;
    }
    if let Some(replicaof) = cli.replicaof {
        conf.replica.replicaof = Some(replicaof);
    }
}
