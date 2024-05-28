use clap::Parser;

#[derive(Parser)]
pub struct Cli {
    #[clap(short, long)]
    pub port: Option<u16>,
    #[clap(long)]
    pub replicaof: Option<String>,
    #[clap(long)]
    pub rdb_path: Option<String>,
}
