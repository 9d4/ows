mod serve;

use serve::Serve;
use usage_rs::{Cli, Subcommands};

#[derive(Cli)]
#[usage(bin = "ows", version = "0.1.0")]
pub(crate) struct Ows {
    #[usage(subcommand)]
    pub(crate) command: Commands,
}

#[derive(Subcommands)]
#[usage(run)]
pub(crate) enum Commands {
    Serve(Serve),
}
