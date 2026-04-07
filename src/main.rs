use anyhow::Result;
use babylonify::{Cli, run};
use clap::Parser;

fn main() -> Result<()> {
    run(Cli::parse())
}
