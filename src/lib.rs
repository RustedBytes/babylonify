mod cli;
mod detect;
mod process;
mod text;

pub use cli::Cli;

use anyhow::{Context, Result};
use std::sync::Arc;

pub fn run(cli: Cli) -> Result<()> {
    if let Some(n) = cli.threads {
        rayon::ThreadPoolBuilder::new()
            .num_threads(n)
            .build_global()
            .with_context(|| format!("Failed to configure Rayon thread pool with {n} threads"))?;
    }

    let target_langs = detect::parse_languages(&cli.lang)?;
    let detector = Arc::new(detect::build_detector());

    process::process_input(&cli, &target_langs, &detector)
}
