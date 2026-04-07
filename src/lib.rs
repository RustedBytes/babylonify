mod cli;
mod detect;
mod process;
mod text;

pub use cli::Cli;

use anyhow::{Context, Result};
use env_logger::Env;
use log::info;
use std::sync::Arc;

pub fn run(cli: Cli) -> Result<()> {
    init_logging();

    if let Some(n) = cli.threads {
        info!("configuring Rayon thread pool with {n} threads");
        rayon::ThreadPoolBuilder::new()
            .num_threads(n)
            .build_global()
            .with_context(|| format!("Failed to configure Rayon thread pool with {n} threads"))?;
    }

    let target_langs = detect::parse_languages(&cli.lang)?;
    info!(
        "keeping rows matching {} target language(s)",
        target_langs.len()
    );
    info!("building language detector");
    let detector = Arc::new(detect::build_detector());

    process::process_input(&cli, &target_langs, &detector)
}

fn init_logging() {
    let env = Env::default().default_filter_or("info");

    let mut builder = env_logger::Builder::from_env(env);
    builder.format_timestamp(None).format_target(false);

    let _ = builder.try_init();
}
