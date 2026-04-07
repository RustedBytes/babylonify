use anyhow::{Result, anyhow};
use clap::{ArgAction, ArgGroup, Parser, ValueHint};
use std::{num::NonZeroUsize, path::PathBuf, thread};

/// Filter a Parquet file by detected language using lingua + polars + rayon.
/// Optionally cleans transcriptions by removing non-alphabetic and non-punctuation symbols.
///
/// Example:
/// ```bash
/// babylonify \
///   --input data.parquet \
///   --output data_uk.parquet \
///   --lang uk \
///   --clean
/// ```
#[derive(Parser, Debug)]
#[command(
    name = "babylonify",
    version,
    about = "Filter Parquet rows by detected language using lingua + polars (+ optional cleaning)",
    group(ArgGroup::new("input_source").required(true).args(&["input", "input_dir"]))
)]
pub struct Cli {
    /// Input Parquet file path or directory containing Parquet files
    #[arg(short, long, value_hint = ValueHint::AnyPath)]
    pub input: Option<PathBuf>,

    /// Input directory with Parquet files (compatibility alias for --input <DIR>)
    #[arg(long, value_hint = ValueHint::DirPath)]
    pub input_dir: Option<PathBuf>,

    /// Output Parquet file path (or directory when --input-dir is used)
    #[arg(short, long, value_hint = ValueHint::FilePath)]
    pub output: PathBuf,

    /// Optional output path for rejected rows (file or directory, matching --output mode)
    #[arg(long, value_hint = ValueHint::AnyPath)]
    pub output_invalid: Option<PathBuf>,

    /// Text column name (default: transcription)
    #[arg(short = 'c', long, default_value = "transcription")]
    pub column: String,

    /// Target language(s) to keep. Repeat the flag to allow multiple languages.
    #[arg(short = 'l', long, action = ArgAction::Append)]
    pub lang: Vec<String>,

    /// Confidence threshold for keeping a detected language match
    #[arg(long, default_value_t = 0.6, value_parser = parse_threshold)]
    pub threshold: f64,

    /// Rayon thread count, defaults to the current core count
    #[arg(long, default_value_t = default_threads())]
    pub threads: usize,

    /// Optional: keep empty/null text values
    #[arg(long, action = ArgAction::SetTrue)]
    pub keep_empty: bool,

    /// Optional: clean text (remove everything except alphabetic and punctuation symbols)
    #[arg(long, action = ArgAction::SetTrue)]
    pub clean: bool,
}

fn default_threads() -> usize {
    thread::available_parallelism()
        .map(NonZeroUsize::get)
        .unwrap_or(1)
}

fn parse_threshold(raw: &str) -> Result<f64> {
    let threshold: f64 = raw
        .parse()
        .map_err(|_| anyhow!("invalid threshold '{raw}', expected a float between 0.0 and 1.0"))?;

    if (0.0..=1.0).contains(&threshold) {
        Ok(threshold)
    } else {
        Err(anyhow!(
            "invalid threshold '{raw}', expected a value between 0.0 and 1.0"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_uses_expected_defaults() {
        let cli = Cli::parse_from([
            "babylonify",
            "--input",
            "in.parquet",
            "--output",
            "out.parquet",
        ]);

        assert_eq!(cli.threshold, 0.6);
        assert_eq!(cli.threads, default_threads());
        assert_eq!(cli.column, "transcription");
        assert_eq!(cli.output_invalid, None);
    }

    #[test]
    fn cli_rejects_out_of_range_thresholds() {
        let err = Cli::try_parse_from([
            "babylonify",
            "--input",
            "in.parquet",
            "--output",
            "out.parquet",
            "--threshold",
            "1.1",
        ])
        .unwrap_err();

        let rendered = err.to_string();
        assert!(rendered.contains("1.1"));
        assert!(rendered.contains("0"));
        assert!(rendered.contains("1"));
    }
}
