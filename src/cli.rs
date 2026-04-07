use clap::{ArgAction, ArgGroup, Parser, ValueHint};
use std::path::PathBuf;

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

    /// Text column name (default: transcription)
    #[arg(short = 'c', long, default_value = "transcription")]
    pub column: String,

    /// Target language(s) to keep. Repeat the flag to allow multiple languages.
    #[arg(short = 'l', long, action = ArgAction::Append)]
    pub lang: Vec<String>,

    /// Optional: set Rayon thread count
    #[arg(long)]
    pub threads: Option<usize>,

    /// Optional: keep empty/null text values
    #[arg(long, action = ArgAction::SetTrue)]
    pub keep_empty: bool,

    /// Optional: clean text (remove everything except alphabetic and punctuation symbols)
    #[arg(long, action = ArgAction::SetTrue)]
    pub clean: bool,
}
