use anyhow::{Context, Result, anyhow};
use clap::{ArgAction, Parser, ValueHint};
use lingua::{Language, LanguageDetector, LanguageDetectorBuilder};
use once_cell::sync::Lazy;
use polars::prelude::*;
use rayon::prelude::*;
use regex::Regex;
use std::{fs::File, path::PathBuf, sync::Arc};

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
    about = "Filter Parquet rows by detected language using lingua + polars (+ optional cleaning)"
)]
struct Cli {
    /// Input Parquet file path
    #[arg(short, long, value_hint = ValueHint::FilePath)]
    input: PathBuf,

    /// Output Parquet file path
    #[arg(short, long, value_hint = ValueHint::FilePath)]
    output: PathBuf,

    /// Text column name (default: transcription)
    #[arg(short = 'c', long, default_value = "transcription")]
    column: String,

    /// Target language (ISO 639-1 or name: uk, en, ru, Ukrainian, etc.)
    #[arg(short = 'l', long, default_value = "uk")]
    lang: String,

    /// Optional: set Rayon thread count
    #[arg(long)]
    threads: Option<usize>,

    /// Optional: keep empty/null text values
    #[arg(long, action = ArgAction::SetTrue)]
    keep_empty: bool,

    /// Optional: clean text (remove everything except alphabetic and punctuation symbols)
    #[arg(long, action = ArgAction::SetTrue)]
    clean: bool,
}

fn parse_language(code: &str) -> Result<Language> {
    let code = code.trim().to_lowercase();
    match code.as_str() {
        "uk" | "ukr" | "ukrainian" | "українська" => Ok(Language::Ukrainian),
        "en" | "eng" | "english" => Ok(Language::English),
        "ru" | "rus" | "russian" | "русский" => Ok(Language::Russian),
        "pl" | "polish" => Ok(Language::Polish),
        "de" | "german" => Ok(Language::German),
        "fr" | "french" => Ok(Language::French),
        "es" | "spanish" => Ok(Language::Spanish),
        other => lingua::Language::all()
            .into_iter()
            .find(|l| format!("{l:?}").to_lowercase() == other)
            .ok_or_else(|| anyhow!("Unknown language: '{}'", code)),
    }
}

fn build_detector() -> LanguageDetector {
    LanguageDetectorBuilder::from_all_languages()
        .with_preloaded_language_models()
        .build()
}

/// Remove all symbols except letters, spaces, and punctuation for Ukrainian, Russian, and English texts.
fn clean_text(text: &str) -> String {
    static WHITESPACE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\s+").unwrap());
    static DROP_NON_L_P_SPACE: Lazy<Regex> = Lazy::new(|| Regex::new(r#"[^ \p{L}\p{P}]"#).unwrap());

    // 1) Collapse all whitespace to single spaces
    let spaced = WHITESPACE.replace_all(text, " ");

    // 2) Remove everything that's not: space, letter, or punctuation
    let cleaned = DROP_NON_L_P_SPACE.replace_all(&spaced, "");

    // Remove tabs
    let cleaned = cleaned.replace("\t", " ");

    // Remove @#%&*() and similar symbols
    let cleaned = cleaned.replace(&['@', '#', '%', '&', '*', '(', ')'][..], "");
    let cleaned = cleaned.replace("  ", " ");

    // 4) Trim edges that may become spaces during normalization
    cleaned.trim().to_string()
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    if let Some(n) = cli.threads {
        rayon::ThreadPoolBuilder::new()
            .num_threads(n)
            .build_global()
            .ok();
    }

    let target_lang = parse_language(&cli.lang)?;
    let detector = Arc::new(build_detector());

    // Read parquet file
    let file = File::open(&cli.input)?;
    let reader = ParquetReader::new(file);
    let df = reader.finish()?;

    // Extract column
    let col = df
        .column(&cli.column)
        .with_context(|| format!("Column '{}' not found", &cli.column))?
        .str()
        .context("Target column is not String")?;

    // Process text: clean + language detection
    let processed: Vec<Option<String>> = col
        .into_iter()
        .map(|opt| {
            opt.map(|s| {
                if cli.clean {
                    clean_text(s)
                } else {
                    s.to_string()
                }
            })
        })
        .collect();

    let mask: Vec<bool> = processed
        .par_iter()
        .map(|opt_text| match opt_text {
            None => cli.keep_empty,
            Some(t) if t.is_empty() => cli.keep_empty,
            Some(t) => detector.detect_language_of(t) == Some(target_lang),
        })
        .collect();

    // Create cleaned DataFrame (replace text column)
    let mask_ch = BooleanChunked::from_slice("mask".into(), &mask);
    let mut filtered = df.filter(&mask_ch)?;

    if cli.clean {
        let cleaned_series = Series::new((&cli.column).into(), processed);
        filtered.with_column(cleaned_series)?;
    }

    // Write output
    let mut out_file =
        File::create(&cli.output).with_context(|| format!("Cannot create {:?}", &cli.output))?;
    ParquetWriter::new(&mut out_file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut filtered)?;

    println!(
        "✅ Filtered {} rows -> {} rows kept (lang = {:?}, cleaned = {})",
        mask.len(),
        filtered.height(),
        target_lang,
        cli.clean
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_language_aliases_work() {
        // A few canonical and alias forms
        assert_eq!(parse_language("uk").unwrap(), Language::Ukrainian);
        assert_eq!(parse_language("UKR").unwrap(), Language::Ukrainian);
        assert_eq!(parse_language("українська").unwrap(), Language::Ukrainian);

        assert_eq!(parse_language("en").unwrap(), Language::English);
        assert_eq!(parse_language("English").unwrap(), Language::English);

        assert_eq!(parse_language("ru").unwrap(), Language::Russian);
        assert_eq!(parse_language("русский").unwrap(), Language::Russian);

        // Unknown should error
        let err = parse_language("xx").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("Unknown language"));
    }

    #[test]
    fn test_clean_text() {
        let raw = "Hello, world! 123 \n\t Привіт, світ! @#$%^&*() 456";
        let cleaned = clean_text(raw);
        assert_eq!(cleaned, "Hello, world! Привіт, світ!");
    }

    #[test]
    fn test_clean_text_only_symbols() {
        let raw = "@#$%^&*() 12345 \n\t";
        let cleaned = clean_text(raw);
        assert_eq!(cleaned, "");
    }

    #[test]
    fn test_clean_text_empty() {
        let raw = "";
        let cleaned = clean_text(raw);
        assert_eq!(cleaned, "");
    }

    #[test]
    fn test_clean_text_whitespace() {
        let raw = "   \n\t  ";
        let cleaned = clean_text(raw);
        assert_eq!(cleaned, "");
    }

    #[test]
    fn test_clean_text_punctuation() {
        let raw = "Hello!!! How's it going???";
        let cleaned = clean_text(raw);
        assert_eq!(cleaned, "Hello!!! How's it going???");
    }
}
