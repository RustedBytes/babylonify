use anyhow::{Context, Result, anyhow};
use clap::{ArgAction, ArgGroup, Parser, ValueHint};
use lingua::{Language, LanguageDetector, LanguageDetectorBuilder};
use once_cell::sync::Lazy;
use polars::prelude::*;
use rayon::prelude::*;
use regex::Regex;
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::Arc,
};

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
struct Cli {
    /// Input Parquet file path
    #[arg(short, long, value_hint = ValueHint::FilePath)]
    input: Option<PathBuf>,

    /// Input directory with Parquet files
    #[arg(long, value_hint = ValueHint::DirPath)]
    input_dir: Option<PathBuf>,

    /// Output Parquet file path (or directory when --input-dir is used)
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

    match (&cli.input, &cli.input_dir) {
        (Some(input_path), None) => {
            process_file(input_path, &cli.output, &cli, target_lang, &detector)?
        }
        (None, Some(input_dir)) => {
            process_directory(input_dir, &cli.output, &cli, target_lang, &detector)?
        }
        _ => unreachable!("clap enforces that exactly one input source is provided"),
    }

    Ok(())
}

fn process_directory(
    input_dir: &Path,
    output_dir: &Path,
    cli: &Cli,
    target_lang: Language,
    detector: &Arc<LanguageDetector>,
) -> Result<()> {
    if output_dir.exists() {
        if !output_dir.is_dir() {
            return Err(anyhow!(
                "Output path '{:?}' must be a directory when --input-dir is used",
                output_dir
            ));
        }
    } else {
        fs::create_dir_all(output_dir).with_context(|| {
            format!(
                "Failed to create output directory at '{}'",
                output_dir.display()
            )
        })?;
    }

    let mut files: Vec<PathBuf> = fs::read_dir(input_dir)
        .with_context(|| format!("Failed to read input directory '{}'", input_dir.display()))?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if entry.file_type().ok()?.is_file()
                && path
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .map(|ext| ext.eq_ignore_ascii_case("parquet"))
                    .unwrap_or(false)
            {
                Some(path)
            } else {
                None
            }
        })
        .collect();

    files.sort();

    if files.is_empty() {
        return Err(anyhow!(
            "No Parquet files found in input directory '{}'",
            input_dir.display()
        ));
    }

    for input_path in files {
        let file_name = input_path
            .file_name()
            .ok_or_else(|| anyhow!("Invalid file name for '{:?}'", input_path))?;
        let output_path = output_dir.join(file_name);
        process_file(&input_path, &output_path, cli, target_lang, detector)?;
    }

    Ok(())
}

fn process_file(
    input_path: &Path,
    output_path: &Path,
    cli: &Cli,
    target_lang: Language,
    detector: &Arc<LanguageDetector>,
) -> Result<()> {
    if output_path.exists() && output_path.is_dir() {
        return Err(anyhow!(
            "Output path '{:?}' points to a directory. Provide a file path instead.",
            output_path
        ));
    }

    // Read parquet file
    let file = File::open(input_path)?;
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
        let cleaned_filtered: Vec<Option<String>> = processed
            .into_iter()
            .zip(mask.iter())
            .filter_map(|(value, keep)| if *keep { Some(value) } else { None })
            .collect();
        let cleaned_series = Series::new((&cli.column).into(), cleaned_filtered);
        filtered.with_column(cleaned_series)?;
    }

    // Write output
    let mut out_file =
        File::create(output_path).with_context(|| format!("Cannot create {:?}", output_path))?;
    ParquetWriter::new(&mut out_file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut filtered)?;

    println!(
        "✅ Filtered {} rows -> {} rows kept (lang = {:?}, cleaned = {}) [{} -> {}]",
        mask.len(),
        filtered.height(),
        target_lang,
        cli.clean,
        input_path.display(),
        output_path.display()
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
