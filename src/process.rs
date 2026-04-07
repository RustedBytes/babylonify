use crate::cli::Cli;
use crate::text::clean_text;
use anyhow::{Context, Result, anyhow};
use lingua::{Language, LanguageDetector};
use polars::prelude::*;
use rayon::prelude::*;
use std::{
    collections::HashSet,
    fs::{self, File},
    path::{Path, PathBuf},
    sync::Arc,
};

pub fn process_input(
    cli: &Cli,
    target_langs: &HashSet<Language>,
    detector: &Arc<LanguageDetector>,
) -> Result<()> {
    match (&cli.input, &cli.input_dir) {
        (Some(input_path), None) => {
            process_input_path(input_path, &cli.output, cli, target_langs, detector)
        }
        (None, Some(input_dir)) => {
            process_directory(input_dir, &cli.output, cli, target_langs, detector)
        }
        _ => unreachable!("clap enforces that exactly one input source is provided"),
    }
}

fn process_input_path(
    input_path: &Path,
    output_path: &Path,
    cli: &Cli,
    target_langs: &HashSet<Language>,
    detector: &Arc<LanguageDetector>,
) -> Result<()> {
    let metadata = fs::metadata(input_path).with_context(|| {
        format!(
            "Input path '{}' does not exist or is inaccessible",
            input_path.display()
        )
    })?;

    if metadata.is_dir() {
        process_directory(input_path, output_path, cli, target_langs, detector)
    } else if metadata.is_file() {
        process_file(input_path, output_path, cli, target_langs, detector)
    } else {
        Err(anyhow!(
            "Input path '{}' must be a Parquet file or a directory containing Parquet files",
            input_path.display()
        ))
    }
}

fn process_directory(
    input_dir: &Path,
    output_dir: &Path,
    cli: &Cli,
    target_langs: &HashSet<Language>,
    detector: &Arc<LanguageDetector>,
) -> Result<()> {
    ensure_output_directory(output_dir)?;

    let files = collect_parquet_files(input_dir)?;
    for input_path in files {
        let output_path = output_path_for_file(output_dir, &input_path)?;
        process_file(&input_path, &output_path, cli, target_langs, detector)?;
    }

    Ok(())
}

fn ensure_output_directory(output_dir: &Path) -> Result<()> {
    if output_dir.exists() {
        if !output_dir.is_dir() {
            return Err(anyhow!(
                "Output path '{:?}' must be a directory when the input is a directory",
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

    Ok(())
}

fn collect_parquet_files(input_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = fs::read_dir(input_dir)
        .with_context(|| format!("Failed to read input directory '{}'", input_dir.display()))?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if entry.file_type().ok()?.is_file() && is_parquet_file(&path) {
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

    Ok(files)
}

fn is_parquet_file(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("parquet"))
        .unwrap_or(false)
}

fn output_path_for_file(output_dir: &Path, input_path: &Path) -> Result<PathBuf> {
    let file_name = input_path
        .file_name()
        .ok_or_else(|| anyhow!("Invalid file name for '{:?}'", input_path))?;
    Ok(output_dir.join(file_name))
}

fn process_file(
    input_path: &Path,
    output_path: &Path,
    cli: &Cli,
    target_langs: &HashSet<Language>,
    detector: &Arc<LanguageDetector>,
) -> Result<()> {
    if output_path.exists() && output_path.is_dir() {
        return Err(anyhow!(
            "Output path '{:?}' points to a directory. Provide a file path instead.",
            output_path
        ));
    }

    let df = read_parquet(input_path)?;
    let processed = process_column(&df, cli)?;
    let mask = build_mask(&processed, cli.keep_empty, target_langs, detector);
    let mut filtered = filter_dataframe(&df, &mask)?;

    if cli.clean {
        replace_text_column(&mut filtered, &cli.column, processed, &mask)?;
    }

    write_parquet(output_path, &mut filtered)?;
    print_summary(
        input_path,
        output_path,
        mask.len(),
        filtered.height(),
        target_langs,
        cli.clean,
    );

    Ok(())
}

fn read_parquet(input_path: &Path) -> Result<DataFrame> {
    let file = File::open(input_path)?;
    let reader = ParquetReader::new(file);
    Ok(reader.finish()?)
}

fn process_column(df: &DataFrame, cli: &Cli) -> Result<Vec<Option<String>>> {
    let column = df
        .column(&cli.column)
        .with_context(|| format!("Column '{}' not found", &cli.column))?
        .str()
        .context("Target column is not String")?;

    Ok(column
        .into_iter()
        .map(|opt| {
            opt.map(|text| {
                if cli.clean {
                    clean_text(text)
                } else {
                    text.to_string()
                }
            })
        })
        .collect())
}

fn build_mask(
    processed: &[Option<String>],
    keep_empty: bool,
    target_langs: &HashSet<Language>,
    detector: &Arc<LanguageDetector>,
) -> Vec<bool> {
    processed
        .par_iter()
        .map(|opt_text| match opt_text {
            None => keep_empty,
            Some(text) if text.is_empty() => keep_empty,
            Some(text) => detector
                .detect_language_of(text)
                .map(|lang| target_langs.contains(&lang))
                .unwrap_or(false),
        })
        .collect()
}

fn filter_dataframe(df: &DataFrame, mask: &[bool]) -> Result<DataFrame> {
    let mask = BooleanChunked::from_slice("mask".into(), mask);
    Ok(df.filter(&mask)?)
}

fn replace_text_column(
    filtered: &mut DataFrame,
    column_name: &str,
    processed: Vec<Option<String>>,
    mask: &[bool],
) -> Result<()> {
    let cleaned_filtered: Vec<Option<String>> = processed
        .into_iter()
        .zip(mask.iter())
        .filter_map(|(value, keep)| if *keep { Some(value) } else { None })
        .collect();
    let cleaned_series = Series::new(column_name.into(), cleaned_filtered);
    filtered.with_column(cleaned_series)?;
    Ok(())
}

fn write_parquet(output_path: &Path, filtered: &mut DataFrame) -> Result<()> {
    let mut out_file =
        File::create(output_path).with_context(|| format!("Cannot create {:?}", output_path))?;
    ParquetWriter::new(&mut out_file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(filtered)?;
    Ok(())
}

fn print_summary(
    input_path: &Path,
    output_path: &Path,
    total_rows: usize,
    kept_rows: usize,
    target_langs: &HashSet<Language>,
    cleaned: bool,
) {
    println!(
        "✅ Filtered {} rows -> {} rows kept (langs = {:?}, cleaned = {}) [{} -> {}]",
        total_rows,
        kept_rows,
        target_langs,
        cleaned,
        input_path.display(),
        output_path.display()
    );
}
