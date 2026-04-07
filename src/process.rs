use crate::cli::Cli;
use crate::text::clean_text;
use anyhow::{Context, Result, anyhow};
use lingua::{Language, LanguageDetector};
use log::info;
use polars::prelude::*;
use rayon::prelude::*;
use std::{
    collections::HashSet,
    fs::{self, File},
    path::{Path, PathBuf},
    sync::Arc,
};

struct Summary<'a> {
    input_path: &'a Path,
    output_path: &'a Path,
    invalid_output_path: Option<&'a Path>,
    total_rows: usize,
    kept_rows: usize,
    invalid_rows: usize,
    target_langs: &'a HashSet<Language>,
    cleaned: bool,
    threshold: f64,
}

pub fn process_input(
    cli: &Cli,
    target_langs: &HashSet<Language>,
    detector: &Arc<LanguageDetector>,
) -> Result<()> {
    match (&cli.input, &cli.input_dir) {
        (Some(input_path), None) => {
            info!("processing input path '{}'", input_path.display());
            process_input_path(
                input_path,
                &cli.output,
                cli.output_invalid.as_deref(),
                cli,
                target_langs,
                detector,
            )
        }
        (None, Some(input_dir)) => {
            info!("processing input directory '{}'", input_dir.display());
            process_directory(
                input_dir,
                &cli.output,
                cli.output_invalid.as_deref(),
                cli,
                target_langs,
                detector,
            )
        }
        _ => unreachable!("clap enforces that exactly one input source is provided"),
    }
}

fn process_input_path(
    input_path: &Path,
    output_path: &Path,
    invalid_output_path: Option<&Path>,
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
        process_directory(
            input_path,
            output_path,
            invalid_output_path,
            cli,
            target_langs,
            detector,
        )
    } else if metadata.is_file() {
        process_file(
            input_path,
            output_path,
            invalid_output_path,
            cli,
            target_langs,
            detector,
        )
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
    invalid_output_dir: Option<&Path>,
    cli: &Cli,
    target_langs: &HashSet<Language>,
    detector: &Arc<LanguageDetector>,
) -> Result<()> {
    ensure_output_directory(output_dir)?;
    if let Some(invalid_output_dir) = invalid_output_dir {
        ensure_output_directory(invalid_output_dir)?;
    }

    let files = collect_parquet_files(input_dir)?;
    info!(
        "found {} Parquet file(s) under '{}'",
        files.len(),
        input_dir.display()
    );
    for input_path in files {
        let output_path = output_path_for_file(output_dir, &input_path)?;
        let invalid_output_path = invalid_output_dir
            .map(|invalid_output_dir| output_path_for_file(invalid_output_dir, &input_path))
            .transpose()?;
        process_file(
            &input_path,
            &output_path,
            invalid_output_path.as_deref(),
            cli,
            target_langs,
            detector,
        )?;
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
    invalid_output_path: Option<&Path>,
    cli: &Cli,
    target_langs: &HashSet<Language>,
    detector: &Arc<LanguageDetector>,
) -> Result<()> {
    ensure_file_output_path(output_path)?;
    if let Some(invalid_output_path) = invalid_output_path {
        ensure_file_output_path(invalid_output_path)?;

        if output_path == invalid_output_path {
            return Err(anyhow!(
                "Output path '{}' and invalid output path '{}' must be different",
                output_path.display(),
                invalid_output_path.display()
            ));
        }
    }

    info!(
        "filtering '{}' into '{}'",
        input_path.display(),
        output_path.display()
    );
    let df = read_parquet(input_path)?;
    let processed = process_column(&df, cli)?;
    let mask = build_mask(
        &processed,
        cli.keep_empty,
        cli.threshold,
        target_langs,
        detector,
    );
    let mut filtered = filter_dataframe(&df, &mask)?;
    let invalid_mask = invert_mask(&mask);
    let mut invalid = invalid_output_path
        .map(|_| filter_dataframe(&df, &invalid_mask))
        .transpose()?;

    if cli.clean {
        replace_text_column(&mut filtered, &cli.column, &processed, &mask)?;
        if let Some(invalid) = invalid.as_mut() {
            replace_text_column(invalid, &cli.column, &processed, &invalid_mask)?;
        }
    }

    write_parquet(output_path, &mut filtered)?;
    if let Some((invalid_output_path, invalid)) = invalid_output_path.zip(invalid.as_mut()) {
        write_parquet(invalid_output_path, invalid)?;
    }
    print_summary(Summary {
        input_path,
        output_path,
        invalid_output_path,
        total_rows: mask.len(),
        kept_rows: filtered.height(),
        invalid_rows: invalid.as_ref().map_or(0, DataFrame::height),
        target_langs,
        cleaned: cli.clean,
        threshold: cli.threshold,
    });

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
    threshold: f64,
    target_langs: &HashSet<Language>,
    detector: &Arc<LanguageDetector>,
) -> Vec<bool> {
    processed
        .par_iter()
        .map(|opt_text| match opt_text {
            None => keep_empty,
            Some(text) if text.is_empty() => keep_empty,
            Some(text) => matches_threshold(text, threshold, target_langs, detector),
        })
        .collect()
}

fn matches_threshold(
    text: &str,
    threshold: f64,
    target_langs: &HashSet<Language>,
    detector: &Arc<LanguageDetector>,
) -> bool {
    detector
        .compute_language_confidence_values(text)
        .into_iter()
        .next()
        .map(|(language, confidence)| target_langs.contains(&language) && confidence >= threshold)
        .unwrap_or(false)
}

fn filter_dataframe(df: &DataFrame, mask: &[bool]) -> Result<DataFrame> {
    let mask = BooleanChunked::from_slice("mask".into(), mask);
    Ok(df.filter(&mask)?)
}

fn invert_mask(mask: &[bool]) -> Vec<bool> {
    mask.iter().map(|keep| !keep).collect()
}

fn replace_text_column(
    filtered: &mut DataFrame,
    column_name: &str,
    processed: &[Option<String>],
    mask: &[bool],
) -> Result<()> {
    let cleaned_filtered: Vec<Option<String>> = processed
        .iter()
        .cloned()
        .zip(mask.iter())
        .filter_map(|(value, keep)| if *keep { Some(value) } else { None })
        .collect();
    let cleaned_series = Series::new(column_name.into(), cleaned_filtered);
    filtered.with_column(cleaned_series)?;
    Ok(())
}

fn ensure_file_output_path(output_path: &Path) -> Result<()> {
    if output_path.exists() && output_path.is_dir() {
        return Err(anyhow!(
            "Output path '{:?}' points to a directory. Provide a file path instead.",
            output_path
        ));
    }

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

fn print_summary(summary: Summary<'_>) {
    match summary.invalid_output_path {
        Some(invalid_output_path) => println!(
            "✅ Filtered {} rows -> {} rows kept, {} rows rejected (langs = {:?}, cleaned = {}, threshold = {}) [{} -> {}, invalid -> {}]",
            summary.total_rows,
            summary.kept_rows,
            summary.invalid_rows,
            summary.target_langs,
            summary.cleaned,
            summary.threshold,
            summary.input_path.display(),
            summary.output_path.display(),
            invalid_output_path.display()
        ),
        None => println!(
            "✅ Filtered {} rows -> {} rows kept (langs = {:?}, cleaned = {}, threshold = {}) [{} -> {}]",
            summary.total_rows,
            summary.kept_rows,
            summary.target_langs,
            summary.cleaned,
            summary.threshold,
            summary.input_path.display(),
            summary.output_path.display()
        ),
    }
}
