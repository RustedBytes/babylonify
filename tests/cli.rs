use assert_cmd::Command;
use predicates::str::contains;
use tempfile::tempdir;

use std::fs::{self, File};
use std::path::Path;

use polars::prelude::*;

/// Create an input Parquet with a default `transcription` column:
/// 0: Ukrainian
/// 1: English
/// 2: Ukrainian with emoji + digits (to test --clean)
/// 3: Null
/// 4: Empty string
fn write_input_parquet(path: &Path) -> PolarsResult<()> {
    let transcriptions = &[
        Some("Привіт світ!"),
        Some("Hello, world!"),
        Some("Привіт, Україно! 😊 123"),
        None,
        Some(""),
    ];
    let ids = &[0i32, 1, 2, 3, 4];

    let mut df = df![
        "id" => ids.as_slice(),
        "transcription" => transcriptions.as_slice(),
    ]?;

    let mut f = File::create(path).expect("create input parquet");
    ParquetWriter::new(&mut f)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut df)
        .map(|_| ())
}

fn write_custom_parquet(path: &Path, column: &str, values: &[Option<&str>]) -> PolarsResult<()> {
    let ids: Vec<i32> = (0..values.len() as i32).collect();
    let text_values = values
        .iter()
        .map(|opt| opt.map(|s| s.to_string()))
        .collect::<Vec<Option<String>>>();
    let mut df = DataFrame::new(vec![
        Series::new("id".into(), ids).into(),
        Series::new(column.into(), text_values).into(),
    ])?;

    let mut f = File::create(path).expect("create custom input parquet");
    ParquetWriter::new(&mut f)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut df)
        .map(|_| ())
}

fn read_parquet(path: &Path) -> PolarsResult<DataFrame> {
    let file = File::open(path)?;
    let reader = ParquetReader::new(file);
    reader.finish()
}

#[test]
fn keeps_only_ukrainian_by_default() {
    let tmp = tempdir().unwrap();
    let in_path = tmp.path().join("in.parquet");
    let out_path = tmp.path().join("out.parquet");

    write_input_parquet(&in_path).unwrap();

    // Run: keep only Ukrainian (default column), do not keep empty, no cleaning
    let mut cmd = Command::cargo_bin("babylonify").unwrap();
    cmd.arg("-i")
        .arg(&in_path)
        .arg("-o")
        .arg(&out_path)
        .arg("-l")
        .arg("uk");

    cmd.assert()
        .success()
        .stdout(contains("✅ Filtered"))
        .stderr(contains("")); // no specific stderr expected

    let df = read_parquet(&out_path).unwrap();
    // Expect rows: #0 and #2 (both Ukrainian). #3 (null) and #4 (empty) are dropped without --keep-empty.
    assert_eq!(df.height(), 2);

    // Ensure the English row (#1) is gone
    let col = df.column("transcription").unwrap().str().unwrap();
    let texts: Vec<_> = col.into_iter().collect();
    assert!(!texts.iter().any(|t| t == &Some("Hello, world!")));
}

#[test]
fn keep_empty_retains_null_and_empty_rows() {
    let tmp = tempdir().unwrap();
    let in_path = tmp.path().join("in.parquet");
    let out_path = tmp.path().join("out.parquet");

    write_input_parquet(&in_path).unwrap();

    // Add --keep-empty so that None and "" are preserved
    let mut cmd = Command::cargo_bin("babylonify").unwrap();
    cmd.arg("-i")
        .arg(&in_path)
        .arg("-o")
        .arg(&out_path)
        .arg("-l")
        .arg("uk")
        .arg("--keep-empty");

    cmd.assert().success();

    let df = read_parquet(&out_path).unwrap();
    // Expected rows:
    //   Ukrainian (#0, #2) + null (#3) + empty (#4) = 4
    assert_eq!(df.height(), 4);
}

#[test]
fn processes_all_parquet_files_in_directory() {
    let tmp = tempdir().unwrap();
    let input_dir = tmp.path().join("inputs");
    let output_dir = tmp.path().join("filtered");

    fs::create_dir_all(&input_dir).unwrap();

    let first = input_dir.join("first.parquet");
    let second = input_dir.join("second.parquet");

    write_input_parquet(&first).unwrap();
    write_input_parquet(&second).unwrap();

    let mut cmd = Command::cargo_bin("babylonify").unwrap();
    cmd.arg("--input-dir")
        .arg(&input_dir)
        .arg("-o")
        .arg(&output_dir)
        .arg("-l")
        .arg("uk");

    cmd.assert().success();

    let out_first = output_dir.join("first.parquet");
    let out_second = output_dir.join("second.parquet");

    assert!(out_first.exists());
    assert!(out_second.exists());

    for output in [&out_first, &out_second] {
        let df = read_parquet(output).unwrap();
        assert_eq!(df.height(), 2);
        let col = df.column("transcription").unwrap().str().unwrap();
        let texts: Vec<_> = col.into_iter().collect();
        assert!(!texts.iter().any(|t| t == &Some("Hello, world!")));
    }
}

#[test]
fn clean_flag_removes_non_letter_characters() {
    let tmp = tempdir().unwrap();
    let in_path = tmp.path().join("in.parquet");
    let out_path = tmp.path().join("out.parquet");

    write_input_parquet(&in_path).unwrap();

    let mut cmd = Command::cargo_bin("babylonify").unwrap();
    cmd.arg("-i")
        .arg(&in_path)
        .arg("-o")
        .arg(&out_path)
        .arg("-l")
        .arg("uk")
        .arg("--clean");

    cmd.assert().success().stdout(contains("cleaned = true"));

    let df = read_parquet(&out_path).unwrap();
    assert_eq!(df.height(), 2);
    let col = df.column("transcription").unwrap().str().unwrap();
    let texts: Vec<_> = col.into_iter().collect();
    let cleaned = texts
        .iter()
        .find_map(|opt| opt.filter(|s| s.contains("Україно")))
        .expect("cleaned Ukrainian row present");
    assert_eq!(cleaned, "Привіт, Україно!");
    assert!(!texts.iter().any(|t| t == &Some("Привіт, Україно! 😊 123")));
}

#[test]
fn uses_custom_column_when_provided() {
    let tmp = tempdir().unwrap();
    let in_path = tmp.path().join("custom.parquet");
    let out_path = tmp.path().join("filtered.parquet");

    write_custom_parquet(
        &in_path,
        "text",
        &[
            Some("Привіт світ!"),
            Some("Hello, world!"),
            Some("Привіт, Україно! 😊 123"),
            None,
        ],
    )
    .unwrap();

    let mut cmd = Command::cargo_bin("babylonify").unwrap();
    cmd.arg("-i")
        .arg(&in_path)
        .arg("-o")
        .arg(&out_path)
        .arg("-c")
        .arg("text")
        .arg("-l")
        .arg("uk");

    cmd.assert().success();

    let df = read_parquet(&out_path).unwrap();
    assert_eq!(df.height(), 2);
    let col = df.column("text").unwrap().str().unwrap();
    let texts: Vec<_> = col.into_iter().collect();
    assert!(!texts.iter().any(|t| t == &Some("Hello, world!")));
    assert!(texts.iter().any(|t| t == &Some("Привіт світ!")));
}

#[test]
fn input_dir_without_parquet_fails() {
    let tmp = tempdir().unwrap();
    let input_dir = tmp.path().join("inputs");
    let output_dir = tmp.path().join("filtered");
    fs::create_dir_all(&input_dir).unwrap();

    let mut cmd = Command::cargo_bin("babylonify").unwrap();
    cmd.arg("--input-dir")
        .arg(&input_dir)
        .arg("-o")
        .arg(&output_dir);

    cmd.assert()
        .failure()
        .stderr(contains("No Parquet files found in input directory"));
}

#[test]
fn fails_when_output_path_is_directory() {
    let tmp = tempdir().unwrap();
    let in_path = tmp.path().join("input.parquet");
    let out_dir = tmp.path().join("as_dir");
    fs::create_dir_all(&out_dir).unwrap();

    write_input_parquet(&in_path).unwrap();

    let mut cmd = Command::cargo_bin("babylonify").unwrap();
    cmd.arg("-i").arg(&in_path).arg("-o").arg(&out_dir);

    cmd.assert()
        .failure()
        .stderr(contains("Output path"))
        .stderr(contains("Provide a file path"));
}
