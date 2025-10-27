use assert_cmd::Command;
use predicates::str::contains;
use tempfile::tempdir;

use std::fs::File;
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
