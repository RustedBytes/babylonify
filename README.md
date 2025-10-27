# babylonify

A small CLI to filter rows in a Parquet file by the detected language of a text column. It relies on [lingua](https://github.com/pemistahl/lingua-rs) for language detection, [polars](https://pola.rs) for columnar data manipulation, and uses Rayon to process rows in parallel. Optionally the tool can clean the text, keeping only alphabetic and punctuation symbols before detection.

## Features
- Detects language for each row in a Parquet column and keeps only the rows matching a target language.
- Accepts ISO 639-1 codes (`uk`, `en`, `ru`, etc.) as well as language names (`Ukrainian`, `English`, …).
- Optional text cleaning step that trims whitespace and drops numbers/symbols before detection.
- Parallel processing with Rayon and compressed Parquet output using Zstandard.
- Handy CLI powered by `clap`, with sensible defaults for transcription-like datasets.

## Prerequisites
- Rust toolchain with edition 2024 support (install via [`rustup`](https://rustup.rs/)).
- Parquet files that contain the text data to filter.

## Installation
```bash
cargo install --path .
```
You can also point `cargo install` at a checked-out path of this repository to build locally in release mode.

## Usage
```bash
babylonify \
  --input data.parquet \
  --output data_uk.parquet \
  --lang uk \
  --column transcription \
  --clean
```

### CLI options
| Flag | Description |
| ---- | ----------- |
| `-i, --input <PATH>` | Path to the source Parquet file. |
| `-o, --output <PATH>` | Where to write the filtered Parquet file. |
| `-c, --column <NAME>` | Column holding the text to inspect (default: `transcription`). |
| `-l, --lang <LANG>` | Target language, accepts ISO code or language name (default: `uk`). |
| `--threads <N>` | Limit Rayon to `N` worker threads. |
| `--keep-empty` | Keep rows where the text column is empty or null. |
| `--clean` | Clean text by removing non-letter, non-punctuation symbols before detection. |

The output Parquet file keeps the original schema; when `--clean` is enabled the text column is replaced with the cleaned content.

## Development
- Run the test suite before contributing: `cargo test`.
- Format and lint using the standard Rust tooling (`cargo fmt`, `cargo clippy`) if you make changes.

## License
This project is distributed under the terms of the MIT License. See `LICENSE` (if provided) for details.
