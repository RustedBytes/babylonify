# babylonify

Babylonify filters rows in a Parquet file (or an entire directory of Parquet files) by the language detected in a text column. It wraps [lingua](https://github.com/pemistahl/lingua-rs) for language detection, [polars](https://pola.rs) for columnar IO, and [Rayon](https://github.com/rayon-rs/rayon) for parallelism, producing compressed Parquet output with Zstandard.

## Highlights
- Detects the language for every row and retains only those matching the requested language.
- Accepts both ISO 639-1 codes (`uk`, `en`, `ru`, …) and language names (`Ukrainian`, `English`, `русский`, …).
- Optional cleaning step removes numbers/emojis/symbols before detection so you can focus on alphabetic content.
- Scales to many files: point the CLI at a directory and it mirrors the structure to an output directory.
- Parallel row processing and streaming Parquet writers keep large datasets responsive.

## Requirements
- Rust toolchain with edition 2024 support installed via [`rustup`](https://rustup.rs/).
- Input Parquet files containing at least one string column with textual data.

## Install
```bash
# from a local checkout
cargo install --path .

# alternatively, build locally without installing
cargo build --release
```
After installation the `babylonify` binary is placed on your Cargo bin path (`~/.cargo/bin` by default).

## Quick start
Filter a single Parquet file, keeping Ukrainian rows from the default `transcription` column and cleaning the text before detection:
```bash
babylonify \
  --input data/transcripts.parquet \
  --output data/transcripts_uk.parquet \
  --lang uk \
  --clean
```

Batch-process every Parquet file in a directory. Outputs reuse the input file names within the provided output directory:
```bash
babylonify \
  --input-dir data/raw/ \
  --output data/filtered/ \
  --lang english
```

## CLI reference
Run `babylonify --help` for the authoritative list. The most important options are:

| Flag | Description |
| ---- | ----------- |
| `-i, --input <PATH>` | Parquet file to filter. Mutually exclusive with `--input-dir`. |
| `--input-dir <DIR>` | Process every Parquet file in a directory (recursively only across the top level). |
| `-o, --output <PATH|DIR>` | Output Parquet path. When used with `--input-dir`, this must be a directory and files are written with their original names. |
| `-c, --column <NAME>` | Name of the text column to inspect. Defaults to `transcription`. |
| `-l, --lang <LANG>` | Target language to keep. ISO codes, common aliases, and full names (case-insensitive) are accepted. Default: `uk`. |
| `--keep-empty` | Preserve rows where the text column is `NULL` or an empty string. |
| `--clean` | Normalize whitespace and strip non-letter/non-punctuation symbols before detection; the cleaned text replaces the original column in the output. |
| `--threads <N>` | Limit the Rayon thread pool to `N` workers if you need deterministic parallelism. |

The output Parquet schema matches the input schema; when `--clean` is supplied the specified text column is replaced with the cleaned content.

### Language aliases
Short codes and localized names for Ukrainian, English, Russian, Polish, German, French, and Spanish are recognized. Any other lingua-supported language can be addressed using its English enum name (for example `italian`, `portuguese`). Unknown values yield a helpful error.

## Development
- Run the test suite: `cargo test`.
- Format and lint with the Rust standard tooling: `cargo fmt` and `cargo clippy`.
- The integration tests under `tests/cli.rs` exercise the end-to-end CLI against synthetic Parquet fixtures.

## License
Babylonify is distributed under the MIT License. See `LICENSE` for the full text.
