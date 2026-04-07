set positional-arguments

default:
    @just --list

fmt:
    cargo fmt --all

lint:
    cargo clippy --all-targets --all-features -- -D warnings

test:
    cargo test --all-features

check:
    cargo fmt --all --check
    cargo clippy --all-targets --all-features -- -D warnings
    cargo test --all-features

run *args:
    cargo run -- {{args}}
