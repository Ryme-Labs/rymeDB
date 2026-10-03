set shell := ["bash", "-cu"]

default:
  just --list

fmt:
  cargo fmt --all

clippy:
  cargo clippy --all-targets --all-features -- -D warnings

check:
  cargo fmt --all -- --check
  cargo clippy --all-targets --all-features -- -D warnings
  cargo test --workspace --locked
  cargo run -p xtask -- check-no-comments

build:
  cargo build --workspace --locked --release

bench-quick:
  cargo run -p ryme-bench --release -- --ops 200000 --concurrency 8 --value-bytes 128

audit:
  cargo deny check
