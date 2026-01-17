# Tester

This crate is a test client used for running end-to-end and performance tests against a coDB server.

## Overview

The tester automatically manages the database server lifecycle - it starts the server before tests and stops it after tests complete.

## Prerequisites

- Build the server binary first:
  ```bash
  cargo build --release --bin server
  ```

## Quick Run

All commands require the `--server-path` argument pointing to the server executable:

```bash
# Performance test example
cargo run -p tester -- --server-path ./target/release/server concurrent-reads-index --runs 1 --threads 8 --records 1000 --bound-size 10

# E2E test example
cargo run -p tester -- --server-path ./target/release/server e2e-select

# Run all E2E tests
cargo run -p tester -- --server-path ./target/release/server e2e-all
```

## Test Types

### Performance Tests
Performance tests live in [tester/src/performance](tester/src/performance) and measure concurrency and throughput.

### E2E Tests
E2E tests live in [tester/src/e2e](tester/src/e2e) and verify end-to-end functionality.
