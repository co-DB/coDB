# Tester

This crate is a test client used for running end-to-end and performance tests against a coDB server.

## Overview

- **E2E Tests**: Automatically manage the database server lifecycle - start the server before tests and stop it after tests complete.
- **Performance Tests**: Require a manually started server for more accurate benchmarking.

## Prerequisites

- Build the server binary first:
  ```bash
  cargo build --release --bin server
  ```

## Quick Run

### E2E Tests

E2E tests require the `--server-path` argument - the server is started/stopped automatically:

```bash
# Run a specific E2E test
cargo run -p tester -- e2e-select --server-path ./target/release/server

# Run all E2E tests
cargo run -p tester -- e2e-all --server-path ./target/release/server
```

### Performance Tests

Performance tests require a **manually started server**:

```bash
# Terminal 1: Start the server
./target/release/server

# Terminal 2: Run performance tests (no --server-path needed)
cargo run -p tester -- concurrent-reads-index --runs 1 --threads 8 --records 1000 --bound-size 10
```

## Test Types

### Performance Tests
Performance tests live in [tester/src/performance](tester/src/performance) and measure concurrency and throughput.

### E2E Tests
E2E tests live in [tester/src/e2e](tester/src/e2e) and verify end-to-end functionality.
