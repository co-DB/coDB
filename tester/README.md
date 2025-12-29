# Tester

This crate is a small test client used only for running end-to-end and performance tests against a running coDB server.

Prerequisites
- Ensure the coDB server is already running before using the tester (the tester connects to the server).

Quick run
- Build & run the tester (example):

```bash
cargo run -p tester -- concurrent-reads-index --runs 1 --threads 8 --records 1000 --bound-size 10
```

Performance tests
- Performance tests live in [tester/src/performance](tester/src/performance).

Adding a new performance test
- Add a new file in [tester/src/performance](tester/src/performance) implementing the `Suite` trait (see existing tests for examples).
- Export the module in [tester/src/performance/mod.rs](tester/src/performance/mod.rs) with `pub mod your_test;`.
- Wire a CLI subcommand and runner in [tester/src/main.rs](tester/src/main.rs) so you can execute the test from the command line.
