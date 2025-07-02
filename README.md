# coDB

coDB is a relational database built from scratch.

## Table of Contents

- [Project structure](#project-structure)
    - [Adding a new crate](#adding-a-new-crate)
    - [Adding internal dependency](#adding-internal-dependency)
    - [Adding external dependecy](#adding-external-dependecy)
- [Running locally](#running-locally)
    - [Prerequisites](#prerequisites)
    - [Running binaries](#running-binaries)
    - [Running tests](#running-tests)
    - [Generating docs](#generating-docs)

## Project structure

The project is organized as a Cargo workspace with multiple crates for modularity and improved compile times.

### Adding a new crate

- **Binary crate:**
    ```shell
    cargo new <BINARY_NAME> --vsc none --bin
    ```
- **Library crate:**
    ```shell
    cargo new <LIBRARY_NAME> --vsc none --lib
    ```

Ensure the root `Cargo.toml` includes the new crate under `[workspace].members`. Add it manually if needed.

### Adding internal dependency

To use one crate from another within the workspace (e.g. `crateA` imports `crateB`), add this to `crateA/Cargo.toml`:

```toml
[dependencies]
crateB = { path = "../crateB" }
```

### Adding external dependecy

TODO: figure out whether to do it in root Cargo.toml or do it in each crate separately. When decision is made update this paragraph.

## Running locally

### Prerequisites

To build and run this project, you need Rust installed (version 1.88 or higher). You can install it from [https://rust-lang.org](https://rust-lang.org) or via `rustup`.

### Running binaries

To run a binary crate:
```shell
cargo run -p <BINARY_NAME>
```

### Running tests

Run all tests:

```shell
cargo test
```

Run tests for a specific crate:

 - run it from crate's directory:
    1. enter crate's directory:
        ```shell
        cd <CRATE_NAME>
        ```
    2. Run tests:
        ```shell
        cargo test
        ```
 
 - run it from root of the project by specyfing crate name:
    - Binary crate:
        ```shell
        cargo test --bin <CRATE_NAME>
        ```
    - Library crate:
        ```shell
        cargo test --lib <CRATE_NAME>
        ```

### Generating docs

To generate docs run:

```shell
cargo doc --document-private-items --open
```