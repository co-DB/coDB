# coDB

coDB is a relational database built from scratch.

## Table of Contents

- [Project structure](#project-structure)
    - [Adding a new crate](#adding-a-new-crate)
    - [Adding internal dependency](#adding-internal-dependency)
    - [Adding external dependecy](#adding-external-dependecy)
    - [Updating rust version](#updating-rust-version)
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

Ensure the root `Cargo.toml` includes the new crate under `[workspace].members` and `new-crate/Cargo.toml` includes `rust-version.workspace = true` under `package`. Add those manually if needed.


### Adding internal dependency

To use one crate from another within the workspace (e.g. `crateA` imports `crateB`), add this to `crateA/Cargo.toml`:

```toml
[dependencies]
crateB = { path = "../crateB" }
```

### Adding external dependecy

If an external crate is needed by only one coDB crate, add it to that crate's Cargo.toml.  
For example, to add `external-1` to the `query` crate, add it in `query/Cargo.toml`.

If an external crate is used by multiple workspace crates, add it to the root `Cargo.toml` under `[workspace.dependencies]` and reference it from each crate with `workspace = true`.  
For example, add `external-common` to the root `Cargo.toml`:

```toml
[workspace.dependencies]
external-common = "x.y"
```

Then in `query/Cargo.toml` and `engine/Cargo.toml`:

```toml
[dependencies]
external-common.workspace = true 
```

### Updating rust version

To update rust version you should update:
- `rust_version` field in root `Cargo.toml`
- `Set up Rust 1.X` step in github actions (`github/workflows/build_and_test.yaml`)

## Running locally

### Prerequisites

To build and run this project, you need Rust installed. You can install it from [https://rust-lang.org](https://rust-lang.org) or via `rustup`.

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