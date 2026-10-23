//! Integration tests for the WASI components (see `tests/`), which build
//! them for `wasm32-wasip2` and run them in Wasmtime: the generic
//! `xstate-wasi` component, with a host that implements its guard, action and
//! delay imports (`component.rs`), and the typed checkout component built
//! with `xstate-wasi-guest`, whose implementations are inside it
//! (`machine_component.rs`).
//!
//! Not a default workspace member, because Wasmtime takes a while to build:
//! run `cargo test -p xstate-wasi-host`.
