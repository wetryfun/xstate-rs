//! Integration tests for the `xstate-wasi` component (see `tests/`), which
//! build it for `wasm32-wasip2` and run it in Wasmtime with a host that
//! implements the component's guard, action and delay imports.
//!
//! Not a default workspace member, because Wasmtime takes a while to build:
//! run `cargo test -p xstate-wasi-host`.
