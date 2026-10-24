//! XState's traces of the JavaScript code-node fixtures (`js_*`, ported
//! from the Rust-expression fixtures by `tools/differential/port-js.mjs`
//! and run there with XState's own evaluator convention), replayed with
//! QuickJS through the xstate-rt harnesses.

// Both harnesses include the shared expressions module.
#![allow(clippy::duplicate_mod)]

use serde_json::Value;
use xstate_js::JsEvaluator;
use xstate_rt::Registry;

/// The languages these harnesses run fixtures for.
pub const LANGUAGES: &[&str] = &["js"];

pub fn register_languages(registry: &mut Registry<Value, Value>) {
    registry.evaluator("js", JsEvaluator::new());
}

#[path = "../../xstate-rt/tests/differential.rs"]
mod differential;

#[path = "../../xstate-rt/tests/system_differential.rs"]
mod system_differential;
