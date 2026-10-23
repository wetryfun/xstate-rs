# xstate-rs

A Rust runtime for [XState v6](https://stately.ai/docs/xstate/v6/) machine definitions. Author machines in TypeScript, export them as JSON with `machineToJSON`, and run them in Rust with guards, actions and delays implemented in Rust.

This is an independent project, not an official Stately package. It targets `xstate@6.0.0-alpha.70`.

## Status

The statechart algorithm is a port of XState's own (`crates/xstate-rt/src/core.rs`), shared by the native interpreter and by XState's JavaScript runtime through WebAssembly: XState's core test suite runs on it (`tools/xstate-suite/run.sh`). Milestone 1 (`xstate-rt` core) is in place: loading and validating `MachineJSON`, compound, parallel, final and history states, guards (including `@xstate.not` and `@xstate.stateIn`), built-in and custom actions, `always`, `after`, delayed `@xstate.raise` and `@xstate.cancel`, `@xstate.emit`, host-driven timers, and persist/restore.

Milestone 2 (actors) is a port of XState's pure system API: `invoke` of machines and async logic, `onDone` / `onError` / `onSnapshot`, state and invoke `timeout` / `onTimeout`, the `systemId` registry, stop cascades, and whole-system snapshots in XState's format. Behavior is checked against XState itself with shared fixtures, at machine and at system level.

Not yet supported (rejected at load time with the JSON path): choice and route states, state and transition `input`, and any `@code` / `@expr`.

## Example

```rust
use std::sync::{Arc, Mutex};
use serde_json::{json, Value};
use xstate_rt::{Actor, Machine, Registry};

let definition = json!({
    "id": "light",
    "initial": "green",
    "context": { "cars": 0 },
    "states": {
        "green": {
            "after": { "1000": { "target": "yellow" } },
            "on": { "car": { "context": { "cars": 1 }, "actions": [{ "type": "honk", "params": "beep" }] } }
        },
        "yellow": { "on": { "next": { "target": "red", "guard": { "type": "busy", "params": { "min": 1 } } } } },
        "red": { "type": "final" }
    }
});

#[derive(serde::Deserialize)]
struct Busy { min: u64 }

let honks = Arc::new(Mutex::new(Vec::new()));
let sink = honks.clone();
let mut registry: Registry<Value, Value> = Registry::new();
registry
    // Custom actions are effects: they run after the step, with their params only.
    .action_with("honk", move |sound: &String| sink.lock().unwrap().push(sound.clone()))
    .guard_with("busy", |args, p: &Busy| args.context["cars"].as_u64().unwrap_or(0) >= p.min);

let machine = Arc::new(Machine::from_json(&definition, &registry)?);
let mut actor = Actor::start(machine, 0)?;
actor.send(json!({ "type": "car" }), 10)?;
assert_eq!(*honks.lock().unwrap(), vec!["beep"]);
assert_eq!(actor.next_deadline(), Some(1000));
actor.advance(1000)?;                                // the host drives time
actor.send(json!({ "type": "next" }), 1200)?;
assert_eq!(actor.value(), json!("red"));
```

Typed contexts and events work the same way: any `C: Serialize + DeserializeOwned + Clone` and any serde event enum implementing `EventType`.

## Development

```sh
cargo test                       # unit, runtime and differential tests
tools/xstate-suite/run.sh        # XState's own test suite on the Rust core (needs pnpm)
cargo clippy --all-targets
cd tools/differential && npm install && npm run generate   # regenerate expected traces from XState
node tools/differential/generate-system.mjs                 # regenerate system-level traces
```

See [CLAUDE.md](CLAUDE.md) for the design, the verified v6 semantics, and the roadmap.
