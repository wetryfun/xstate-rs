# xstate-rs

A Rust runtime for [XState v6](https://stately.ai/docs/xstate/v6/) machine definitions. Author machines in TypeScript, export them as JSON with `machineToJSON`, and run them in Rust with guards, actions and delays implemented in Rust.

This is an independent project, not an official Stately package. It targets `xstate@6.0.0-alpha.70`.

## Status

The statechart algorithm is a port of XState's own (`crates/xstate-rt/src/core.rs`), shared by the native interpreter and by XState's JavaScript runtime through WebAssembly: XState's core test suite runs on it (`tools/xstate-suite/run.sh`). Milestone 1 (`xstate-rt` core) is in place: loading and validating `MachineJSON`, compound, parallel, final and history states, guards (including `@xstate.not` and `@xstate.stateIn`), built-in and custom actions, `always`, `after`, delayed `@xstate.raise` and `@xstate.cancel`, `@xstate.emit`, host-driven timers, and persist/restore.

Milestone 2 (actors) is a port of XState's pure system API: `invoke` of machines and async logic, `onDone` / `onError` / `onSnapshot`, state and invoke `timeout` / `onTimeout`, the `systemId` registry, stop cascades, and whole-system snapshots in XState's format. Behavior is checked against XState itself with shared fixtures, at machine and at system level, and with randomly generated machines and actor systems (milestone 4).

Milestone 3 (codegen) turns a machine authored in TypeScript with Zod schemas into typed Rust: `tools/extract/extract.mjs` writes a `<name>.xstate.json` bundle, and `xstate-codegen` (from `build.rs`) generates context, event and params types plus an implementation trait. See [`examples/checkout`](examples/checkout).

Milestone 5 (WASI) is a WASI 0.2 component, `crates/xstate-wasi` (WIT in `crates/xstate-wasi/wit/xstate.wit`), that loads any definition at run time and calls the host for guards, actions and delays. Hosts in any language with a component runtime can run machines with it.

Milestone 6 (Kafka) is `crates/xstate-kafka`, a service for any Kafka-API broker (Apache Kafka, Redpanda, ...): actors sharded by input partition, one worker thread per core, each batch committing emitted events, changelog snapshots (MessagePack, compacted topic) and input offsets in one Kafka transaction. With `stateDir` set, workers keep local checkpoints so a restart reads only the changelog tail; `checkpoints` adds more checkpoint stores, such as a Git archive where every checkpoint is a commit of readable per-actor JSON on a per-partition ref (pushed to a remote, which a new node restores from). A worker that is busy (a long commit, restore or checkpoint save) pauses its partitions and keeps polling, so it stays in its consumer group. Invoked async actors become requests on an effects topic, completed by `xstate.system.effect.result` records on the input topic. Changelog snapshots are XState's own `SystemSnapshot` by default; a compact form and a protobuf codec (`proto/xstate_snapshot.proto`) are opt-in. Input events are JSON, protobuf (an `Event` message generated from the machine's event schemas: `xstate-kafka proto events <bundle>`) or MessagePack, chosen per record by its `content-type` header; Avro (Schema Registry wire format, schemas generated from the machine's event and context schemas) works for input events, output events and changelog snapshots. `connect/mqtt.yaml` bridges MQTT devices into the input topic with Redpanda Connect, and `dedupe` skips redelivered events by a per-device sequence number. Machine definitions can change under running actors: earlier versions keep running or are migrated (`previousVersions`), and a record that cannot be restored becomes a dead letter instead of stopping the service. To try it end to end on your machine: `cd crates/xstate-kafka/deploy && docker compose up -d --build && ./smoke.sh --no-up` (see its README). It is tested against librdkafka's in-process mock cluster. The `xstate-kafka` binary runs a machine from a JSON config file with Prometheus metrics: `cargo run --release -p xstate-kafka --features kafka -- service.json`.

Computed values are XState's own code nodes in the `rust` language: `{ "@code": "addLine", "@lang": "rust" }` in a context patch, an assign, an event, params, a guard, a delay, an invoke input or an output names a Rust function (`Registry::expr`, or the generated `expr_add_line` trait method) that receives the context, event and params. XState runs the same JSON with an evaluator for `rust`.

Not yet supported (rejected at load time with the JSON path): `@code` / `@expr` in other languages, and `spawn` / `sendTo` from JSON actions (waiting for XState's own JSON form). Rust transition functions (`{ "@code": "pick", "@lang": "rust" }` as a transition) work as XState's `to` functions, with `enq`.

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

Typed contexts and events work the same way: any `C: Serialize + DeserializeOwned + Clone` and any serde event enum implementing `EventType`. Codegen writes those types for you:

```rust
// build.rs
xstate_codegen::Builder::new().machine("machines/checkout.xstate.json").build().unwrap();

// src/lib.rs
pub mod checkout { xstate_rt::include_machine!("checkout"); }

impl checkout::CheckoutImpl for Shop {
    fn guard_within_limit(&self, args: &GuardArgs<'_, Context, Event>, p: &WithinLimitParams) -> bool {
        matches!(args.event, Ev::User(Event::Checkout { total }) if *total <= p.max)
    }
    // ... one method per action, delay and actor the machine references
}

let mut actor = checkout::Actor::start(Arc::new(checkout::machine(shop)?), 0)?;
actor.send(Event::Checkout { total: 120.0 }, 10)?;
```

## Development

```sh
cargo test                       # unit, runtime and differential tests
tools/xstate-suite/run.sh        # XState's own test suite on the Rust core (needs pnpm)
cargo clippy --all-targets --workspace
cargo test -p xstate-wasi-host      # the WASI component, run in Wasmtime
cargo test -p xstate-kafka --features kafka   # the service, on librdkafka's mock cluster
cargo bench -p xstate-rt --bench interpreter     # interpreter hot path
cd tools/differential && npm install && npm run generate   # regenerate expected traces from XState
node tools/differential/generate-system.mjs                 # regenerate system-level traces
tools/differential/fuzz.sh 1000 10                          # random machines and actor systems vs XState
cd tools/extract && npm install                             # once, for the extract script
node tools/extract/extract.mjs examples/checkout/machines/*.ts   # re-extract bundles (--check to verify)
```

See [CLAUDE.md](CLAUDE.md) for the design, the verified v6 semantics, and the roadmap.
