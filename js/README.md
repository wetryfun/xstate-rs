# @xmachines packages

TypeScript packages that implement the XMachines Run, Streams, Git and Broker
RFCs in pure JavaScript, over WHATWG Web Streams, with upstream XState as the
default engine. The Rust service in `crates/xstate-kafka` is the
high-throughput, exactly-once path; these packages are the JavaScript one.

| Package | What it is |
| --- | --- |
| `@xmachines/streams` | Framing transforms (`ndjson`, `sse`, `json`), durability as a transform (`durable(log)`), `replay`, `project`, `filter`, and write-through helpers that keep acknowledgements (`through`, `durableWritable`, `fanIn`) |
| `@xmachines/run` | Runs: ports `in` / `out` / `err` / `snapshot`, Git-compatible ids, time as an input (`xmachines.time`), atomic batches; `host` (clock, timers, journal) and `runner` (runs by name) |
| `@xmachines/run/xstate` | The engine on upstream XState's pure system API |
| `@xmachines/run/native` | The engine on the xstate-rs runtime as a Node addon (`crates/xstate-node`, injected) |
| `@xmachines/run/wasi` | The engine on the xstate-rs runtime as a WASI component (transpiled with jco, injected) |
| `@xmachines/git` | Git durability on isomorphic-git: machines are orphan branches, runs are forks, batches and snapshots are commits (`GitStore`, `GitJournal`) |
| `@xmachines/git/browser` | The same for browsers: installs the global `Buffer` isomorphic-git needs, before it loads |
| `@xmachines/git/http` | A pure-JavaScript Git Smart HTTP server (`/<repo>.git/**`) that refuses history rewrites |
| `@xmachines/broker` | `BrokerConsumer` / `BrokerProducer` / `PartitionLease`, and `serve`: record keys to runs, from leased partitions |
| `@xmachines/broker/memory` | An in-memory broker with Kafka's model, for tests and local runs |
| `@xmachines/broker-kafka` | The transport over injected `@platformatic/kafka` clients |
| `@xmachines/broker-redpanda` | `@platformatic/kafka` options for Redpanda, and the Kafka transport |

```ts
import * as fs from 'node:fs';
import { Consumer, Producer } from '@platformatic/kafka';
import { serve } from '@xmachines/broker';
import { kafkaConsumer, kafkaProducer, redpandaClientOptions } from '@xmachines/broker-redpanda';
import { GitStore } from '@xmachines/git';
import { runner } from '@xmachines/run';
import { xstateEngine } from '@xmachines/run/xstate';

const options = redpandaClientOptions({ brokers: ['localhost:9092'] });
const serving = serve({
  consumer: kafkaConsumer({ consumer: new Consumer({ ...options, groupId: 'orders' }) }),
  producer: kafkaProducer({ producer: new Producer(options) }),
  topics: { input: 'orders', output: 'orders-out', deadLetter: 'orders-dead', snapshots: 'orders-snapshots' },
  open: runner({ engine: xstateEngine(), machine: orderMachineJSON, journals: new GitStore({ fs, gitdir: 'orders.git' }) }),
});
```

## Runs (Run RFC)

A run is one execution lineage of a machine, driven only by ordered events.

- **Ports.** `in` (a `WritableStream` of events or atomic batches), `out`
  (emitted events), `err` (rejected batches), `snapshot` (the latest state,
  read-only). A write to `in` resolves once the chunk is applied and its
  outputs are enqueued; `out` and `err` apply backpressure (cancel one to drop
  it), `snapshot` keeps only the latest value.
- **Determinism.** A run never reads a clock: `{ type: "xmachines.time",
  now }` advances its clock and fires due timers; other events apply at the
  current clock. The same machine and inputs give the same outputs and state.
- **Atomic batches.** A batch that fails (an engine error, a machine in
  `error`, an input that is not an event) leaves the run as it was and goes
  to `err`, time events included.
- **Identity.** A machine is the Git commit of its `machine.json`; a run is
  the commit that forks it with `run.json` (`runId(definition, { name })`).
  The ids are computed with Web Crypto and are the same commit hashes Git
  stores.
- **Engines.** `xstateEngine()`, `nativeEngine({ addon })` and
  `wasiEngine({ actors })` all implement XState's pure system semantics and
  persist XState's `SystemSnapshot` JSON, so a run's state moves between
  engines (tested in every direction).
- **Async work.** An invoked async actor's work waits as an external
  effect, listed in `snapshot.work` (`{ id, actor, src, input }`), and
  completes through an input event: `resolved(id, data)` / `rejected(id,
  data)` (XState's `xstate.system.effect.result`), journaled and replayed
  like any input. `host({ work: { fetchUser: (input, { signal }) => ... } })`
  runs it, aborts it when the invoking state is left, and runs it again after
  a restore if it is still pending (make side effects idempotent by `id`).
- **Hosting.** `host` stamps each input with the time (a leading time event
  in the same batch), appends it to the run's `Journal`, applies it, sends
  time events when timers are due, and journals snapshots. `runner` opens
  runs by name, restoring from the latest snapshot and replaying the batches
  after it (without emitting their outputs again).

## Git (Git RFC)

`GitStore` keeps machines and runs in a repository (bare, created on first
use; the fs is injected): `refs/heads/machines/<id>` is a machine's orphan
branch, `refs/heads/runs/<run id>` a run, whose commits are its batches
(`events.ndjson`) and snapshots (`snapshot.json`). Refs only move forward.
`smartHttp({ fs, resolve, receive })` serves repositories to `git clone`,
`fetch` and `push` (fast-forward only; deletes and rewrites are refused with
a reason), so runs replicate with plain Git.

`push(store, remote)` and `pull(store, remote)` replicate machines and runs
through isomorphic-git's HTTP client (injected: `isomorphic-git/http/web` or
`/http/node`) with any Smart HTTP server. Both are append-only: a push only
fast-forwards, and a pull takes a remote branch only when it extends the
local one (diverged runs are reported, never merged).

## In the browser

Everything but the Node addon, the Kafka transport and the Smart HTTP server
runs in browsers (those use only Web Streams, Web Crypto and `fetch`;
streams are read through readers, so browsers without async iteration of
`ReadableStream`, such as Safari, work too):

```ts
import { GitStore, pull, push } from '@xmachines/git/browser'; // first: it installs Buffer
import LightningFS from '@isomorphic-git/lightning-fs';
import http from 'isomorphic-git/http/web';
import { runner } from '@xmachines/run';
import { xstateEngine } from '@xmachines/run/xstate';
import { project, sse } from '@xmachines/streams';

const store = new GitStore({ fs: new LightningFS('runs'), gitdir: '/runs.git' }); // IndexedDB
const remote = { http, url: 'https://example.com/git/runs.git' };
await pull(store, remote);
const run = await runner({ engine: xstateEngine(), machine, journals: store })('front-door');
run.out.pipeTo(render);
await (await fetch('/events')).body!.pipeThrough(sse.decode()).pipeThrough(project((m) => m.data)).pipeTo(run.in);
await push(store, remote);
```

A server on another origin needs CORS: `smartHttp({ ..., cors: 'https://app.example.com' })`
(or `true`, a list, or a predicate). Web Crypto needs a secure context
(HTTPS, or localhost).

## Brokers (Broker RFC)

`serve` maps each record key to a run (`open(key)`), writes a partition's
records in order, produces `out` to the output topic, `err` and unprocessable
records to the dead-letter topic (`reason` header), snapshots to a
compacted snapshot topic, and pending work to a work topic for workers that
answer with effect results on the input topic. A run that falls behind stops its partition's
reading, and the lease pauses fetching. Offsets are committed at least once:
after every record up to them was applied and the outputs enqueued by then
were acknowledged.

## Development

```sh
cd js && npm install
npm test           # node --test; sources run directly (Node 22 strips types)
npm run typecheck
npm run build      # dist/ for publishing
```

Optional binaries (tests that need them skip without them):

- Kafka tests: librdkafka's mock cluster, `cargo build -p xstate-kafka
  --features kafka --example mock_cluster` (or `XSTATE_MOCK_CLUSTER`).
- Native engine: `crates/xstate-node/build.sh` (or `XSTATE_NODE_ADDON`).
- WASI engine: `cargo build -p xstate-wasi --target wasm32-wasip2 --release
  --target-dir target/wasi` (or `XSTATE_WASI_COMPONENT`).
- Browser test: Playwright with its Chromium (`require('playwright')`); the
  page is bundled with esbuild.
