# @xmachines/broker packages

TypeScript packages that serve machine runs from Kafka-API brokers over Web
Streams, following the xmachin.es Broker RFC. The Rust service in
`crates/xstate-kafka` is the high-throughput, exactly-once path; these
packages are the JavaScript one (at-least-once), for machines whose runs
live in JS or in the xstate-rs runtime as a WASI component.

| Package | What it is |
| --- | --- |
| `@xmachines/broker` | `BrokerConsumer` / `BrokerProducer` / `PartitionLease`, `serve` (the runner), `producerSink`, `createLease` (for transport authors) |
| `@xmachines/broker/memory` | An in-memory broker with Kafka's model, for tests and local runs |
| `@xmachines/broker/xstate` | Runs as XState actors (`xstateRuns`), with a `SnapshotStore` |
| `@xmachines/broker/wasi` | Runs on the xstate-rs runtime: the `xstate-wasi` component transpiled with jco (`wasiRuns`, `componentImports`) |
| `@xmachines/broker-kafka` | The transport for KafkaJS-style clients (KafkaJS, or `@confluentinc/kafka-javascript`'s KafkaJS API) |
| `@xmachines/broker-redpanda` | The Kafka transport with a KafkaJS client set up for Redpanda (SCRAM over TLS, idempotent producer, murmur2 keys) |

```ts
import { serve } from '@xmachines/broker';
import { xstateRuns, MemorySnapshotStore } from '@xmachines/broker/xstate';
import { redpanda } from '@xmachines/broker-redpanda';

const rp = redpanda({ brokers: ['localhost:9092'] });
const serving = serve({
  consumer: rp.consumer({ groupId: 'orders' }),
  producer: rp.producer(),
  topics: { input: 'orders', output: 'orders-out', deadLetter: 'orders-dead' },
  createRun: xstateRuns({ machine: orderMachineJSON, store: new MemorySnapshotStore() }),
});
```

How `serve` runs (the RFC's execution model):

- **Partition leasing.** Each partition assigned to the consumer group member
  is a `PartitionLease`: its records in offset order, `commit`, and a signal
  aborted when a rebalance revokes it. One holder per partition is the
  single-writer guarantee for its runs.
- **Runs.** A record's key is its run id; the run (`{ in: WritableStream,
  out: ReadableStream }`) is created on its first record. Inputs are written
  to `in` in partition order; `out` is produced to the output topic under the
  run id. Records without a key, undecodable values and inputs a run rejects
  go to the dead-letter topic with a `reason` header (a run that rejected an
  input ends; the next input starts a new one).
- **Backpressure.** A run that is behind (its `in` writer not ready) stops the
  partition's reading; a lease whose buffer passes its high-water mark pauses
  fetching that partition (`consumer.pause`) and resumes when the reader
  catches up. Outputs wait on the producer the same way (`producerSink`
  batches sends, with a cap on sends in flight).
- **At-least-once.** An offset is committed once every record up to it was
  processed (its `in` write resolved) and the outputs enqueued by then were
  acknowledged. After a rebalance or crash, records after the last commit are
  delivered again, so runs should tolerate redelivery.
- **Durability.** Run state goes wherever the run puts it: `xstateRuns` and
  `wasiRuns` save the persisted snapshot to a `SnapshotStore` before an input
  counts as processed, and restore from it when a partition moves.

## Development

```sh
cd js && npm install
npm test           # node --test, sources run directly (Node 22 strips types)
npm run typecheck
npm run build      # dist/ for publishing
```

The Kafka tests run against librdkafka's mock cluster (the Kafka protocol in
a Rust process): `cargo build -p xstate-kafka --features kafka --example
mock_cluster` first (or set `XSTATE_MOCK_CLUSTER`). The WASI tests need the
component: `cargo build -p xstate-wasi --target wasm32-wasip2 --release
--target-dir target/wasi` (or set `XSTATE_WASI_COMPONENT`). Both are skipped
when the binaries are missing.
