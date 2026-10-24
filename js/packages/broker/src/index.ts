/**
 * `@xmachines/broker`: the broker interfaces (`BrokerConsumer`,
 * `BrokerProducer`, `PartitionLease`) and `serve`, which runs machine
 * instances keyed by record key from leased partitions over Web Streams.
 * Transports: `@xmachines/broker-kafka`, `@xmachines/broker-redpanda`, and
 * `@xmachines/broker/memory`. Runs: `@xmachines/broker/xstate` (XState actors) and
 * `@xmachines/broker/wasi` (the xstate-rs runtime as a WASI component).
 */
export { LeaseRevokedError } from './types.ts';
export type { BrokerConsumer, BrokerProducer, BrokerRecord, OutboundRecord, PartitionLease } from './types.ts';
export { createLease } from './lease.ts';
export type { LeaseHandle, LeaseOptions, LeaseTransport } from './lease.ts';
export { producerSink } from './sink.ts';
export type { ProducerSink, SinkOptions } from './sink.ts';
export { serve } from './serve.ts';
export { MemorySnapshotStore } from './store.ts';
export type { SnapshotStore } from './store.ts';
export type { Run, ServeOptions, Serving, Topics } from './serve.ts';
