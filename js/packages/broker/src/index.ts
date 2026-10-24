/**
 * `@xmachines/broker`: the broker interfaces (`BrokerConsumer`,
 * `BrokerProducer`, `PartitionLease`) and `serve`, which maps each record key
 * to a run (`@xmachines/run`) and drives it from leased partitions over Web
 * Streams. Transports: `@xmachines/broker-kafka` (an injected
 * `@platformatic/kafka` client), `@xmachines/broker-redpanda`, and
 * `@xmachines/broker/memory`.
 */
export { LeaseRevokedError } from './types.ts';
export type { BrokerConsumer, BrokerProducer, BrokerRecord, OutboundRecord, PartitionLease } from './types.ts';
export { createLease } from './lease.ts';
export type { LeaseHandle, LeaseOptions, LeaseTransport } from './lease.ts';
export { producerSink } from './sink.ts';
export type { ProducerSink, SinkOptions } from './sink.ts';
export { serve } from './serve.ts';
export type { BrokerRun, ServeOptions, Serving, Topics } from './serve.ts';
