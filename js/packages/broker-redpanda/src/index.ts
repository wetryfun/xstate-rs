/**
 * `@xmachines/broker-redpanda`: `@xmachines/broker` for Redpanda. Redpanda
 * speaks the Kafka API, so the transport is `@xmachines/broker-kafka` over
 * injected `@platformatic/kafka` clients; this package builds those
 * clients' options for a Redpanda cluster (SASL/SCRAM, Redpanda's default
 * mechanism, over TLS when credentials are given; idempotent producers, which
 * Redpanda supports) and re-exports the adapters.
 */
export { kafkaConsumer, kafkaProducer } from '@xmachines/broker-kafka';
export type { KafkaConsumerOptions, KafkaProducerOptions } from '@xmachines/broker-kafka';

export interface RedpandaOptions {
  /** Seed brokers, like `["redpanda-0:9092"]`. */
  readonly brokers: readonly string[];
  readonly clientId?: string;
  /** SASL credentials; with them, TLS is on unless `tls` is false. */
  readonly sasl?: {
    readonly mechanism: 'SCRAM-SHA-256' | 'SCRAM-SHA-512' | 'PLAIN';
    readonly username: string;
    readonly password: string;
  };
  /** TLS: `true`, Node TLS options, or `false`. */
  readonly tls?: boolean | Record<string, unknown>;
}

/** Options shared by `new Consumer(...)`, `new Producer(...)` and `new Admin(...)`. */
export interface RedpandaClientOptions {
  readonly clientId: string;
  readonly bootstrapBrokers: string[];
  readonly sasl?: { mechanism: 'SCRAM-SHA-256' | 'SCRAM-SHA-512' | 'PLAIN'; username: string; password: string };
  readonly tls?: Record<string, unknown>;
}

/** `@platformatic/kafka` client options for a Redpanda cluster. */
export function redpandaClientOptions(options: RedpandaOptions): RedpandaClientOptions {
  const tls = options.tls ?? (options.sasl ? true : false);
  return {
    clientId: options.clientId ?? 'xmachines',
    bootstrapBrokers: [...options.brokers],
    ...(options.sasl && { sasl: { ...options.sasl } }),
    ...(tls && { tls: tls === true ? {} : tls }),
  };
}

/** Producer options for Redpanda: idempotent, acknowledged by all in-sync replicas. */
export function redpandaProducerOptions(options: RedpandaOptions): RedpandaClientOptions & { idempotent: true; acks: -1 } {
  return { ...redpandaClientOptions(options), idempotent: true, acks: -1 };
}
