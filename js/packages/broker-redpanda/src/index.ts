/**
 * `@xmachines/broker-redpanda`: `@xmachines/broker` for Redpanda. Redpanda
 * speaks the Kafka API, so this is `@xmachines/broker-kafka` with a KafkaJS
 * client set up for it: SASL/SCRAM (Redpanda's mechanism) over TLS when
 * given credentials, idempotent producers (Redpanda supports them), and
 * Kafka's own key partitioner (murmur2), so keys land where other Kafka
 * clients put them.
 */
import { kafkaConsumer, kafkaProducer, type KafkaConsumerOptions, type KafkaLike, type KafkaProducerOptions } from '@xmachines/broker-kafka';
import type { BrokerConsumer, BrokerProducer } from '@xmachines/broker';
import kafkajs from 'kafkajs';

const { Kafka, logLevel, Partitioners } = kafkajs;

export interface RedpandaOptions {
  /** Seed brokers, like `["redpanda-0:9092"]`. */
  readonly brokers: readonly string[];
  readonly clientId?: string;
  /** SASL credentials; with them, TLS is on unless `ssl` is false. */
  readonly sasl?: {
    readonly mechanism: 'scram-sha-256' | 'scram-sha-512' | 'plain';
    readonly username: string;
    readonly password: string;
  };
  /** TLS: `true`, Node TLS options, or `false`. */
  readonly ssl?: boolean | Record<string, unknown>;
  /** More KafkaJS client settings. */
  readonly client?: Record<string, unknown>;
}

export interface Redpanda {
  /** The KafkaJS client, for admin work (creating topics, ...). */
  readonly kafka: InstanceType<typeof Kafka>;
  consumer(options: KafkaConsumerOptions): BrokerConsumer;
  producer(options?: KafkaProducerOptions): BrokerProducer;
}

/** A Redpanda cluster's consumers and producers. */
export function redpanda(options: RedpandaOptions): Redpanda {
  const kafka = new Kafka({
    clientId: options.clientId ?? 'xmachines',
    brokers: [...options.brokers],
    logLevel: logLevel.WARN,
    ...(options.sasl ? { sasl: { ...options.sasl } as never, ssl: options.ssl ?? true } : {}),
    ...(!options.sasl && options.ssl !== undefined ? { ssl: options.ssl as never } : {}),
    ...options.client,
  });
  const client = kafka as unknown as KafkaLike;
  return {
    kafka,
    consumer: (o) => kafkaConsumer(client, o),
    producer: (o = {}) =>
      kafkaProducer(client, {
        ...o,
        config: { idempotent: true, maxInFlightRequests: 1, createPartitioner: Partitioners.DefaultPartitioner, ...o.config },
      }),
  };
}
