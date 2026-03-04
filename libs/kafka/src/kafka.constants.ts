import { CompressionTypes } from 'kafkajs';

export const KAFKA_CLIENT = 'KAFKA_CLIENT';

/**
 * Default send options for high throughput: Snappy compression, acks=-1, timeout.
 */
export const HIGH_THROUGHPUT_SEND_OPTIONS = {
  compression: CompressionTypes.Snappy,
  acks: -1 as const,
  timeout: 30_000,
};
