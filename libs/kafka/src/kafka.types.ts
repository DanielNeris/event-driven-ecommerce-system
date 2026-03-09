import type { EventType, TopicName } from '@app/contracts';

/**
 * Environment variables expected by libs/kafka
 */
export type KafkaEnv = {
  /**
   * Comma-separated list of brokers
   * Example: "localhost:9092" or "b1:9092,b2:9092"
   */
  KAFKA_BROKERS: string;
};

export interface AvroRecordSchema<TPayload> {
  topic: TopicName;
  type: EventType;
  payload?: TPayload;
  key?: string;
  correlationId?: string;
  version?: number;
  occurredAt?: string;
  headers?: Record<string, string>;
  eventId?: string;
}
