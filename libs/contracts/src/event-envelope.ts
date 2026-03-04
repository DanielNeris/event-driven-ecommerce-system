/**
 * Standard event envelope for all events in the system.
 * Matches architecture: eventId, type, version, occurredAt, correlationId, payload.
 */

import type { EventType, TopicName } from './topics';

export type EventEnvelope<TPayload> = {
  eventId: string; // UUID for idempotency
  type?: string; // Event name (e.g. "OrderCreated")
  version?: number; // Version of event schema
  occurredAt: string; // ISO date
  correlationId?: string; // Order/correlation ID (partition key = orderId)
  traceId?: string; // Optional tracing ID
  payload?: TPayload; // Actual event data
  headers?: Record<string, string>; // Optional headers
};

export type EmitOptions<TPayload> = {
  topic: TopicName;
  type?: EventType;
  payload?: TPayload;
  version?: number;
  key?: string; // Partition key (orderId)
  correlationId?: string;
  headers?: Record<string, string>;
  /** For outbox replay: use stored eventId and occurredAt */
  eventId?: string;
  occurredAt?: string;
};

/** Dead Letter Queue payload: original message + failure metadata for replay/inspection. */
export type DlqPayload = {
  sourceTopic: string;
  partitionKey: string;
  originalMessage: unknown;
  error: string;
  attemptCount: number;
  failedAt: string; // ISO
  consumer?: string;
};
