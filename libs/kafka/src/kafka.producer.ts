import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { randomUUID } from 'crypto';
import { lastValueFrom } from 'rxjs';
import { KAFKA_CLIENT } from './kafka.constants';
import { EventEnvelope, EmitOptions, DlqPayload } from '@app/contracts';

export type EmitToDlqOptions = {
  dlqTopic: string;
  sourceTopic: string;
  partitionKey: string;
  originalMessage: unknown;
  error: string;
  attemptCount: number;
  consumer?: string;
};

@Injectable()
export class KafkaProducer implements OnModuleInit {
  constructor(@Inject(KAFKA_CLIENT) private readonly client: ClientKafka) {}

  async onModuleInit() {
    await this.client.connect();
  }

  /**
   * Emit an event with a standard envelope.
   * `key` is important for partitioning and per-entity ordering.
   */
  async emit<TPayload>({
    topic,
    type,
    payload,
    key,
    version = 1,
    correlationId,
    headers,
    eventId: outboxEventId,
    occurredAt: outboxOccurredAt,
  }: EmitOptions<TPayload>) {
    const envelope: EventEnvelope<TPayload> = {
      eventId: outboxEventId ?? randomUUID(),
      type,
      version,
      correlationId: correlationId ?? key,
      traceId: headers?.['X-Trace-Id'],
      occurredAt: outboxOccurredAt ?? new Date().toISOString(),
      payload,
      headers,
    };

    await lastValueFrom(
      this.client.emit(topic, {
        key: key ?? correlationId,
        value: envelope,
        headers,
      }),
    );
  }

  /**
   * Send a failed message to the Dead Letter Queue for replay or inspection.
   */
  async emitToDlq({
    dlqTopic,
    sourceTopic,
    partitionKey,
    originalMessage,
    error,
    attemptCount,
    consumer,
  }: EmitToDlqOptions): Promise<void> {
    const payload: DlqPayload = {
      sourceTopic,
      partitionKey,
      originalMessage,
      error,
      attemptCount,
      failedAt: new Date().toISOString(),
      consumer,
    };
    await lastValueFrom(
      this.client.emit(dlqTopic, {
        key: partitionKey,
        value: payload,
      }),
    );
  }

  /**
   * Emit a message with Schema Registry–encoded value (buffer). No envelope.
   * Use for topics that use Confluent serialization (Avro).
   */
  async emitEncoded(
    topic: string,
    key: string,
    valueBuffer: Buffer,
    headers?: Record<string, string>,
  ): Promise<void> {
    await lastValueFrom(
      this.client.emit(topic, {
        key,
        value: valueBuffer,
        headers,
      }),
    );
  }
}
