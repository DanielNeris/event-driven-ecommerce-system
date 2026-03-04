import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { TOPICS } from '@app/contracts';
import type { EventEnvelope } from '@app/contracts';
import { KafkaProducer, withRetryAndDlq } from '@app/kafka';
import { OrderReadModelService } from './order-read-model.service';

const CONSUMER = 'order-read-model';

@Controller()
export class OrderReadModelController {
  private readonly logger = new Logger(OrderReadModelController.name);

  constructor(
    private readonly readModel: OrderReadModelService,
    private readonly producer: KafkaProducer,
  ) {}

  private async applyWithDlq(
    raw: string | Record<string, unknown>,
    envelope: EventEnvelope<Record<string, unknown>>,
    sourceTopic: string,
  ): Promise<void> {
    const rawKey =
      envelope.payload?.orderId ??
      envelope.correlationId ??
      envelope.eventId ??
      'unknown';
    const partitionKey = typeof rawKey === 'string' ? rawKey : 'unknown';

    try {
      await withRetryAndDlq(
        this.producer,
        {
          dlqTopic: TOPICS.ORDER_DLQ,
          sourceTopic,
          partitionKey,
          originalMessage: raw,
          consumer: CONSUMER,
        },
        () => this.readModel.applyEvent(envelope),
      );
    } catch (e) {
      this.logger.warn(
        `Apply event failed (${sourceTopic}), sent to DLQ: ${(e as Error).message}`,
      );
    }
  }

  @EventPattern(TOPICS.ORDER_EVENTS)
  async handleOrderEvent(
    @Payload() raw: string | Record<string, unknown>,
  ): Promise<void> {
    const envelope = (
      typeof raw === 'string' ? JSON.parse(raw) : raw
    ) as EventEnvelope<Record<string, unknown>>;
    await this.applyWithDlq(raw, envelope, TOPICS.ORDER_EVENTS);
  }

  @EventPattern(TOPICS.INVENTORY_EVENTS)
  async handleInventoryEvent(
    @Payload() raw: string | Record<string, unknown>,
  ): Promise<void> {
    const envelope = (
      typeof raw === 'string' ? JSON.parse(raw) : raw
    ) as EventEnvelope<Record<string, unknown>>;
    await this.applyWithDlq(raw, envelope, TOPICS.INVENTORY_EVENTS);
  }

  @EventPattern(TOPICS.PAYMENT_EVENTS)
  async handlePaymentEvent(
    @Payload() raw: string | Record<string, unknown>,
  ): Promise<void> {
    const envelope = (
      typeof raw === 'string' ? JSON.parse(raw) : raw
    ) as EventEnvelope<Record<string, unknown>>;
    await this.applyWithDlq(raw, envelope, TOPICS.PAYMENT_EVENTS);
  }

  @EventPattern(TOPICS.SHIPPING_EVENTS)
  async handleShippingEvent(
    @Payload() raw: string | Record<string, unknown>,
  ): Promise<void> {
    const envelope = (
      typeof raw === 'string' ? JSON.parse(raw) : raw
    ) as EventEnvelope<Record<string, unknown>>;
    await this.applyWithDlq(raw, envelope, TOPICS.SHIPPING_EVENTS);
  }

  @EventPattern(TOPICS.NOTIFICATION_EVENTS)
  async handleNotificationEvent(
    @Payload() raw: string | Record<string, unknown>,
  ): Promise<void> {
    const envelope = (
      typeof raw === 'string' ? JSON.parse(raw) : raw
    ) as EventEnvelope<Record<string, unknown>>;
    await this.applyWithDlq(raw, envelope, TOPICS.NOTIFICATION_EVENTS);
  }
}
