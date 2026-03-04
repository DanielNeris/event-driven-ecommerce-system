import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { TOPICS, EVENT_TYPES } from '@app/contracts';
import type { EventEnvelope } from '@app/contracts';
import { KafkaProducer, withRetryAndDlq } from '@app/kafka';
import { MessageriaService } from './messageria.service';

const CONSUMER = 'messageria-service';

@Controller()
export class MessageriaEventsController {
  private readonly logger = new Logger(MessageriaEventsController.name);

  constructor(
    private readonly messageria: MessageriaService,
    private readonly producer: KafkaProducer,
  ) {}

  private async processOrderEventWithDlq(
    raw: string | Record<string, unknown>,
    envelope: EventEnvelope<Record<string, unknown>>,
    sourceTopic: string,
    handler: (orderId: string) => Promise<void>,
  ): Promise<void> {
    const eventId = envelope?.eventId;
    if (!eventId) return;
    if (await this.messageria.isEventProcessed(eventId)) return;
    const orderId = envelope.payload?.orderId ?? envelope.correlationId;
    if (typeof orderId !== 'string') return;

    try {
      await withRetryAndDlq(
        this.producer,
        {
          dlqTopic: TOPICS.NOTIFICATION_DLQ,
          sourceTopic,
          partitionKey: orderId,
          originalMessage: raw,
          consumer: CONSUMER,
        },
        async () => {
          await this.messageria.markEventProcessed(eventId);
          await handler(orderId);
        },
      );
    } catch (e) {
      this.logger.warn(
        `Event failed for ${orderId} (${sourceTopic}), sent to DLQ: ${(e as Error).message}`,
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
    if (envelope?.type === EVENT_TYPES.OrderCreated) {
      await this.processOrderEventWithDlq(
        raw,
        envelope,
        TOPICS.ORDER_EVENTS,
        (id) => this.messageria.handleOrderCreated(id),
      );
    } else if (envelope?.type === EVENT_TYPES.OrderConfirmed) {
      await this.processOrderEventWithDlq(
        raw,
        envelope,
        TOPICS.ORDER_EVENTS,
        (id) => this.messageria.handleOrderConfirmed(id),
      );
    } else if (envelope?.type === EVENT_TYPES.OrderCancelled) {
      await this.processOrderEventWithDlq(
        raw,
        envelope,
        TOPICS.ORDER_EVENTS,
        (id) => this.messageria.handleOrderCancelled(id),
      );
    }
  }

  @EventPattern(TOPICS.PAYMENT_EVENTS)
  async handlePaymentEvent(
    @Payload() raw: string | Record<string, unknown>,
  ): Promise<void> {
    const envelope = (
      typeof raw === 'string' ? JSON.parse(raw) : raw
    ) as EventEnvelope<Record<string, unknown>>;
    if (envelope?.type !== EVENT_TYPES.PaymentCaptured) return;
    await this.processOrderEventWithDlq(
      raw,
      envelope,
      TOPICS.PAYMENT_EVENTS,
      (id) => this.messageria.handlePaymentCaptured(id),
    );
  }

  @EventPattern(TOPICS.SHIPPING_EVENTS)
  async handleShippingEvent(
    @Payload() raw: string | Record<string, unknown>,
  ): Promise<void> {
    const envelope = (
      typeof raw === 'string' ? JSON.parse(raw) : raw
    ) as EventEnvelope<Record<string, unknown>>;
    if (envelope?.type !== EVENT_TYPES.ShipmentCreated) return;
    await this.processOrderEventWithDlq(
      raw,
      envelope,
      TOPICS.SHIPPING_EVENTS,
      (id) => this.messageria.handleShipmentCreated(id),
    );
  }
}
