import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { TOPICS, EVENT_TYPES } from '@app/contracts';
import type { EventEnvelope } from '@app/contracts';
import { KafkaProducer, withRetryAndDlq } from '@app/kafka';
import { OrderService } from './order.service';

const CONSUMER = 'order-service';

@Controller()
export class OrderEventsController {
  private readonly logger = new Logger(OrderEventsController.name);

  constructor(
    private readonly orderService: OrderService,
    private readonly producer: KafkaProducer,
  ) {}

  @EventPattern(TOPICS.SHIPPING_EVENTS)
  async handleShippingEvent(
    @Payload() raw: string | Record<string, unknown>,
  ): Promise<void> {
    const envelope = (
      typeof raw === 'string' ? JSON.parse(raw) : raw
    ) as EventEnvelope<Record<string, unknown>>;
    if (envelope?.type !== EVENT_TYPES.ShipmentCreated) return;
    const eventId = envelope.eventId;
    if (!eventId) return;
    if (await this.orderService.isEventProcessed(eventId, CONSUMER)) return;
    const orderId = envelope.payload?.orderId ?? envelope.correlationId;
    if (typeof orderId !== 'string') return;

    try {
      await withRetryAndDlq(
        this.producer,
        {
          dlqTopic: TOPICS.ORDER_DLQ,
          sourceTopic: TOPICS.SHIPPING_EVENTS,
          partitionKey: orderId,
          originalMessage: raw,
          consumer: CONSUMER,
        },
        async () => {
          await this.orderService.markEventProcessed(eventId, CONSUMER);
          const updated = await this.orderService.markConfirmed(orderId);
          if (updated)
            this.logger.log(`Order ${orderId} confirmed (ShipmentCreated)`);
        },
      );
    } catch (e) {
      this.logger.warn(
        `ShipmentCreated failed for ${orderId}, sent to DLQ: ${(e as Error).message}`,
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
    if (envelope?.type !== EVENT_TYPES.PaymentFailed) return;
    const eventId = envelope.eventId;
    if (!eventId) return;
    if (await this.orderService.isEventProcessed(eventId, CONSUMER)) return;
    const orderId = envelope.payload?.orderId ?? envelope.correlationId;
    if (typeof orderId !== 'string') return;

    try {
      await withRetryAndDlq(
        this.producer,
        {
          dlqTopic: TOPICS.ORDER_DLQ,
          sourceTopic: TOPICS.PAYMENT_EVENTS,
          partitionKey: orderId,
          originalMessage: raw,
          consumer: CONSUMER,
        },
        async () => {
          await this.orderService.markEventProcessed(eventId, CONSUMER);
          const updated = await this.orderService.markCancelled(orderId);
          if (updated)
            this.logger.log(`Order ${orderId} cancelled (PaymentFailed)`);
        },
      );
    } catch (e) {
      this.logger.warn(
        `PaymentFailed failed for ${orderId}, sent to DLQ: ${(e as Error).message}`,
      );
    }
  }
}
