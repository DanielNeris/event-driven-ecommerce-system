import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { TOPICS, EVENT_TYPES } from '@app/contracts';
import type { EventEnvelope } from '@app/contracts';
import { KafkaProducer, withRetryAndDlq } from '@app/kafka';
import { ShippingService } from './shipping.service';

const CONSUMER = 'shipping-service';

@Controller()
export class ShippingEventsController {
  private readonly logger = new Logger(ShippingEventsController.name);

  constructor(
    private readonly shipping: ShippingService,
    private readonly producer: KafkaProducer,
  ) {}

  @EventPattern(TOPICS.PAYMENT_EVENTS)
  async handlePaymentEvent(
    @Payload() raw: string | Record<string, unknown>,
  ): Promise<void> {
    const envelope = (
      typeof raw === 'string' ? JSON.parse(raw) : raw
    ) as EventEnvelope<Record<string, unknown>>;
    if (envelope?.type !== EVENT_TYPES.PaymentCaptured) return;
    const eventId = envelope.eventId;
    if (!eventId) return;
    if (await this.shipping.isEventProcessed(eventId)) return;
    const orderId = envelope.payload?.orderId ?? envelope.correlationId;
    if (typeof orderId !== 'string') return;

    try {
      await withRetryAndDlq(
        this.producer,
        {
          dlqTopic: TOPICS.SHIPPING_DLQ,
          sourceTopic: TOPICS.PAYMENT_EVENTS,
          partitionKey: orderId,
          originalMessage: raw,
          consumer: CONSUMER,
        },
        async () => {
          await this.shipping.markEventProcessed(eventId);
          await this.shipping.handlePaymentCaptured(orderId);
        },
      );
    } catch (e) {
      this.logger.warn(
        `PaymentCaptured failed for ${orderId}, sent to DLQ: ${(e as Error).message}`,
      );
    }
  }
}
