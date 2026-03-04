import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { TOPICS, EVENT_TYPES } from '@app/contracts';
import type { EventEnvelope } from '@app/contracts';
import { KafkaProducer, withRetryAndDlq } from '@app/kafka';
import { PaymentService } from './payment.service';

const CONSUMER = 'payment-service';

@Controller()
export class PaymentEventsController {
  private readonly logger = new Logger(PaymentEventsController.name);

  constructor(
    private readonly payment: PaymentService,
    private readonly producer: KafkaProducer,
  ) {}

  @EventPattern(TOPICS.INVENTORY_EVENTS)
  async handleInventoryEvent(
    @Payload() raw: string | Record<string, unknown>,
  ): Promise<void> {
    const envelope = (
      typeof raw === 'string' ? JSON.parse(raw) : raw
    ) as EventEnvelope<Record<string, unknown>>;
    if (envelope?.type !== EVENT_TYPES.StockReserved) return;
    const eventId = envelope.eventId;
    if (!eventId) return;
    if (await this.payment.isEventProcessed(eventId)) return;
    const orderId = envelope.payload?.orderId ?? envelope.correlationId;
    if (typeof orderId !== 'string') return;

    try {
      await withRetryAndDlq(
        this.producer,
        {
          dlqTopic: TOPICS.PAYMENT_DLQ,
          sourceTopic: TOPICS.INVENTORY_EVENTS,
          partitionKey: orderId,
          originalMessage: raw,
          consumer: CONSUMER,
        },
        async () => {
          await this.payment.markEventProcessed(eventId);
          await this.payment.handleStockReserved(orderId);
        },
      );
    } catch (e) {
      this.logger.warn(
        `StockReserved failed for ${orderId}, sent to DLQ: ${(e as Error).message}`,
      );
    }
  }
}
