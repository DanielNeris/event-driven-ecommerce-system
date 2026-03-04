import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { TOPICS, EVENT_TYPES } from '@app/contracts';
import type { EventEnvelope } from '@app/contracts';
import { KafkaProducer, withRetryAndDlq } from '@app/kafka';
import { InventoryService } from './inventory.service';

const CONSUMER = 'inventory-service';

interface OrderItem {
  productId: string;
  quantity: number;
}

@Controller()
export class InventoryEventsController {
  private readonly logger = new Logger(InventoryEventsController.name);

  constructor(
    private readonly inventory: InventoryService,
    private readonly producer: KafkaProducer,
  ) {}

  @EventPattern(TOPICS.ORDER_EVENTS)
  async handleOrderEvent(
    @Payload() raw: string | Record<string, unknown>,
  ): Promise<void> {
    const envelope = (
      typeof raw === 'string' ? JSON.parse(raw) : raw
    ) as EventEnvelope<Record<string, unknown>>;
    if (envelope?.type !== EVENT_TYPES.OrderCreated) return;
    const eventId = envelope.eventId;
    if (!eventId) return;
    if (await this.inventory.isEventProcessed(eventId)) return;
    const orderId = envelope.payload?.orderId ?? envelope.correlationId;
    const items = envelope.payload?.items;
    if (typeof orderId !== 'string' || !Array.isArray(items)) return;

    try {
      await withRetryAndDlq(
        this.producer,
        {
          dlqTopic: TOPICS.INVENTORY_DLQ,
          sourceTopic: TOPICS.ORDER_EVENTS,
          partitionKey: orderId,
          originalMessage: raw,
          consumer: CONSUMER,
        },
        async () => {
          await this.inventory.markEventProcessed(eventId);
          await this.inventory.handleOrderCreated(
            orderId,
            items as OrderItem[],
          );
        },
      );
    } catch (e) {
      this.logger.warn(
        `OrderCreated failed for ${orderId}, sent to DLQ: ${(e as Error).message}`,
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
    if (await this.inventory.isEventProcessed(eventId)) return;
    const orderId = envelope.payload?.orderId ?? envelope.correlationId;
    if (typeof orderId !== 'string') return;

    try {
      await withRetryAndDlq(
        this.producer,
        {
          dlqTopic: TOPICS.INVENTORY_DLQ,
          sourceTopic: TOPICS.PAYMENT_EVENTS,
          partitionKey: orderId,
          originalMessage: raw,
          consumer: CONSUMER,
        },
        async () => {
          await this.inventory.markEventProcessed(eventId);
          await this.inventory.handlePaymentFailed(orderId);
        },
      );
    } catch (e) {
      this.logger.warn(
        `PaymentFailed (release) failed for ${orderId}, sent to DLQ: ${(e as Error).message}`,
      );
    }
  }
}
