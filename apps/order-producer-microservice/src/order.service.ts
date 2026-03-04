import { Injectable } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { KafkaProducer } from '@app/kafka';
import { TOPICS, EVENT_TYPES } from '@app/contracts';
import { PrismaService } from './prisma.service';

export type OrderItem = { productId: string; quantity: number };

@Injectable()
export class OrderService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly kafka: KafkaProducer,
  ) {}

  async createOrder(
    items: OrderItem[],
  ): Promise<{ orderId: string; status: string }> {
    const orderId = randomUUID();
    const eventId = randomUUID();
    const occurredAt = new Date();
    const payload = { orderId, items };

    await this.prisma.$transaction(async (tx) => {
      await tx.order.create({
        data: {
          id: orderId,
          status: 'PENDING',
          items: items as object,
        },
      });
      await tx.outbox.create({
        data: {
          eventId,
          topic: TOPICS.ORDER_EVENTS,
          type: EVENT_TYPES.OrderCreated,
          payload: payload as object,
          correlationId: orderId,
          occurredAt,
        },
      });
    });

    return { orderId, status: 'PENDING' };
  }

  async getOrder(
    orderId: string,
  ): Promise<{ orderId: string; status: string; items: OrderItem[] } | null> {
    const order = await this.prisma.order.findUnique({
      where: { id: orderId },
      select: { id: true, status: true, items: true },
    });
    if (!order) return null;
    return {
      orderId: order.id,
      status: order.status,
      items: (order.items as OrderItem[]) ?? [],
    };
  }

  async markConfirmed(orderId: string): Promise<boolean> {
    const updated = await this.prisma.order.updateMany({
      where: { id: orderId, status: 'PENDING' },
      data: { status: 'CONFIRMED' },
    });
    if (updated.count === 0) return false;
    await this.kafka.emit({
      topic: TOPICS.ORDER_EVENTS,
      type: EVENT_TYPES.OrderConfirmed,
      payload: { orderId },
      key: orderId,
      correlationId: orderId,
    });
    return true;
  }

  async markCancelled(orderId: string): Promise<boolean> {
    const updated = await this.prisma.order.updateMany({
      where: { id: orderId, status: 'PENDING' },
      data: { status: 'CANCELLED' },
    });
    if (updated.count === 0) return false;
    await this.kafka.emit({
      topic: TOPICS.ORDER_EVENTS,
      type: EVENT_TYPES.OrderCancelled,
      payload: { orderId },
      key: orderId,
      correlationId: orderId,
    });
    return true;
  }

  async isEventProcessed(eventId: string, consumer: string): Promise<boolean> {
    const found = await this.prisma.processedEvent.findUnique({
      where: { eventId_consumer: { eventId, consumer } },
    });
    return found != null;
  }

  async markEventProcessed(eventId: string, consumer: string): Promise<void> {
    await this.prisma.processedEvent.upsert({
      where: { eventId_consumer: { eventId, consumer } },
      create: { eventId, consumer },
      update: {},
    });
  }
}
