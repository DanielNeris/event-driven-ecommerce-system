import { Injectable, Logger } from '@nestjs/common';
import { KafkaProducer } from '@app/kafka';
import { TOPICS, EVENT_TYPES } from '@app/contracts';
import { PrismaService } from './prisma.service';

const CONSUMER = 'messageria-service';

@Injectable()
export class MessageriaService {
  private readonly logger = new Logger(MessageriaService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly kafka: KafkaProducer,
  ) {}

  async isEventProcessed(eventId: string): Promise<boolean> {
    const found = await this.prisma.processedEvent.findUnique({
      where: { eventId_consumer: { eventId, consumer: CONSUMER } },
    });
    return found != null;
  }

  async markEventProcessed(eventId: string): Promise<void> {
    await this.prisma.processedEvent.upsert({
      where: { eventId_consumer: { eventId, consumer: CONSUMER } },
      create: { eventId, consumer: CONSUMER },
      update: {},
    });
  }

  async sendNotification(orderId: string, kind: string): Promise<void> {
    this.logger.log(`Notification sent (${kind}): ${orderId}`);
    await this.kafka.emit({
      topic: TOPICS.NOTIFICATION_EVENTS,
      type: EVENT_TYPES.NotificationSent,
      payload: { orderId, kind },
      key: orderId,
      correlationId: orderId,
    });
  }

  async handleOrderCreated(orderId: string): Promise<void> {
    await this.sendNotification(orderId, 'ORDER_CREATED');
  }

  async handleOrderConfirmed(orderId: string): Promise<void> {
    await this.sendNotification(orderId, 'ORDER_CONFIRMED');
  }

  async handleOrderCancelled(orderId: string): Promise<void> {
    await this.sendNotification(orderId, 'ORDER_CANCELLED');
  }

  async handlePaymentCaptured(orderId: string): Promise<void> {
    await this.sendNotification(orderId, 'ORDER_PAID');
  }

  async handleShipmentCreated(orderId: string): Promise<void> {
    await this.sendNotification(orderId, 'ORDER_SHIPPED');
  }
}
