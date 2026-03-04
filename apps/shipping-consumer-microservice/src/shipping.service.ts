import { Injectable, Logger } from '@nestjs/common';
import { KafkaProducer } from '@app/kafka';
import { TOPICS, EVENT_TYPES } from '@app/contracts';
import { PrismaService } from './prisma.service';

const CONSUMER = 'shipping-service';

@Injectable()
export class ShippingService {
  private readonly logger = new Logger(ShippingService.name);

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

  async handlePaymentCaptured(orderId: string): Promise<void> {
    await this.prisma.shipment.upsert({
      where: { orderId },
      create: { orderId },
      update: {},
    });
    await this.kafka.emit({
      topic: TOPICS.SHIPPING_EVENTS,
      type: EVENT_TYPES.ShipmentCreated,
      payload: { orderId },
      key: orderId,
      correlationId: orderId,
    });
    this.logger.log(`Shipment created for order ${orderId}`);
  }
}
