import { Injectable, Logger } from '@nestjs/common';
import { KafkaProducer } from '@app/kafka';
import { TOPICS, EVENT_TYPES } from '@app/contracts';
import { PrismaService } from './prisma.service';

const CONSUMER = 'payment-service';

@Injectable()
export class PaymentService {
  private readonly logger = new Logger(PaymentService.name);

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

  async handleStockReserved(orderId: string): Promise<void> {
    await this.kafka.emit({
      topic: TOPICS.PAYMENT_EVENTS,
      type: EVENT_TYPES.PaymentCaptured,
      payload: { orderId },
      key: orderId,
      correlationId: orderId,
    });
    this.logger.log(`Payment captured for order ${orderId}`);
  }

  handleStockReservationFailed(orderId: string): void {
    this.logger.log(
      `Stock reservation failed for order ${orderId}, skipping payment`,
    );
  }
}
