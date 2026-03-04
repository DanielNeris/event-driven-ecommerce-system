import {
  Injectable,
  OnModuleInit,
  OnModuleDestroy,
  Logger,
} from '@nestjs/common';
import { KafkaProducer } from '@app/kafka';
import { PrismaService } from './prisma.service';
import { type TopicName, type EventType } from '@app/contracts';

const POLL_MS = 500;

@Injectable()
export class OutboxWorkerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxWorkerService.name);
  private timer: ReturnType<typeof setInterval> | null = null;

  constructor(
    private readonly prisma: PrismaService,
    private readonly kafka: KafkaProducer,
  ) {}

  onModuleInit() {
    this.timer = setInterval(() => {
      void this.tick();
    }, POLL_MS);
  }

  onModuleDestroy() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  private async tick() {
    try {
      const rows = await this.prisma.outbox.findMany({
        where: { publishedAt: null },
        orderBy: { id: 'asc' },
        take: 10,
      });
      for (const row of rows) {
        try {
          await this.kafka.emit({
            topic: row.topic as TopicName,
            type: row.type as EventType,
            payload: row.payload as object,
            key: row.correlationId ?? undefined,
            correlationId: row.correlationId ?? undefined,
            eventId: row.eventId,
            occurredAt: row.occurredAt.toISOString(),
          });
          await this.prisma.outbox.update({
            where: { eventId: row.eventId },
            data: { publishedAt: new Date() },
          });
        } catch (e) {
          this.logger.warn(
            `Outbox publish failed for ${row.eventId}: ${(e as Error).message}`,
          );
        }
      }
    } catch (e) {
      this.logger.warn(`Outbox tick error: ${(e as Error).message}`);
    }
  }
}
