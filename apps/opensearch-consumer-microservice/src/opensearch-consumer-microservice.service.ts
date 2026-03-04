import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { OpenSearchService, EVENTS_INDEX } from '@app/opensearch';
import type { EventEnvelope } from '@app/contracts';

const BATCH_SIZE = 50;
const FLUSH_MS = 2000;

interface BufferedDoc {
  id: string;
  source: Record<string, unknown>;
}

@Injectable()
export class OpensearchConsumerMicroserviceService
  implements OnModuleInit, OnModuleDestroy
{
  private readonly logger = new Logger(
    OpensearchConsumerMicroserviceService.name,
  );
  private buffer: BufferedDoc[] = [];
  private flushTimer: ReturnType<typeof setInterval> | null = null;

  constructor(private readonly openSearch: OpenSearchService) {}

  async onModuleInit(): Promise<void> {
    await this.openSearch.ensureIndex(EVENTS_INDEX);
    this.logger.log(`Index "${EVENTS_INDEX}" ensured`);
    this.scheduleFlush();
  }

  onModuleDestroy(): void {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
    this.flush();
  }

  /**
   * Parse Kafka message and enqueue event for bulk index.
   * Uses eventId for idempotent indexing (re-deliveries overwrite same doc).
   */
  indexEvent(raw: string | Record<string, unknown>): void {
    try {
      const envelope = (
        typeof raw === 'string' ? JSON.parse(raw) : raw
      ) as EventEnvelope<Record<string, unknown>>;
      const eventId = envelope?.eventId;
      if (!eventId || typeof eventId !== 'string') {
        this.logger.warn('Event missing eventId, skipping');
        return;
      }

      const source: Record<string, unknown> = {
        eventId: envelope.eventId,
        type: envelope.type,
        version: envelope.version,
        occurredAt: envelope.occurredAt,
        correlationId: envelope.correlationId,
        traceId: envelope.traceId,
        payload: envelope.payload,
      };

      this.buffer = this.buffer.filter((b) => b.id !== eventId);
      this.buffer.push({ id: eventId, source });
      if (this.buffer.length >= BATCH_SIZE) {
        this.flush();
      }
    } catch (e) {
      this.logger.warn(
        `Failed to parse/enqueue event: ${(e as Error).message}`,
      );
    }
  }

  private scheduleFlush(): void {
    if (this.flushTimer) return;
    this.flushTimer = setInterval(() => {
      this.flush();
    }, FLUSH_MS);
  }

  private flush(): void {
    if (this.buffer.length === 0) return;
    const batch = this.buffer.splice(0, this.buffer.length);
    const byId = new Map<string, Record<string, unknown>>();
    for (const { id, source } of batch) byId.set(id, source);
    const items = Array.from(byId.entries(), ([id, source]) => ({
      id,
      source,
    }));
    this.openSearch
      .bulkIndex(EVENTS_INDEX, items)
      .then(({ count, errors }) => {
        this.logger.log(`Bulk indexed ${count} event(s). errors=${errors}`);
      })
      .catch((e) => {
        this.logger.error('Bulk index failed', e);
      });
  }
}
