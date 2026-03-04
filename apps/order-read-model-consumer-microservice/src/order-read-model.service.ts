import { Injectable, OnModuleInit, Logger } from '@nestjs/common';
import { OpenSearchService } from '@app/opensearch';
import { ORDER_INDEX } from '@app/opensearch';
import type { EventEnvelope } from '@app/contracts';
import { EVENT_TYPES } from '@app/contracts';

@Injectable()
export class OrderReadModelService implements OnModuleInit {
  private readonly logger = new Logger(OrderReadModelService.name);

  constructor(private readonly openSearch: OpenSearchService) {}

  async onModuleInit(): Promise<void> {
    await this.openSearch.ensureIndex(ORDER_INDEX);
  }

  private timelineLabel(
    envelope: EventEnvelope<Record<string, unknown>>,
  ): string {
    if (
      envelope.type === EVENT_TYPES.NotificationSent &&
      envelope.payload?.kind !== undefined
    ) {
      const kind = envelope.payload.kind;
      return `NotificationSent:${typeof kind === 'string' ? kind : 'unknown'}`;
    }
    return envelope.type ?? 'Unknown';
  }

  async applyEvent(
    envelope: EventEnvelope<Record<string, unknown>>,
  ): Promise<void> {
    const orderId = envelope.payload?.orderId ?? envelope.correlationId;
    const type = envelope.type;
    if (!orderId || typeof orderId !== 'string' || !type) return;

    const client = this.openSearch.getClient();
    const id = orderId;
    const label = this.timelineLabel(envelope);

    try {
      const getRes = await client
        .get({ index: ORDER_INDEX, id })
        .catch(() => ({ body: { found: false } }));
      const found =
        (getRes as { body?: { found?: boolean } }).body?.found === true;
      const existing = found
        ? (
            getRes as {
              body: { _source?: { status?: string; timeline?: string[] } };
            }
          ).body._source
        : null;
      const timeline: string[] = Array.isArray(existing?.timeline)
        ? [...existing.timeline]
        : [];
      if (!timeline.includes(label)) timeline.push(label);

      let status = existing?.status ?? 'PENDING';
      if (type === 'OrderConfirmed') status = 'CONFIRMED';
      if (type === 'OrderCancelled') status = 'CANCELLED';

      await client.index({
        index: ORDER_INDEX,
        id,
        body: { orderId, status, timeline },
        refresh: true,
      });
      this.logger.debug(`Updated order ${orderId} timeline: ${label}`);
    } catch (e) {
      this.logger.warn(
        `Order read model update failed for ${orderId}: ${(e as Error).message}`,
      );
    }
  }
}
