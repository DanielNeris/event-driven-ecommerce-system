import { Injectable, Logger } from '@nestjs/common';
import { KafkaProducer } from '@app/kafka';
import { RedisService } from '@app/redis';
import { TOPICS, EVENT_TYPES } from '@app/contracts';
import { PrismaService } from './prisma.service';

/** Key must be per-product (not per-order) so only one order at a time reserves that product; value = orderId for safe release. */
const LOCK_PREFIX = 'product-lock:';
const CONSUMER = 'inventory-service';
const LOCK_TTL_SECONDS = 900; // 15 min for reservation expiry; lock is released right after reserve
const LOCK_RETRY_BASE_MS = 200;
const LOCK_RETRY_ATTEMPTS = 80; // ~16–24s max wait with jitter so burst of same-product orders can queue

interface OrderItem {
  productId: string;
  quantity: number;
}

@Injectable()
export class InventoryService {
  private readonly logger = new Logger(InventoryService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly redis: RedisService,
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

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private async acquireLockWithRetry(
    key: string,
    orderId: string,
    ttl: number,
  ): Promise<boolean> {
    for (let attempt = 0; attempt < LOCK_RETRY_ATTEMPTS; attempt++) {
      const acquired = await this.redis.setNxEx(key, orderId, ttl);
      if (acquired) return true;
      const jitter = Math.floor(Math.random() * 150) + 50; // 50–200ms
      await this.sleep(LOCK_RETRY_BASE_MS + jitter);
    }
    return false;
  }

  /**
   * Merge items by productId (sum quantities) so we lock and reserve once per product.
   * Prevents failing when the same product appears twice in the order (duplicate key in Redis).
   */
  private aggregateByProductId(items: OrderItem[]): OrderItem[] {
    const byId = new Map<string, number>();
    for (const { productId, quantity } of items) {
      byId.set(productId, (byId.get(productId) ?? 0) + quantity);
    }
    return Array.from(byId.entries()).map(([productId, quantity]) => ({
      productId,
      quantity,
    }));
  }

  async handleOrderCreated(orderId: string, items: OrderItem[]): Promise<void> {
    const ttl = LOCK_TTL_SECONDS;
    const lockedKeys: string[] = [];
    const aggregated = this.aggregateByProductId(items);

    try {
      for (const item of aggregated) {
        const key = `${LOCK_PREFIX}${item.productId}`;
        const acquired = await this.acquireLockWithRetry(key, orderId, ttl);
        if (!acquired) {
          await this.releaseLocks(lockedKeys, orderId);
          await this.kafka.emit({
            topic: TOPICS.INVENTORY_EVENTS,
            type: EVENT_TYPES.StockReservationFailed,
            payload: {
              orderId,
              reason: 'PRODUCT_LOCKED',
              productId: item.productId,
            },
            key: orderId,
            correlationId: orderId,
          });
          return;
        }
        lockedKeys.push(key);
      }

      const reserved: OrderItem[] = [];
      for (const item of aggregated) {
        const updated = await this.prisma.product.updateMany({
          where: {
            productId: item.productId,
            availableStock: { gte: item.quantity },
          },
          data: {
            availableStock: { decrement: item.quantity },
          },
        });
        if (updated.count === 0) {
          await this.releaseLocks(lockedKeys, orderId);
          for (const r of reserved) {
            await this.prisma.product.update({
              where: { productId: r.productId },
              data: { availableStock: { increment: r.quantity } },
            });
          }
          await this.kafka.emit({
            topic: TOPICS.INVENTORY_EVENTS,
            type: EVENT_TYPES.StockReservationFailed,
            payload: {
              orderId,
              reason: 'OUT_OF_STOCK',
              productId: item.productId,
            },
            key: orderId,
            correlationId: orderId,
          });
          return;
        }
        reserved.push(item);
      }

      const expiresAt = new Date(Date.now() + ttl * 1000);
      for (const item of reserved) {
        await this.prisma.reservation.create({
          data: {
            orderId,
            productId: item.productId,
            quantity: item.quantity,
            status: 'RESERVED',
            expiresAt,
          },
        });
      }

      await this.kafka.emit({
        topic: TOPICS.INVENTORY_EVENTS,
        type: EVENT_TYPES.StockReserved,
        payload: { orderId, items: reserved },
        key: orderId,
        correlationId: orderId,
      });
      this.logger.log(`Stock reserved for order ${orderId}`);
      await this.releaseLocks(lockedKeys, orderId);
    } finally {
      // Locks released on success above; on failure path we already released in the branch
    }
  }

  private async releaseLocks(keys: string[], orderId: string): Promise<void> {
    for (const key of keys) {
      await this.redis.deleteIfValue(key, orderId);
    }
  }

  async handlePaymentFailed(orderId: string): Promise<void> {
    const reservations = await this.prisma.reservation.findMany({
      where: { orderId, status: 'RESERVED' },
      select: { productId: true, quantity: true },
    });
    for (const r of reservations) {
      await this.prisma.product.update({
        where: { productId: r.productId },
        data: { availableStock: { increment: r.quantity } },
      });
      const key = `${LOCK_PREFIX}${r.productId}`;
      await this.redis.deleteIfValue(key, orderId);
    }
    await this.prisma.reservation.updateMany({
      where: { orderId },
      data: { status: 'RELEASED' },
    });
    await this.kafka.emit({
      topic: TOPICS.INVENTORY_EVENTS,
      type: EVENT_TYPES.StockReleased,
      payload: { orderId, reason: 'PAYMENT_FAILED' },
      key: orderId,
      correlationId: orderId,
    });
    this.logger.log(`Stock released for order ${orderId}`);
  }
}
