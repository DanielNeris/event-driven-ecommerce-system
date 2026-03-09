import { Injectable, Logger } from '@nestjs/common';
import { Interval } from '@nestjs/schedule';
import { KafkaProducer } from '@app/kafka';
import { RedisService } from '@app/redis';
import { TOPICS, EVENT_TYPES } from '@app/contracts';
import { PrismaService } from './prisma.service';
import type { Prisma } from './generated/prisma/client';
import {
  LOCK_PREFIX,
  CONSUMER,
  LOCK_TTL_SECONDS,
  RESERVATION_EXPIRY_SECONDS,
  LOCK_RETRY_BASE_MS,
  LOCK_RETRY_ATTEMPTS,
  EXPIRED_RESERVATION_POLL_MS,
  OUT_OF_STOCK_CODE,
  EXPIRY_JOB_LOCK_KEY,
  EXPIRY_JOB_LOCK_TTL_SECONDS,
} from './inventory.constants';
import type { OrderItem } from './inventory.types';

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

  async markEventProcessedTx(
    tx: Prisma.TransactionClient,
    eventId: string,
  ): Promise<void> {
    await tx.processedEvent.upsert({
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
      const jitter = Math.floor(Math.random() * 150) + 50;
      await this.sleep(LOCK_RETRY_BASE_MS + jitter);
    }
    return false;
  }

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

  async handleOrderCreated(
    eventId: string,
    orderId: string,
    items: OrderItem[],
  ): Promise<void> {
    if (await this.isEventProcessed(eventId)) {
      this.logger.warn(
        `Skipping duplicated OrderCreated event ${eventId} for order ${orderId}`,
      );
      return;
    }

    const lockedKeys: string[] = [];
    const aggregated = this.aggregateByProductId(items).sort((a, b) =>
      a.productId.localeCompare(b.productId),
    );

    try {
      for (const item of aggregated) {
        const key = `${LOCK_PREFIX}${item.productId}`;
        const acquired = await this.acquireLockWithRetry(
          key,
          orderId,
          LOCK_TTL_SECONDS,
        );
        if (!acquired) {
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

      await this.prisma.$transaction(async (tx: Prisma.TransactionClient) => {
        const alreadyProcessed = await tx.processedEvent.findUnique({
          where: { eventId_consumer: { eventId, consumer: CONSUMER } },
        });
        if (alreadyProcessed) {
          return;
        }

        for (const item of aggregated) {
          const updated = await tx.product.updateMany({
            where: {
              productId: item.productId,
              availableStock: { gte: item.quantity },
            },
            data: {
              availableStock: { decrement: item.quantity },
            },
          });
          if (updated.count === 0) {
            const err = new Error(`Out of stock for product ${item.productId}`);
            (err as Error & { code: string; productId: string }).code =
              OUT_OF_STOCK_CODE;
            (err as Error & { code: string; productId: string }).productId =
              item.productId;
            throw err;
          }
        }

        const expiresAt = new Date(
          Date.now() + RESERVATION_EXPIRY_SECONDS * 1000,
        );
        for (const item of aggregated) {
          await tx.reservation.create({
            data: {
              orderId,
              productId: item.productId,
              quantity: item.quantity,
              status: 'RESERVED',
              expiresAt,
            },
          });
        }

        await this.markEventProcessedTx(tx, eventId);
      });

      await this.kafka.emit({
        topic: TOPICS.INVENTORY_EVENTS,
        type: EVENT_TYPES.StockReserved,
        payload: { orderId, items: aggregated },
        key: orderId,
        correlationId: orderId,
      });
      this.logger.log(`Stock reserved for order ${orderId}`);
    } catch (e) {
      const outOfStock = e as Error & { code?: string; productId?: string };
      if (outOfStock?.code === OUT_OF_STOCK_CODE && outOfStock?.productId) {
        await this.kafka.emit({
          topic: TOPICS.INVENTORY_EVENTS,
          type: EVENT_TYPES.StockReservationFailed,
          payload: {
            orderId,
            reason: 'OUT_OF_STOCK',
            productId: outOfStock.productId,
          },
          key: orderId,
          correlationId: orderId,
        });
        return;
      }
      this.logger.error(
        `Reserve failed for order ${orderId}: ${(e as Error).message}`,
        (e as Error).stack,
      );
      await this.kafka.emit({
        topic: TOPICS.INVENTORY_EVENTS,
        type: EVENT_TYPES.StockReservationFailed,
        payload: {
          orderId,
          reason: 'TECHNICAL_FAILURE',
          error: (e as Error).message,
        },
        key: orderId,
        correlationId: orderId,
      });
      throw e;
    } finally {
      await this.releaseLocks(lockedKeys, orderId);
    }
  }

  private async releaseLocks(keys: string[], orderId: string): Promise<void> {
    for (const key of keys) {
      await this.redis.deleteIfValue(key, orderId);
    }
  }

  @Interval(EXPIRED_RESERVATION_POLL_MS)
  async releaseExpiredReservations(): Promise<void> {
    const acquired = await this.redis.setNxEx(
      EXPIRY_JOB_LOCK_KEY,
      CONSUMER,
      EXPIRY_JOB_LOCK_TTL_SECONDS,
    );
    if (!acquired) {
      return;
    }

    try {
      const expired = await this.prisma.reservation.findMany({
        where: {
          status: 'RESERVED',
          expiresAt: { lt: new Date() },
        },
        select: {
          id: true,
          orderId: true,
          productId: true,
          quantity: true,
        },
      });
      if (expired.length === 0) return;

      let released = 0;
      const affectedOrders = new Set<string>();

      for (const r of expired) {
        const wasReleased = await this.prisma.$transaction(
          async (tx: Prisma.TransactionClient) => {
            const updated = await tx.reservation.updateMany({
              where: { id: r.id, status: 'RESERVED' },
              data: { status: 'EXPIRED' },
            });
            if (updated.count === 0) return false;
            await tx.product.update({
              where: { productId: r.productId },
              data: { availableStock: { increment: r.quantity } },
            });
            return true;
          },
        );
        if (!wasReleased) continue;
        released++;
        affectedOrders.add(r.orderId);
      }

      if (released > 0) {
        this.logger.log(
          `Released ${released} expired reservation(s) (orders: ${Array.from(affectedOrders).join(', ')})`,
        );
      }
    } catch (e) {
      this.logger.warn(
        `releaseExpiredReservations error: ${(e as Error).message}`,
      );
    } finally {
      await this.redis.deleteIfValue(EXPIRY_JOB_LOCK_KEY, CONSUMER);
    }
  }

  async handlePaymentFailed(eventId: string, orderId: string): Promise<void> {
    if (await this.isEventProcessed(eventId)) {
      this.logger.warn(
        `Skipping duplicated PaymentFailed event ${eventId} for order ${orderId}`,
      );
      return;
    }

    const reservations = await this.prisma.reservation.findMany({
      where: { orderId, status: 'RESERVED' },
      select: { id: true, productId: true, quantity: true },
    });

    let releasedCount = 0;

    for (const r of reservations) {
      const wasReleased = await this.prisma.$transaction(
        async (tx: Prisma.TransactionClient) => {
          const updated = await tx.reservation.updateMany({
            where: { id: r.id, status: 'RESERVED' },
            data: { status: 'RELEASED' },
          });
          if (updated.count === 0) return false;
          await tx.product.update({
            where: { productId: r.productId },
            data: { availableStock: { increment: r.quantity } },
          });
          return true;
        },
      );
      if (!wasReleased) continue;
      releasedCount++;
      const key = `${LOCK_PREFIX}${r.productId}`;
      await this.redis.deleteIfValue(key, orderId);
    }

    await this.prisma.$transaction(async (tx: Prisma.TransactionClient) => {
      await this.markEventProcessedTx(tx, eventId);
    });

    await this.kafka.emit({
      topic: TOPICS.INVENTORY_EVENTS,
      type: EVENT_TYPES.StockReleased,
      payload: {
        orderId,
        reason: 'PAYMENT_FAILED',
        releasedCount,
      },
      key: orderId,
      correlationId: orderId,
    });
    this.logger.log(
      `Stock release processed for order ${orderId} (releasedCount=${releasedCount})`,
    );
  }
}
