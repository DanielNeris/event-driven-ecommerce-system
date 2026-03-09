/**
 * Functional test: Inventory Consumer (InventoryService handleOrderCreated, handlePaymentFailed).
 * OrderCreated -> lock, decrement stock, reservation, StockReserved; PaymentFailed -> release stock, StockReleased.
 */
import { Test, TestingModule } from '@nestjs/testing';
import { of } from 'rxjs';
import { RedisService } from '@app/redis';
import { KafkaProducer } from '@app/kafka';

jest.mock('@app/inventory-consumer-microservice/prisma.service', () => ({
  PrismaService: class PrismaService {},
}));

import { InventoryService } from '@app/inventory-consumer-microservice/inventory.service';
import { PrismaService } from '@app/inventory-consumer-microservice/prisma.service';
import { TOPICS, EVENT_TYPES } from '@app/contracts';

describe('Inventory flow (functional)', () => {
  let service: InventoryService;
  let redisSetNxEx: jest.Mock;
  let redisDeleteIfValue: jest.Mock;
  let productUpdateMany: jest.Mock;
  let productUpdate: jest.Mock;
  let reservationCreate: jest.Mock;
  let kafkaEmit: jest.Mock;

  beforeEach(async () => {
    redisSetNxEx = jest.fn().mockResolvedValue(true);
    redisDeleteIfValue = jest.fn().mockResolvedValue(undefined);
    productUpdateMany = jest.fn().mockResolvedValue({ count: 1 });
    productUpdate = jest.fn().mockResolvedValue(undefined);
    reservationCreate = jest.fn().mockResolvedValue(undefined);
    kafkaEmit = jest.fn().mockReturnValue(of(undefined));

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        InventoryService,
        {
          provide: PrismaService,
          useValue: {
            product: { updateMany: productUpdateMany, update: productUpdate },
            reservation: {
              create: reservationCreate,
              findMany: jest.fn().mockResolvedValue([]),
              updateMany: jest.fn().mockResolvedValue(undefined),
            },
            processedEvent: {
              findUnique: jest.fn().mockResolvedValue(null),
              upsert: jest.fn().mockResolvedValue(undefined),
            },
            $transaction: jest
              .fn()
              .mockImplementation(
                async (fn: (tx: unknown) => Promise<void>) => {
                  const tx = {
                    product: {
                      updateMany: productUpdateMany,
                      update: productUpdate,
                    },
                    reservation: { create: reservationCreate },
                    processedEvent: {
                      findUnique: jest.fn().mockResolvedValue(null),
                      upsert: jest.fn().mockResolvedValue(undefined),
                    },
                  };
                  return fn(tx);
                },
              ),
          },
        },
        {
          provide: RedisService,
          useValue: {
            setNxEx: redisSetNxEx,
            deleteIfValue: redisDeleteIfValue,
          },
        },
        { provide: KafkaProducer, useValue: { emit: kafkaEmit } },
      ],
    }).compile();

    service = module.get(InventoryService);
  });

  it('should reserve stock and emit StockReserved when lock acquired and stock available', async () => {
    const eventId = 'ev-reserve-1';
    const orderId = 'order-inv-1';
    const items = [{ productId: 'prod-1', quantity: 2 }];

    await service.handleOrderCreated(eventId, orderId, items);

    expect(redisSetNxEx).toHaveBeenCalled();
    expect(productUpdateMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { productId: 'prod-1', availableStock: { gte: 2 } },
        data: { availableStock: { decrement: 2 } },
      }),
    );
    expect(reservationCreate).toHaveBeenCalledWith({
      data: expect.objectContaining({
        orderId,
        productId: 'prod-1',
        quantity: 2,
        status: 'RESERVED',
      }),
    });
    expect(kafkaEmit).toHaveBeenCalledWith(
      expect.objectContaining({
        topic: TOPICS.INVENTORY_EVENTS,
        type: EVENT_TYPES.StockReserved,
        payload: expect.objectContaining({ orderId, items: expect.any(Array) }),
        key: orderId,
      }),
    );
    expect(redisDeleteIfValue).toHaveBeenCalled();
  });

  it('should emit StockReservationFailed when out of stock (updateMany count 0)', async () => {
    productUpdateMany.mockResolvedValue({ count: 0 });

    await service.handleOrderCreated('ev-oos', 'order-oos', [
      { productId: 'prod-3', quantity: 99 },
    ]);

    expect(kafkaEmit).toHaveBeenCalledWith(
      expect.objectContaining({
        type: EVENT_TYPES.StockReservationFailed,
        payload: expect.objectContaining({ reason: 'OUT_OF_STOCK' }),
      }),
    );
  });

  it('should release reservations and emit StockReleased on handlePaymentFailed', async () => {
    const eventId = 'ev-release-1';
    const orderId = 'order-release-1';
    const findMany = jest.fn().mockResolvedValue([
      { id: 'res-1', productId: 'p1', quantity: 1 },
      { id: 'res-2', productId: 'p2', quantity: 2 },
    ]);
    const updateMany = jest.fn().mockResolvedValue({ count: 1 });
    const processedEventUpsert = jest.fn().mockResolvedValue(undefined);

    const module = await Test.createTestingModule({
      providers: [
        InventoryService,
        {
          provide: PrismaService,
          useValue: {
            product: { updateMany: productUpdateMany, update: productUpdate },
            reservation: {
              create: reservationCreate,
              findMany,
              updateMany,
            },
            processedEvent: {
              findUnique: jest.fn().mockResolvedValue(null),
              upsert: processedEventUpsert,
            },
            $transaction: jest
              .fn()
              .mockImplementation(
                async (fn: (tx: unknown) => Promise<unknown>) => {
                  const tx = {
                    product: {
                      updateMany: productUpdateMany,
                      update: productUpdate,
                    },
                    reservation: { updateMany },
                    processedEvent: { upsert: processedEventUpsert },
                  };
                  return fn(tx);
                },
              ),
          },
        },
        {
          provide: RedisService,
          useValue: {
            setNxEx: redisSetNxEx,
            deleteIfValue: redisDeleteIfValue,
          },
        },
        { provide: KafkaProducer, useValue: { emit: kafkaEmit } },
      ],
    }).compile();

    const invService = module.get(InventoryService);
    await invService.handlePaymentFailed(eventId, orderId);

    expect(findMany).toHaveBeenCalledWith({
      where: { orderId, status: 'RESERVED' },
      select: { id: true, productId: true, quantity: true },
    });
    expect(updateMany).toHaveBeenCalledTimes(2); // claim each reservation: RESERVED -> RELEASED
    expect(updateMany).toHaveBeenCalledWith({
      where: { id: 'res-1', status: 'RESERVED' },
      data: { status: 'RELEASED' },
    });
    expect(updateMany).toHaveBeenCalledWith({
      where: { id: 'res-2', status: 'RESERVED' },
      data: { status: 'RELEASED' },
    });
    expect(productUpdate).toHaveBeenCalledTimes(2);
    expect(processedEventUpsert).toHaveBeenCalled();
    expect(kafkaEmit).toHaveBeenCalledWith(
      expect.objectContaining({
        topic: TOPICS.INVENTORY_EVENTS,
        type: EVENT_TYPES.StockReleased,
        payload: expect.objectContaining({
          orderId,
          reason: 'PAYMENT_FAILED',
          releasedCount: 2,
        }),
      }),
    );
  });
});
