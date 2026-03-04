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
    const orderId = 'order-inv-1';
    const items = [{ productId: 'prod-1', quantity: 2 }];

    await service.handleOrderCreated(orderId, items);

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
        payload: { orderId, items },
        key: orderId,
      }),
    );
    expect(redisDeleteIfValue).toHaveBeenCalled();
  });

  it('should emit StockReservationFailed when out of stock (updateMany count 0)', async () => {
    productUpdateMany.mockResolvedValue({ count: 0 });

    await service.handleOrderCreated('order-oos', [
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
    const orderId = 'order-release-1';
    const findMany = jest.fn().mockResolvedValue([
      { productId: 'p1', quantity: 1 },
      { productId: 'p2', quantity: 2 },
    ]);
    const updateMany = jest.fn().mockResolvedValue(undefined);

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
              upsert: jest.fn().mockResolvedValue(undefined),
            },
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
    await invService.handlePaymentFailed(orderId);

    expect(findMany).toHaveBeenCalledWith({
      where: { orderId, status: 'RESERVED' },
      select: { productId: true, quantity: true },
    });
    expect(productUpdate).toHaveBeenCalledTimes(2);
    expect(updateMany).toHaveBeenCalledWith({
      where: { orderId },
      data: { status: 'RELEASED' },
    });
    expect(kafkaEmit).toHaveBeenCalledWith(
      expect.objectContaining({
        topic: TOPICS.INVENTORY_EVENTS,
        type: EVENT_TYPES.StockReleased,
        payload: { orderId, reason: 'PAYMENT_FAILED' },
      }),
    );
  });
});
