/**
 * Functional test: Order Producer (OrderService createOrder, getOrder).
 * Transactional outbox: order + outbox row in one transaction.
 */
import { Test, TestingModule } from '@nestjs/testing';
import { KafkaProducer } from '@app/kafka';

jest.mock('@app/order-producer-microservice/prisma.service', () => ({
  PrismaService: class PrismaService {},
}));

import { OrderService } from '@app/order-producer-microservice/order.service';
import { PrismaService } from '@app/order-producer-microservice/prisma.service';
import { TOPICS, EVENT_TYPES } from '@app/contracts';

describe('Order Producer flow (functional)', () => {
  let service: OrderService;
  let orderCreate: jest.Mock;
  let outboxCreate: jest.Mock;
  let transactionMock: jest.Mock;

  beforeEach(async () => {
    orderCreate = jest.fn().mockResolvedValue(undefined);
    outboxCreate = jest.fn().mockResolvedValue(undefined);
    transactionMock = jest
      .fn()
      .mockImplementation(async (cb: (tx: unknown) => Promise<void>) => {
        await cb({
          order: { create: orderCreate },
          outbox: { create: outboxCreate },
        });
      });

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        OrderService,
        {
          provide: PrismaService,
          useValue: { $transaction: transactionMock },
        },
        {
          provide: KafkaProducer,
          useValue: { emit: jest.fn().mockResolvedValue(undefined) },
        },
      ],
    }).compile();

    service = module.get(OrderService);
  });

  it('should create order and outbox row in one transaction', async () => {
    const items = [{ productId: 'prod-1', quantity: 1 }];
    const result = await service.createOrder(items);

    expect(result.status).toBe('PENDING');
    expect(typeof result.orderId).toBe('string');
    expect(transactionMock).toHaveBeenCalledTimes(1);
    expect(orderCreate).toHaveBeenCalledWith({
      data: expect.objectContaining({
        status: 'PENDING',
        items,
      }),
    });
    expect(outboxCreate).toHaveBeenCalledWith({
      data: expect.objectContaining({
        topic: TOPICS.ORDER_EVENTS,
        type: EVENT_TYPES.OrderCreated,
        payload: expect.objectContaining({ items }),
      }),
    });
  });
});

describe('Order Producer getOrder (functional)', () => {
  let service: OrderService;
  let findUnique: jest.Mock;

  beforeEach(async () => {
    findUnique = jest.fn().mockResolvedValue(null);

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        OrderService,
        {
          provide: PrismaService,
          useValue: {
            $transaction: jest.fn(),
            order: { findUnique: findUnique },
          },
        },
        { provide: KafkaProducer, useValue: { emit: jest.fn() } },
      ],
    }).compile();

    service = module.get(OrderService);
  });

  it('should return null when order not found', async () => {
    const result = await service.getOrder('unknown-id');
    expect(result).toBeNull();
    expect(findUnique).toHaveBeenCalledWith({
      where: { id: 'unknown-id' },
      select: { id: true, status: true, items: true },
    });
  });

  it('should return order when found', async () => {
    findUnique.mockResolvedValue({
      id: 'ord-1',
      status: 'CONFIRMED',
      items: [{ productId: 'p1', quantity: 2 }],
    });
    const result = await service.getOrder('ord-1');
    expect(result).toEqual({
      orderId: 'ord-1',
      status: 'CONFIRMED',
      items: [{ productId: 'p1', quantity: 2 }],
    });
  });
});
