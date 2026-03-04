/**
 * Functional test: Order Consumer (OrderService markConfirmed, markCancelled).
 * Consumes ShipmentCreated -> CONFIRMED; PaymentFailed -> CANCELLED.
 */
import { Test, TestingModule } from '@nestjs/testing';
import { of } from 'rxjs';
import { KafkaProducer } from '@app/kafka';

jest.mock('@app/order-consumer-microservice/prisma.service', () => ({
  PrismaService: class PrismaService {},
}));

import { OrderService } from '@app/order-consumer-microservice/order.service';
import { PrismaService } from '@app/order-consumer-microservice/prisma.service';
import { TOPICS, EVENT_TYPES } from '@app/contracts';

describe('Order Consumer flow (functional)', () => {
  let service: OrderService;
  let orderUpdateMany: jest.Mock;
  let kafkaEmit: jest.Mock;

  beforeEach(async () => {
    orderUpdateMany = jest.fn().mockResolvedValue({ count: 1 });
    kafkaEmit = jest.fn().mockReturnValue(of(undefined));

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        OrderService,
        {
          provide: PrismaService,
          useValue: {
            order: { updateMany: orderUpdateMany },
            processedEvent: {
              findUnique: jest.fn().mockResolvedValue(null),
              upsert: jest.fn().mockResolvedValue(undefined),
            },
          },
        },
        { provide: KafkaProducer, useValue: { emit: kafkaEmit } },
      ],
    }).compile();

    service = module.get(OrderService);
  });

  it('should mark order CONFIRMED and emit OrderConfirmed', async () => {
    const result = await service.markConfirmed('order-123');

    expect(result).toBe(true);
    expect(orderUpdateMany).toHaveBeenCalledWith({
      where: { id: 'order-123', status: 'PENDING' },
      data: { status: 'CONFIRMED' },
    });
    expect(kafkaEmit).toHaveBeenCalledWith(
      expect.objectContaining({
        topic: TOPICS.ORDER_EVENTS,
        type: EVENT_TYPES.OrderConfirmed,
        payload: { orderId: 'order-123' },
        key: 'order-123',
      }),
    );
  });

  it('should mark order CANCELLED and emit OrderCancelled', async () => {
    const result = await service.markCancelled('order-456');

    expect(result).toBe(true);
    expect(orderUpdateMany).toHaveBeenCalledWith({
      where: { id: 'order-456', status: 'PENDING' },
      data: { status: 'CANCELLED' },
    });
    expect(kafkaEmit).toHaveBeenCalledWith(
      expect.objectContaining({
        topic: TOPICS.ORDER_EVENTS,
        type: EVENT_TYPES.OrderCancelled,
        payload: { orderId: 'order-456' },
      }),
    );
  });

  it('should return false when order not PENDING (updateMany count 0)', async () => {
    orderUpdateMany.mockResolvedValue({ count: 0 });

    const result = await service.markConfirmed('order-none');
    expect(result).toBe(false);
    expect(kafkaEmit).not.toHaveBeenCalled();
  });
});
