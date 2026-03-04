/**
 * Functional test: Payment Consumer (PaymentService handleStockReserved).
 * Consumes StockReserved -> emits PaymentCaptured.
 */
import { Test, TestingModule } from '@nestjs/testing';
import { of } from 'rxjs';
import { KafkaProducer } from '@app/kafka';

jest.mock('@app/payment-consumer-microservice/prisma.service', () => ({
  PrismaService: class PrismaService {},
}));

import { PaymentService } from '@app/payment-consumer-microservice/payment.service';
import { PrismaService } from '@app/payment-consumer-microservice/prisma.service';
import { TOPICS, EVENT_TYPES } from '@app/contracts';

describe('Payment flow (functional)', () => {
  let service: PaymentService;
  let kafkaEmit: jest.Mock;

  beforeEach(async () => {
    kafkaEmit = jest.fn().mockReturnValue(of(undefined));

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        PaymentService,
        {
          provide: PrismaService,
          useValue: {
            processedEvent: {
              findUnique: jest.fn().mockResolvedValue(null),
              upsert: jest.fn().mockResolvedValue(undefined),
            },
          },
        },
        { provide: KafkaProducer, useValue: { emit: kafkaEmit } },
      ],
    }).compile();

    service = module.get(PaymentService);
  });

  it('should emit PaymentCaptured when handleStockReserved is called', async () => {
    await service.handleStockReserved('order-pay-1');

    expect(kafkaEmit).toHaveBeenCalledWith(
      expect.objectContaining({
        topic: TOPICS.PAYMENT_EVENTS,
        type: EVENT_TYPES.PaymentCaptured,
        payload: { orderId: 'order-pay-1' },
        key: 'order-pay-1',
        correlationId: 'order-pay-1',
      }),
    );
  });

  it('handleStockReservationFailed should not throw', () => {
    expect(() =>
      service.handleStockReservationFailed('order-fail-1'),
    ).not.toThrow();
  });
});
