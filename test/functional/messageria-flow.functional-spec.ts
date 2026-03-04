/**
 * Functional test: Messageria Consumer (MessageriaService notifications).
 * Sends notification and emits NotificationSent for each event type.
 */
import { Test, TestingModule } from '@nestjs/testing';
import { of } from 'rxjs';
import { KafkaProducer } from '@app/kafka';

jest.mock('@app/messageria-consumer-microservice/prisma.service', () => ({
  PrismaService: class PrismaService {},
}));

import { MessageriaService } from '@app/messageria-consumer-microservice/messageria.service';
import { PrismaService } from '@app/messageria-consumer-microservice/prisma.service';
import { TOPICS, EVENT_TYPES } from '@app/contracts';

describe('Messageria flow (functional)', () => {
  let service: MessageriaService;
  let kafkaEmit: jest.Mock;

  beforeEach(async () => {
    kafkaEmit = jest.fn().mockReturnValue(of(undefined));

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MessageriaService,
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

    service = module.get(MessageriaService);
  });

  it('should emit NotificationSent with kind ORDER_CREATED for handleOrderCreated', async () => {
    await service.handleOrderCreated('ord-msg-1');
    expect(kafkaEmit).toHaveBeenCalledWith(
      expect.objectContaining({
        topic: TOPICS.NOTIFICATION_EVENTS,
        type: EVENT_TYPES.NotificationSent,
        payload: { orderId: 'ord-msg-1', kind: 'ORDER_CREATED' },
        key: 'ord-msg-1',
      }),
    );
  });

  it('should emit NotificationSent with kind ORDER_CONFIRMED for handleOrderConfirmed', async () => {
    await service.handleOrderConfirmed('ord-msg-2');
    expect(kafkaEmit).toHaveBeenCalledWith(
      expect.objectContaining({
        payload: { orderId: 'ord-msg-2', kind: 'ORDER_CONFIRMED' },
      }),
    );
  });

  it('should emit NotificationSent with kind ORDER_CANCELLED for handleOrderCancelled', async () => {
    await service.handleOrderCancelled('ord-msg-3');
    expect(kafkaEmit).toHaveBeenCalledWith(
      expect.objectContaining({
        payload: { orderId: 'ord-msg-3', kind: 'ORDER_CANCELLED' },
      }),
    );
  });

  it('should emit ORDER_PAID for handlePaymentCaptured and ORDER_SHIPPED for handleShipmentCreated', async () => {
    await service.handlePaymentCaptured('ord-pay');
    expect(kafkaEmit).toHaveBeenCalledWith(
      expect.objectContaining({
        payload: { orderId: 'ord-pay', kind: 'ORDER_PAID' },
      }),
    );
    kafkaEmit.mockClear();
    await service.handleShipmentCreated('ord-ship');
    expect(kafkaEmit).toHaveBeenCalledWith(
      expect.objectContaining({
        payload: { orderId: 'ord-ship', kind: 'ORDER_SHIPPED' },
      }),
    );
  });
});
