/**
 * Functional test: Shipping Consumer (ShippingService handlePaymentCaptured).
 * Consumes PaymentCaptured -> creates shipment, emits ShipmentCreated.
 */
import { Test, TestingModule } from '@nestjs/testing';
import { of } from 'rxjs';
import { KafkaProducer } from '@app/kafka';

jest.mock('@app/shipping-consumer-microservice/prisma.service', () => ({
  PrismaService: class PrismaService {},
}));

import { ShippingService } from '@app/shipping-consumer-microservice/shipping.service';
import { PrismaService } from '@app/shipping-consumer-microservice/prisma.service';
import { TOPICS, EVENT_TYPES } from '@app/contracts';

describe('Shipping flow (functional)', () => {
  let service: ShippingService;
  let shipmentUpsert: jest.Mock;
  let kafkaEmit: jest.Mock;

  beforeEach(async () => {
    shipmentUpsert = jest.fn().mockResolvedValue(undefined);
    kafkaEmit = jest.fn().mockReturnValue(of(undefined));

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ShippingService,
        {
          provide: PrismaService,
          useValue: {
            shipment: { upsert: shipmentUpsert },
            processedEvent: {
              findUnique: jest.fn().mockResolvedValue(null),
              upsert: jest.fn().mockResolvedValue(undefined),
            },
          },
        },
        { provide: KafkaProducer, useValue: { emit: kafkaEmit } },
      ],
    }).compile();

    service = module.get(ShippingService);
  });

  it('should upsert shipment and emit ShipmentCreated', async () => {
    await service.handlePaymentCaptured('order-ship-1');

    expect(shipmentUpsert).toHaveBeenCalledWith({
      where: { orderId: 'order-ship-1' },
      create: { orderId: 'order-ship-1' },
      update: {},
    });
    expect(kafkaEmit).toHaveBeenCalledWith(
      expect.objectContaining({
        topic: TOPICS.SHIPPING_EVENTS,
        type: EVENT_TYPES.ShipmentCreated,
        payload: { orderId: 'order-ship-1' },
        key: 'order-ship-1',
      }),
    );
  });
});
