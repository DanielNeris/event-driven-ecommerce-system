import { Test, TestingModule } from '@nestjs/testing';
import { OpensearchConsumerMicroserviceController } from '@app/opensearch-consumer-microservice/opensearch-consumer-microservice.controller';
import { OpensearchConsumerMicroserviceService } from '@app/opensearch-consumer-microservice/opensearch-consumer-microservice.service';

describe('OpensearchConsumerMicroserviceController', () => {
  let controller: OpensearchConsumerMicroserviceController;
  let indexEvent: jest.Mock;

  beforeEach(async () => {
    indexEvent = jest.fn();
    const module: TestingModule = await Test.createTestingModule({
      controllers: [OpensearchConsumerMicroserviceController],
      providers: [
        {
          provide: OpensearchConsumerMicroserviceService,
          useValue: { indexEvent },
        },
      ],
    }).compile();

    controller = module.get(OpensearchConsumerMicroserviceController);
  });

  it('should delegate order event (string) to service.indexEvent', () => {
    const raw = JSON.stringify({
      eventId: 'ev-1',
      type: 'OrderCreated',
      occurredAt: new Date().toISOString(),
      payload: {},
    });
    controller.handleOrderEvent(raw);
    expect(indexEvent).toHaveBeenCalledWith(raw);
  });

  it('should delegate order event (object) to service.indexEvent', () => {
    const payload = {
      eventId: 'ev-2',
      type: 'OrderConfirmed',
      occurredAt: new Date().toISOString(),
      payload: { orderId: 'ord-1' },
    };
    controller.handleOrderEvent(payload);
    expect(indexEvent).toHaveBeenCalledWith(payload);
  });

  it('should delegate inventory event to service.indexEvent', () => {
    const raw = JSON.stringify({
      eventId: 'ev-inv',
      type: 'StockReserved',
      occurredAt: new Date().toISOString(),
      payload: {},
    });
    controller.handleInventoryEvent(raw);
    expect(indexEvent).toHaveBeenCalledWith(raw);
  });

  it('should delegate payment event to service.indexEvent', () => {
    const raw = JSON.stringify({
      eventId: 'ev-pay',
      type: 'PaymentAuthorized',
      occurredAt: new Date().toISOString(),
      payload: {},
    });
    controller.handlePaymentEvent(raw);
    expect(indexEvent).toHaveBeenCalledWith(raw);
  });

  it('should delegate shipping event to service.indexEvent', () => {
    const raw = JSON.stringify({
      eventId: 'ev-ship',
      type: 'ShipmentCreated',
      occurredAt: new Date().toISOString(),
      payload: {},
    });
    controller.handleShippingEvent(raw);
    expect(indexEvent).toHaveBeenCalledWith(raw);
  });

  it('should delegate notification event to service.indexEvent', () => {
    const raw = JSON.stringify({
      eventId: 'ev-notif',
      type: 'NotificationSent',
      occurredAt: new Date().toISOString(),
      payload: {},
    });
    controller.handleNotificationEvent(raw);
    expect(indexEvent).toHaveBeenCalledWith(raw);
  });
});
