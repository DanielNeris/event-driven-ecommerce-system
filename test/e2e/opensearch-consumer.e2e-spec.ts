import { Test, TestingModule } from '@nestjs/testing';
import { OpensearchConsumerMicroserviceModule } from '@app/opensearch-consumer-microservice/opensearch-consumer-microservice.module';
import { OpensearchConsumerMicroserviceController } from '@app/opensearch-consumer-microservice/opensearch-consumer-microservice.controller';
import { OpensearchConsumerMicroserviceService } from '@app/opensearch-consumer-microservice/opensearch-consumer-microservice.service';
import { OpenSearchService, EVENTS_INDEX } from '@app/opensearch';

describe('OpensearchConsumerMicroservice (e2e)', () => {
  let controller: OpensearchConsumerMicroserviceController;
  let service: OpensearchConsumerMicroserviceService;
  let bulkIndex: jest.Mock;

  beforeAll(async () => {
    bulkIndex = jest.fn().mockResolvedValue({ count: 0, errors: false });
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [OpensearchConsumerMicroserviceModule],
    })
      .overrideProvider(OpenSearchService)
      .useValue({
        ensureIndex: jest.fn().mockResolvedValue(undefined),
        bulkIndex,
      })
      .compile();

    controller = moduleFixture.get(OpensearchConsumerMicroserviceController);
    service = moduleFixture.get(OpensearchConsumerMicroserviceService);
    await service.onModuleInit();
  });

  it('handleOrderEvent enqueues event and flush on destroy sends to OpenSearch', async () => {
    const payload = JSON.stringify({
      eventId: 'e2e-id-1',
      type: 'OrderCreated',
      occurredAt: new Date().toISOString(),
      payload: { orderId: 'ord-1', items: [] },
    });
    controller.handleOrderEvent(payload);
    expect(bulkIndex).not.toHaveBeenCalled();
    service.onModuleDestroy();
    await Promise.resolve();
    expect(bulkIndex).toHaveBeenCalledWith(
      EVENTS_INDEX,
      expect.arrayContaining([
        expect.objectContaining({
          id: 'e2e-id-1',
          source: expect.objectContaining({
            eventId: 'e2e-id-1',
            type: 'OrderCreated',
            payload: expect.anything(),
          }),
        }),
      ]),
    );
  });
});
