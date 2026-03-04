/**
 * Functional test: OpenSearch Consumer (event audit index).
 * indexEvent enqueues; flush bulk-indexes to OpenSearch.
 */
import { Test, TestingModule } from '@nestjs/testing';
import { OpensearchConsumerMicroserviceService } from '@app/opensearch-consumer-microservice/opensearch-consumer-microservice.service';
import { OpenSearchService } from '@app/opensearch';
import { EVENTS_INDEX } from '@app/opensearch';

describe('OpenSearch Consumer flow (functional)', () => {
  let service: OpensearchConsumerMicroserviceService;
  let ensureIndex: jest.Mock;
  let bulkSpy: jest.Mock;

  beforeEach(async () => {
    ensureIndex = jest.fn().mockResolvedValue(undefined);
    bulkSpy = jest.fn().mockResolvedValue({ count: 1, errors: false });

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        OpensearchConsumerMicroserviceService,
        {
          provide: OpenSearchService,
          useValue: {
            ensureIndex: ensureIndex,
            bulkIndex: bulkSpy,
          },
        },
      ],
    }).compile();

    service = module.get(OpensearchConsumerMicroserviceService);
    return service.onModuleInit();
  });

  afterEach(() => {
    service.onModuleDestroy();
  });

  it('should ensure EVENTS_INDEX on init', () => {
    expect(ensureIndex).toHaveBeenCalledWith(EVENTS_INDEX);
  });

  it('should enqueue event and bulkIndex on flush (via onModuleDestroy)', () => {
    const envelope = {
      eventId: 'evt-audit-1',
      type: 'OrderCreated',
      version: 1,
      occurredAt: new Date().toISOString(),
      correlationId: 'ord-1',
      payload: { orderId: 'ord-1', items: [] },
    };
    service.indexEvent(envelope);
    service.onModuleDestroy();

    expect(bulkSpy).toHaveBeenCalledWith(EVENTS_INDEX, [
      expect.objectContaining({
        id: 'evt-audit-1',
        source: expect.objectContaining({
          eventId: 'evt-audit-1',
          type: 'OrderCreated',
          correlationId: 'ord-1',
        }),
      }),
    ]);
  });

  it('should skip event without eventId', () => {
    service.indexEvent({ type: 'OrderCreated', payload: {} } as never);
    service.indexEvent(JSON.stringify({ type: 'OrderCreated' }));
    expect(ensureIndex).toHaveBeenCalledWith(EVENTS_INDEX);
  });
});
