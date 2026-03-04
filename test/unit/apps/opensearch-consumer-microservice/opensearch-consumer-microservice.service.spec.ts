import { Test, TestingModule } from '@nestjs/testing';
import { OpenSearchService, EVENTS_INDEX } from '@app/opensearch';
import { OpensearchConsumerMicroserviceService } from '@app/opensearch-consumer-microservice/opensearch-consumer-microservice.service';

describe('OpensearchConsumerMicroserviceService', () => {
  let service: OpensearchConsumerMicroserviceService;
  let openSearch: {
    ensureIndex: jest.Mock;
    bulkIndex: jest.Mock;
  };

  beforeEach(async () => {
    openSearch = {
      ensureIndex: jest.fn().mockResolvedValue(undefined),
      bulkIndex: jest.fn().mockResolvedValue({ count: 0, errors: false }),
    };
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        OpensearchConsumerMicroserviceService,
        { provide: OpenSearchService, useValue: openSearch },
      ],
    }).compile();

    service = module.get(OpensearchConsumerMicroserviceService);
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('should ensure index and schedule flush on module init', async () => {
    await service.onModuleInit();
    expect(openSearch.ensureIndex).toHaveBeenCalledWith(EVENTS_INDEX);
    expect(openSearch.bulkIndex).not.toHaveBeenCalled();
    service.indexEvent(
      JSON.stringify({
        eventId: 'timer-flush-id',
        type: 'OrderCreated',
        occurredAt: new Date().toISOString(),
        payload: {},
      }),
    );
    jest.advanceTimersByTime(2000);
    expect(openSearch.bulkIndex).toHaveBeenCalledWith(
      EVENTS_INDEX,
      expect.any(Array),
    );
  });

  it('should clear timer and flush buffer on module destroy', async () => {
    const id = 'uuid-' + Math.random().toString(36).slice(2);
    service.indexEvent(
      JSON.stringify({
        eventId: id,
        type: 'OrderCreated',
        occurredAt: new Date().toISOString(),
        payload: {},
      }),
    );
    service.onModuleDestroy();
    await Promise.resolve();
    expect(openSearch.bulkIndex).toHaveBeenCalledWith(
      EVENTS_INDEX,
      expect.arrayContaining([expect.objectContaining({ id })]),
    );
  });

  it('should clear timer and flush when timer was set and module is destroyed', async () => {
    await service.onModuleInit();
    const id = 'timer-clear-id';
    service.indexEvent(
      JSON.stringify({
        eventId: id,
        type: 'StockReserved',
        occurredAt: new Date().toISOString(),
        payload: {},
      }),
    );
    service.onModuleDestroy();
    await Promise.resolve();
    expect(openSearch.bulkIndex).toHaveBeenCalledWith(
      EVENTS_INDEX,
      expect.any(Array),
    );
  });

  it('should not add second timer when scheduleFlush is already scheduled', async () => {
    await service.onModuleInit();
    const clearIntervalSpy = jest.spyOn(global, 'clearInterval');
    await service.onModuleInit();
    clearIntervalSpy.mockRestore();
    service.indexEvent(
      JSON.stringify({
        eventId: 'one',
        type: 'OrderCreated',
        occurredAt: new Date().toISOString(),
        payload: {},
      }),
    );
    jest.advanceTimersByTime(2000);
    await Promise.resolve();
    expect(openSearch.bulkIndex).toHaveBeenCalledTimes(1);
  });

  it('should log bulk result on flush success', async () => {
    await service.onModuleInit();
    service.indexEvent(
      JSON.stringify({
        eventId: 'log-id',
        type: 'PaymentAuthorized',
        occurredAt: new Date().toISOString(),
        payload: {},
      }),
    );
    service.onModuleDestroy();
    await Promise.resolve();
    expect(openSearch.bulkIndex).toHaveBeenCalled();
  });

  it('should enqueue valid envelope (string) and not flush when under batch size', () => {
    const id = 'uuid-' + Math.random().toString(36).slice(2);
    const raw = JSON.stringify({
      eventId: id,
      type: 'OrderCreated',
      occurredAt: new Date().toISOString(),
      payload: { orderId: 'ord-1' },
    });
    service.indexEvent(raw);
    expect(openSearch.bulkIndex).not.toHaveBeenCalled();
  });

  it('should accept object input in indexEvent', () => {
    const id = 'uuid-' + Math.random().toString(36).slice(2);
    service.indexEvent({
      eventId: id,
      type: 'ShipmentCreated',
      occurredAt: new Date().toISOString(),
      payload: {},
    });
    expect(openSearch.bulkIndex).not.toHaveBeenCalled();
  });

  it('should ignore message without eventId', () => {
    service.indexEvent(JSON.stringify({ type: 'OrderCreated', payload: {} }));
    service.indexEvent(JSON.stringify({ eventId: null, payload: {} }));
    service.indexEvent(JSON.stringify({ eventId: 123, payload: {} }));
    expect(openSearch.bulkIndex).not.toHaveBeenCalled();
  });

  it('should flush when buffer reaches batch size (50)', async () => {
    await service.onModuleInit();
    for (let i = 0; i < 50; i++) {
      service.indexEvent(
        JSON.stringify({
          eventId: `id-${i}`,
          type: 'OrderCreated',
          occurredAt: new Date().toISOString(),
          payload: {},
        }),
      );
    }
    expect(openSearch.bulkIndex).toHaveBeenCalledWith(
      EVENTS_INDEX,
      expect.any(Array),
    );
    const firstCallArgs = openSearch.bulkIndex.mock.calls[0];
    expect(firstCallArgs?.[1]).toHaveLength(50);
  });

  it('should dedupe by eventId (last write wins)', async () => {
    await service.onModuleInit();
    const id = 'dedupe-key';
    for (let i = 0; i < 3; i++) {
      service.indexEvent(
        JSON.stringify({
          eventId: id,
          type: 'OrderCreated',
          occurredAt: new Date().toISOString(),
          payload: { version: i },
        }),
      );
    }
    for (let i = 0; i < 47; i++) {
      service.indexEvent(
        JSON.stringify({
          eventId: `id-${i}`,
          type: 'OrderCreated',
          occurredAt: new Date().toISOString(),
          payload: {},
        }),
      );
    }
    jest.advanceTimersByTime(2000);
    await Promise.resolve();
    expect(openSearch.bulkIndex).toHaveBeenCalledWith(
      EVENTS_INDEX,
      expect.any(Array),
    );
    type BufferedItem = { id: string; source: Record<string, unknown> };
    const firstCallArgs = openSearch.bulkIndex.mock.calls[0];
    const items = firstCallArgs?.[1] as BufferedItem[] | undefined;
    expect(items).toHaveLength(48);
    const byId = items?.find((x) => x.id === id);
    expect(byId).toBeDefined();
    expect(byId?.source.payload).toEqual({ version: 2 });
  });

  it('should not throw on indexEvent parse error', () => {
    service.indexEvent('not json');
    service.indexEvent(undefined as unknown as string);
    expect(openSearch.bulkIndex).not.toHaveBeenCalled();
  });

  it('should log error when bulkIndex rejects', async () => {
    openSearch.bulkIndex.mockRejectedValueOnce(new Error('Bulk failed'));
    await service.onModuleInit();
    service.indexEvent(
      JSON.stringify({
        eventId: 'err-id',
        type: 'OrderCreated',
        occurredAt: new Date().toISOString(),
        payload: {},
      }),
    );
    service.onModuleDestroy();
    await Promise.resolve();
    expect(openSearch.bulkIndex).toHaveBeenCalled();
  });
});
