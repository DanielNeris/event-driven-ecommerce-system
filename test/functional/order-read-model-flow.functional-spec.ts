/**
 * Functional test: order read model flow.
 * Applies a sequence of events and asserts the OpenSearch document (timeline, status).
 */
import { Test, TestingModule } from '@nestjs/testing';
import { OpenSearchService } from '@app/opensearch';
import { ORDER_INDEX } from '@app/opensearch';
import { OrderReadModelService } from '@app/order-read-model-consumer-microservice/order-read-model.service';
import { EVENT_TYPES } from '@app/contracts';

describe('Order read model flow (functional)', () => {
  let service: OrderReadModelService;
  let clientGet: jest.Mock;
  let clientIndex: jest.Mock;
  let storedDocs: Map<
    string,
    { orderId: string; status: string; timeline: string[] }
  >;

  beforeEach(async () => {
    storedDocs = new Map();
    clientGet = jest
      .fn()
      .mockImplementation(({ id }: { index: string; id: string }) => {
        const doc = storedDocs.get(id);
        return Promise.resolve({
          body: doc ? { found: true, _source: doc } : { found: false },
        });
      });
    clientIndex = jest
      .fn()
      .mockImplementation(
        ({
          id,
          body,
        }: {
          index: string;
          id: string;
          body: Record<string, unknown>;
        }) => {
          storedDocs.set(
            id,
            body as { orderId: string; status: string; timeline: string[] },
          );
          return Promise.resolve({ body: { result: 'updated' } });
        },
      );

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        OrderReadModelService,
        {
          provide: OpenSearchService,
          useValue: {
            ensureIndex: jest.fn().mockResolvedValue(undefined),
            getClient: () => ({
              get: clientGet,
              index: clientIndex,
            }),
          },
        },
      ],
    }).compile();

    service = module.get(OrderReadModelService);
    await service.onModuleInit();
  });

  it('should build timeline and set status PENDING on OrderCreated', async () => {
    const orderId = 'order-flow-1';
    await service.applyEvent({
      eventId: 'e1',
      type: EVENT_TYPES.OrderCreated,
      payload: { orderId },
      correlationId: orderId,
      occurredAt: new Date().toISOString(),
    });

    expect(clientIndex).toHaveBeenCalledWith(
      expect.objectContaining({
        index: ORDER_INDEX,
        id: orderId,
        body: expect.objectContaining({
          orderId,
          status: 'PENDING',
          timeline: ['OrderCreated'],
        }),
      }),
    );
  });

  it('should update status to CONFIRMED on OrderConfirmed', async () => {
    const orderId = 'order-flow-2';
    storedDocs.set(orderId, {
      orderId,
      status: 'PENDING',
      timeline: ['OrderCreated', 'StockReserved'],
    });

    await service.applyEvent({
      eventId: 'e2',
      type: EVENT_TYPES.OrderConfirmed,
      payload: { orderId },
      correlationId: orderId,
      occurredAt: new Date().toISOString(),
    });

    expect(clientIndex).toHaveBeenCalledWith(
      expect.objectContaining({
        id: orderId,
        body: expect.objectContaining({
          status: 'CONFIRMED',
          timeline: expect.arrayContaining([
            'OrderCreated',
            'StockReserved',
            'OrderConfirmed',
          ]),
        }),
      }),
    );
  });

  it('should update status to CANCELLED on OrderCancelled', async () => {
    const orderId = 'order-flow-3';
    storedDocs.set(orderId, {
      orderId,
      status: 'PENDING',
      timeline: ['OrderCreated'],
    });

    await service.applyEvent({
      eventId: 'e3',
      type: EVENT_TYPES.OrderCancelled,
      payload: { orderId },
      correlationId: orderId,
      occurredAt: new Date().toISOString(),
    });

    expect(clientIndex).toHaveBeenCalledWith(
      expect.objectContaining({
        id: orderId,
        body: expect.objectContaining({
          status: 'CANCELLED',
          timeline: expect.arrayContaining(['OrderCreated', 'OrderCancelled']),
        }),
      }),
    );
  });

  it('should skip event when orderId or type is missing', async () => {
    await service.applyEvent({
      eventId: 'e4',
      type: '',
      payload: {},
      correlationId: 'ord-x',
      occurredAt: new Date().toISOString(),
    });
    expect(clientIndex).not.toHaveBeenCalled();

    await service.applyEvent({
      eventId: 'e5',
      type: EVENT_TYPES.OrderCreated,
      payload: {},
      correlationId: undefined,
      occurredAt: new Date().toISOString(),
    });
    expect(clientIndex).not.toHaveBeenCalled();
  });
});
