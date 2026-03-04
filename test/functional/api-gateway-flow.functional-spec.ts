/**
 * Functional test: API Gateway (OrdersProxyService).
 * createOrder, getOrder, searchOrders with mocked HTTP and OpenSearch.
 */
import { Test, TestingModule } from '@nestjs/testing';
import { of } from 'rxjs';
import { HttpService } from '@nestjs/axios';
import { OrdersProxyService } from '@app/api-gateway/orders-proxy.service';
import { OpenSearchService } from '@app/opensearch';
import { ORDER_INDEX } from '@app/opensearch';

describe('API Gateway flow (functional)', () => {
  let service: OrdersProxyService;
  let httpPost: jest.Mock;
  let httpGet: jest.Mock;
  let openSearchSearch: jest.Mock;
  let openSearchEnsureIndex: jest.Mock;

  beforeEach(async () => {
    httpPost = jest
      .fn()
      .mockReturnValue(
        of({ data: { orderId: 'ord-gateway-1', status: 'PENDING' } }),
      );
    httpGet = jest.fn().mockReturnValue(
      of({
        data: { orderId: 'ord-gateway-1', status: 'PENDING', items: [] },
      }),
    );
    openSearchEnsureIndex = jest.fn().mockResolvedValue(undefined);
    openSearchSearch = jest.fn().mockResolvedValue({
      body: {
        hits: {
          hits: [
            { _source: { orderId: 'ord-1', status: 'PENDING', timeline: [] } },
            {
              _source: { orderId: 'ord-2', status: 'CONFIRMED', timeline: [] },
            },
          ],
        },
      },
    });

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        OrdersProxyService,
        {
          provide: HttpService,
          useValue: {
            post: httpPost,
            get: httpGet,
          },
        },
        {
          provide: OpenSearchService,
          useValue: {
            ensureIndex: openSearchEnsureIndex,
            getClient: () => ({
              search: openSearchSearch,
            }),
          },
        },
      ],
    }).compile();

    service = module.get(OrdersProxyService);
  });

  it('should proxy createOrder to order service and return orderId and status', async () => {
    const body = { items: [{ productId: 'prod-1', quantity: 2 }] };
    const result = await service.createOrder(body);

    expect(result).toEqual({ orderId: 'ord-gateway-1', status: 'PENDING' });
    expect(httpPost).toHaveBeenCalledWith(
      expect.stringMatching(/\/orders$/),
      body,
    );
  });

  it('should proxy getOrder to order service', async () => {
    const result = await service.getOrder('ord-gateway-1');

    expect(result).toEqual({
      orderId: 'ord-gateway-1',
      status: 'PENDING',
      items: [],
    });
    expect(httpGet).toHaveBeenCalledWith(
      expect.stringContaining('/orders/ord-gateway-1'),
    );
  });

  it('should search orders in OpenSearch and return hits', async () => {
    const result = await service.searchOrders();

    expect(openSearchEnsureIndex).toHaveBeenCalledWith(ORDER_INDEX);
    expect(openSearchSearch).toHaveBeenCalledWith(
      expect.objectContaining({
        index: ORDER_INDEX,
        body: expect.objectContaining({ query: { match_all: {} }, size: 50 }),
      }),
    );
    expect(result).toHaveLength(2);
    expect(result[0]).toEqual({
      orderId: 'ord-1',
      status: 'PENDING',
      timeline: [],
    });
  });

  it('should filter search by status when status param provided', async () => {
    await service.searchOrders(undefined, 'CONFIRMED');

    expect(openSearchSearch).toHaveBeenCalledWith(
      expect.objectContaining({
        body: expect.objectContaining({
          query: {
            bool: { must: [{ term: { 'status.keyword': 'CONFIRMED' } }] },
          },
        }),
      }),
    );
  });
});
