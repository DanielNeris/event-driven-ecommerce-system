import { Injectable } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { firstValueFrom } from 'rxjs';
import { env } from '@app/config';
import { OpenSearchService } from '@app/opensearch';
import { ORDER_INDEX } from '@app/opensearch';

@Injectable()
export class OrdersProxyService {
  constructor(
    private readonly http: HttpService,
    private readonly openSearch: OpenSearchService,
  ) {}

  get orderBaseUrl(): string {
    return env.ORDER_SERVICE_URL.replace(/\/$/, '');
  }

  async createOrder(body: {
    items: Array<{ productId: string; quantity: number }>;
  }): Promise<Record<string, unknown>> {
    const response = await firstValueFrom(
      this.http.post<Record<string, unknown>>(
        `${this.orderBaseUrl}/orders`,
        body,
      ),
    );
    return response.data;
  }

  async getOrder(orderId: string): Promise<Record<string, unknown>> {
    const response = await firstValueFrom(
      this.http.get<Record<string, unknown>>(
        `${this.orderBaseUrl}/orders/${orderId}`,
      ),
    );
    return response.data;
  }

  async searchOrders(query?: string, status?: string): Promise<unknown[]> {
    await this.openSearch.ensureIndex(ORDER_INDEX);
    const client = this.openSearch.getClient();
    const must: Record<string, unknown>[] = [];
    if (query) {
      must.push({ match: { orderId: query } });
    }
    if (status) {
      must.push({ term: { 'status.keyword': status } });
    }
    const body = must.length
      ? { query: { bool: { must } } }
      : { query: { match_all: {} } };
    const res = await client.search({
      index: ORDER_INDEX,
      body: { ...body, size: 50 },
    });
    const hits =
      (res.body as { hits?: { hits?: Array<{ _source: unknown }> } }).hits
        ?.hits ?? [];
    return hits.map((h) => h._source);
  }
}
