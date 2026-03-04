import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { Client } from '@opensearch-project/opensearch';
import { getOpenSearchNode } from './opensearch.config';

@Injectable()
export class OpenSearchService implements OnModuleDestroy {
  private client: Client | null = null;

  getClient(): Client {
    if (!this.client) {
      const node = getOpenSearchNode();
      this.client = new Client({ node });
    }
    return this.client;
  }

  /**
   * Ensure an index exists; create it if not (like the Java demo).
   */
  async ensureIndex(indexName: string): Promise<void> {
    const client = this.getClient();
    const res = await client.indices.exists({ index: indexName });
    const exists = (res as { body?: boolean }).body === true;
    if (!exists) {
      await client.indices.create({ index: indexName });
    }
  }

  /**
   * Bulk index documents. Each item is { id, source }. Uses index action with _id.
   */
  async bulkIndex(
    indexName: string,
    items: Array<{ id: string; source: Record<string, unknown> }>,
  ): Promise<{ count: number; errors: boolean }> {
    if (items.length === 0) {
      return { count: 0, errors: false };
    }
    const body = items.flatMap(({ id, source }) => [
      { index: { _index: indexName, _id: id } },
      source,
    ]);
    const client = this.getClient();
    const response = await client.bulk({ body, index: indexName });
    const errors = response.body?.errors === true;
    const count = response.body?.items?.length ?? 0;
    return { count, errors };
  }

  onModuleDestroy(): void {
    this.client = null;
  }
}
