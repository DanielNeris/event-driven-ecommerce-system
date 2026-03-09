import { Injectable } from '@nestjs/common';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { getSchemaRegistryUrl } from './schema-registry.config';

/**
 * Nest wrapper for @kafkajs/confluent-schema-registry.
 * Inject where you need to register, encode, or decode schemas.
 */
@Injectable()
export class SchemaRegistryService {
  private registry: SchemaRegistry | null = null;

  /**
   * Lazy-initialized Schema Registry client (same pattern as OpenSearchService).
   */
  getRegistry(): SchemaRegistry {
    if (!this.registry) {
      this.registry = new SchemaRegistry({
        host: getSchemaRegistryUrl(),
      });
    }
    return this.registry;
  }
}
