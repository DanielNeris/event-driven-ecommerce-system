import { env } from '@app/config';

/** OpenSearch node URL from validated env. */
export function getOpenSearchNode(): string {
  return env.OPENSEARCH_NODE;
}
