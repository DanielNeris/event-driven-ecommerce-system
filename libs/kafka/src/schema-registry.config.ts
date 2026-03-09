import { env } from '@app/config';

/** Schema Registry URL from validated env. */
export function getSchemaRegistryUrl(): string {
  return env.SCHEMA_REGISTRY_URL;
}
