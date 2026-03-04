import { z } from 'zod';

/**
 * Single source of truth for environment variables.
 * Validated at startup; missing required vars or invalid values throw ZodError.
 * Defaults allow local dev without a full .env.
 */
export const envSchema = z.object({
  /** Kafka broker list (comma-separated). Use localhost:9092 locally, kafka:19092 in Docker. */
  KAFKA_BROKERS: z.string().default('localhost:9092'),
  /** Silence KafkaJS v2 partitioner warning. Set to 1 to hide. */
  KAFKAJS_NO_PARTITIONER_WARNING: z.string().optional().default('1'),
  /** OpenSearch node URL. Use http://opensearch:9200 in Docker. */
  OPENSEARCH_NODE: z.string().default('http://localhost:9200'),
  /** Redis URL. Use redis://redis:6379 in Docker. */
  REDIS_URL: z.string().default('redis://localhost:6379'),
  /** HTTP server port (api-gateway 3000; order service 3001; etc.). */
  PORT: z
    .string()
    .default('3000')
    .transform((v) => Number.parseInt(v, 10)),
  /** Per-service PostgreSQL URLs (for Docker: postgresql://user:pass@postgresql:5432/dbname). */
  ORDER_DATABASE_URL: z.string().optional(),
  INVENTORY_DATABASE_URL: z.string().optional(),
  PAYMENT_DATABASE_URL: z.string().optional(),
  SHIPPING_DATABASE_URL: z.string().optional(),
  MESSAGERIA_DATABASE_URL: z.string().optional(),
  /** Order service URL for api-gateway to proxy (e.g. http://order:3001). */
  ORDER_SERVICE_URL: z.string().default('http://localhost:3001'),
});

export type Env = z.infer<typeof envSchema>;

/** Parsed env — call once at app startup (e.g. when config is first loaded). */
function parseEnv(): Env {
  return envSchema.parse(process.env);
}

/**
 * Validated env, populated when any code imports from @app/config.
 * Ensures we never lose track of env vars and fail fast on invalid/missing required values.
 */
export const env: Env = parseEnv();
