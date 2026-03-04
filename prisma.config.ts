import 'dotenv/config';
import { defineConfig, env } from 'prisma/config';

/**
 * Used by Prisma CLI (migrate deploy, seed, etc.).
 * - Always set DATABASE_URL before running (migrate-deploy script sets it per service).
 * - Do NOT set migrations.path here: it would force the same migrations for every
 *   --schema, so each service must use its own migrations dir (default per schema).
 */
export default defineConfig({
  schema: 'apps/order-producer-microservice/prisma/schema.prisma',
  datasource: {
    url: env('DATABASE_URL'),
  },
});
