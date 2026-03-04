import { env } from '@app/config';

export function getRedisUrl(): string {
  return env.REDIS_URL;
}
