import { env } from '@app/config';

/** Kafka broker list from validated env (single source of truth). */
export function getKafkaBrokers(): string[] {
  return env.KAFKA_BROKERS.split(',')
    .map((b) => b.trim())
    .filter(Boolean);
}
