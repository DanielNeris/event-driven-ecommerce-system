import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import Redis from 'ioredis';
import { getRedisUrl } from './redis.config';

const LUA_DELETE_IF_VALUE = `
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
else
  return 0
end
`;

@Injectable()
export class RedisService implements OnModuleInit, OnModuleDestroy {
  private client: Redis | null = null;

  onModuleInit(): void {
    this.client = new Redis(getRedisUrl());
  }

  onModuleDestroy(): void {
    this.client?.disconnect();
    this.client = null;
  }

  getClient(): Redis {
    if (!this.client) throw new Error('Redis not connected');
    return this.client;
  }

  async get(key: string): Promise<string | null> {
    return this.getClient().get(key);
  }

  async set(key: string, value: string, ttlSeconds?: number): Promise<void> {
    if (ttlSeconds != null) {
      await this.getClient().set(key, value, 'EX', ttlSeconds);
    } else {
      await this.getClient().set(key, value);
    }
  }

  /**
   * Set key only if not exists, with TTL (e.g. product lock).
   * Returns true if lock was acquired.
   */
  async setNxEx(
    key: string,
    value: string,
    ttlSeconds: number,
  ): Promise<boolean> {
    const result = await this.getClient().set(
      key,
      value,
      'EX',
      ttlSeconds,
      'NX',
    );
    return result === 'OK';
  }

  /**
   * Delete key only if current value equals expectedValue (compare-and-delete for safe lock release).
   */
  async deleteIfValue(key: string, expectedValue: string): Promise<boolean> {
    const result = await this.getClient().eval(
      LUA_DELETE_IF_VALUE,
      1,
      key,
      expectedValue,
    );
    return result === 1;
  }

  async del(key: string): Promise<void> {
    await this.getClient().del(key);
  }
}
