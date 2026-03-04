import { INestMicroservice, Type } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { Transport } from '@nestjs/microservices';
import { getKafkaBrokers } from './kafka.config';

/**
 * Bootstraps a Nest Kafka microservice with consistent config.
 * Use this in apps/*-consumer/main.ts
 *
 * Example:
 * await bootstrapKafkaConsumer(AppModule, 'orders-consumer-group')
 */
export async function bootstrapKafkaConsumer(
  rootModule: Type<object>,
  groupId: string,
  options?: {
    fromBeginning?: boolean;
  },
): Promise<INestMicroservice> {
  const app = await NestFactory.createMicroservice(rootModule, {
    transport: Transport.KAFKA,
    options: {
      client: {
        brokers: getKafkaBrokers(),
        requestTimeout: 30_000,
        connectionTimeout: 10_000,
        retry: {
          retries: 10,
          initialRetryTime: 500,
          maxRetryTime: 30_000,
        },
      },
      consumer: {
        groupId,
        sessionTimeout: 30_000,
        rebalanceTimeout: 60_000,
        retry: {
          retries: 10,
          initialRetryTime: 500,
          maxRetryTime: 30_000,
        },
      },
      subscribe: {
        fromBeginning: options?.fromBeginning ?? false,
      },
      run: {
        autoCommit: false,
      },
    },
  });

  await app.listen();
  return app;
}
