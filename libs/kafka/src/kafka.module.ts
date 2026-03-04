import './register-snappy';
import { Global, Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { KAFKA_CLIENT, HIGH_THROUGHPUT_SEND_OPTIONS } from './kafka.constants';
import { getKafkaBrokers } from './kafka.config';
import { KafkaProducer } from './kafka.producer';

/**
 * Global Kafka module providing a shared producer (ClientKafka).
 * High-throughput: Snappy compression, acks=1, 30s timeout.
 * Usage: import KafkaModule once (e.g. in api-gateway AppModule).
 */
@Global()
@Module({
  imports: [
    ClientsModule.register([
      {
        name: KAFKA_CLIENT,
        transport: Transport.KAFKA,
        options: {
          client: {
            brokers: getKafkaBrokers(),
          },
          producerOnlyMode: true,
          /** Applied to every producer send (compression, acks, timeout). */
          send: HIGH_THROUGHPUT_SEND_OPTIONS,
        },
      },
    ]),
  ],
  providers: [KafkaProducer],
  exports: [KafkaProducer],
})
export class KafkaModule {}
