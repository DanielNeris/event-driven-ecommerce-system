import { bootstrapKafkaConsumer } from '@app/kafka';
import { OpensearchConsumerMicroserviceModule } from './opensearch-consumer-microservice.module';

async function bootstrap() {
  await bootstrapKafkaConsumer(
    OpensearchConsumerMicroserviceModule,
    'opensearch-consumer',
  );
}
void bootstrap();
