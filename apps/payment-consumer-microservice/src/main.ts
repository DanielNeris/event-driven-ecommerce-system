import { bootstrapKafkaConsumer } from '@app/kafka';
import { AppModule } from './app.module';

async function bootstrap() {
  await bootstrapKafkaConsumer(AppModule, 'payment-service');
}
void bootstrap();
