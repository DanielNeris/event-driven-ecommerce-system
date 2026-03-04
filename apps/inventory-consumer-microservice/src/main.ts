import { bootstrapKafkaConsumer } from '@app/kafka';
import { AppModule } from './app.module';

async function bootstrap() {
  await bootstrapKafkaConsumer(AppModule, 'inventory-service');
}
void bootstrap();
