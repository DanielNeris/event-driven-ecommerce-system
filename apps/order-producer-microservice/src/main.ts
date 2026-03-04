import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { env } from '@app/config';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  const port = env.PORT;
  await app.listen(port);
  console.log(`Order producer (HTTP + outbox) on port ${port}`);
}
void bootstrap();
