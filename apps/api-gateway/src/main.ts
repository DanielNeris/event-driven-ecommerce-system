import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import { env } from '@app/config';
import { Logger } from '@nestjs/common';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  const port = env.PORT;

  const config = new DocumentBuilder()
    .setTitle('Event-Driven Commerce API')
    .setDescription(
      'API Gateway for order creation, retrieval and search (read model).',
    )
    .setVersion('1.0')
    .addTag('orders')
    .build();
  const document = SwaggerModule.createDocument(app, config);
  SwaggerModule.setup('api', app, document);

  await app.listen(port);
  Logger.log(`Api Gateway listening on port ${port}`);
  Logger.log(`Swagger UI: http://localhost:${port}/api`);
}
void bootstrap();
