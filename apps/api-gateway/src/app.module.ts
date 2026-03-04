import { Module } from '@nestjs/common';
import { APP_PIPE } from '@nestjs/core';
import { ConfigModule } from '@nestjs/config';
import { HttpModule } from '@nestjs/axios';
import { ZodValidationPipe } from 'nestjs-zod';
import { registerEnv } from '@app/config';
import { OpenSearchModule } from '@app/opensearch';
import { OrdersController } from './orders.controller';
import { OrdersProxyService } from './orders-proxy.service';

@Module({
  imports: [
    ConfigModule.forRoot({ load: [registerEnv], isGlobal: true }),
    HttpModule.register({ timeout: 10000, maxRedirects: 5 }),
    OpenSearchModule,
  ],
  controllers: [OrdersController],
  providers: [
    OrdersProxyService,
    {
      provide: APP_PIPE,
      useClass: ZodValidationPipe,
    },
  ],
})
export class AppModule {}
