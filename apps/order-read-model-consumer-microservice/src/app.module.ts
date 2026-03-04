import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { registerEnv } from '@app/config';
import { KafkaModule } from '@app/kafka';
import { OpenSearchModule } from '@app/opensearch';
import { OrderReadModelController } from './order-read-model.controller';
import { OrderReadModelService } from './order-read-model.service';

@Module({
  imports: [
    ConfigModule.forRoot({ load: [registerEnv], isGlobal: true }),
    KafkaModule,
    OpenSearchModule,
  ],
  controllers: [OrderReadModelController],
  providers: [OrderReadModelService],
})
export class AppModule {}
