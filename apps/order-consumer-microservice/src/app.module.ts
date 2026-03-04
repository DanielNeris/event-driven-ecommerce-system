import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { registerEnv } from '@app/config';
import { KafkaModule } from '@app/kafka';
import { PrismaService } from './prisma.service';
import { OrderService } from './order.service';
import { OrderEventsController } from './order-events.controller';

@Module({
  imports: [
    ConfigModule.forRoot({ load: [registerEnv], isGlobal: true }),
    KafkaModule,
  ],
  controllers: [OrderEventsController],
  providers: [PrismaService, OrderService],
})
export class AppModule {}
