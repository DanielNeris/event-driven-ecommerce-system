import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { registerEnv } from '@app/config';
import { KafkaModule } from '@app/kafka';
import { PrismaService } from './prisma.service';
import { OrdersController } from './orders.controller';
import { OrderService } from './order.service';
import { OutboxWorkerService } from './outbox-worker.service';
@Module({
  imports: [
    ConfigModule.forRoot({ load: [registerEnv], isGlobal: true }),
    KafkaModule,
  ],
  controllers: [OrdersController],
  providers: [PrismaService, OrderService, OutboxWorkerService],
})
export class AppModule {}
