import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
import { registerEnv } from '@app/config';
import { KafkaModule } from '@app/kafka';
import { RedisModule } from '@app/redis';
import { PrismaService } from './prisma.service';
import { InventoryEventsController } from './inventory-events.controller';
import { InventoryService } from './inventory.service';

@Module({
  imports: [
    ConfigModule.forRoot({ load: [registerEnv], isGlobal: true }),
    ScheduleModule.forRoot(),
    KafkaModule,
    RedisModule,
  ],
  controllers: [InventoryEventsController],
  providers: [PrismaService, InventoryService],
})
export class AppModule {}
