import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { registerEnv } from '@app/config';
import { KafkaModule } from '@app/kafka';
import { PrismaService } from './prisma.service';
import { ShippingEventsController } from './shipping-events.controller';
import { ShippingService } from './shipping.service';

@Module({
  imports: [
    ConfigModule.forRoot({ load: [registerEnv], isGlobal: true }),
    KafkaModule,
  ],
  controllers: [ShippingEventsController],
  providers: [PrismaService, ShippingService],
})
export class AppModule {}
