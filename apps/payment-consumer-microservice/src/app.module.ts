import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { registerEnv } from '@app/config';
import { KafkaModule } from '@app/kafka';
import { PrismaService } from './prisma.service';
import { PaymentEventsController } from './payment-events.controller';
import { PaymentService } from './payment.service';

@Module({
  imports: [
    ConfigModule.forRoot({ load: [registerEnv], isGlobal: true }),
    KafkaModule,
  ],
  controllers: [PaymentEventsController],
  providers: [PrismaService, PaymentService],
})
export class AppModule {}
