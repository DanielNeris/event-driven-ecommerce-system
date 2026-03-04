import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { registerEnv } from '@app/config';
import { KafkaModule } from '@app/kafka';
import { PrismaService } from './prisma.service';
import { MessageriaEventsController } from './messageria-events.controller';
import { MessageriaService } from './messageria.service';

@Module({
  imports: [
    ConfigModule.forRoot({ load: [registerEnv], isGlobal: true }),
    KafkaModule,
  ],
  controllers: [MessageriaEventsController],
  providers: [PrismaService, MessageriaService],
})
export class AppModule {}
