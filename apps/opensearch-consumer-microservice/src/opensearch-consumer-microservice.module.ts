import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { registerEnv } from '@app/config';
import { OpenSearchModule } from '@app/opensearch';
import { OpensearchConsumerMicroserviceController } from './opensearch-consumer-microservice.controller';
import { OpensearchConsumerMicroserviceService } from './opensearch-consumer-microservice.service';

@Module({
  imports: [
    ConfigModule.forRoot({ load: [registerEnv], isGlobal: true }),
    OpenSearchModule,
  ],
  controllers: [OpensearchConsumerMicroserviceController],
  providers: [OpensearchConsumerMicroserviceService],
})
export class OpensearchConsumerMicroserviceModule {}
