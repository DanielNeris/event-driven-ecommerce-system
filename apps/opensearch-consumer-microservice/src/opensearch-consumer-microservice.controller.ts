import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { TOPICS } from '@app/contracts';
import { OpensearchConsumerMicroserviceService } from './opensearch-consumer-microservice.service';

@Controller()
export class OpensearchConsumerMicroserviceController {
  constructor(
    private readonly service: OpensearchConsumerMicroserviceService,
  ) {}

  @EventPattern(TOPICS.ORDER_EVENTS)
  handleOrderEvent(@Payload() raw: string | Record<string, unknown>): void {
    this.service.indexEvent(raw);
  }

  @EventPattern(TOPICS.INVENTORY_EVENTS)
  handleInventoryEvent(@Payload() raw: string | Record<string, unknown>): void {
    this.service.indexEvent(raw);
  }

  @EventPattern(TOPICS.PAYMENT_EVENTS)
  handlePaymentEvent(@Payload() raw: string | Record<string, unknown>): void {
    this.service.indexEvent(raw);
  }

  @EventPattern(TOPICS.SHIPPING_EVENTS)
  handleShippingEvent(@Payload() raw: string | Record<string, unknown>): void {
    this.service.indexEvent(raw);
  }

  @EventPattern(TOPICS.NOTIFICATION_EVENTS)
  handleNotificationEvent(
    @Payload() raw: string | Record<string, unknown>,
  ): void {
    this.service.indexEvent(raw);
  }
}
