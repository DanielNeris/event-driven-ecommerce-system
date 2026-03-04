import {
  Controller,
  Get,
  Post,
  Body,
  Param,
  NotFoundException,
} from '@nestjs/common';
import { OrderService } from './order.service';

@Controller('orders')
export class OrdersController {
  constructor(private readonly orderService: OrderService) {}

  @Post()
  async createOrder(
    @Body() body: { items: Array<{ productId: string; quantity: number }> },
  ) {
    const items = body?.items ?? [];
    if (!Array.isArray(items) || items.length === 0) {
      throw new NotFoundException('items array required');
    }
    return this.orderService.createOrder(items);
  }

  @Get(':orderId')
  async getOrder(@Param('orderId') orderId: string) {
    const order = await this.orderService.getOrder(orderId);
    if (!order) throw new NotFoundException('Order not found');
    return order;
  }
}
