import { Controller, Get, Post, Body, Param, Query } from '@nestjs/common';
import {
  ApiTags,
  ApiOperation,
  ApiBody,
  ApiQuery,
  ApiParam,
  ApiResponse,
} from '@nestjs/swagger';
import { OrdersProxyService } from './orders-proxy.service';
import { CreateOrderDto, SearchOrdersQueryDto, OrderIdParamDto } from './dto';

@ApiTags('orders')
@Controller('orders')
export class OrdersController {
  constructor(private readonly proxy: OrdersProxyService) {}

  @Post()
  @ApiOperation({
    summary: 'Create order',
    description: 'Creates an order and publishes OrderCreated (event-driven).',
  })
  @ApiBody({
    description:
      'Order items (productId + quantity). At least one item required.',
    schema: {
      type: 'object',
      required: ['items'],
      properties: {
        items: {
          type: 'array',
          minItems: 1,
          items: {
            type: 'object',
            required: ['productId', 'quantity'],
            properties: {
              productId: { type: 'string', example: 'prod-001' },
              quantity: { type: 'number', minimum: 1, example: 2 },
            },
          },
        },
      },
    },
  })
  @ApiResponse({
    status: 201,
    description: 'Order created.',
    schema: {
      type: 'object',
      properties: {
        orderId: { type: 'string', format: 'uuid' },
        status: { type: 'string', example: 'PENDING' },
      },
    },
  })
  @ApiResponse({ status: 400, description: 'Validation error.' })
  async createOrder(
    @Body() body: CreateOrderDto,
  ): Promise<Record<string, unknown>> {
    return this.proxy.createOrder(body);
  }

  @Get('search')
  @ApiOperation({
    summary: 'Search orders',
    description:
      'Search orders from read model (OpenSearch). Optional query and status filter.',
  })
  @ApiQuery({
    name: 'q',
    required: false,
    description: 'Search by orderId (exact match)',
  })
  @ApiQuery({
    name: 'status',
    required: false,
    description: 'Filter by status',
    enum: ['PENDING', 'CONFIRMED', 'CANCELLED'],
  })
  @ApiResponse({
    status: 200,
    description: 'List of order documents from read model.',
  })
  async searchOrders(@Query() query: SearchOrdersQueryDto): Promise<unknown[]> {
    return this.proxy.searchOrders(query.q, query.status);
  }

  @Get(':orderId')
  @ApiOperation({
    summary: 'Get order by ID',
    description: 'Returns order details from order service.',
  })
  @ApiParam({ name: 'orderId', description: 'Order UUID' })
  @ApiResponse({ status: 200, description: 'Order details.' })
  @ApiResponse({ status: 404, description: 'Order not found.' })
  async getOrder(
    @Param() params: OrderIdParamDto,
  ): Promise<Record<string, unknown>> {
    return this.proxy.getOrder(params.orderId);
  }
}
