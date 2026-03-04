import { z } from 'zod';
import { createZodDto } from 'nestjs-zod';

const createOrderItemSchema = z.object({
  productId: z.string().min(1, 'productId é obrigatório'),
  quantity: z.number().int().positive('quantity deve ser um inteiro positivo'),
});

export const createOrderSchema = z.object({
  items: z
    .array(createOrderItemSchema)
    .min(1, 'items deve ter pelo menos um item'),
});

export type CreateOrderDtoType = z.infer<typeof createOrderSchema>;

export class CreateOrderDto extends createZodDto(createOrderSchema) {}
