import { z } from 'zod';
import { createZodDto } from 'nestjs-zod';

export const orderIdParamSchema = z.object({
  orderId: z.string().uuid('orderId deve ser um UUID válido'),
});

export type OrderIdParamDtoType = z.infer<typeof orderIdParamSchema>;

export class OrderIdParamDto extends createZodDto(orderIdParamSchema) {}
