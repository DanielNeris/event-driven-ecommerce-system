import { z } from 'zod';
import { createZodDto } from 'nestjs-zod';

export const searchOrdersQuerySchema = z.object({
  q: z.string().optional(),
  status: z.string().optional(),
});

export type SearchOrdersQueryDtoType = z.infer<typeof searchOrdersQuerySchema>;

export class SearchOrdersQueryDto extends createZodDto(
  searchOrdersQuerySchema,
) {}
