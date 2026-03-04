import { bootstrapKafkaConsumer } from '@app/kafka/kafka.consumer';
import type { Type } from '@nestjs/common';

const mockListen = jest.fn().mockResolvedValue(undefined);
const microserviceInstance = { listen: mockListen };
const mockCreateMicroservice = jest
  .fn()
  .mockResolvedValue(microserviceInstance);

jest.mock('@nestjs/core', () => ({
  NestFactory: {
    createMicroservice: (...args: unknown[]) =>
      mockCreateMicroservice(...args) as Promise<{
        listen: () => Promise<void>;
      }>,
  },
}));

describe('bootstrapKafkaConsumer', () => {
  const rootModule = class AppModule {} as Type<object>;

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should create microservice with Kafka transport and listen', async () => {
    await bootstrapKafkaConsumer(rootModule, 'test-group');

    expect(mockCreateMicroservice).toHaveBeenCalledWith(rootModule, {
      transport: expect.any(Number),
      options: expect.objectContaining({
        consumer: { groupId: 'test-group' },
        run: { autoCommit: false },
      }),
    });
    expect(mockListen).toHaveBeenCalled();
  });

  it('should pass fromBeginning when provided', async () => {
    await bootstrapKafkaConsumer(rootModule, 'my-group', {
      fromBeginning: true,
    });

    expect(mockCreateMicroservice).toHaveBeenCalledWith(
      rootModule,
      expect.objectContaining({
        options: expect.objectContaining({
          subscribe: { fromBeginning: true },
        }),
      }),
    );
  });
});
