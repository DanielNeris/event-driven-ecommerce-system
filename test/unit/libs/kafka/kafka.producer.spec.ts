import { Test, TestingModule } from '@nestjs/testing';
import { of } from 'rxjs';
import { KafkaProducer } from '@app/kafka';
import { KAFKA_CLIENT } from '@app/kafka/kafka.constants';
import { TOPICS } from '@app/contracts';

describe('KafkaProducer', () => {
  let producer: KafkaProducer;
  let clientEmit: jest.Mock;
  let connect: jest.Mock;

  beforeEach(async () => {
    connect = jest.fn().mockResolvedValue(undefined);
    clientEmit = jest.fn().mockReturnValue(of(undefined));
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        KafkaProducer,
        {
          provide: KAFKA_CLIENT,
          useValue: {
            connect,
            emit: clientEmit,
          },
        },
      ],
    }).compile();

    producer = module.get(KafkaProducer);
  });

  it('should connect client on module init', async () => {
    await producer.onModuleInit();
    expect(connect).toHaveBeenCalled();
  });

  it('should send envelope with key and payload to topic on emit', async () => {
    await producer.onModuleInit();
    const payload = { id: 'evt-1', title: 'Test event' };
    const key = 'partition-key-1';
    const traceId = 'trace-abc';

    await producer.emit({
      topic: TOPICS.ORDER_EVENTS,
      payload,
      key,
      headers: { 'X-Trace-Id': traceId },
      version: 1,
    });

    expect(clientEmit).toHaveBeenCalledWith(
      TOPICS.ORDER_EVENTS,
      expect.objectContaining({
        key,
        value: expect.objectContaining({
          payload,
          version: 1,
          traceId,
        }),
        headers: expect.objectContaining({ 'X-Trace-Id': traceId }),
      }),
    );
  });

  it('should use default version 1 when version is omitted on emit', async () => {
    await producer.onModuleInit();
    await producer.emit({
      topic: TOPICS.ORDER_EVENTS,
      payload: { id: 'evt-2' },
      key: 'key-2',
    });
    expect(clientEmit).toHaveBeenCalledWith(
      TOPICS.ORDER_EVENTS,
      expect.objectContaining({
        value: expect.objectContaining({ version: 1 }),
      }),
    );
  });

  it('should work without headers (traceId undefined) on emit', async () => {
    await producer.onModuleInit();
    await producer.emit({
      topic: TOPICS.ORDER_EVENTS,
      payload: { id: 'evt-3' },
      key: 'key-3',
    });
    expect(clientEmit).toHaveBeenCalledWith(
      TOPICS.ORDER_EVENTS,
      expect.objectContaining({
        value: expect.objectContaining({
          traceId: undefined,
          payload: { id: 'evt-3' },
        }),
      }),
    );
  });

  describe('emitToDlq', () => {
    it('should send DlqPayload to dlqTopic with partitionKey as key', async () => {
      await producer.emitToDlq({
        dlqTopic: TOPICS.ORDER_DLQ,
        sourceTopic: TOPICS.ORDER_EVENTS,
        partitionKey: 'order-123',
        originalMessage: { eventId: 'e1', type: 'OrderCreated', payload: {} },
        error: 'Processing failed',
        attemptCount: 4,
        consumer: 'inventory-service',
      });

      expect(clientEmit).toHaveBeenCalledWith(
        TOPICS.ORDER_DLQ,
        expect.objectContaining({
          key: 'order-123',
          value: expect.objectContaining({
            sourceTopic: TOPICS.ORDER_EVENTS,
            partitionKey: 'order-123',
            error: 'Processing failed',
            attemptCount: 4,
            consumer: 'inventory-service',
            failedAt: expect.any(String),
          }),
        }),
      );
    });

    it('should work without consumer', async () => {
      await producer.emitToDlq({
        dlqTopic: TOPICS.PAYMENT_DLQ,
        sourceTopic: TOPICS.INVENTORY_EVENTS,
        partitionKey: 'ord-2',
        originalMessage: {},
        error: 'Timeout',
        attemptCount: 1,
      });

      expect(clientEmit).toHaveBeenCalledWith(
        TOPICS.PAYMENT_DLQ,
        expect.objectContaining({
          value: expect.objectContaining({
            consumer: undefined,
            attemptCount: 1,
          }),
        }),
      );
    });
  });
});
