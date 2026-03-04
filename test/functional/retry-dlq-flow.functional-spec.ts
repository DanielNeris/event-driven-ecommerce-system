/**
 * Functional test: retry + DLQ flow.
 * Asserts that withRetryAndDlq retries on failure and sends to DLQ on final failure.
 */
import type { KafkaProducer } from '@app/kafka';
import { withRetryAndDlq } from '@app/kafka/retry-dlq.helper';
import { TOPICS } from '@app/contracts';

describe('Retry + DLQ flow (functional)', () => {
  it('should succeed when process returns and never call emitToDlq', async () => {
    const emitToDlq = jest.fn().mockResolvedValue(undefined);
    const producer = { emitToDlq } as unknown as KafkaProducer;

    const result = await withRetryAndDlq(
      producer,
      {
        dlqTopic: TOPICS.INVENTORY_DLQ,
        sourceTopic: TOPICS.ORDER_EVENTS,
        partitionKey: 'order-1',
        originalMessage: {
          type: 'OrderCreated',
          payload: { orderId: 'order-1' },
        },
        consumer: 'inventory-service',
        retry: { maxAttempts: 2, baseDelayMs: 1, maxDelayMs: 10 },
      },
      () => Promise.resolve({ done: true }),
    );

    expect(result).toEqual({ done: true });
    expect(emitToDlq).not.toHaveBeenCalled();
  });

  it('should call emitToDlq with correct payload when process always fails', async () => {
    const emitToDlq = jest.fn().mockResolvedValue(undefined);
    const producer = { emitToDlq } as unknown as KafkaProducer;
    const originalMessage = { eventId: 'evt-1', type: 'StockReserved' };

    await expect(
      withRetryAndDlq(
        producer,
        {
          dlqTopic: TOPICS.PAYMENT_DLQ,
          sourceTopic: TOPICS.INVENTORY_EVENTS,
          partitionKey: 'order-2',
          originalMessage,
          consumer: 'payment-service',
          retry: { maxAttempts: 2, baseDelayMs: 1, maxDelayMs: 5 },
        },
        () => Promise.reject(new Error('DB connection failed')),
      ),
    ).rejects.toThrow('DB connection failed');

    expect(emitToDlq).toHaveBeenCalledTimes(1);
    expect(emitToDlq).toHaveBeenCalledWith({
      dlqTopic: TOPICS.PAYMENT_DLQ,
      sourceTopic: TOPICS.INVENTORY_EVENTS,
      partitionKey: 'order-2',
      originalMessage,
      error: 'DB connection failed',
      attemptCount: 2,
      consumer: 'payment-service',
    });
  });
});
