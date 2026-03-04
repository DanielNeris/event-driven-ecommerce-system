import type { KafkaProducer } from '@app/kafka';
import { withRetryAndDlq } from '@app/kafka/retry-dlq.helper';

describe('withRetryAndDlq', () => {
  let emitToDlq: jest.Mock;

  beforeEach(() => {
    emitToDlq = jest.fn().mockResolvedValue(undefined);
  });

  function mockProducer(): KafkaProducer {
    return { emitToDlq } as unknown as KafkaProducer;
  }

  const baseOpts = {
    dlqTopic: 'test.dlq',
    sourceTopic: 'test.events',
    partitionKey: 'pk-1',
    originalMessage: { foo: 'bar' },
  };

  it('returns result when process succeeds on first try', async () => {
    const producer = mockProducer();
    const result = await withRetryAndDlq(producer, baseOpts, () =>
      Promise.resolve(42),
    );
    expect(result).toBe(42);
    expect(emitToDlq).not.toHaveBeenCalled();
  });

  it('retries and returns when process succeeds on second attempt', async () => {
    const producer = mockProducer();
    let attempts = 0;
    const result = await withRetryAndDlq(
      producer,
      {
        ...baseOpts,
        retry: { maxAttempts: 3, baseDelayMs: 10, maxDelayMs: 50 },
      },
      () => {
        attempts++;
        if (attempts < 2) return Promise.reject(new Error('fail'));
        return Promise.resolve('ok');
      },
    );
    expect(result).toBe('ok');
    expect(attempts).toBe(2);
    expect(emitToDlq).not.toHaveBeenCalled();
  });

  it('calls emitToDlq and rethrows when all attempts fail', async () => {
    const producer = mockProducer();
    const err = new Error('final failure');

    await expect(
      withRetryAndDlq(
        producer,
        {
          ...baseOpts,
          consumer: 'test-consumer',
          retry: { maxAttempts: 2, baseDelayMs: 1, maxDelayMs: 5 },
        },
        () => Promise.reject(err),
      ),
    ).rejects.toThrow('final failure');

    expect(emitToDlq).toHaveBeenCalledTimes(1);
    expect(emitToDlq).toHaveBeenCalledWith({
      dlqTopic: baseOpts.dlqTopic,
      sourceTopic: baseOpts.sourceTopic,
      partitionKey: baseOpts.partitionKey,
      originalMessage: baseOpts.originalMessage,
      error: 'final failure',
      attemptCount: 2,
      consumer: 'test-consumer',
    });
  });

  it('converts non-Error throw to Error message', async () => {
    const producer = mockProducer();

    await expect(
      withRetryAndDlq(
        producer,
        { ...baseOpts, retry: { maxAttempts: 1 } },
        () => Promise.reject(new Error('string error')),
      ),
    ).rejects.toThrow('string error');

    expect(emitToDlq).toHaveBeenCalledWith(
      expect.objectContaining({ error: 'string error', attemptCount: 1 }),
    );
  });
});
