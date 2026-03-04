import type { KafkaProducer } from './kafka.producer';

export type RetryDlqOptions = {
  maxAttempts?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
};

const DEFAULT_OPTIONS: Required<RetryDlqOptions> = {
  maxAttempts: 4,
  baseDelayMs: 500,
  maxDelayMs: 15_000,
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Runs process() with exponential backoff retry. On final failure, sends the
 * message to the DLQ and rethrows (caller may catch and not rethrow to commit offset).
 */
export async function withRetryAndDlq<T, TPayload = unknown>(
  producer: KafkaProducer,
  opts: {
    dlqTopic: string;
    sourceTopic: string;
    partitionKey: string;
    originalMessage: TPayload;
    consumer?: string;
    retry?: RetryDlqOptions;
  },
  process: () => Promise<T>,
): Promise<T> {
  const { maxAttempts, baseDelayMs, maxDelayMs } = {
    ...DEFAULT_OPTIONS,
    ...opts.retry,
  };
  let lastError: Error | null = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await process();
    } catch (e) {
      lastError = e instanceof Error ? e : new Error(String(e));
      if (attempt === maxAttempts) {
        await producer.emitToDlq({
          dlqTopic: opts.dlqTopic,
          sourceTopic: opts.sourceTopic,
          partitionKey: opts.partitionKey,
          originalMessage: opts.originalMessage,
          error: lastError.message,
          attemptCount: maxAttempts,
          consumer: opts.consumer,
        });
        throw lastError;
      }
      const delay = Math.min(
        baseDelayMs * Math.pow(2, attempt - 1) +
          Math.floor(Math.random() * 200),
        maxDelayMs,
      );
      await sleep(delay);
    }
  }
  throw lastError ?? new Error('Unknown error');
}
