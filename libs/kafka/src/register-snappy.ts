/**
 * Registers Snappy compression codec with KafkaJS.
 *
 * KafkaJS does not include Snappy by default; this must run before creating
 * any Kafka producer/consumer that uses CompressionTypes.Snappy.
 * Safe to call multiple times (idempotent).
 *
 * @see https://kafka.js.org/docs/producing#snappy
 * @see https://github.com/jlandersen/vscode-kafka/blob/main/src/client/compression.ts
 */
import { CompressionCodecs, CompressionTypes } from 'kafkajs';

let registered = false;

export function registerCompressionCodecs(): void {
  if (registered) {
    return;
  }

  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const snappyModule = require('kafkajs-snappy') as
      | (() => unknown)
      | { default?: () => unknown };
    // KafkaJS calls Codecs[type]() – it expects a function that returns the codec.
    const codec =
      typeof snappyModule === 'function'
        ? snappyModule
        : (): unknown =>
            (snappyModule && 'default' in snappyModule
              ? snappyModule.default
              : snappyModule) ?? snappyModule;
    CompressionCodecs[CompressionTypes.Snappy] = codec;
    registered = true;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(
      `Snappy compression requires kafkajs-snappy. Install it with: pnpm add kafkajs-snappy. Cause: ${message}`,
    );
  }
}

// Run on module load so importing KafkaModule registers the codec
registerCompressionCodecs();
