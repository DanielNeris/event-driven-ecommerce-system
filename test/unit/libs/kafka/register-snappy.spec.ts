/**
 * register-snappy runs on module load. We test success path by mocking
 * kafkajs and kafkajs-snappy. Jest must load the module via require for mocks to apply.
 */
/* eslint-disable @typescript-eslint/no-require-imports */
const codecs: Record<string, unknown> = {};
const Snappy = 'Snappy';

jest.mock('kafkajs', () => ({
  CompressionCodecs: codecs,
  CompressionTypes: { Snappy },
}));

const mockSnappyFn = jest.fn();
jest.mock('kafkajs-snappy', () => mockSnappyFn);

describe('register-snappy', () => {
  beforeAll(() => {
    require('@app/kafka/register-snappy');
  });

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should register Snappy codec on load when kafkajs-snappy is a function', () => {
    expect(codecs[Snappy]).toBe(mockSnappyFn);
  });

  it('should not overwrite codec on second call (idempotent)', () => {
    const mod = require('@app/kafka/register-snappy') as {
      registerCompressionCodecs: () => void;
    };
    expect(codecs[Snappy]).toBe(mockSnappyFn);
    mod.registerCompressionCodecs();
    expect(codecs[Snappy]).toBe(mockSnappyFn);
  });
});
