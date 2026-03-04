import { envSchema } from '@app/config';

describe('envSchema', () => {
  const originalEnv = process.env;

  afterEach(() => {
    process.env = originalEnv;
  });

  it('should parse with defaults when env is empty', () => {
    process.env = {};
    const result = envSchema.parse(process.env);
    expect(result.KAFKA_BROKERS).toBe('localhost:9092');
    expect(result.OPENSEARCH_NODE).toBe('http://localhost:9200');
    expect(result.PORT).toBe(3000);
  });

  it('should use provided env values', () => {
    process.env = {
      KAFKA_BROKERS: 'kafka:19092',
      OPENSEARCH_NODE: 'http://opensearch:9200',
      PORT: '3001',
    };
    const result = envSchema.parse(process.env);
    expect(result.KAFKA_BROKERS).toBe('kafka:19092');
    expect(result.OPENSEARCH_NODE).toBe('http://opensearch:9200');
    expect(result.PORT).toBe(3001);
  });
});
