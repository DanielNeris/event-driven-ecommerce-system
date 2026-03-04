import { ConfigService } from '@nestjs/config';
import { getEnvConfig, registerEnv } from '@app/config';

describe('config', () => {
  describe('getEnvConfig', () => {
    it('should return value from ConfigService when ENV is set', () => {
      const env = {
        KAFKA_BROKERS: 'kafka:19092',
        OPENSEARCH_NODE: 'http://opensearch:9200',
        PORT: 3001,
      };
      const get = jest.fn().mockReturnValue(env);
      const configService = { get } as unknown as ConfigService;
      expect(getEnvConfig(configService)).toBe(env);
      expect(get).toHaveBeenCalledWith('ENV');
    });

    it('should return env when ConfigService has no ENV', () => {
      const get = jest.fn().mockReturnValue(undefined);
      const configService = { get } as unknown as ConfigService;
      const result = getEnvConfig(configService);
      expect(result).toHaveProperty('KAFKA_BROKERS');
      expect(result).toHaveProperty('OPENSEARCH_NODE');
      expect(result).toHaveProperty('PORT');
    });
  });

  describe('registerEnv', () => {
    it('should return env with KAFKA_BROKERS, OPENSEARCH_NODE, PORT from factory', () => {
      const value = registerEnv();
      expect(value).toHaveProperty('KAFKA_BROKERS');
      expect(value).toHaveProperty('OPENSEARCH_NODE');
      expect(value).toHaveProperty('PORT');
    });
  });
});
