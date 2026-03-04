/**
 * Environment variables expected by libs/kafka
 */
export type KafkaEnv = {
  /**
   * Comma-separated list of brokers
   * Example: "localhost:9092" or "b1:9092,b2:9092"
   */
  KAFKA_BROKERS: string;
};
