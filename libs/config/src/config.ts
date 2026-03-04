import { ConfigService } from '@nestjs/config';
import { registerAs } from '@nestjs/config';
import type { ConfigType } from '@nestjs/config';
import { env, envSchema, type Env } from './env.schema';

export { env, envSchema, type Env };

const ENV_NAMESPACE = 'ENV';

/** Register validated env under ConfigService.get('ENV'). Use with ConfigModule.forRoot({ load: [registerEnv] }). */
export const registerEnv = registerAs(ENV_NAMESPACE, (): Env => env);

export type EnvConfig = ConfigType<typeof registerEnv>;

/** Typed access to env from ConfigService (e.g. in main after app.get(ConfigService)). */
export function getEnvConfig(configService: ConfigService): EnvConfig {
  const value = configService.get<EnvConfig>(ENV_NAMESPACE);
  if (value) return value;
  return env;
}
