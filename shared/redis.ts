import { Redis, type RedisOptions } from 'ioredis';
import { logger } from './logger.js';

export type RedisClientRole = 'default' | 'subscriber' | 'blocking';

export interface RedisConnectionConfig {
  url?: string;
  host?: string;
  port?: number;
  password?: string;
  db?: number;
  options?: RedisOptions;
}

/**
 * Builds default connection options with production-grade retry strategy
 * and keepalive parameters.
 */
export function getDefaultRedisOptions(role: RedisClientRole = 'default'): RedisOptions {
  return {
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: Number(process.env.REDIS_PORT) || 6379,
    password: process.env.REDIS_PASSWORD || undefined,
    db: Number(process.env.REDIS_DB) || 0,
    connectionName: `agent-dlq:${role}:${process.pid}`,
    maxRetriesPerRequest: role === 'blocking' ? null : 20, // null is recommended for blocking commands
    enableReadyCheck: true,
    autoResubscribe: role === 'subscriber',
    keepAlive: 10000,
    retryStrategy(times: number) {
      // Exponential backoff capped at 3 seconds
      const delay = Math.min(times * 150, 3000);
      logger.warn({ role, times, delay }, `[Redis] Connection failed. Retrying in ${delay}ms...`);
      return delay;
    },
    reconnectOnError(err) {
      const targetErrors = ['READONLY', 'ECONNRESET', 'ETIMEDOUT'];
      const shouldReconnect = targetErrors.some((code) => err.message.includes(code));
      if (shouldReconnect) {
        logger.warn({ role, error: err.message }, '[Redis] Reconnecting due to targeted error condition');
        return true;
      }
      return false;
    },
  };
}

/**
 * Factory for creating managed ioredis clients.
 *
 * Distributed Systems Design Note:
 * Dedicated connections must be provisioned for:
 * 1. Blocking commands (e.g. XREADGROUP BLOCK): Holding the connection blocks multiplexing for other requests.
 * 2. Pub/Sub Subscribers: Once SUBSCRIBE is issued, the connection enters subscriber mode and rejects data commands.
 * 3. Default CRUD: High throughput general read/write pipeline (HSET, HGET, XADD).
 */
export function createRedisClient(
  role: RedisClientRole = 'default',
  overrideConfig?: RedisConnectionConfig
): Redis {
  const baseOptions = getDefaultRedisOptions(role);
  const redisUrl = overrideConfig?.url || process.env.REDIS_URL;

  let client: Redis;

  if (redisUrl) {
    client = new Redis(redisUrl, {
      ...baseOptions,
      ...overrideConfig?.options,
    });
  } else {
    client = new Redis({
      ...baseOptions,
      host: overrideConfig?.host || baseOptions.host,
      port: overrideConfig?.port || baseOptions.port,
      password: overrideConfig?.password || baseOptions.password,
      db: overrideConfig?.db ?? baseOptions.db,
      ...overrideConfig?.options,
    });
  }

  client.on('connect', () => {
    logger.info({ role }, `[Redis:${role}] Connected to Redis server`);
  });

  client.on('ready', () => {
    logger.info({ role }, `[Redis:${role}] Connection ready for commands`);
  });

  client.on('error', (err) => {
    logger.error({ role, err: err.message }, `[Redis:${role}] Client error encountered`);
  });

  client.on('close', () => {
    logger.warn({ role }, `[Redis:${role}] Connection closed`);
  });

  client.on('reconnecting', () => {
    logger.info({ role }, `[Redis:${role}] Reconnecting...`);
  });

  return client;
}
