import { Redis, type RedisOptions } from 'ioredis';
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
export declare function getDefaultRedisOptions(role?: RedisClientRole): RedisOptions;
/**
 * Factory for creating managed ioredis clients.
 *
 * Distributed Systems Design Note:
 * Dedicated connections must be provisioned for:
 * 1. Blocking commands (e.g. XREADGROUP BLOCK): Holding the connection blocks multiplexing for other requests.
 * 2. Pub/Sub Subscribers: Once SUBSCRIBE is issued, the connection enters subscriber mode and rejects data commands.
 * 3. Default CRUD: High throughput general read/write pipeline (HSET, HGET, XADD).
 */
export declare function createRedisClient(role?: RedisClientRole, overrideConfig?: RedisConnectionConfig): Redis;
