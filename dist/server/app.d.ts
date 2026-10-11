import { type FastifyInstance } from 'fastify';
import type { Redis } from 'ioredis';
export interface ServerAppOptions {
    redisClient: Redis;
}
/**
 * Builds and configures the Fastify Control Plane application.
 */
export declare function buildServer(options: ServerAppOptions): Promise<FastifyInstance>;
