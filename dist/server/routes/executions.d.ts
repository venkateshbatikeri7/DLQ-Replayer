import type { FastifyPluginAsync } from 'fastify';
import type { Redis } from 'ioredis';
export interface ExecutionsRouteOptions {
    redisClient: Redis;
}
/**
 * Fastify route plugin for inspecting checkpoint states and DLQ events.
 */
export declare const executionsRoutes: FastifyPluginAsync<ExecutionsRouteOptions>;
