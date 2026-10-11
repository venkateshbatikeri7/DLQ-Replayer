import type { FastifyPluginAsync } from 'fastify';
import type { Redis } from 'ioredis';
export interface ReplayRouteOptions {
    redisClient: Redis;
}
/**
 * Fastify route plugin for the Replay Control Plane.
 *
 * Exposes:
 * - POST /api/replay/:execution_id
 * Publishes a COMMAND:RESUME message to the Redis Pub/Sub channel `channel:agent_commands`.
 */
export declare const replayRoutes: FastifyPluginAsync<ReplayRouteOptions>;
