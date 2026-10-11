import type { FastifyPluginAsync, FastifyRequest, FastifyReply } from 'fastify';
import type { Redis } from 'ioredis';
import { REDIS_KEYS, type AgentCommandPayload } from '../../shared/types.js';

interface ReplayParams {
  execution_id: string;
}

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
export const replayRoutes: FastifyPluginAsync<ReplayRouteOptions> = async (
  fastify,
  options
) => {
  const { redisClient } = options;

  fastify.post<{ Params: ReplayParams }>(
    '/api/replay/:execution_id',
    {
      schema: {
        params: {
          type: 'object',
          required: ['execution_id'],
          properties: {
            execution_id: {
              type: 'string',
              minLength: 1,
              description: 'The unique execution ID of the agent workflow to replay',
            },
          },
        },
        response: {
          200: {
            type: 'object',
            properties: {
              success: { type: 'boolean' },
              message: { type: 'string' },
              executionId: { type: 'string' },
              command: { type: 'string' },
              subscribersNotified: { type: 'number' },
              existingCheckpointsCount: { type: 'number' },
              timestamp: { type: 'string' },
            },
          },
        },
      },
    },
    async (request: FastifyRequest<{ Params: ReplayParams }>, reply: FastifyReply) => {
      const { execution_id: executionId } = request.params;

      // Check current checkpoints stored in Redis Hash
      const hashKey = REDIS_KEYS.executionHash(executionId);
      const existingFields = await redisClient.hkeys(hashKey);

      // Prepare command payload
      const commandPayload: AgentCommandPayload = {
        command: 'COMMAND:RESUME',
        executionId,
        timestamp: new Date().toISOString(),
        source: 'control-plane-api',
      };

      // Publish to Pub/Sub channel
      const subscribersCount = await redisClient.publish(
        REDIS_KEYS.CHANNEL_AGENT_COMMANDS,
        JSON.stringify(commandPayload)
      );

      request.log.info(
        {
          executionId,
          subscribersNotified: subscribersCount,
          checkpointsFound: existingFields.length,
        },
        `[Control Plane] Published COMMAND:RESUME to ${REDIS_KEYS.CHANNEL_AGENT_COMMANDS}`
      );

      return reply.code(200).send({
        success: true,
        message: 'Replay command successfully dispatched to agent orchestrator',
        executionId,
        command: commandPayload.command,
        subscribersNotified: subscribersCount,
        existingCheckpointsCount: existingFields.length,
        timestamp: commandPayload.timestamp,
      });
    }
  );
};
