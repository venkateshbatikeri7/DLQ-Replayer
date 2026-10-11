import type { FastifyPluginAsync, FastifyRequest, FastifyReply } from 'fastify';
import type { Redis } from 'ioredis';
import { REDIS_KEYS, type ExecutionStepResult } from '../../shared/types.js';

interface ExecutionParams {
  execution_id: string;
}

interface EventQueryParams {
  count?: number;
}

export interface ExecutionsRouteOptions {
  redisClient: Redis;
}

/**
 * Fastify route plugin for inspecting checkpoint states and DLQ events.
 */
export const executionsRoutes: FastifyPluginAsync<ExecutionsRouteOptions> = async (
  fastify,
  options
) => {
  const { redisClient } = options;

  /**
   * GET /api/executions/:execution_id
   * Inspect all step checkpoints stored in Redis Hash for this execution.
   */
  fastify.get<{ Params: ExecutionParams }>(
    '/api/executions/:execution_id',
    async (request: FastifyRequest<{ Params: ExecutionParams }>, reply: FastifyReply) => {
      const { execution_id: executionId } = request.params;
      const hashKey = REDIS_KEYS.executionHash(executionId);

      const rawCheckpoints = await redisClient.hgetall(hashKey);
      const parsedCheckpoints: Record<string, ExecutionStepResult> = {};

      for (const [stepIndex, jsonVal] of Object.entries(rawCheckpoints)) {
        try {
          parsedCheckpoints[stepIndex] = JSON.parse(jsonVal);
        } catch {
          // Skip unparseable records
        }
      }

      const totalSteps = Object.keys(parsedCheckpoints).length;

      return reply.code(200).send({
        executionId,
        totalCompletedSteps: totalSteps,
        checkpoints: parsedCheckpoints,
      });
    }
  );

  /**
   * DELETE /api/executions/:execution_id
   * Clears the checkpoint hash to allow a full fresh re-run from step 0.
   */
  fastify.delete<{ Params: ExecutionParams }>(
    '/api/executions/:execution_id',
    async (request: FastifyRequest<{ Params: ExecutionParams }>, reply: FastifyReply) => {
      const { execution_id: executionId } = request.params;
      const hashKey = REDIS_KEYS.executionHash(executionId);

      const deletedCount = await redisClient.del(hashKey);

      return reply.code(200).send({
        success: true,
        executionId,
        message: deletedCount > 0 ? 'Checkpoints cleared' : 'No checkpoints found to delete',
      });
    }
  );

  /**
   * GET /api/events
   * Reads recent events from the DLQ Redis Stream (stream:agent_events).
   */
  fastify.get<{ Querystring: EventQueryParams }>(
    '/api/events',
    async (request: FastifyRequest<{ Querystring: EventQueryParams }>, reply: FastifyReply) => {
      const count = Number(request.query.count) || 20;

      // XREVRANGE reads the most recent stream messages
      const rawEntries = (await redisClient.xrevrange(
        REDIS_KEYS.STREAM_AGENT_EVENTS,
        '+',
        '-',
        'COUNT',
        count
      )) as Array<[string, string[]]>;

      const events = rawEntries.map(([id, fields]) => {
        const parsed: Record<string, string> = { id };
        for (let i = 0; i < fields.length; i += 2) {
          parsed[fields[i]] = fields[i + 1];
        }
        return parsed;
      });

      return reply.code(200).send({
        count: events.length,
        events,
      });
    }
  );
};
