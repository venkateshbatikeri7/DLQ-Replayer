import { REDIS_KEYS } from '../../shared/types.js';
/**
 * Fastify route plugin for inspecting checkpoint states and DLQ events.
 */
export const executionsRoutes = async (fastify, options) => {
    const { redisClient } = options;
    /**
     * GET /api/executions/:execution_id
     * Inspect all step checkpoints stored in Redis Hash for this execution.
     */
    fastify.get('/api/executions/:execution_id', async (request, reply) => {
        const { execution_id: executionId } = request.params;
        const hashKey = REDIS_KEYS.executionHash(executionId);
        const rawCheckpoints = await redisClient.hgetall(hashKey);
        const parsedCheckpoints = {};
        for (const [stepIndex, jsonVal] of Object.entries(rawCheckpoints)) {
            try {
                parsedCheckpoints[stepIndex] = JSON.parse(jsonVal);
            }
            catch {
                // Skip unparseable records
            }
        }
        const totalSteps = Object.keys(parsedCheckpoints).length;
        return reply.code(200).send({
            executionId,
            totalCompletedSteps: totalSteps,
            checkpoints: parsedCheckpoints,
        });
    });
    /**
     * DELETE /api/executions/:execution_id
     * Clears the checkpoint hash to allow a full fresh re-run from step 0.
     */
    fastify.delete('/api/executions/:execution_id', async (request, reply) => {
        const { execution_id: executionId } = request.params;
        const hashKey = REDIS_KEYS.executionHash(executionId);
        const deletedCount = await redisClient.del(hashKey);
        return reply.code(200).send({
            success: true,
            executionId,
            message: deletedCount > 0 ? 'Checkpoints cleared' : 'No checkpoints found to delete',
        });
    });
    /**
     * GET /api/events
     * Reads recent events from the DLQ Redis Stream (stream:agent_events).
     */
    fastify.get('/api/events', async (request, reply) => {
        const count = Number(request.query.count) || 20;
        // XREVRANGE reads the most recent stream messages
        const rawEntries = (await redisClient.xrevrange(REDIS_KEYS.STREAM_AGENT_EVENTS, '+', '-', 'COUNT', count));
        const events = rawEntries.map(([id, fields]) => {
            const parsed = { id };
            for (let i = 0; i < fields.length; i += 2) {
                parsed[fields[i]] = fields[i + 1];
            }
            return parsed;
        });
        return reply.code(200).send({
            count: events.length,
            events,
        });
    });
};
//# sourceMappingURL=executions.js.map