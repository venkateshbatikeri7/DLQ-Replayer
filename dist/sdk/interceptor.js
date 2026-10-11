import { REDIS_KEYS, } from '../shared/types.js';
import { AgentExecutionHaltedError, CheckpointStorageError } from './errors.js';
import { logger as defaultLogger } from '../shared/logger.js';
/**
 * AgentCheckpointInterceptor
 *
 * Intercepts AI Agent tool executions with deterministic checkpointing and DLQ integration:
 * 1. Checks Redis Hash (`hash:execution:{execution_id}`) for existing step checkpoint.
 * 2. On REPLAY CACHE HIT: Bypasses tool execution and returns stored result instantly (<1ms).
 * 3. On FRESH EXECUTION: Executes tool function.
 *    - On SUCCESS: Persists step result via `HSET` and advances workflow.
 *    - On FAILURE: Catches error, publishes `STATUS:FAILED` payload with stack trace
 *      to Redis Stream (`stream:agent_events`) via `XADD`, and throws AgentExecutionHaltedError.
 */
export class AgentCheckpointInterceptor {
    redis;
    defaultTtlSeconds;
    logger;
    constructor(redisClient, options) {
        this.redis = redisClient;
        this.defaultTtlSeconds = options?.defaultTtlSeconds ?? 86400; // 24 hours
        this.logger = options?.logger ?? defaultLogger;
    }
    /**
     * Core interceptor method. Executes a tool step or returns its cached result if already completed.
     */
    async executeStep(options) {
        const { executionId, stepIndex, toolName, args, execute } = options;
        const hashKey = REDIS_KEYS.executionHash(executionId);
        const stepField = String(stepIndex);
        // =========================================================================
        // STEP 1: Check Checkpoint Cache (Redis Hash)
        // =========================================================================
        let cachedPayload = null;
        try {
            cachedPayload = await this.redis.hget(hashKey, stepField);
        }
        catch (redisErr) {
            this.logger.error({ executionId, stepIndex, toolName, error: redisErr }, '[SDK] Failed to query checkpoint hash in Redis. Aborting step to prevent inconsistent state.');
            throw new CheckpointStorageError(`Failed to read checkpoint for execution "${executionId}" at step ${stepIndex}`, redisErr);
        }
        if (cachedPayload !== null) {
            try {
                const parsed = JSON.parse(cachedPayload);
                this.logger.info({
                    executionId,
                    stepIndex,
                    toolName,
                    persistedAt: parsed.timestamp,
                    durationMs: parsed.durationMs,
                }, `⏩ [REPLAY CACHE HIT] Bypassing tool "${toolName}" at step ${stepIndex} (Result served from Redis)`);
                return parsed.result;
            }
            catch (parseErr) {
                this.logger.warn({ executionId, stepIndex, toolName, err: parseErr }, '[SDK] Corrupted checkpoint JSON encountered in Redis Hash. Re-executing tool step.');
            }
        }
        // =========================================================================
        // STEP 2: Execute Fresh Tool Call
        // =========================================================================
        this.logger.info({ executionId, stepIndex, toolName }, `⚡ [FRESH EXECUTION] Running tool "${toolName}" at step ${stepIndex}...`);
        const startTime = performance.now();
        try {
            const result = await execute(args);
            const durationMs = Math.round(performance.now() - startTime);
            // =======================================================================
            // STEP 3: On SUCCESS -> Atomically Persist to Redis Hash
            // =======================================================================
            const stepRecord = {
                stepIndex,
                toolName,
                result,
                status: 'SUCCESS',
                timestamp: new Date().toISOString(),
                durationMs,
            };
            const serializedRecord = JSON.stringify(stepRecord);
            const ttl = options.hashTtlSeconds ?? this.defaultTtlSeconds;
            // Pipeline HSET and EXPIRE for atomicity and memory safety
            const pipeline = this.redis.pipeline();
            pipeline.hset(hashKey, stepField, serializedRecord);
            if (ttl > 0) {
                pipeline.expire(hashKey, ttl);
            }
            await pipeline.exec();
            this.logger.info({ executionId, stepIndex, toolName, durationMs }, `✅ [CHECKPOINT SAVED] Tool "${toolName}" completed successfully in ${durationMs}ms.`);
            return result;
        }
        catch (toolError) {
            // =======================================================================
            // STEP 4: On FAILURE -> Push to Dead-Letter Queue (Redis Stream) via XADD
            // =======================================================================
            const err = toolError instanceof Error ? toolError : new Error(String(toolError));
            const timestamp = new Date().toISOString();
            const dlqPayload = {
                status: 'STATUS:FAILED',
                executionId,
                stepIndex: String(stepIndex),
                toolName,
                errorMessage: err.message,
                errorTrace: err.stack || err.message,
                timestamp,
                metadata: args !== undefined ? JSON.stringify(args) : undefined,
            };
            let streamEventId;
            try {
                // Flatten payload for Redis Stream XADD: [key1, val1, key2, val2, ...]
                const streamEntries = [];
                for (const [key, value] of Object.entries(dlqPayload)) {
                    if (value !== undefined) {
                        streamEntries.push(key, String(value));
                    }
                }
                const xaddResult = await this.redis.xadd(REDIS_KEYS.STREAM_AGENT_EVENTS, '*', // Auto-generate Redis Stream entry ID (timestamp-sequence)
                ...streamEntries);
                streamEventId = xaddResult ?? undefined;
                this.logger.error({
                    executionId,
                    stepIndex,
                    toolName,
                    streamEventId,
                    errorMessage: err.message,
                }, `🚨 [DLQ DISPATCH] Tool failure captured! Dispatched to "${REDIS_KEYS.STREAM_AGENT_EVENTS}". Gracefully halting agent workflow.`);
            }
            catch (streamErr) {
                this.logger.fatal({ executionId, stepIndex, toolName, streamErr, originalError: err.message }, '[SDK] Failed to dispatch failure to Redis Stream DLQ!');
            }
            // Gracefully halt the agent by throwing an explicit, typed halting error
            throw new AgentExecutionHaltedError({
                message: `Agent execution "${executionId}" halted at step ${stepIndex} due to failure in tool "${toolName}": ${err.message}`,
                executionId,
                stepIndex,
                toolName,
                streamEventId,
                originalError: toolError,
            });
        }
    }
    /**
     * Functional middleware wrapper. Wraps an arbitrary asynchronous tool function
     * with automatic checkpointing and DLQ error trapping.
     */
    wrapTool(toolName, toolFn) {
        return (executionId, stepIndex, args) => {
            return this.executeStep({
                executionId,
                stepIndex,
                toolName,
                args,
                execute: (a) => toolFn(a),
            });
        };
    }
    /**
     * Retrieves all completed checkpoints for an execution. Useful for diagnostics.
     */
    async getCheckpoints(executionId) {
        const hashKey = REDIS_KEYS.executionHash(executionId);
        const data = await this.redis.hgetall(hashKey);
        const result = {};
        for (const [field, raw] of Object.entries(data)) {
            try {
                result[field] = JSON.parse(raw);
            }
            catch {
                // Skip unparseable records
            }
        }
        return result;
    }
    /**
     * Deletes all checkpoints for an execution, forcing an un-cached cold start.
     */
    async clearCheckpoints(executionId) {
        const hashKey = REDIS_KEYS.executionHash(executionId);
        await this.redis.del(hashKey);
        this.logger.info({ executionId }, '[SDK] Cleared checkpoints for execution');
    }
}
//# sourceMappingURL=interceptor.js.map