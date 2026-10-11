import type { Redis } from 'ioredis';
import { type ExecutionStepResult, type ExecuteStepOptions } from '../shared/types.js';
import { type Logger } from '../shared/logger.js';
export interface InterceptorOptions {
    /**
     * Optional default TTL in seconds for checkpoint hashes.
     * Helps prevent Redis unbounded memory growth in production.
     * Default: 86400 (24 hours)
     */
    defaultTtlSeconds?: number;
    /**
     * Custom logger instance. Defaults to shared Pino logger.
     */
    logger?: Logger;
}
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
export declare class AgentCheckpointInterceptor {
    private readonly redis;
    private readonly defaultTtlSeconds;
    private readonly logger;
    constructor(redisClient: Redis, options?: InterceptorOptions);
    /**
     * Core interceptor method. Executes a tool step or returns its cached result if already completed.
     */
    executeStep<TArgs = unknown, TResult = unknown>(options: ExecuteStepOptions<TArgs, TResult>): Promise<TResult>;
    /**
     * Functional middleware wrapper. Wraps an arbitrary asynchronous tool function
     * with automatic checkpointing and DLQ error trapping.
     */
    wrapTool<TArgs, TResult>(toolName: string, toolFn: (args: TArgs) => Promise<TResult>): (executionId: string, stepIndex: number, args: TArgs) => Promise<TResult>;
    /**
     * Retrieves all completed checkpoints for an execution. Useful for diagnostics.
     */
    getCheckpoints(executionId: string): Promise<Record<string, ExecutionStepResult>>;
    /**
     * Deletes all checkpoints for an execution, forcing an un-cached cold start.
     */
    clearCheckpoints(executionId: string): Promise<void>;
}
