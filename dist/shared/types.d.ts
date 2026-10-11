/**
 * Core type definitions and Redis key conventions for the AI Agent Checkpoint Replayer
 * and Dead-Letter Queue (DLQ) distributed architecture.
 */
export declare const REDIS_KEYS: {
    /**
     * Redis Hash storing completed step checkpoints for an execution.
     * Format: hash:execution:{execution_id}
     * Fields: step index (e.g. "0", "1", "2") -> Serialized ExecutionStepResult
     */
    readonly executionHash: (executionId: string) => string;
    /**
     * Redis Stream for agent event telemetry and failure notifications (Dead-Letter Queue).
     */
    readonly STREAM_AGENT_EVENTS: "stream:agent_events";
    /**
     * Redis Pub/Sub channel for control plane commands (e.g. COMMAND:RESUME).
     */
    readonly CHANNEL_AGENT_COMMANDS: "channel:agent_commands";
    /**
     * Default consumer group name for DLQ stream workers.
     */
    readonly CONSUMER_GROUP_DLQ: "dlq-consumers";
};
/**
 * Result record persisted in Redis Hash on successful tool execution.
 */
export interface ExecutionStepResult<T = unknown> {
    stepIndex: number;
    toolName: string;
    result: T;
    status: 'SUCCESS';
    cached?: boolean;
    timestamp: string;
    durationMs: number;
}
/**
 * Dead-Letter Queue / Audit Stream event payload written via XADD.
 */
export interface AgentEventPayload {
    status: 'STATUS:FAILED' | 'STATUS:SUCCESS' | 'STATUS:STARTED';
    executionId: string;
    stepIndex: string;
    toolName: string;
    errorMessage: string;
    errorTrace: string;
    timestamp: string;
    metadata?: string;
}
/**
 * Command payload published to Redis Pub/Sub for workflow orchestration.
 */
export interface AgentCommandPayload {
    command: 'COMMAND:RESUME' | 'COMMAND:ABORT';
    executionId: string;
    timestamp: string;
    source?: string;
}
/**
 * Parameters passed to the step execution interceptor.
 */
export interface ExecuteStepOptions<TArgs = any, TResult = unknown> {
    executionId: string;
    stepIndex: number;
    toolName: string;
    args?: TArgs;
    execute: (args: TArgs) => Promise<TResult>;
    /**
     * Optional TTL in seconds for the execution hash key.
     * Recommended to avoid unbounded memory growth in Redis.
     */
    hashTtlSeconds?: number;
}
