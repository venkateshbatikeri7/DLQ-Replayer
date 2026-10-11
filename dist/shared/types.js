/**
 * Core type definitions and Redis key conventions for the AI Agent Checkpoint Replayer
 * and Dead-Letter Queue (DLQ) distributed architecture.
 */
export const REDIS_KEYS = {
    /**
     * Redis Hash storing completed step checkpoints for an execution.
     * Format: hash:execution:{execution_id}
     * Fields: step index (e.g. "0", "1", "2") -> Serialized ExecutionStepResult
     */
    executionHash: (executionId) => `hash:execution:${executionId}`,
    /**
     * Redis Stream for agent event telemetry and failure notifications (Dead-Letter Queue).
     */
    STREAM_AGENT_EVENTS: 'stream:agent_events',
    /**
     * Redis Pub/Sub channel for control plane commands (e.g. COMMAND:RESUME).
     */
    CHANNEL_AGENT_COMMANDS: 'channel:agent_commands',
    /**
     * Default consumer group name for DLQ stream workers.
     */
    CONSUMER_GROUP_DLQ: 'dlq-consumers',
};
//# sourceMappingURL=types.js.map