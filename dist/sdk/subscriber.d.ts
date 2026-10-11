import type { Redis } from 'ioredis';
import { type AgentCommandPayload } from '../shared/types.js';
import { type Logger } from '../shared/logger.js';
export interface ResumeListenerOptions {
    logger?: Logger;
}
/**
 * AgentResumeListener
 *
 * Subscribes to the Redis Pub/Sub channel (`channel:agent_commands`) to receive
 * real-time orchestration signals (such as `COMMAND:RESUME`) dispatched from
 * the Fastify Control Plane.
 */
export declare class AgentResumeListener {
    private readonly subRedis;
    private readonly emitter;
    private readonly logger;
    private isSubscribed;
    constructor(subscriberRedisClient: Redis, options?: ResumeListenerOptions);
    private setupSubscription;
    /**
     * Registers a callback for whenever a resume command is published for a specific execution ID.
     */
    onResume(executionId: string, callback: (payload: AgentCommandPayload) => void): () => void;
    /**
     * Returns a promise that resolves when a `COMMAND:RESUME` is received for the given executionId.
     */
    waitForResume(executionId: string, timeoutMs?: number): Promise<AgentCommandPayload>;
    /**
     * Unsubscribes and cleans up resources.
     */
    close(): Promise<void>;
}
