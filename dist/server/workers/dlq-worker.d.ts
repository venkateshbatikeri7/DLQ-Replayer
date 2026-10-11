import type { Redis } from 'ioredis';
import { type Logger } from '../../shared/logger.js';
export interface DlqWorkerOptions {
    groupName?: string;
    consumerName?: string;
    blockTimeoutMs?: number;
    batchSize?: number;
    logger?: Logger;
}
/**
 * Dead-Letter Queue (DLQ) Background Stream Consumer Worker.
 *
 * Utilizes Redis Streams `XREADGROUP` to continuously pull failure events
 * from `stream:agent_events`.
 *
 * Distributed Systems Design Features:
 * - Idempotent Consumer Group initialization (`MKSTREAM` + `BUSYGROUP` handling).
 * - Consumer ACK (`XACK`) protocol ensures at-least-once delivery semantics.
 * - Non-blocking event loop friendly: uses cooperative sleeping and graceful shutdown flags.
 * - Dedicated blocking connection isolation so web server traffic is not blocked.
 */
export declare class DlqWorker {
    private readonly redis;
    private readonly groupName;
    private readonly consumerName;
    private readonly blockTimeoutMs;
    private readonly batchSize;
    private readonly logger;
    private isRunning;
    private workerPromise;
    constructor(blockingRedisClient: Redis, options?: DlqWorkerOptions);
    /**
     * Initializes the Redis Stream consumer group idempotently.
     */
    initializeGroup(): Promise<void>;
    /**
     * Starts the continuous background consumption loop.
     */
    start(): Promise<void>;
    /**
     * Continuous stream consumption loop.
     */
    private consumeLoop;
    /**
     * Parses and processes an individual stream message, then issues an XACK.
     */
    private processMessage;
    /**
     * Gracefully stops the worker loop.
     */
    stop(): Promise<void>;
}
