import type { Redis } from 'ioredis';
import { REDIS_KEYS, type AgentEventPayload } from '../../shared/types.js';
import { logger as defaultLogger, type Logger } from '../../shared/logger.js';

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
export class DlqWorker {
  private readonly redis: Redis;
  private readonly groupName: string;
  private readonly consumerName: string;
  private readonly blockTimeoutMs: number;
  private readonly batchSize: number;
  private readonly logger: Logger;
  private isRunning = false;
  private workerPromise: Promise<void> | null = null;

  constructor(blockingRedisClient: Redis, options?: DlqWorkerOptions) {
    this.redis = blockingRedisClient;
    this.groupName = options?.groupName ?? REDIS_KEYS.CONSUMER_GROUP_DLQ;
    this.consumerName = options?.consumerName ?? `dlq-consumer-${process.pid}-${Math.random().toString(36).substring(7)}`;
    this.blockTimeoutMs = options?.blockTimeoutMs ?? 2000;
    this.batchSize = options?.batchSize ?? 10;
    this.logger = options?.logger ?? defaultLogger;
  }

  /**
   * Initializes the Redis Stream consumer group idempotently.
   */
  public async initializeGroup(): Promise<void> {
    try {
      // MKSTREAM creates the stream if it does not already exist
      // '$' tells consumer group to only read new messages from this point forward
      await this.redis.xgroup(
        'CREATE',
        REDIS_KEYS.STREAM_AGENT_EVENTS,
        this.groupName,
        '$',
        'MKSTREAM'
      );
      this.logger.info(
        { stream: REDIS_KEYS.STREAM_AGENT_EVENTS, group: this.groupName },
        '[DLQ Worker] Created consumer group for stream'
      );
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      if (message.includes('BUSYGROUP')) {
        this.logger.debug(
          { stream: REDIS_KEYS.STREAM_AGENT_EVENTS, group: this.groupName },
          '[DLQ Worker] Consumer group already exists. Reusing existing group.'
        );
      } else {
        this.logger.error(
          { stream: REDIS_KEYS.STREAM_AGENT_EVENTS, error: message },
          '[DLQ Worker] Unexpected error initializing consumer group'
        );
        throw err;
      }
    }
  }

  /**
   * Starts the continuous background consumption loop.
   */
  public async start(): Promise<void> {
    if (this.isRunning) return;

    await this.initializeGroup();
    this.isRunning = true;
    this.logger.info(
      { group: this.groupName, consumer: this.consumerName },
      '🚀 [DLQ Worker] Background consumer worker started. Listening for failure events...'
    );

    this.workerPromise = this.consumeLoop();
  }

  /**
   * Continuous stream consumption loop.
   */
  private async consumeLoop(): Promise<void> {
    while (this.isRunning) {
      try {
        // Read new messages with '>' for this consumer group
        // Returns [ [ streamKey, [ [ messageId, [ field1, val1, field2, val2, ... ] ], ... ] ] ]
        const response = (await this.redis.xreadgroup(
          'GROUP',
          this.groupName,
          this.consumerName,
          'COUNT',
          this.batchSize,
          'BLOCK',
          this.blockTimeoutMs,
          'STREAMS',
          REDIS_KEYS.STREAM_AGENT_EVENTS,
          '>'
        )) as Array<[string, Array<[string, string[]]>]> | null;

        if (!response || response.length === 0) {
          // Block timeout elapsed without new messages; continue next iteration
          continue;
        }

        for (const [_streamKey, messages] of response) {
          for (const [messageId, fieldArray] of messages) {
            await this.processMessage(messageId, fieldArray);
          }
        }
      } catch (err: unknown) {
        if (!this.isRunning) break;

        const errorMsg = err instanceof Error ? err.message : String(err);
        this.logger.error({ error: errorMsg }, '[DLQ Worker] Error reading from stream. Backing off 2s...');
        // Pause briefly before retrying to prevent busy loop during Redis reconnects
        await new Promise((resolve) => setTimeout(resolve, 2000));
      }
    }
  }

  /**
   * Parses and processes an individual stream message, then issues an XACK.
   */
  private async processMessage(messageId: string, rawFields: string[]): Promise<void> {
    try {
      const payload: Partial<AgentEventPayload> = {};
      for (let i = 0; i < rawFields.length; i += 2) {
        const key = rawFields[i] as keyof AgentEventPayload;
        const value = rawFields[i + 1];
        if (key && value !== undefined) {
          payload[key] = value as any;
        }
      }

      if (payload.status === 'STATUS:FAILED') {
        // Prominent visual dead-letter queue alert banner for terminal operator
        console.error('\n' + '='.repeat(78));
        console.error(`🚨 [DLQ ALERT] AGENT TOOL EXECUTION FAILURE CAPTURED`);
        console.error(`   Stream ID    : ${messageId}`);
        console.error(`   Execution ID : ${payload.executionId}`);
        console.error(`   Step Index   : ${payload.stepIndex}`);
        console.error(`   Tool Name    : ${payload.toolName}`);
        console.error(`   Timestamp    : ${payload.timestamp}`);
        console.error(`   Error Message: ${payload.errorMessage}`);
        if (payload.metadata) {
          console.error(`   Tool Input   : ${payload.metadata}`);
        }
        console.error(`   Trace        :\n${payload.errorTrace}`);
        console.error('='.repeat(78) + '\n');

        this.logger.warn(
          {
            streamId: messageId,
            executionId: payload.executionId,
            stepIndex: payload.stepIndex,
            tool: payload.toolName,
            err: payload.errorMessage,
          },
          '[DLQ Worker] Logged agent failure event'
        );
      } else {
        this.logger.info(
          { streamId: messageId, status: payload.status, executionId: payload.executionId },
          '[DLQ Worker] Processed general agent event'
        );
      }

      // Acknowledge processing completion to remove from Pending Entries List (PEL)
      await this.redis.xack(REDIS_KEYS.STREAM_AGENT_EVENTS, this.groupName, messageId);
    } catch (processErr) {
      this.logger.error(
        { messageId, error: processErr },
        '[DLQ Worker] Failed to process stream message. Leaving unacknowledged for retry.'
      );
    }
  }

  /**
   * Gracefully stops the worker loop.
   */
  public async stop(): Promise<void> {
    this.logger.info('[DLQ Worker] Stopping stream consumer worker...');
    this.isRunning = false;
    if (this.workerPromise) {
      await this.workerPromise;
      this.workerPromise = null;
    }
    this.logger.info('[DLQ Worker] Stream consumer worker cleanly stopped.');
  }
}
