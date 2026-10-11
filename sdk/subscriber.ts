import type { Redis } from 'ioredis';
import { EventEmitter } from 'node:events';
import { REDIS_KEYS, type AgentCommandPayload } from '../shared/types.js';
import { logger as defaultLogger, type Logger } from '../shared/logger.js';

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
export class AgentResumeListener {
  private readonly subRedis: Redis;
  private readonly emitter: EventEmitter;
  private readonly logger: Logger;
  private isSubscribed = false;

  constructor(subscriberRedisClient: Redis, options?: ResumeListenerOptions) {
    this.subRedis = subscriberRedisClient;
    this.emitter = new EventEmitter();
    this.logger = options?.logger ?? defaultLogger;

    this.setupSubscription();
  }

  private setupSubscription(): void {
    this.subRedis.subscribe(REDIS_KEYS.CHANNEL_AGENT_COMMANDS, (err, count) => {
      if (err) {
        this.logger.error(
          { error: err.message },
          `[SDK:Subscriber] Failed to subscribe to channel: ${REDIS_KEYS.CHANNEL_AGENT_COMMANDS}`
        );
        return;
      }
      this.isSubscribed = true;
      this.logger.info(
        { channel: REDIS_KEYS.CHANNEL_AGENT_COMMANDS, activeSubscriptions: count },
        '[SDK:Subscriber] Successfully subscribed to agent command channel'
      );
    });

    this.subRedis.on('message', (channel, message) => {
      if (channel !== REDIS_KEYS.CHANNEL_AGENT_COMMANDS) return;

      try {
        const payload = JSON.parse(message) as AgentCommandPayload;
        this.logger.info(
          { command: payload.command, executionId: payload.executionId },
          `📥 [COMMAND RECEIVED] Incoming signal: ${payload.command} for execution: ${payload.executionId}`
        );

        // Emit general event and execution-specific event
        this.emitter.emit('command', payload);
        this.emitter.emit(`resume:${payload.executionId}`, payload);
      } catch (parseErr) {
        this.logger.warn(
          { rawMessage: message, err: parseErr },
          '[SDK:Subscriber] Received non-JSON or invalid command payload'
        );
      }
    });
  }

  /**
   * Registers a callback for whenever a resume command is published for a specific execution ID.
   */
  public onResume(executionId: string, callback: (payload: AgentCommandPayload) => void): () => void {
    const eventName = `resume:${executionId}`;
    this.emitter.on(eventName, callback);
    return () => this.emitter.off(eventName, callback);
  }

  /**
   * Returns a promise that resolves when a `COMMAND:RESUME` is received for the given executionId.
   */
  public async waitForResume(executionId: string, timeoutMs = 60000): Promise<AgentCommandPayload> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.emitter.off(`resume:${executionId}`, handler);
        reject(new Error(`Timed out after ${timeoutMs}ms waiting for resume signal for execution: ${executionId}`));
      }, timeoutMs);

      const handler = (payload: AgentCommandPayload) => {
        clearTimeout(timer);
        resolve(payload);
      };

      this.emitter.once(`resume:${executionId}`, handler);
    });
  }

  /**
   * Unsubscribes and cleans up resources.
   */
  public async close(): Promise<void> {
    this.emitter.removeAllListeners();
    if (this.isSubscribed) {
      await this.subRedis.unsubscribe(REDIS_KEYS.CHANNEL_AGENT_COMMANDS);
      this.isSubscribed = false;
    }
  }
}
