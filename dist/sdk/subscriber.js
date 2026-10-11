import { EventEmitter } from 'node:events';
import { REDIS_KEYS } from '../shared/types.js';
import { logger as defaultLogger } from '../shared/logger.js';
/**
 * AgentResumeListener
 *
 * Subscribes to the Redis Pub/Sub channel (`channel:agent_commands`) to receive
 * real-time orchestration signals (such as `COMMAND:RESUME`) dispatched from
 * the Fastify Control Plane.
 */
export class AgentResumeListener {
    subRedis;
    emitter;
    logger;
    isSubscribed = false;
    constructor(subscriberRedisClient, options) {
        this.subRedis = subscriberRedisClient;
        this.emitter = new EventEmitter();
        this.logger = options?.logger ?? defaultLogger;
        this.setupSubscription();
    }
    setupSubscription() {
        this.subRedis.subscribe(REDIS_KEYS.CHANNEL_AGENT_COMMANDS, (err, count) => {
            if (err) {
                this.logger.error({ error: err.message }, `[SDK:Subscriber] Failed to subscribe to channel: ${REDIS_KEYS.CHANNEL_AGENT_COMMANDS}`);
                return;
            }
            this.isSubscribed = true;
            this.logger.info({ channel: REDIS_KEYS.CHANNEL_AGENT_COMMANDS, activeSubscriptions: count }, '[SDK:Subscriber] Successfully subscribed to agent command channel');
        });
        this.subRedis.on('message', (channel, message) => {
            if (channel !== REDIS_KEYS.CHANNEL_AGENT_COMMANDS)
                return;
            try {
                const payload = JSON.parse(message);
                this.logger.info({ command: payload.command, executionId: payload.executionId }, `📥 [COMMAND RECEIVED] Incoming signal: ${payload.command} for execution: ${payload.executionId}`);
                // Emit general event and execution-specific event
                this.emitter.emit('command', payload);
                this.emitter.emit(`resume:${payload.executionId}`, payload);
            }
            catch (parseErr) {
                this.logger.warn({ rawMessage: message, err: parseErr }, '[SDK:Subscriber] Received non-JSON or invalid command payload');
            }
        });
    }
    /**
     * Registers a callback for whenever a resume command is published for a specific execution ID.
     */
    onResume(executionId, callback) {
        const eventName = `resume:${executionId}`;
        this.emitter.on(eventName, callback);
        return () => this.emitter.off(eventName, callback);
    }
    /**
     * Returns a promise that resolves when a `COMMAND:RESUME` is received for the given executionId.
     */
    async waitForResume(executionId, timeoutMs = 60000) {
        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                this.emitter.off(`resume:${executionId}`, handler);
                reject(new Error(`Timed out after ${timeoutMs}ms waiting for resume signal for execution: ${executionId}`));
            }, timeoutMs);
            const handler = (payload) => {
                clearTimeout(timer);
                resolve(payload);
            };
            this.emitter.once(`resume:${executionId}`, handler);
        });
    }
    /**
     * Unsubscribes and cleans up resources.
     */
    async close() {
        this.emitter.removeAllListeners();
        if (this.isSubscribed) {
            await this.subRedis.unsubscribe(REDIS_KEYS.CHANNEL_AGENT_COMMANDS);
            this.isSubscribed = false;
        }
    }
}
//# sourceMappingURL=subscriber.js.map