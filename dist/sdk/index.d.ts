export * from './interceptor.js';
export * from './subscriber.js';
export * from './errors.js';
export * from '../shared/types.js';
import type { Redis } from 'ioredis';
import { AgentCheckpointInterceptor, type InterceptorOptions } from './interceptor.js';
import { AgentResumeListener, type ResumeListenerOptions } from './subscriber.js';
/**
 * Convenience factory to create a fully wired Agent SDK instance.
 */
export declare function createAgentSdk(clients: {
    commandClient: Redis;
    subscriberClient?: Redis;
    options?: InterceptorOptions & ResumeListenerOptions;
}): {
    interceptor: AgentCheckpointInterceptor;
    subscriber: AgentResumeListener | undefined;
};
