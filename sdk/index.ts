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
export function createAgentSdk(clients: {
  commandClient: Redis;
  subscriberClient?: Redis;
  options?: InterceptorOptions & ResumeListenerOptions;
}) {
  const interceptor = new AgentCheckpointInterceptor(clients.commandClient, clients.options);
  const subscriber = clients.subscriberClient
    ? new AgentResumeListener(clients.subscriberClient, clients.options)
    : undefined;

  return {
    interceptor,
    subscriber,
  };
}
