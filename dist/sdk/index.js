export * from './interceptor.js';
export * from './subscriber.js';
export * from './errors.js';
export * from '../shared/types.js';
import { AgentCheckpointInterceptor } from './interceptor.js';
import { AgentResumeListener } from './subscriber.js';
/**
 * Convenience factory to create a fully wired Agent SDK instance.
 */
export function createAgentSdk(clients) {
    const interceptor = new AgentCheckpointInterceptor(clients.commandClient, clients.options);
    const subscriber = clients.subscriberClient
        ? new AgentResumeListener(clients.subscriberClient, clients.options)
        : undefined;
    return {
        interceptor,
        subscriber,
    };
}
//# sourceMappingURL=index.js.map