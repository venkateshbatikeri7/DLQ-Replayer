/**
 * Error thrown when an agent tool fails and the SDK gracefully halts execution
 * after writing the failure event to the Dead-Letter Queue stream.
 */
export class AgentExecutionHaltedError extends Error {
    executionId;
    stepIndex;
    toolName;
    streamEventId;
    originalError;
    constructor(params) {
        super(params.message);
        this.name = 'AgentExecutionHaltedError';
        this.executionId = params.executionId;
        this.stepIndex = params.stepIndex;
        this.toolName = params.toolName;
        this.streamEventId = params.streamEventId;
        this.originalError = params.originalError;
        // Preserve stack trace of original error if available
        if (params.originalError instanceof Error && params.originalError.stack) {
            this.stack = `${this.stack}\nCaused by: ${params.originalError.stack}`;
        }
    }
}
/**
 * Error thrown when Redis checkpoint persistence or deserialization fails.
 */
export class CheckpointStorageError extends Error {
    cause;
    constructor(message, cause) {
        super(message);
        this.cause = cause;
        this.name = 'CheckpointStorageError';
    }
}
//# sourceMappingURL=errors.js.map