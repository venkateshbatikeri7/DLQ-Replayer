/**
 * Error thrown when an agent tool fails and the SDK gracefully halts execution
 * after writing the failure event to the Dead-Letter Queue stream.
 */
export declare class AgentExecutionHaltedError extends Error {
    readonly executionId: string;
    readonly stepIndex: number;
    readonly toolName: string;
    readonly streamEventId?: string;
    readonly originalError: unknown;
    constructor(params: {
        message: string;
        executionId: string;
        stepIndex: number;
        toolName: string;
        streamEventId?: string;
        originalError: unknown;
    });
}
/**
 * Error thrown when Redis checkpoint persistence or deserialization fails.
 */
export declare class CheckpointStorageError extends Error {
    readonly cause?: unknown | undefined;
    constructor(message: string, cause?: unknown | undefined);
}
