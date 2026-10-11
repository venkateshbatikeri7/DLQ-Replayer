/**
 * Error thrown when an agent tool fails and the SDK gracefully halts execution
 * after writing the failure event to the Dead-Letter Queue stream.
 */
export class AgentExecutionHaltedError extends Error {
  public readonly executionId: string;
  public readonly stepIndex: number;
  public readonly toolName: string;
  public readonly streamEventId?: string;
  public readonly originalError: unknown;

  constructor(params: {
    message: string;
    executionId: string;
    stepIndex: number;
    toolName: string;
    streamEventId?: string;
    originalError: unknown;
  }) {
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
  constructor(message: string, public readonly cause?: unknown) {
    super(message);
    this.name = 'CheckpointStorageError';
  }
}
