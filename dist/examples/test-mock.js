import MockRedis from 'ioredis-mock';
import { AgentCheckpointInterceptor } from '../sdk/interceptor.js';
import { AgentExecutionHaltedError } from '../sdk/errors.js';
import { REDIS_KEYS } from '../shared/types.js';
async function runMockTestSuite() {
    console.log('\n======================================================');
    console.log('🧪 RUNNING AI AGENT REPLAY & DLQ TEST SUITE (MOCK REDIS)');
    console.log('======================================================\n');
    // Create mock Redis client (typed as Redis)
    const mockRedis = new MockRedis();
    const interceptor = new AgentCheckpointInterceptor(mockRedis);
    const executionId = `exec-test-${Date.now()}`;
    let step0ExecutionCount = 0;
    let step1ExecutionCount = 0;
    let step2ExecutionCount = 0;
    let paymentToolShouldFail = true;
    // Step 0: Search user
    const step0Tool = async (input) => {
        step0ExecutionCount++;
        return { userId: input.userId, name: 'Alice Smith', tier: 'premium' };
    };
    // Step 1: Calculate invoice
    const step1Tool = async (input) => {
        step1ExecutionCount++;
        return { total: input.amount * 1.18, currency: 'USD' };
    };
    // Step 2: External payment tool (initially fails)
    const step2Tool = async (input) => {
        step2ExecutionCount++;
        if (paymentToolShouldFail) {
            throw new Error('503 Service Unavailable: Remote Payment Gateway Down');
        }
        return { transactionId: 'tx_98765', status: 'PAID', charged: input.total };
    };
    // ---------------------------------------------------------------------------
    // PASS 1: Run workflow. Steps 0 & 1 succeed, Step 2 fails and halts gracefully.
    // ---------------------------------------------------------------------------
    console.log('▶️ [PASS 1] Starting initial agent workflow run...');
    // Step 0
    const user = await interceptor.executeStep({
        executionId,
        stepIndex: 0,
        toolName: 'search_user',
        args: { userId: 'usr_123' },
        execute: step0Tool,
    });
    console.log('  Step 0 Result:', user);
    // Step 1
    const invoice = await interceptor.executeStep({
        executionId,
        stepIndex: 1,
        toolName: 'calculate_invoice',
        args: { amount: 100 },
        execute: step1Tool,
    });
    console.log('  Step 1 Result:', invoice);
    // Step 2 (Expected to fail)
    let failedAsExpected = false;
    try {
        await interceptor.executeStep({
            executionId,
            stepIndex: 2,
            toolName: 'charge_payment_gateway',
            args: { total: invoice.total },
            execute: step2Tool,
        });
    }
    catch (err) {
        if (err instanceof AgentExecutionHaltedError) {
            failedAsExpected = true;
            console.log('  ✅ Caught expected AgentExecutionHaltedError:');
            console.log('     Halted at step:', err.stepIndex);
            console.log('     Failing tool  :', err.toolName);
            console.log('     Error message :', err.message);
        }
        else {
            throw err;
        }
    }
    if (!failedAsExpected) {
        throw new Error('Test assertion failed: Step 2 was expected to fail and halt');
    }
    // Verify checkpoint states in Redis Hash
    const hashKey = REDIS_KEYS.executionHash(executionId);
    const step0Check = await mockRedis.hget(hashKey, '0');
    const step1Check = await mockRedis.hget(hashKey, '1');
    const step2Check = await mockRedis.hget(hashKey, '2');
    if (!step0Check || !step1Check) {
        throw new Error('Checkpoints for step 0 and step 1 must be present in Redis Hash');
    }
    if (step2Check) {
        throw new Error('Failed step 2 must NOT have saved a success checkpoint');
    }
    console.log('\n📊 Checkpoints verified in Redis Hash: Steps 0 & 1 persisted, Step 2 unpersisted.');
    console.log(`   Step 0 executions: ${step0ExecutionCount} (expected: 1)`);
    console.log(`   Step 1 executions: ${step1ExecutionCount} (expected: 1)`);
    console.log(`   Step 2 executions: ${step2ExecutionCount} (expected: 1)`);
    // ---------------------------------------------------------------------------
    // PASS 2: Replay the workflow!
    // External service is healed. Steps 0 & 1 must bypass tool execution.
    // ---------------------------------------------------------------------------
    console.log('\n▶️ [PASS 2] REPLAY TRIGGERED (Bypassing completed steps)...');
    paymentToolShouldFail = false; // Issue resolved by operator / upstream
    const replayedUser = await interceptor.executeStep({
        executionId,
        stepIndex: 0,
        toolName: 'search_user',
        args: { userId: 'usr_123' },
        execute: step0Tool,
    });
    const replayedInvoice = await interceptor.executeStep({
        executionId,
        stepIndex: 1,
        toolName: 'calculate_invoice',
        args: { amount: 100 },
        execute: step1Tool,
    });
    const payment = await interceptor.executeStep({
        executionId,
        stepIndex: 2,
        toolName: 'charge_payment_gateway',
        args: { total: replayedInvoice.total },
        execute: step2Tool,
    });
    console.log('  Step 2 Result on Replay:', payment);
    // Assert execution counts
    console.log('\n📈 FINAL VERIFICATION OF IDEMPOTENCY & EXECUTION COUNTS:');
    console.log(`   Step 0 total tool executions: ${step0ExecutionCount} (MUST be 1 -> 0 reruns!)`);
    console.log(`   Step 1 total tool executions: ${step1ExecutionCount} (MUST be 1 -> 0 reruns!)`);
    console.log(`   Step 2 total tool executions: ${step2ExecutionCount} (MUST be 2 -> 1 failure + 1 success)`);
    if (step0ExecutionCount !== 1 || step1ExecutionCount !== 1 || step2ExecutionCount !== 2) {
        throw new Error('Execution counts did not match expected idempotent replay pattern!');
    }
    console.log('\n🎉 ALL TESTS PASSED! Checkpoint replayer and DLQ interceptor logic is 100% verified.\n');
}
runMockTestSuite().catch((err) => {
    console.error('❌ Test suite failed:', err);
    process.exit(1);
});
//# sourceMappingURL=test-mock.js.map