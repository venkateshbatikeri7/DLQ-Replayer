import 'dotenv/config';
import { createRedisClient } from '../shared/redis.js';
import { AgentCheckpointInterceptor } from '../sdk/interceptor.js';
import { AgentResumeListener } from '../sdk/subscriber.js';
import { AgentExecutionHaltedError } from '../sdk/errors.js';
import { logger } from '../shared/logger.js';

// Configuration
const EXECUTION_ID = process.env.SIMULATE_EXEC_ID || `agent-workflow-${Date.now()}`;
const SERVER_URL = process.env.CONTROL_PLANE_URL || 'http://127.0.0.1:3000';

async function main() {
  logger.info({ executionId: EXECUTION_ID }, '🤖 Starting AI Agent Workflow Simulation...');

  // Initialize Redis connections
  const commandRedis = createRedisClient('default');
  const subscriberRedis = createRedisClient('subscriber');

  const interceptor = new AgentCheckpointInterceptor(commandRedis);
  const resumeListener = new AgentResumeListener(subscriberRedis);

  let shouldFailExternalTool = true;

  // The Agent Workflow Runner function
  async function runWorkflow(): Promise<void> {
    logger.info({ executionId: EXECUTION_ID }, '🚀 [Agent] Running workflow pipeline...');

    // -------------------------------------------------------------------------
    // STEP 0: Query User Database
    // -------------------------------------------------------------------------
    const userProfile = await interceptor.executeStep({
      executionId: EXECUTION_ID,
      stepIndex: 0,
      toolName: 'database_query_user',
      args: { customerId: 'cust_8492' },
      execute: async (args) => {
        // Simulating 50ms database latency
        await new Promise((r) => setTimeout(r, 50));
        return {
          id: args?.customerId,
          name: 'Sarah Connor',
          creditBalance: 250.0,
        };
      },
    });
    logger.info({ userProfile }, 'Step 0 output available');

    // -------------------------------------------------------------------------
    // STEP 1: Query LLM for Recommendation
    // -------------------------------------------------------------------------
    const llmSummary = await interceptor.executeStep({
      executionId: EXECUTION_ID,
      stepIndex: 1,
      toolName: 'llm_generate_recommendation',
      args: { user: userProfile.name, balance: userProfile.creditBalance },
      execute: async (args) => {
        // Simulating 200ms LLM completion latency
        await new Promise((r) => setTimeout(r, 200));
        return {
          plan: 'Enterprise Tier',
          recommendedDiscount: '10%',
          computedTotal: 180.0,
        };
      },
    });
    logger.info({ llmSummary }, 'Step 1 output available');

    // -------------------------------------------------------------------------
    // STEP 2: External Payment Gateway (Flaky / Dependent on 3rd party API)
    // -------------------------------------------------------------------------
    const paymentReceipt = await interceptor.executeStep({
      executionId: EXECUTION_ID,
      stepIndex: 2,
      toolName: 'external_payment_gateway_charge',
      args: { amount: llmSummary.computedTotal },
      execute: async (args) => {
        if (shouldFailExternalTool) {
          throw new Error('503 Service Unavailable: Remote Payment Gateway Timed Out');
        }
        return {
          chargeId: 'ch_live_992124892',
          status: 'SUCCESS',
          amountPaid: args?.amount,
        };
      },
    });
    logger.info({ paymentReceipt }, 'Step 2 output available');

    // -------------------------------------------------------------------------
    // STEP 3: Notification Dispatch
    // -------------------------------------------------------------------------
    const notification = await interceptor.executeStep({
      executionId: EXECUTION_ID,
      stepIndex: 3,
      toolName: 'send_confirmation_email',
      args: { email: 'sarah@skynet-defense.org', chargeId: paymentReceipt.chargeId },
      execute: async (args) => {
        return { sent: true, recipient: args?.email };
      },
    });

    logger.info({ notification }, '🎉 WORKFLOW FULLY COMPLETED! All steps finished.');
  }

  // ---------------------------------------------------------------------------
  // Set up Replay Listener for real-time control plane commands
  // ---------------------------------------------------------------------------
  resumeListener.onResume(EXECUTION_ID, async (command) => {
    logger.info(
      { command: command.command, executionId: command.executionId },
      '🔔 [Agent Orchestrator] COMMAND:RESUME signal received from Control Plane! Resuming agent...'
    );

    // Simulate that the external provider issue has been remediated
    shouldFailExternalTool = false;

    try {
      await runWorkflow();
      logger.info('✅ Replayed workflow succeeded! Cleaning up simulation.');
      await cleanup();
    } catch (err) {
      logger.error({ err }, 'Workflow failed again during replay');
    }
  });

  // ---------------------------------------------------------------------------
  // Initial Run (Pass 1)
  // ---------------------------------------------------------------------------
  try {
    await runWorkflow();
  } catch (err) {
    if (err instanceof AgentExecutionHaltedError) {
      logger.warn(
        {
          executionId: err.executionId,
          stepIndex: err.stepIndex,
          toolName: err.toolName,
        },
        '🛑 [Agent Pipeline] Workflow safely halted due to tool failure. Waiting for replay signal...'
      );

      console.log('\n' + '-'.repeat(70));
      console.log(`ℹ️  To trigger replay via the Control Plane, send an HTTP POST request:`);
      console.log(`   curl -X POST ${SERVER_URL}/api/replay/${EXECUTION_ID}`);
      console.log(`   Or run: node -e "fetch('${SERVER_URL}/api/replay/${EXECUTION_ID}', {method:'POST'}).then(r=>r.json()).then(console.log)"`);
      console.log('-'.repeat(70) + '\n');
    } else {
      logger.error({ err }, 'Unexpected error encountered');
      await cleanup();
    }
  }

  async function cleanup() {
    await resumeListener.close();
    await commandRedis.quit();
    await subscriberRedis.quit();
    process.exit(0);
  }
}

main().catch((err) => {
  logger.fatal({ err }, 'Simulator failed');
  process.exit(1);
});
