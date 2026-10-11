import 'dotenv/config';
import { createRedisClient } from '../shared/redis.js';
import { logger } from '../shared/logger.js';
import { buildServer } from './app.js';
import { DlqWorker } from './workers/dlq-worker.js';

const PORT = Number(process.env.PORT) || 3000;
const HOST = process.env.HOST || '0.0.0.0';

async function bootstrap() {
  logger.info('Initializing AI Agent Replay Control Plane & DLQ Monitor...');

  // Dedicated Redis clients for isolation
  const commandRedis = createRedisClient('default');
  const blockingRedis = createRedisClient('blocking');

  // Build Fastify web server
  const server = await buildServer({ redisClient: commandRedis });

  // Initialize and start background DLQ Stream consumer worker
  const dlqWorker = new DlqWorker(blockingRedis);
  await dlqWorker.start();

  // Start HTTP listener
  try {
    const address = await server.listen({ port: PORT, host: HOST });
    logger.info(`✨ Control Plane server listening on ${address}`);
    logger.info(`👉 POST /api/replay/:execution_id to trigger workflow replay`);
    logger.info(`👉 GET  /api/executions/:execution_id to inspect checkpoint state`);
    logger.info(`👉 GET  /api/events to query DLQ events`);
  } catch (err) {
    logger.fatal({ err }, 'Failed to bind server port');
    process.exit(1);
  }

  // Graceful shutdown handling
  let isShuttingDown = false;
  const gracefulShutdown = async (signal: string) => {
    if (isShuttingDown) return;
    isShuttingDown = true;

    logger.warn({ signal }, `Received ${signal}. Initiating graceful shutdown...`);

    try {
      // 1. Stop background stream consumer
      await dlqWorker.stop();

      // 2. Close Fastify web server (drain in-flight HTTP requests)
      await server.close();
      logger.info('HTTP server closed');

      // 3. Disconnect Redis clients cleanly
      await commandRedis.quit();
      await blockingRedis.quit();
      logger.info('Redis connections closed cleanly');

      logger.info('Shutdown complete. Goodbye.');
      process.exit(0);
    } catch (shutdownErr) {
      logger.error({ shutdownErr }, 'Error during graceful shutdown');
      process.exit(1);
    }
  };

  process.on('SIGINT', () => gracefulShutdown('SIGINT'));
  process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
}

bootstrap().catch((err) => {
  logger.fatal({ err }, 'Fatal bootstrap error');
  process.exit(1);
});
