import Fastify from 'fastify';
import cors from '@fastify/cors';
import { replayRoutes } from './routes/replay.js';
import { executionsRoutes } from './routes/executions.js';
/**
 * Builds and configures the Fastify Control Plane application.
 */
export async function buildServer(options) {
    const { redisClient } = options;
    const app = Fastify({
        logger: {
            level: process.env.LOG_LEVEL || 'info',
        },
        disableRequestLogging: false,
    });
    // Enable CORS for external dashboards and UI clients
    await app.register(cors, {
        origin: true,
    });
    // Liveness / readiness probe
    app.get('/health', async (_req, reply) => {
        try {
            const pong = await redisClient.ping();
            return reply.code(200).send({
                status: 'healthy',
                timestamp: new Date().toISOString(),
                redis: pong === 'PONG' ? 'connected' : 'degraded',
            });
        }
        catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            return reply.code(503).send({
                status: 'unhealthy',
                timestamp: new Date().toISOString(),
                redisError: msg,
            });
        }
    });
    // Register API routes
    await app.register(replayRoutes, { redisClient });
    await app.register(executionsRoutes, { redisClient });
    // Global error handler
    app.setErrorHandler((error, request, reply) => {
        request.log.error(error);
        const statusCode = error.statusCode ?? 500;
        return reply.status(statusCode).send({
            success: false,
            statusCode,
            error: error.name,
            message: error.message,
        });
    });
    return app;
}
//# sourceMappingURL=app.js.map