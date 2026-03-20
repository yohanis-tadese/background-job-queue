// Shared Redis connection config — reads from environment variables.
// BullMQ creates its own internal Redis client from this plain object.
export const connection = {
    host:     process.env.REDIS_HOST     ?? 'localhost',
    port:     Number(process.env.REDIS_PORT ?? 6379),
    password: process.env.REDIS_PASSWORD  || undefined,
    tls:      process.env.REDIS_TLS === 'true' ? {} : undefined,
    maxRetriesPerRequest: null as null,
};
