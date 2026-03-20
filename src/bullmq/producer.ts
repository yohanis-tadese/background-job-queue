import { Queue, JobsOptions } from 'bullmq';
import { connection } from './connection.js';

/**
 * createProducer(queueName)
 *
 * Returns a producer tied to a specific queue.
 *
 * Default behaviour applied to every job:
 *   - 3 retry attempts with exponential backoff (1s → 2s → 4s)
 *   - Keeps last 100 completed jobs in Redis (for inspection)
 *   - Keeps last 500 failed jobs in Redis (for debugging)
 *
 * Per-job overrides:
 *   producer.addJob('name', data, { priority: 1 })        // run before others
 *   producer.addJob('name', data, { delay: 5000 })        // start after 5s
 *   producer.addJob('name', data, { attempts: 5 })        // override retry count
 *   producer.addJob('name', data, { jobId: 'my-id' })     // deduplicate by id
 */
export function createProducer(queueName: string) {
    const queue = new Queue(queueName, {
        connection,
        defaultJobOptions: {
            attempts: 3,
            backoff: {
                type: 'exponential',  // delay doubles each retry: 1s, 2s, 4s
                delay: 1000,
            },
            removeOnComplete: { count: 100 }, // keep last 100 completed in Redis
            removeOnFail:     { count: 500 }, // keep last 500 failed in Redis
        },
    });

    return {
        // options is optional — uses the defaults above if not provided
        addJob: (name: string, data: unknown, options?: JobsOptions) =>
            queue.add(name, data, options),

        close: () => queue.close(),

        queue,
    };
}
