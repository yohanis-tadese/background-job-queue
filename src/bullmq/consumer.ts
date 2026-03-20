import { Worker, Job, WorkerOptions } from 'bullmq';
import { connection } from './connection.js';

/**
 * createConsumer(queueName, processor, options?)
 *
 * Creates a production-ready worker with:
 *   - Concurrency: 5 jobs processed in parallel by default
 *   - Graceful shutdown: finishes active jobs before exiting on SIGTERM/SIGINT
 *   - Stalled detection: warns if a job starts but never finishes (worker crash)
 *   - Smart failure logging: "retrying" vs "permanently failed"
 *
 * In your processor you can:
 *   await job.updateProgress(50);         // report 50% progress
 *   throw new Error('...');               // triggers retry (up to attempts limit)
 *
 * Override concurrency:
 *   createConsumer('q', processor, { concurrency: 10 })
 */
export function createConsumer(
    queueName: string,
    processor: (job: Job) => Promise<unknown>,
    options?: Partial<WorkerOptions>,
) {
    const worker = new Worker(queueName, processor, {
        connection,
        concurrency: 5,  // process 5 jobs at the same time
        ...options,
    });

    // Log retrying vs permanently failed differently
    worker.on('failed', (job, err) => {
        const attempts    = job?.opts.attempts ?? 1;
        const attemptsMade = job?.attemptsMade ?? 0;
        const isLastAttempt = attemptsMade >= attempts;

        if (isLastAttempt) {
            console.error(`[${queueName}] PERMANENTLY FAILED job ${job?.id} (${job?.name}) after ${attemptsMade} attempts: ${err.message}`);
        } else {
            console.warn(`[${queueName}] Retrying job ${job?.id} (${job?.name}) — attempt ${attemptsMade}/${attempts}: ${err.message}`);
        }
    });

    // A stalled job = worker picked it up but never sent a heartbeat (process died)
    // BullMQ auto-requeues it; this just surfaces the warning
    worker.on('stalled', (jobId) => {
        console.warn(`[${queueName}] Job ${jobId} stalled — worker may have crashed. Requeueing.`);
    });

    // Graceful shutdown — wait for active jobs to finish before the process exits
    const shutdown = async (signal: string) => {
        console.log(`\n[${queueName}] ${signal} received — waiting for active jobs to finish...`);
        await worker.close();
        console.log(`[${queueName}] Worker closed cleanly.`);
    };

    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT',  () => shutdown('SIGINT'));

    return worker;
}
