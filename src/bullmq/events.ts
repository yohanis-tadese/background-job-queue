import { QueueEvents } from 'bullmq';
import { connection } from './connection.js';

/**
 * createQueueEvents(queueName)
 *
 * Attaches a full lifecycle event logger to any queue.
 * Covers every state a job can reach, including retries, stalls, and progress.
 *
 * Events covered:
 *   waiting            → job queued, no worker yet
 *   active             → worker picked it up, running now
 *   progress           → job reported a progress percentage
 *   completed          → job finished successfully
 *   failed             → one attempt failed (may still retry)
 *   retries-exhausted  → all attempts used up, job is permanently dead
 *   stalled            → worker died mid-job, BullMQ requeued it
 *   delayed            → job is scheduled to run later
 *   removed            → job was manually removed from the queue
 */
export function createQueueEvents(queueName: string) {
    const queueEvents = new QueueEvents(queueName, { connection });

    queueEvents.on('waiting', ({ jobId }) => {
        console.log(`[${queueName}] [waiting]           job ${jobId} — queued`);
    });

    queueEvents.on('active', ({ jobId }) => {
        console.log(`[${queueName}] [active]            job ${jobId} — processing`);
    });

    queueEvents.on('progress', ({ jobId, data }) => {
        console.log(`[${queueName}] [progress]          job ${jobId} — ${data}%`);
    });

    queueEvents.on('completed', ({ jobId, returnvalue }) => {
        console.log(`[${queueName}] [completed]         job ${jobId} — result: ${returnvalue}`);
    });

    queueEvents.on('failed', ({ jobId, failedReason }) => {
        console.warn(`[${queueName}] [failed/retrying]   job ${jobId} — ${failedReason}`);
    });

    // Fired when ALL retry attempts are exhausted — job will not run again
    queueEvents.on('retries-exhausted', ({ jobId, attemptsMade }) => {
        console.error(`[${queueName}] [DEAD]              job ${jobId} — permanently failed after ${attemptsMade} attempts`);
    });

    // Fired when BullMQ detects a job was picked up but never finished
    queueEvents.on('stalled', ({ jobId }) => {
        console.warn(`[${queueName}] [stalled]           job ${jobId} — requeued`);
    });

    queueEvents.on('delayed', ({ jobId, delay }) => {
        console.log(`[${queueName}] [delayed]            job ${jobId} — starts in ${delay}ms`);
    });

    queueEvents.on('removed', ({ jobId }) => {
        console.log(`[${queueName}] [removed]            job ${jobId}`);
    });

    return queueEvents;
}
