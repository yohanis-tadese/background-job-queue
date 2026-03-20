import { QueueEvents } from 'bullmq';
import { JobStatus }   from '@prisma/client';
import { prisma }      from '../db/client.js';
import { connection }  from './connection.js';

/**
 * trackQueueInDB(queueName)
 *
 * Subscribes to every lifecycle event on the queue and writes the state
 * change to the JobRecord table in PostgreSQL.
 *
 * Full state machine tracked in DB:
 *
 *   QUEUED  ──▶  PROCESSING  ──▶  COMPLETED
 *                    │
 *                    ▼
 *                  FAILED  ──▶  PROCESSING (retry)
 *                    │
 *                    ▼ (all attempts exhausted)
 *                   DEAD
 *
 * Returns the QueueEvents instance so callers can use waitUntilFinished().
 */
export function trackQueueInDB(queueName: string) {
    const queueEvents = new QueueEvents(queueName, { connection });

    // Job picked up by a worker — set to PROCESSING
    // Also fires when a failed job is being retried (resets from FAILED → PROCESSING)
    queueEvents.on('active', async ({ jobId }) => {
        await prisma.jobRecord
            .update({
                where: { jobId },
                data:  {
                    status:    JobStatus.PROCESSING,
                    startedAt: new Date(),
                    error:     null, // clear previous error on retry
                },
            })
            .catch((err) =>
                console.error(`[tracker] active update failed for job ${jobId}:`, err.message),
            );
    });

    // One attempt failed — record the error, mark FAILED
    // If attempts remain, BullMQ will re-queue it and 'active' fires again
    queueEvents.on('failed', async ({ jobId, failedReason }) => {
        await prisma.jobRecord
            .update({
                where: { jobId },
                data:  {
                    status:      JobStatus.FAILED,
                    error:       failedReason,
                    failedAt:    new Date(),
                    attemptsMade: { increment: 1 },
                },
            })
            .catch((err) =>
                console.error(`[tracker] failed update failed for job ${jobId}:`, err.message),
            );
    });

    // All retry attempts exhausted — job will never run again
    queueEvents.on('retries-exhausted', async ({ jobId, attemptsMade }) => {
        await prisma.jobRecord
            .update({
                where: { jobId },
                data:  {
                    status:       JobStatus.DEAD,
                    attemptsMade: Number(attemptsMade),
                },
            })
            .catch((err) =>
                console.error(`[tracker] retries-exhausted update failed for job ${jobId}:`, err.message),
            );
    });

    // Processor returned successfully — save result
    queueEvents.on('completed', async ({ jobId, returnvalue }) => {
        await prisma.jobRecord
            .update({
                where: { jobId },
                data:  {
                    status:      JobStatus.COMPLETED,
                    result:      returnvalue
                                     ? (typeof returnvalue === 'string' ? JSON.parse(returnvalue) : returnvalue)
                                     : null,
                    completedAt: new Date(),
                },
            })
            .catch((err) =>
                console.error(`[tracker] completed update failed for job ${jobId}:`, err.message),
            );
    });

    // Worker picked up the job but the process died — BullMQ requeued it
    queueEvents.on('stalled', ({ jobId }) => {
        console.warn(`[tracker][${queueName}] job ${jobId} stalled — worker may have crashed, requeueing`);
    });

    return queueEvents;
}
