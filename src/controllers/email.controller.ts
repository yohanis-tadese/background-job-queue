import { JobStatus } from '@prisma/client';
import { prisma }    from '../db/client.js';
import { createProducer } from '../bullmq/producer.js';

// One producer instance shared across all controller functions.
// Do not create a new producer per request — each one opens a Redis connection.
const producer = createProducer('email-queue');

// ─── Types ────────────────────────────────────────────────────────────────────

interface WelcomeEmailPayload {
    to:      string;
    subject: string;
    name:    string;
}

interface ResetEmailPayload {
    to:      string;
    subject: string;
    token:   string;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

// Creates a JobRecord in the database the moment a job is queued.
// The tracker.ts then updates this record as the job moves through its lifecycle.
async function recordJob(
    jobId:     string,
    jobName:   string,
    payload:   unknown,
) {
    await prisma.jobRecord.create({
        data: {
            jobId,
            queueName: 'email-queue',
            jobName,
            status:    JobStatus.QUEUED,
            payload:   payload as object,
        },
    });
}

// ─── Controllers ──────────────────────────────────────────────────────────────

/**
 * sendWelcomeEmail
 *
 * Called when a new user signs up.
 * Queues the email job and immediately records it in the database.
 * Returns the BullMQ Job so the caller can wait on it if needed.
 */
export async function sendWelcomeEmail(name: string, to: string) {
    const payload: WelcomeEmailPayload = {
        to,
        name,
        subject: `Welcome to the app, ${name}!`,
    };

    const job = await producer.addJob('send-welcome-email', payload);
    await recordJob(job.id!, 'send-welcome-email', payload);

    console.log(`[email.controller] Queued welcome email → ${to} (job ${job.id})`);
    return job;
}

/**
 * sendPasswordReset
 *
 * Called when a user requests a password reset.
 * Uses priority: 1 so it jumps ahead of welcome emails — users waiting
 * for a reset link are more time-sensitive than welcome messages.
 */
export async function sendPasswordReset(to: string, token: string) {
    const payload: ResetEmailPayload = {
        to,
        token,
        subject: 'Reset your password',
    };

    const job = await producer.addJob('send-reset-email', payload, {
        priority: 1, // process before lower-priority jobs
    });
    await recordJob(job.id!, 'send-reset-email', payload);

    console.log(`[email.controller] Queued reset email   → ${to} (job ${job.id})`);
    return job;
}

/**
 * getJobHistory
 *
 * Returns recent job records for a given queue.
 * Use this to display job history in an admin dashboard or debug view.
 */
export async function getJobHistory(limit = 20) {
    return prisma.jobRecord.findMany({
        where:   { queueName: 'email-queue' },
        orderBy: { createdAt: 'desc' },
        take:    limit,
        select: {
            jobId:        true,
            jobName:      true,
            status:       true,
            payload:      true,
            result:       true,
            error:        true,
            attemptsMade: true,
            createdAt:    true,
            startedAt:    true,
            completedAt:  true,
            failedAt:     true,
        },
    });
}

/**
 * getFailedJobs
 *
 * Returns all jobs that failed or are permanently dead.
 * Use this to surface issues and manually re-trigger them.
 */
export async function getFailedJobs() {
    return prisma.jobRecord.findMany({
        where: {
            status: { in: [JobStatus.FAILED, JobStatus.DEAD] },
            queueName: 'email-queue',
        },
        orderBy: { failedAt: 'desc' },
    });
}
