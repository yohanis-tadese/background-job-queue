/**
 * index.ts — All-in-one test: producer + consumer + events in a single process.
 *
 * Run with:  npm run dev
 *
 * This lets you see the full BullMQ flow without opening 3 terminals.
 * In production you would split these into separate long-running processes.
 */

import { Queue, Worker, QueueEvents, Job } from 'bullmq';

const QUEUE_NAME = 'email-queue';

// Plain config — avoids the ioredis version conflict with BullMQ's bundled copy.
const connection = { host: 'localhost', port: 6379, maxRetriesPerRequest: null as null };

// 1. EVENT LISTENER — attach first so we don't miss any events.
const queueEvents = new QueueEvents(QUEUE_NAME, { connection });

queueEvents.on('waiting',   ({ jobId }) =>              console.log(`[waiting]   Job ${jobId} queued`));
queueEvents.on('active',    ({ jobId }) =>              console.log(`[active]    Job ${jobId} started`));
queueEvents.on('completed', ({ jobId, returnvalue }) => console.log(`[completed] Job ${jobId} done — result: ${returnvalue}`));
queueEvents.on('failed',    ({ jobId, failedReason }) => console.log(`[failed]    Job ${jobId} failed — ${failedReason}`));

// 2. CONSUMER — processes jobs as they arrive.
const worker = new Worker(QUEUE_NAME, async (job: Job) => {
    console.log(`\n  Processing [${job.name}] id=${job.id}`);
    console.log(`  To: ${job.data.to} | Subject: ${job.data.subject}`);
    await new Promise((resolve) => setTimeout(resolve, 600)); // simulate work
    return { sent: true, to: job.data.to };
}, { connection });

worker.on('failed', (job, err) => console.error(`  Worker error on job ${job?.id}: ${err.message}`));

// 3. PRODUCER — add jobs, then wait for them to finish, then clean up.
const queue = new Queue(QUEUE_NAME, { connection });

console.log('Adding jobs to the queue...\n');

const [job1, job2] = await Promise.all([
    queue.add('send-welcome-email', { to: 'alice@example.com', subject: 'Welcome!' }),
    queue.add('send-reset-email',   { to: 'bob@example.com',   subject: 'Reset your password' }),
]);

console.log(`Added: job1=${job1.id}, job2=${job2.id}\n`);

// Wait until both jobs report completion via QueueEvents.
await Promise.all([
    job1.waitUntilFinished(queueEvents),
    job2.waitUntilFinished(queueEvents),
]);

console.log('\nAll jobs finished. Shutting down.');
await worker.close();
await queue.close();
await queueEvents.close();
