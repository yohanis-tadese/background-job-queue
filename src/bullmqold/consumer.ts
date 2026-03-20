/**
 * consumer.ts — Picks up jobs from the queue and processes them.
 *
 * Run with:  npm run consumer
 *
 * Think of this as the "worker process" — it stays alive, polls Redis,
 * and executes your job handler whenever a new job arrives.
 * You can run multiple consumers in parallel for throughput.
 */

import { Worker, Job } from 'bullmq';

const connection = { host: 'localhost', port: 6379, maxRetriesPerRequest: null as null };

// The Worker watches "email-queue" and calls this function for each job.
// The return value is saved as the job's result (visible in events).
async function processEmail(job: Job) {
    console.log(`\nProcessing [${job.name}] id=${job.id}`);
    console.log('  To:      ', job.data.to);
    console.log('  Subject: ', job.data.subject);

    // Simulate the actual work (e.g., calling an email API).
    await new Promise((resolve) => setTimeout(resolve, 800));

    console.log(`  Sent to ${job.data.to}`);
    return { sent: true, to: job.data.to };
}

const worker = new Worker('email-queue', processEmail, { connection });

// These events fire in THIS process after the job handler returns.
worker.on('completed', (job) => {
    console.log(`  [completed] job ${job.id} finished successfully`);
});

worker.on('failed', (job, err) => {
    console.error(`  [failed]    job ${job?.id} failed — ${err.message}`);
});

console.log('Consumer is running and waiting for jobs on "email-queue"...');
console.log('(Keep this terminal open. Run npm run producer in another terminal.)');
