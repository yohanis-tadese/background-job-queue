/**
 * producer.ts — Adds jobs to the queue.
 *
 * Run with:  npm run producer
 *
 * Think of this as the "sender" — it describes WHAT needs to be done
 * and hands it off to BullMQ. The worker picks it up separately.
 */

import { Queue } from 'bullmq';
// Pass a plain config — BullMQ uses its own bundled ioredis internally.
// Instantiating ioredis yourself causes a type conflict between the two copies.
const connection = { host: 'localhost', port: 6379, maxRetriesPerRequest: null as null };

// A Queue is a named channel. Any worker listening to "email-queue"
// will receive the jobs we add here.
const emailQueue = new Queue('email-queue', { connection });

// --- Add jobs ---
const job1 = await emailQueue.add('send-welcome-email', {
    to: 'alice@example.com',
    subject: 'Welcome to the app!',
    body: 'Thanks for signing up, Alice.',
});

const job2 = await emailQueue.add('send-reset-email', {
    to: 'bob@example.com',
    subject: 'Reset your password',
    body: 'Click here to reset your password, Bob.',
});

console.log(`Job added: [${job1.name}] id=${job1.id}`);
console.log(`Job added: [${job2.name}] id=${job2.id}`);
console.log('Done. Jobs are now sitting in Redis waiting for a worker.');

// Close the queue connection — the producer's job is finished.
await emailQueue.close();
