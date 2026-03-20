/**
 * events.ts — Listens to every job lifecycle event on the queue.
 *
 * Run with:  npm run events
 *
 * QueueEvents connects to Redis and streams events from ALL workers,
 * even ones running in different processes or machines.
 * Great for logging, monitoring, or triggering follow-up actions.
 *
 * Job lifecycle:  waiting → active → completed (or failed)
 */

import { QueueEvents } from 'bullmq';

const connection = { host: 'localhost', port: 6379, maxRetriesPerRequest: null as null };

// Must use the exact same queue name as producer.ts and consumer.ts.
const queueEvents = new QueueEvents('email-queue', { connection });

queueEvents.on('waiting', ({ jobId }) => {
    console.log(`[waiting]   Job ${jobId} is queued — no worker has picked it up yet`);
});

queueEvents.on('active', ({ jobId, prev }) => {
    console.log(`[active]    Job ${jobId} is now being processed (was: ${prev})`);
});

queueEvents.on('completed', ({ jobId, returnvalue }) => {
    console.log(`[completed] Job ${jobId} finished — result: ${returnvalue}`);
});

queueEvents.on('failed', ({ jobId, failedReason }) => {
    console.log(`[failed]    Job ${jobId} failed — reason: ${failedReason}`);
});

queueEvents.on('delayed', ({ jobId, delay }) => {
    console.log(`[delayed]   Job ${jobId} is delayed by ${delay}ms`);
});

console.log('Event listener running on "email-queue"...');
console.log('(Run npm run producer in another terminal to see events fire.)');
