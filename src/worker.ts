import 'dotenv/config';
import { createConsumer }   from './bullmq/consumer.js';
import { trackQueueInDB }   from './bullmq/tracker.js';
import { emailProcessor }   from './processors/email.processor.js';
import { prisma }           from './db/client.js';

// Track every job state change → writes to PostgreSQL
trackQueueInDB('email-queue');

// Wire the processor — logic lives in processors/email.processor.ts
createConsumer('email-queue', emailProcessor);

console.log('[worker] Running — listening on "email-queue"');
console.log('[worker] Press Ctrl+C to stop gracefully\n');

// Close Prisma cleanly on shutdown (consumer handles the worker itself)
process.on('SIGTERM', async () => { await prisma.$disconnect(); });
process.on('SIGINT',  async () => { await prisma.$disconnect(); });
