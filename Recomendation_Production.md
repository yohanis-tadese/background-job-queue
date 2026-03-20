# BullMQ — Production Recommendations

> This document covers 8 improvements that take this BullMQ system from a
> working demo to a production-ready service. Each section explains the problem,
> the solution, and the exact code to implement it.

---

## Priority Order — Start Here

| # | What | Do First? | Why |
|---|---|---|---|
| 1 | Environment-based config | Yes — before anything else | Hardcoded values break in every real environment |
| 2 | Separate worker process | Yes — before deploying | Mixing producer and consumer in one process causes failures |
| 3 | Bull Board dashboard | Yes — gives immediate visibility | You cannot operate a queue system you cannot see |
| 4 | Job type registry | Soon | Prevents an entire class of silent runtime bugs |
| 5 | Dead letter queue | Soon | Permanently failed jobs must go somewhere |
| 6 | Worker health check | Before Kubernetes/Docker | Without it, crashed workers look healthy to the platform |
| 7 | Rate limiting | When you have high volume | Protects downstream APIs from being overwhelmed |
| 8 | Metrics collection | As you scale | Tells you when to scale up and what is breaking |

---

## 1. Environment-Based Config

### The Problem

The current `connection.ts` has the Redis host, port, and password hardcoded:

```typescript
// CURRENT — breaks the moment you deploy anywhere
export const connection = {
    host: 'localhost',
    port: 6379,
    maxRetriesPerRequest: null as null,
};
```

- `localhost` only works on your own machine
- Every environment (dev, staging, production) has a different Redis host
- Putting passwords in code means they end up in git history

### The Solution

Read all connection values from environment variables. Provide sensible local
defaults so the app still works with `npm run dev` without any setup.

```typescript
// src/bullmq/connection.ts
export const connection = {
    host:     process.env.REDIS_HOST     ?? 'localhost',
    port:     Number(process.env.REDIS_PORT ?? 6379),
    password: process.env.REDIS_PASSWORD ?? undefined,
    tls:      process.env.REDIS_TLS === 'true' ? {} : undefined,
    maxRetriesPerRequest: null as null,
};
```

Create a `.env` file for local development (never commit this to git):

```bash
# .env — local development only
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=
REDIS_TLS=false
```

Production environment variables are set in your hosting platform
(AWS, Railway, Render, Fly.io, Kubernetes secrets — never in code):

```bash
# Production values set in your deployment platform
REDIS_HOST=your-redis.cloud.com
REDIS_PORT=6380
REDIS_PASSWORD=your-secret-password
REDIS_TLS=true
```

Install `dotenv` to load `.env` in development:

```bash
npm install dotenv
```

Add to the top of `src/index.ts` and `src/worker.ts`:

```typescript
import 'dotenv/config';
```

### Why This Matters

Without this, every deployment requires editing source code. With it, you
deploy the same code everywhere and only change environment variables.

---

## 2. Separate Worker Process

### The Problem

Right now `src/index.ts` runs the producer, consumer, and events all in one
process. This is fine for learning but wrong for production:

```
CURRENT (wrong for production)
┌────────────────────────────────┐
│         One Process            │
│  Web Server + Worker + Events  │  ← if the web server crashes, the worker dies too
│                                │  ← cannot scale them independently
│  npm run dev                   │  ← both start and stop together
└────────────────────────────────┘
```

### The Solution

Split into two completely separate entry points:

```
PRODUCTION (correct)
┌─────────────────────────┐        ┌──────────────────────────────┐
│   Web Server / API      │        │   Worker Process             │
│                         │        │                              │
│   src/server.ts         │        │   src/worker.ts              │
│                         │        │                              │
│   - Express routes      │        │   - createConsumer()         │
│   - createProducer()    │──Redis─▶   - createQueueEvents()      │
│   - Receives requests   │        │   - No HTTP server           │
│   - Adds jobs           │        │   - Runs 24/7                │
│                         │        │   - Scale by adding replicas │
└─────────────────────────┘        └──────────────────────────────┘
```

```typescript
// src/worker.ts — standalone worker, this is all it does
import 'dotenv/config';
import { createConsumer }    from './bullmq/consumer.js';
import { createQueueEvents } from './bullmq/events.js';

// Observe events
createQueueEvents('email-queue');

// Process jobs — define your logic here
createConsumer('email-queue', async (job) => {
    if (job.name === 'send-welcome-email') {
        // call your email service
        return { sent: true };
    }
    if (job.name === 'send-reset-email') {
        // call your email service
        return { sent: true };
    }
});

console.log('Worker is running. Waiting for jobs...');
// No close() — this process stays alive forever and waits for jobs
```

```typescript
// src/server.ts — web server, adds jobs when users do things
import 'dotenv/config';
import express from 'express';
import { createProducer } from './bullmq/producer.js';

const app      = express();
const producer = createProducer('email-queue');

app.use(express.json());

app.post('/signup', async (req, res) => {
    const { email } = req.body;

    // Add the job and respond immediately — don't wait for the email to send
    await producer.addJob('send-welcome-email', { to: email, subject: 'Welcome!' });

    res.json({ message: 'Signed up. Welcome email on its way.' });
});

app.listen(3000, () => console.log('Server running on port 3000'));
```

Add the scripts to `package.json`:

```json
"scripts": {
    "dev":    "node --loader ts-node/esm src/index.ts",
    "worker": "node --loader ts-node/esm src/worker.ts",
    "server": "node --loader ts-node/esm src/server.ts"
}
```

### Why This Matters

| Concern | One process | Two processes |
|---|---|---|
| Web server crashes | Worker dies with it | Worker keeps running |
| Scaling | Scale everything or nothing | Scale worker independently |
| Deployment | Restarting server kills active jobs | Worker finishes jobs, then restarts |
| Resource usage | Worker competes with web server for CPU/RAM | Each has its own resources |

---

## 3. Bull Board — Visual Dashboard

### The Problem

Right now the only way to see what is happening in your queues is reading log
output. You cannot see:
- How many jobs are waiting
- Which jobs failed and why
- The data inside a specific job
- Whether a stuck job can be retried

### The Solution

Bull Board is an official BullMQ UI. One npm install gives you a full dashboard.

```bash
npm install @bull-board/express @bull-board/api
```

```typescript
// src/dashboard.ts
import express                    from 'express';
import { createBullBoard }        from '@bull-board/api';
import { BullMQAdapter }          from '@bull-board/api/bullMQAdapter.js';
import { ExpressAdapter }         from '@bull-board/express';
import { Queue }                  from 'bullmq';
import { connection }             from './bullmq/connection.js';

const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath('/admin/queues');

// Register every queue you want to monitor
createBullBoard({
    queues: [
        new BullMQAdapter(new Queue('email-queue', { connection })),
        new BullMQAdapter(new Queue('image-queue', { connection })),
    ],
    serverAdapter,
});

const app = express();
app.use('/admin/queues', serverAdapter.getRouter());

app.listen(3001, () => {
    console.log('Bull Board dashboard: http://localhost:3001/admin/queues');
});
```

Run it alongside your worker:

```bash
node --loader ts-node/esm src/dashboard.ts
```

### What You Get

```
http://localhost:3001/admin/queues

┌─────────────────────────────────────────────────┐
│  email-queue                                    │
│  Waiting: 12   Active: 3   Failed: 2   Done: 847│
│                                                 │
│  Failed Jobs:                                   │
│  ├── job#45  send-reset-email   "SMTP timeout"  │
│  │   Data: { to: "bob@example.com" }            │
│  │   [Retry]  [Delete]                          │
│  └── job#67  send-welcome-email "Rate limited"  │
│      Data: { to: "carol@example.com" }          │
│      [Retry]  [Delete]                          │
└─────────────────────────────────────────────────┘
```

You can retry failed jobs with one click — no code required.

### Why This Matters

You cannot operate a production queue system without visibility. Bull Board is
the fastest way to get that visibility and it integrates in under 20 lines.

---

## 4. Job Type Registry

### The Problem

Job names are currently plain strings scattered across producer and consumer:

```typescript
// producer adds this
producer.addJob('send-welcome-email', { to: 'alice@example.com' });

// consumer expects this — but TypeScript cannot verify the names match
if (job.name === 'send-welcone-email') { ... }  // typo — silent bug at runtime
```

There is no compile-time check that the name the producer sends matches what the
consumer handles. A typo causes jobs to be silently dropped.

### The Solution

Define every job name and its payload shape in one place:

```typescript
// src/bullmq/jobs.ts

// Every job name and its exact data shape, defined once
export interface JobPayloads {
    'send-welcome-email': { to: string; subject: string };
    'send-reset-email':   { to: string; subject: string };
    'resize-image':       { fileId: number; width: number; height: number };
    'generate-pdf':       { reportId: string; userId: string };
}

// The union type of all valid job names: 'send-welcome-email' | 'resize-image' | ...
export type JobName = keyof JobPayloads;
```

Update `producer.ts` to accept only registered job names:

```typescript
import { JobName, JobPayloads } from './jobs.js';

// name must be a known job name, data must match that job's payload shape
addJob<T extends JobName>(name: T, data: JobPayloads[T], options?: JobsOptions)
```

```typescript
// Now TypeScript catches mistakes at compile time

producer.addJob('send-welcone-email', { to: '...' });
//               ^^^^^^^^^^^^^^^ Error: not a valid job name

producer.addJob('send-welcome-email', { typo: '...' });
//                                      ^^^^^ Error: 'typo' does not exist on type
```

### Why This Matters

Without this, a typo in a job name causes jobs to queue up and never be
processed — with no error, no warning, just silent failure. The registry
turns that into a compile error you catch before deploying.

---

## 5. Dead Letter Queue

### The Problem

When a job exhausts all retry attempts, the current system logs an error and
stops. The job data is lost in Redis and nothing is done about it:

```typescript
// current behaviour — log and forget
console.error(`PERMANENTLY FAILED job ${job?.id}`);
```

In a real system, permanently failed jobs represent **real user impact** —
an email not sent, a payment not processed, a file not generated. You need
to record them, alert your team, and provide a way to recover.

### The Solution

Move permanently failed jobs to a dead letter queue and alert your team:

```typescript
// src/bullmq/consumer.ts — add inside createConsumer

import { createProducer } from './producer.js';

// A dedicated queue that collects all permanently failed jobs
const deadLetterProducer = createProducer('dead-letter-queue');

worker.on('failed', (job, err) => {
    const attempts     = job?.opts.attempts ?? 1;
    const attemptsMade = job?.attemptsMade ?? 0;
    const isLastAttempt = attemptsMade >= attempts;

    if (isLastAttempt) {
        // 1. Save to dead letter queue for inspection and manual retry
        deadLetterProducer.addJob('dead-job', {
            originalQueue: queueName,
            jobName:       job?.name,
            jobData:       job?.data,
            jobId:         job?.id,
            error:         err.message,
            failedAt:      new Date().toISOString(),
            attemptsMade,
        });

        // 2. Alert your team — replace with Slack/PagerDuty/Sentry as needed
        console.error(`
            ========================================
            DEAD JOB — ACTION REQUIRED
            Queue:   ${queueName}
            Job:     ${job?.name} (id: ${job?.id})
            Error:   ${err.message}
            Attempts: ${attemptsMade}/${attempts}
            ========================================
        `);
    }
});
```

You can then inspect dead jobs in Bull Board and retry them manually once
the underlying problem is fixed (e.g. email service was down for an hour).

### Why This Matters

Permanently failed jobs represent work that never happened. Without a dead
letter queue you have no record of what failed, no way to recover it, and no
way to alert the right person. With it, you lose nothing.

---

## 6. Worker Health Check

### The Problem

When you run the worker inside Kubernetes or Docker, the platform needs a way
to check if the worker is still alive and healthy. Without a health endpoint:

- A worker that crashes looks identical to a healthy worker from the outside
- Kubernetes keeps sending traffic to dead pods
- Jobs pile up with no worker processing them — silently

### The Solution

Add a minimal HTTP server to the worker process that responds to health checks:

```typescript
// src/worker.ts — add alongside the worker setup
import http from 'http';

const worker = createConsumer('email-queue', processor);

// Kubernetes calls GET /health every 30 seconds
// Respond 200 if healthy, 503 if shutting down
const healthServer = http.createServer((req, res) => {
    if (req.url === '/health') {
        const isHealthy = !worker.closing;
        res.writeHead(isHealthy ? 200 : 503);
        res.end(JSON.stringify({
            status:  isHealthy ? 'ok' : 'closing',
            queue:   'email-queue',
            time:    new Date().toISOString(),
        }));
    }
});

healthServer.listen(3000, () => {
    console.log('Health check available at http://localhost:3000/health');
});
```

Configure Kubernetes to use it:

```yaml
# kubernetes/worker-deployment.yaml
livenessProbe:
  httpGet:
    path: /health
    port: 3000
  initialDelaySeconds: 10
  periodSeconds: 30
  failureThreshold: 3

readinessProbe:
  httpGet:
    path: /health
    port: 3000
  initialDelaySeconds: 5
  periodSeconds: 10
```

### Why This Matters

Without a health check, a crashed worker silently stops processing jobs and
nothing alerts you. With it, Kubernetes automatically restarts the pod when
the health check fails.

---

## 7. Rate Limiting

### The Problem

If 10,000 users sign up at the same time, the producer adds 10,000 jobs
instantly. With `concurrency: 5`, the worker sends 5 emails per second to
your email API. Most email APIs have rate limits (e.g. 100 per minute).
Without rate limiting, your worker gets rate-limited errors, jobs fail,
and retry attempts start piling up.

### The Solution

Add a rate limiter to the worker so it never exceeds your API's limit:

```typescript
// src/bullmq/consumer.ts — add limiter to worker options
const worker = new Worker(queueName, processor, {
    connection,
    concurrency: 5,
    limiter: {
        max:      100,    // maximum 100 jobs
        duration: 60000,  // per 60,000ms (1 minute)
    },
    ...options,
});
```

Set the limit slightly **below** your API's actual limit to give yourself
a safety margin. If your email provider allows 500/minute, set 400/minute.

You can also set different limits per queue:

```typescript
// Email API allows 100/min
createConsumer('email-queue', emailProcessor, {
    limiter: { max: 100, duration: 60000 }
});

// Image resize is CPU-bound — limit by concurrency instead
createConsumer('image-queue', imageProcessor, {
    concurrency: 2  // only 2 at a time to avoid high CPU
});

// PDF generation has no external limit — run as fast as possible
createConsumer('pdf-queue', pdfProcessor, {
    concurrency: 10
});
```

### Why This Matters

Without rate limiting, high job volume causes cascading failures — jobs fail,
retries pile up, backoff delays grow, and the queue becomes a backlog that
takes hours to drain. Rate limiting prevents the failure in the first place.

---

## 8. Metrics Collection

### The Problem

Right now you have no idea:
- How many jobs are waiting at any given time
- How long jobs typically take to process
- Whether the queue is growing faster than the worker can drain it
- When to add more worker replicas

### The Solution

Periodically collect queue counts and push them to your metrics platform.
The most important metric is **queue depth** (jobs waiting) — if it keeps
growing, your worker is too slow.

```typescript
// src/bullmq/metrics.ts
import { Queue } from 'bullmq';
import { connection } from './connection.js';

export async function collectQueueMetrics(queueName: string) {
    const queue  = new Queue(queueName, { connection });

    const counts = await queue.getJobCounts(
        'waiting',
        'active',
        'completed',
        'failed',
        'delayed',
        'paused',
    );

    // Log it — replace with your metrics platform (Prometheus, Datadog, etc.)
    console.log(`[metrics] ${queueName}`, counts);
    // → { waiting: 12, active: 3, completed: 4521, failed: 7, delayed: 2 }

    await queue.close();
    return counts;
}

// Call this on a schedule — every 30 seconds is typical
setInterval(() => collectQueueMetrics('email-queue'), 30_000);
```

Key metrics to watch and what they mean:

| Metric | Warning Sign | Action |
|---|---|---|
| `waiting` growing over time | Worker is too slow | Add more worker replicas |
| `failed` spiking | Downstream service is failing | Check email/API provider status |
| `active` stuck at max concurrency | Worker is saturated | Scale up concurrency or replicas |
| `delayed` growing | Jobs are being scheduled but not run | Check worker is running |

---

## Implementation Checklist

Work through these in order — each one builds on the previous:

```
[ ] 1. Add environment variables to connection.ts
[ ]    Create .env file for local dev
[ ]    Add .env to .gitignore

[ ] 2. Create src/worker.ts as standalone entry point
[ ]    Add "worker" script to package.json
[ ]    Remove close() calls from worker.ts (it runs forever)

[ ] 3. Install Bull Board
[ ]    Create src/dashboard.ts
[ ]    Register all queues in the dashboard

[ ] 4. Create src/bullmq/jobs.ts with JobPayloads interface
[ ]    Update producer.ts addJob() to use generics
[ ]    Fix any type errors in index.ts and worker.ts

[ ] 5. Add dead letter queue handling in consumer.ts
[ ]    Create 'dead-letter-queue' consumer to log/store dead jobs
[ ]    Add team alert notification (Slack webhook or email)

[ ] 6. Add health check HTTP server to worker.ts
[ ]    Test it responds 200 when healthy
[ ]    Add liveness/readiness probes to Kubernetes config

[ ] 7. Add limiter to createConsumer() options
[ ]    Set limit based on your email provider's rate limit
[ ]    Test with high job volume

[ ] 8. Create src/bullmq/metrics.ts
[ ]    Add setInterval to collect counts
[ ]    Connect to Prometheus, Datadog, or CloudWatch
```

---

## Quick Reference — What Each File Becomes

```
src/
├── index.ts          ← development only (all-in-one demo)
├── server.ts         ← production web server (adds jobs, no worker)
├── worker.ts         ← production worker (processes jobs, no HTTP)
├── dashboard.ts      ← Bull Board UI (run separately)
└── bullmq/
    ├── connection.ts ← reads from process.env
    ├── producer.ts   ← typed addJob<T extends JobName>()
    ├── consumer.ts   ← + rate limiter, dead letter queue
    ├── events.ts     ← unchanged
    ├── jobs.ts       ← NEW: JobPayloads interface + JobName type
    └── metrics.ts    ← NEW: collectQueueMetrics()
```
