# BullMQ Production Guide

> Complete explanation of the current codebase — every file, every decision,
> and the full flow from HTTP request to database record.

---

## Project Structure

```
src/
├── index.ts                        ← Express web server (starts HTTP server)
├── worker.ts                       ← Background worker (runs separately, stays alive)
│
├── bullmq/                         ← Reusable queue infrastructure
│   ├── connection.ts               ← Redis config read from .env
│   ├── producer.ts                 ← createProducer()
│   ├── consumer.ts                 ← createConsumer()
│   ├── events.ts                   ← createQueueEvents()
│   └── tracker.ts                  ← Writes every job event to PostgreSQL
│
├── processors/                     ← Business logic — what each job actually does
│   └── email.processor.ts          ← emailProcessor() — the only email logic
│
├── controllers/                    ← Called by routes — queue jobs + query DB
│   └── email.controller.ts
│
├── routes/                         ← HTTP endpoints
│   └── email.routes.ts
│
└── db/
    └── client.ts                   ← Prisma singleton (one DB connection)

prisma/
└── schema.prisma                   ← JobRecord model + JobStatus enum
```

---

## Architecture — How Every Layer Connects

```
HTTP Request
     │
     ▼
routes/email.routes.ts          ← receives the request, calls controller
     │
     ▼
controllers/email.controller.ts ← queues job + creates DB record
     │                   │
     │                   ▼
     │            prisma.jobRecord.create()   → PostgreSQL { status: QUEUED }
     │
     ▼
bullmq/producer.ts              ← queue.add() → job enters Redis
     │
     │   (worker.ts polling Redis independently)
     ▼
bullmq/tracker.ts               ← QueueEvents fires 'active'
     │                            → prisma.update { status: PROCESSING }
     │
     ▼
processors/email.processor.ts  ← runs the actual job logic
     │
     ▼
bullmq/tracker.ts               ← QueueEvents fires 'completed' or 'failed'
                                  → prisma.update { status: COMPLETED/FAILED }
```

**The key rule:** the HTTP response (`202 Accepted`) is returned immediately
after the job is queued — not after the email is sent. The worker runs
completely independently in a separate process.

---

## Layer 1 — `bullmq/connection.ts`

Single shared Redis config. Every BullMQ class (Queue, Worker, QueueEvents)
uses this same object.

```typescript
export const connection = {
    host:     process.env.REDIS_HOST     ?? 'localhost',
    port:     Number(process.env.REDIS_PORT ?? 6379),
    password: process.env.REDIS_PASSWORD  || undefined,
    tls:      process.env.REDIS_TLS === 'true' ? {} : undefined,
    maxRetriesPerRequest: null as null,
};
```

**Why a plain object instead of `new Redis()`?**
BullMQ v5+ ships its own internal copy of ioredis inside
`node_modules/bullmq/node_modules/ioredis`. If you instantiate `new Redis()`
from the top-level ioredis package, TypeScript sees two structurally
incompatible types and throws a compile error. A plain config object avoids
this — BullMQ creates its own connection internally.

**Why `maxRetriesPerRequest: null`?**
BullMQ uses long-running blocking Redis commands (`BLPOP`, `XREAD`). ioredis's
default `maxRetriesPerRequest` would abort these with a timeout error. Setting
it to `null` disables the timeout for BullMQ's connections.

---

## Layer 2 — `bullmq/producer.ts`

```typescript
export function createProducer(queueName: string) {
    const queue = new Queue(queueName, {
        connection,
        defaultJobOptions: {
            attempts: 3,
            backoff: { type: 'exponential', delay: 1000 },
            removeOnComplete: { count: 100 },
            removeOnFail:     { count: 500 },
        },
    });

    return {
        addJob: (name: string, data: unknown, options?: JobsOptions) =>
            queue.add(name, data, options),
        close: () => queue.close(),
        queue,
    };
}
```

**Default options applied to every job:**

| Option | Value | Reason |
|---|---|---|
| `attempts: 3` | 3 total tries | A network blip should not permanently lose a job |
| `backoff: exponential` | 1s → 2s → 4s | Gives the failing service time to recover |
| `removeOnComplete: 100` | Keep last 100 | Prevents Redis filling up with old completed jobs |
| `removeOnFail: 500` | Keep last 500 | More failures kept for debugging purposes |

**Per-job overrides** via the third argument:

```typescript
// Skip the queue — process before everything else
producer.addJob('send-reset-email', data, { priority: 1 });

// Schedule for the future — starts after 5 minutes
producer.addJob('send-reminder', data, { delay: 5 * 60 * 1000 });

// Override retry count for this specific job
producer.addJob('critical-job', data, { attempts: 10 });

// Deduplicate — if this jobId already exists, skip adding
producer.addJob('daily-report', data, { jobId: 'report-2024-01-15' });
```

---

## Layer 3 — `bullmq/consumer.ts`

```typescript
export function createConsumer(
    queueName: string,
    processor: (job: Job) => Promise<unknown>,
    options?: Partial<WorkerOptions>,
) {
    const worker = new Worker(queueName, processor, {
        connection,
        concurrency: 5,
        ...options,
    });

    worker.on('failed', (job, err) => {
        const isLastAttempt = (job?.attemptsMade ?? 0) >= (job?.opts.attempts ?? 1);
        if (isLastAttempt) {
            console.error(`PERMANENTLY FAILED ...`);
        } else {
            console.warn(`Retrying ...`);
        }
    });

    worker.on('stalled', (jobId) => { ... });

    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT',  () => shutdown('SIGINT'));

    return worker;
}
```

**What each production feature does:**

**Concurrency (`concurrency: 5`)**
Without this, the worker processes one job at a time. With `concurrency: 5`,
it runs 5 jobs in parallel — critical for I/O-heavy work like calling email
APIs or querying databases. Override per queue:
```typescript
createConsumer('email-queue',  processor, { concurrency: 20 }); // I/O heavy
createConsumer('video-encode', processor, { concurrency: 2  }); // CPU heavy
```

**Smart failure logging**
`failed` fires on every attempt including ones that will retry. Without
distinguishing retrying from permanently failed, you'd get false alerts on
every first-attempt failure. The code only logs `ERROR` on the last attempt.

**Stalled job detection**
A stalled job means the worker picked it up but never sent a heartbeat — the
process was killed or the machine crashed mid-job. BullMQ detects this
automatically via Redis TTL and requeues it. The `stalled` event just surfaces
the warning.

**Graceful shutdown (SIGTERM / SIGINT)**
When Kubernetes, Docker, or Ctrl+C sends a stop signal:
- Without graceful shutdown → process dies instantly, active jobs become stalled
- With graceful shutdown → worker stops accepting new jobs, finishes current ones, exits cleanly

---

## Layer 4 — `bullmq/events.ts`

```typescript
export function createQueueEvents(queueName: string) {
    const queueEvents = new QueueEvents(queueName, { connection });

    queueEvents.on('waiting',          ...);  // job in Redis, no worker yet
    queueEvents.on('active',           ...);  // worker started processing
    queueEvents.on('progress',         ...);  // job called updateProgress()
    queueEvents.on('completed',        ...);  // processor returned successfully
    queueEvents.on('failed',           ...);  // one attempt failed (may retry)
    queueEvents.on('retries-exhausted',...);  // all attempts used up — job is dead
    queueEvents.on('stalled',          ...);  // worker crashed mid-job
    queueEvents.on('delayed',          ...);  // job scheduled for the future
    queueEvents.on('removed',          ...);  // job manually deleted

    return queueEvents;
}
```

**`QueueEvents` vs `worker.on()`:**

| | `worker.on('completed', ...)` | `queueEvents.on('completed', ...)` |
|---|---|---|
| Scope | Only fires in the same process | Fires in any process on any machine |
| Use case | Worker-local side effects | Logging, monitoring, chaining queues |
| Works from server? | No | Yes |

`QueueEvents` uses Redis pub/sub — it receives a stream of events regardless
of which process or machine the worker is running on. This is how `tracker.ts`
updates the database from the server process even though the worker runs separately.

---

## Layer 5 — `bullmq/tracker.ts`

This is the bridge between BullMQ and PostgreSQL. It subscribes to `QueueEvents`
and writes every state change to the `JobRecord` table.

```typescript
export function trackQueueInDB(queueName: string) {
    const queueEvents = new QueueEvents(queueName, { connection });

    queueEvents.on('active', async ({ jobId }) => {
        await prisma.jobRecord.update({
            where: { jobId },
            data:  { status: PROCESSING, startedAt: new Date(), error: null },
        });
    });

    queueEvents.on('failed', async ({ jobId, failedReason }) => {
        await prisma.jobRecord.update({
            where: { jobId },
            data:  { status: FAILED, error: failedReason, attemptsMade: { increment: 1 } },
        });
    });

    queueEvents.on('retries-exhausted', async ({ jobId, attemptsMade }) => {
        await prisma.jobRecord.update({
            where: { jobId },
            data:  { status: DEAD, attemptsMade: Number(attemptsMade) },
        });
    });

    queueEvents.on('completed', async ({ jobId, returnvalue }) => {
        await prisma.jobRecord.update({
            where: { jobId },
            data:  {
                status:      COMPLETED,
                result:      typeof returnvalue === 'string'
                                 ? JSON.parse(returnvalue)
                                 : returnvalue,
                completedAt: new Date(),
            },
        });
    });

    return queueEvents;
}
```

**Why `typeof returnvalue === 'string' ? JSON.parse(...) : returnvalue`?**
BullMQ's QueueEvents `completed` event passes `returnvalue` as either a JSON
string or an already-parsed object depending on the version. This guard handles
both cases safely — without it, calling `JSON.parse()` on an object produces
the string `"[object Object]"` which throws a `SyntaxError`.

**Full DB state machine:**

```
controller creates record   → { status: QUEUED    }
tracker 'active'    fires   → { status: PROCESSING, startedAt }
tracker 'failed'    fires   → { status: FAILED,    error, attemptsMade++ }
tracker 'active'    fires   → { status: PROCESSING }  ← on retry
tracker 'completed' fires   → { status: COMPLETED, result, completedAt }
       OR
tracker 'retries-exhausted' → { status: DEAD }
```

---

## Layer 6 — `processors/email.processor.ts`

The single source of truth for email job logic. Both `worker.ts` and any other
entry point import this — the logic is never duplicated.

```typescript
export async function emailProcessor(job: Job) {
    await job.updateProgress(0);

    if (job.name === 'send-welcome-email') {
        const { to, name, subject } = job.data;
        await job.updateProgress(50);
        await simulateEmailSend({ to, subject, body: `Hi ${name}, welcome!` });
        await job.updateProgress(100);
        return { sent: true, to, sentAt: new Date().toISOString() };
    }

    if (job.name === 'send-reset-email') {
        const { to, token, subject } = job.data;
        await simulateEmailSend({ to, subject, body: `Token: ${token}` });
        return { sent: true, to, sentAt: new Date().toISOString() };
    }

    throw new Error(`Unknown job type: "${job.name}"`);
}
```

**`job.updateProgress(n)`** — broadcasts a `progress` event (0–100) that
`QueueEvents` surfaces as a `progress` event. Useful for long jobs where you
want to track completion percentage in a dashboard.

**`throw new Error(...)`** — any uncaught throw triggers BullMQ's retry
mechanism. The job goes back to `waiting` after the backoff delay, up to
the configured `attempts` limit.

**`return { ... }`** — whatever you return is serialized and stored as
`returnvalue` in Redis. The tracker then saves it to the `result` column
in PostgreSQL.

**To add a new email job type**, add one `if` block here:
```typescript
if (job.name === 'send-invoice-email') {
    const { to, invoiceId } = job.data;
    // your logic
    return { sent: true };
}
```

---

## Layer 7 — `controllers/email.controller.ts`

The only place in the application that is allowed to call `producer.addJob()`.
Every job addition also creates a matching `JobRecord` in the database.

```typescript
const producer = createProducer('email-queue');
// One producer instance shared across all functions
// Never create a new producer per request — each opens a Redis connection

export async function sendWelcomeEmail(name: string, to: string) {
    const payload = { to, name, subject: `Welcome to the app, ${name}!` };

    const job = await producer.addJob('send-welcome-email', payload);
    await prisma.jobRecord.create({ data: { jobId: job.id!, ... } });

    return job;
}

export async function sendPasswordReset(to: string, token: string) {
    // priority: 1 — reset emails jump ahead of welcome emails
    // Users waiting for a reset link are more time-sensitive
    const job = await producer.addJob('send-reset-email', payload, { priority: 1 });
    await prisma.jobRecord.create({ data: { jobId: job.id!, ... } });

    return job;
}

export async function getJobHistory(limit = 20) { ... }  // for /api/email/history
export async function getFailedJobs()           { ... }  // for /api/email/failed
```

---

## Layer 8 — `routes/email.routes.ts`

Thin HTTP layer. No logic here — just receives request, calls controller,
returns response.

```typescript
// POST /api/email/welcome  { name, to }
emailRoutes.post('/welcome', async (req, res) => {
    const job = await sendWelcomeEmail(req.body.name, req.body.to);
    res.status(202).json({ jobId: job.id, status: 'queued' });
});

// POST /api/email/reset    { to, token }
emailRoutes.post('/reset', async (req, res) => {
    const job = await sendPasswordReset(req.body.to, req.body.token);
    res.status(202).json({ jobId: job.id, status: 'queued' });
});

// GET /api/email/history   → last 20 JobRecords from PostgreSQL
// GET /api/email/failed    → all FAILED + DEAD records
```

**Why `202 Accepted` and not `200 OK`?**
`200 OK` means the work is done. `202 Accepted` means the request was received
and the work has been queued — it is the correct HTTP status for async operations.

---

## Layer 9 — `worker.ts`

The standalone background process. 13 lines — wires the layers together and
stays alive.

```typescript
import 'dotenv/config';
import { createConsumer }  from './bullmq/consumer.js';
import { trackQueueInDB }  from './bullmq/tracker.js';
import { emailProcessor }  from './processors/email.processor.js';
import { prisma }          from './db/client.js';

trackQueueInDB('email-queue');              // DB updates on every event
createConsumer('email-queue', emailProcessor); // process jobs

// Prisma disconnect on shutdown (consumer handles the BullMQ worker itself)
process.on('SIGTERM', async () => { await prisma.$disconnect(); });
process.on('SIGINT',  async () => { await prisma.$disconnect(); });
```

---

## Layer 10 — `index.ts`

The web server. 10 lines — starts Express and mounts routes.

```typescript
import 'dotenv/config';
import express         from 'express';
import { emailRoutes } from './routes/email.routes.js';

const app = express();
app.use(express.json());
app.use('/api/email', emailRoutes);
app.listen(process.env.PORT ?? 3000);
```

Nothing about BullMQ, Redis, Prisma, or processors here. The server only
knows about HTTP.

---

## Database — `prisma/schema.prisma`

```prisma
model JobRecord {
  id           String    @id @default(uuid())
  jobId        String    @unique    // BullMQ's auto-assigned job ID
  queueName    String               // 'email-queue'
  jobName      String               // 'send-welcome-email'
  status       JobStatus @default(QUEUED)
  payload      Json                 // input data { to, name, subject }
  result       Json?                // { sent: true, to, sentAt } on success
  error        String?              // error message on failure
  attemptsMade Int       @default(0)
  createdAt    DateTime  @default(now())
  updatedAt    DateTime  @updatedAt
  startedAt    DateTime?
  completedAt  DateTime?
  failedAt     DateTime?
}

enum JobStatus { QUEUED  PROCESSING  COMPLETED  FAILED  DEAD }
```

**Indexes explained:**

| Index | Query it enables |
|---|---|
| `@@index([queueName])` | `WHERE queueName = 'email-queue'` |
| `@@index([status])` | `WHERE status = 'FAILED'` |
| `@@index([createdAt])` | `ORDER BY createdAt DESC` (history page) |
| `@@index([jobName])` | `WHERE jobName = 'send-welcome-email'` |

---

## Complete Job Lifecycle

From the moment a request hits the server to the final DB record:

```
1. POST /api/email/welcome arrives
        │
2. email.routes.ts calls sendWelcomeEmail()
        │
3. email.controller.ts:
        ├── producer.addJob()         Redis: job enters 'waiting' list
        └── prisma.jobRecord.create() DB:    { status: QUEUED, createdAt }
        │
        │   HTTP response sent: 202 { jobId: "1", status: "queued" }
        │
4. worker.ts (separate process) polls Redis
   Worker picks up job
        │
5. tracker.ts receives 'active' event via QueueEvents
        └── prisma.jobRecord.update() DB:    { status: PROCESSING, startedAt }
        │
6. emailProcessor() runs:
        ├── job.updateProgress(0)
        ├── job.updateProgress(50)
        ├── simulateEmailSend()       your real email API call goes here
        ├── job.updateProgress(100)
        └── return { sent: true, to, sentAt }
        │
7. tracker.ts receives 'completed' event
        └── prisma.jobRecord.update() DB:    { status: COMPLETED, result, completedAt }

Final DB row:
{
  jobId:        "1",
  jobName:      "send-welcome-email",
  status:       "COMPLETED",
  payload:      { to: "alice@example.com", name: "Alice", subject: "Welcome..." },
  result:       { sent: true, to: "alice@example.com", sentAt: "2024-01-15T..." },
  error:        null,
  attemptsMade: 0,
  createdAt:    "2024-01-15T10:00:00.000Z",
  startedAt:    "2024-01-15T10:00:00.100Z",
  completedAt:  "2024-01-15T10:00:00.500Z"
}
```

---

## Retry Flow (Job Fails Then Recovers)

```
producer.addJob()              DB: { status: QUEUED }
        │
Worker picks up (attempt 1)    DB: { status: PROCESSING }
emailProcessor() throws Error  DB: { status: FAILED, error: "...", attemptsMade: 1 }
        │
BullMQ waits 1s (backoff)
        │
Worker picks up (attempt 2)    DB: { status: PROCESSING, error: null }
emailProcessor() throws Error  DB: { status: FAILED, error: "...", attemptsMade: 2 }
        │
BullMQ waits 2s (backoff)
        │
Worker picks up (attempt 3)    DB: { status: PROCESSING, error: null }
emailProcessor() returns       DB: { status: COMPLETED, result: {...} }

   — OR if attempt 3 also fails —

emailProcessor() throws Error  DB: { status: FAILED, attemptsMade: 3 }
retries-exhausted fires        DB: { status: DEAD }
```

---

## Adding a New Queue

The `bullmq/` layer is fully reusable. Adding a new queue requires:

1. **A new processor** in `src/processors/`
```typescript
// src/processors/pdf.processor.ts
export async function pdfProcessor(job: Job) {
    if (job.name === 'generate-invoice-pdf') {
        // your logic
        return { fileUrl: '...' };
    }
    throw new Error(`Unknown job: ${job.name}`);
}
```

2. **New functions in a controller**
```typescript
// src/controllers/pdf.controller.ts
const producer = createProducer('pdf-queue');

export async function generateInvoicePdf(reportId: string) {
    const job = await producer.addJob('generate-invoice-pdf', { reportId });
    await prisma.jobRecord.create({ data: { jobId: job.id!, queueName: 'pdf-queue', ... } });
    return job;
}
```

3. **New routes**
```typescript
pdfRoutes.post('/invoice', async (req, res) => {
    const job = await generateInvoicePdf(req.body.reportId);
    res.status(202).json({ jobId: job.id });
});
```

4. **Register in `worker.ts`**
```typescript
trackQueueInDB('pdf-queue');
createConsumer('pdf-queue', pdfProcessor);
```

The `bullmq/` folder — `connection.ts`, `producer.ts`, `consumer.ts`,
`events.ts`, `tracker.ts` — needs zero changes.

---

## Running the System

```bash
# Terminal 1 — web server
yarn dev       # or: npm run dev

# Terminal 2 — background worker
yarn worker    # or: npm run worker
```

```bash
# Test endpoints
curl -X POST http://localhost:3000/api/email/welcome \
  -H "Content-Type: application/json" \
  -d '{ "name": "Alice", "to": "alice@example.com" }'

curl -X POST http://localhost:3000/api/email/reset \
  -H "Content-Type: application/json" \
  -d '{ "to": "bob@example.com", "token": "tok_abc123" }'

curl http://localhost:3000/api/email/history
curl http://localhost:3000/api/email/failed
```
