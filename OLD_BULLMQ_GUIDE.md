# BullMQ — Beginner's Complete Flow Guide

> **What is BullMQ?**
> BullMQ is a job queue library for Node.js. It lets one part of your app say
> _"do this work later"_, and another part actually does that work — reliably,
> even if the server crashes in between.

---

## The Big Picture (Read This First)

Imagine a **restaurant**:

| Restaurant | BullMQ |
|---|---|
| Waiter writes an order ticket | **Producer** adds a job |
| The ticket board (kitchen window) | **Queue** (stored in Redis) |
| Chef reads and cooks the order | **Worker / Consumer** processes the job |
| Manager watches the kitchen | **QueueEvents** listens to everything |
| Fridge that holds tickets safely | **Redis** (the database behind it all) |

The key insight: the **waiter** and **chef** work independently.
The waiter doesn't wait for the food to be cooked. He hands off the ticket and moves on.

---

## The 4 Core Pieces

```
┌─────────────┐        ┌─────────────────────────────────┐        ┌──────────────┐
│             │  add   │                                 │  poll  │              │
│  Producer   │ ──────>│   Queue  (lives inside Redis)   │ <───── │    Worker    │
│  (your app) │        │                                 │        │  (consumer)  │
└─────────────┘        └─────────────────────────────────┘        └──────────────┘
                                        │
                                        │ events
                                        ▼
                               ┌─────────────────┐
                               │   QueueEvents   │
                               │  (observer/log) │
                               └─────────────────┘
```

### 1. Queue
- A **named list** of jobs stored in Redis.
- It is just a name + a Redis connection. It does not process anything.
- Multiple producers can add to the same queue.
- Multiple workers can read from the same queue.

### 2. Producer
- The code that **creates and adds jobs** to the queue.
- Each job has a **name** (what type of job it is) and **data** (the payload).
- After adding, the producer's job is done. It moves on immediately.

### 3. Worker (Consumer)
- A **long-running process** that watches the queue.
- When a job appears, the worker picks it up, runs your function, and marks the job complete.
- If the function throws an error, BullMQ marks the job as failed and can retry it automatically.

### 4. QueueEvents
- Connects to Redis and **streams events** about every job.
- You can listen from a completely separate process or machine.
- Used for logging, dashboards, sending notifications, chaining jobs, etc.

---

## The Complete Job Lifecycle

This is every state a job moves through, in order:

```
Producer calls queue.add()
         │
         ▼
    ┌─────────┐
    │ waiting │  ← Job is sitting in Redis, waiting for a free worker
    └────┬────┘
         │  Worker picks it up
         ▼
    ┌────────┐
    │ active │  ← Worker is running your function right now
    └────┬───┘
         │
    ┌────┴────────────────────┐
    │                         │
    ▼                         ▼
┌───────────┐           ┌────────┐
│ completed │           │ failed │  ← Your function threw an error
└───────────┘           └───┬────┘
                            │  (if retries configured)
                            ▼
                      ┌──────────┐
                      │ delayed  │  ← Waiting before next retry attempt
                      └──────────┘
```

Each state change fires an event on `QueueEvents`.

---

## Your Project File by File

```
bullmq-1/
├── index.ts          ← All-in-one demo (run this to test everything)
├── src/
│   ├── producer.ts   ← Only adds jobs, then exits
│   ├── consumer.ts   ← Long-running, processes jobs
│   └── events.ts     ← Long-running, logs all events
└── BULLMQ_GUIDE.md   ← This file
```

---

## Step-by-Step: What Happens When You Run `npm run dev`

Here is `index.ts` broken down line by line with explanation:

### Step 1 — Define the queue name and Redis connection

```typescript
const QUEUE_NAME = 'email-queue';

const connection = { host: 'localhost', port: 6379, maxRetriesPerRequest: null };
```

- `QUEUE_NAME` must be **identical** in producer, worker, and event listener.
  If the names don't match, they talk to different queues and nothing works.
- `connection` is just a plain object with Redis address.
  BullMQ uses its own internal Redis client, so you don't need to import ioredis yourself.

---

### Step 2 — Start the Event Listener (QueueEvents)

```typescript
const queueEvents = new QueueEvents(QUEUE_NAME, { connection });

queueEvents.on('waiting',   ({ jobId }) => console.log(`Job ${jobId} queued`));
queueEvents.on('active',    ({ jobId }) => console.log(`Job ${jobId} started`));
queueEvents.on('completed', ({ jobId, returnvalue }) => console.log(`Job ${jobId} done`));
queueEvents.on('failed',    ({ jobId, failedReason }) => console.log(`Job ${jobId} failed`));
```

- We attach this **first** so we don't miss events that fire immediately after adding jobs.
- `QueueEvents` reads from Redis pub/sub — it does not do any job processing.
- `returnvalue` is whatever your worker function returned (as a JSON string).

---

### Step 3 — Start the Worker (Consumer)

```typescript
const worker = new Worker(QUEUE_NAME, async (job: Job) => {
    console.log(`Processing [${job.name}] id=${job.id}`);
    console.log(`To: ${job.data.to} | Subject: ${job.data.subject}`);

    await new Promise((resolve) => setTimeout(resolve, 600)); // simulate work

    return { sent: true, to: job.data.to };
}, { connection });
```

- The second argument is your **processor function** — this is where your actual logic lives.
- `job.name` → the job type name (e.g. `'send-welcome-email'`)
- `job.data` → the payload you passed when adding the job
- Whatever you `return` from this function becomes the job's result
- The worker runs this function **once per job**, automatically

---

### Step 4 — Add Jobs (Producer)

```typescript
const queue = new Queue(QUEUE_NAME, { connection });

const [job1, job2] = await Promise.all([
    queue.add('send-welcome-email', { to: 'alice@example.com', subject: 'Welcome!' }),
    queue.add('send-reset-email',   { to: 'bob@example.com',   subject: 'Reset your password' }),
]);
```

- `queue.add(name, data)` — name is the job type, data is any object you want
- `queue.add()` returns a `Job` object with an `id` assigned by BullMQ
- Both jobs are added at the same time with `Promise.all`
- At this point the jobs are in Redis and the worker will pick them up almost immediately

---

### Step 5 — Wait for Both Jobs to Finish

```typescript
await Promise.all([
    job1.waitUntilFinished(queueEvents),
    job2.waitUntilFinished(queueEvents),
]);
```

- `waitUntilFinished()` blocks until the job transitions to `completed` or `failed`
- It listens via `QueueEvents` — that's why you need to pass it in
- This is useful for tests and scripts. In a web server you usually don't wait — you fire and forget.

---

### Step 6 — Clean Up

```typescript
await worker.close();
await queue.close();
await queueEvents.close();
```

- Always close connections when done, or the process will hang.
- In production (long-running servers), you usually never close these — they stay open forever.

---

## What the Console Output Looks Like

```
Adding jobs to the queue...

Added: job1=1, job2=2

[waiting]   Job 1 queued
[waiting]   Job 2 queued
[active]    Job 1 started

  Processing [send-welcome-email] id=1
  To: alice@example.com | Subject: Welcome!

[completed] Job 1 done — result: {"sent":true,"to":"alice@example.com"}
[active]    Job 2 started

  Processing [send-reset-email] id=2
  To: bob@example.com | Subject: Reset your password

[completed] Job 2 done — result: {"sent":true,"to":"bob@example.com"}

All jobs finished. Shutting down.
```

Notice the order:
1. Both jobs enter `waiting` almost instantly
2. The worker takes them one at a time (`active`)
3. Each job moves to `completed` after the processor function returns
4. Events fire in real time as the state changes

---

## How to Run

### For learning and development — 1 terminal is all you need

```bash
npm run dev
```

`index.ts` already combines all three pieces (events + consumer + producer) in a single
process. You see everything — the events firing, the worker processing, the results —
in one place. This is the best way to learn and experiment.

### Why do the 3 separate scripts even exist?

```bash
npm run events      # Terminal 1
npm run consumer    # Terminal 2
npm run producer    # Terminal 3
```

These exist to **simulate how it works in production**, where each piece runs as a
separate process (or even separate machines):

| Script | Role in production |
|---|---|
| `npm run consumer` | A background microservice running 24/7, processing jobs |
| `npm run producer` | Called inside your web server when a user triggers an action |
| `npm run events` | A logging/monitoring service watching everything |

The 3-terminal setup teaches you that BullMQ pieces are **independent** — they only
communicate through Redis, not direct function calls. But for learning, `npm run dev`
gives you the same flow in one window.

> **Rule of thumb:** Use `npm run dev` while learning. Split into separate processes
> only when you're ready to deploy to production.

---

## Common Beginner Mistakes

| Mistake | What Happens | Fix |
|---|---|---|
| Queue name doesn't match | Jobs are added to one queue, worker watches a different one | Use a shared constant for the name |
| Worker process not running | Jobs pile up in Redis forever (this is actually fine — they wait) | Start the consumer |
| Redis not running | `ECONNREFUSED` error immediately | Start Redis first |
| Forgetting `maxRetriesPerRequest: null` | BullMQ throws an error on startup | Always include it in the connection config |
| Closing connections too early | Worker stops before processing all jobs | Wait for jobs to finish before closing |

---

## Key Takeaway

```
Producer  →  Queue (Redis)  →  Worker  →  QueueEvents
  adds           stores          runs        observes
```

BullMQ's power is in **decoupling** work from the request that triggers it.
Your user gets a fast response. The slow work (email, PDF, resize image) happens in the background — reliably, with retries, with logging — completely separate from your web server.
