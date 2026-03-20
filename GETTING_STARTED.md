# BullMQ + PostgreSQL + Express

> Production-ready background job queue system with full database activity tracking.
> Built with BullMQ, Express, Prisma, PostgreSQL, and Redis.

---

## Tech Stack

| Technology | Role |
|---|---|
| [BullMQ](https://docs.bullmq.io) | Job queue — adds and processes background jobs |
| [Redis](https://redis.io) | Message broker — BullMQ stores all job data here |
| [Express](https://expressjs.com) | HTTP web server |
| [Prisma](https://www.prisma.io) | ORM — reads and writes job records to PostgreSQL |
| [PostgreSQL](https://www.postgresql.org) | Database — stores full job lifecycle history |
| [TypeScript](https://www.typescriptlang.org) | Language |

---

## Prerequisites

Make sure these are installed on your machine before starting:

| Tool | Minimum Version | Check |
|---|---|---|
| Node.js | 18 | `node -v` |
| npm or yarn | npm 8 / yarn 1.22 | `npm -v` or `yarn -v` |
| PostgreSQL | 14 | `psql --version` |
| Redis | 6 | `redis-server --version` |

---

## Project Structure

```
bullmq-1/
├── .env                          ← environment variables (never commit this)
├── .gitignore
├── package.json
├── tsconfig.json
├── prisma/
│   └── schema.prisma             ← database model definition
└── src/
    ├── index.ts                  ← web server entry point (Express + routes)
    ├── worker.ts                 ← background worker entry point (runs separately)
    │
    ├── bullmq/                   ← reusable queue infrastructure
    │   ├── connection.ts         ← Redis config (reads from .env)
    │   ├── producer.ts           ← createProducer()
    │   ├── consumer.ts           ← createConsumer()
    │   ├── events.ts             ← createQueueEvents()
    │   └── tracker.ts            ← writes every job event to PostgreSQL
    │
    ├── processors/               ← job logic (what each job type actually does)
    │   └── email.processor.ts
    │
    ├── controllers/              ← business logic (called by routes)
    │   └── email.controller.ts
    │
    ├── routes/                   ← HTTP endpoints
    │   └── email.routes.ts
    │
    └── db/
        └── client.ts             ← Prisma singleton
```

---

## Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/bullmq-1.git
cd bullmq-1
```

---

## Step 1 — Install Packages

**npm**
```bash
npm install
```

**yarn**
```bash
yarn install
```

This installs everything listed in `package.json`:

| Package | Type | Purpose |
|---|---|---|
| `bullmq` | dependency | Job queue built on Redis |
| `ioredis` | dependency | Redis client (used internally by BullMQ) |
| `express` | dependency | HTTP web server |
| `@prisma/client` | dependency | PostgreSQL ORM client (runtime) |
| `dotenv` | dependency | Loads `.env` file into `process.env` |
| `typescript` | dependency | TypeScript language support |
| `ts-node-dev` | dependency | Runs TypeScript directly without a build step |
| `prisma` | devDependency | Prisma CLI — runs migrations, generates client |

---

## Step 2 — Configure Environment

Create a `.env` file at the project root:

```bash
cp .env.example .env    # if .env.example exists, or create it manually
```

Fill in the values:

```env
# Database
DATABASE_URL="postgresql://postgres:dev_jhon@localhost:5432/bullmq_server?schema=public"

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=
REDIS_TLS=false

# App
NODE_ENV=development
```

> `.env` is listed in `.gitignore` and is never pushed to GitHub.
> Each developer creates their own local copy.

Change `DATABASE_URL` to match your PostgreSQL credentials:

```
postgresql://YOUR_USERNAME:YOUR_PASSWORD@localhost:5432/bullmq_server?schema=public
```

---

## Step 3 — Create the PostgreSQL Database

The database must exist before running migrations.

**Connect to PostgreSQL:**

```bash
psql -U postgres
```

**Inside the PostgreSQL shell:**

```sql
CREATE DATABASE bullmq_server;
\q
```

**Verify the connection:**

```bash
psql -U postgres -d bullmq_server -c "\dt"
# Output: "Did not find any relations." — empty database, correct
```

---

## Step 4 — Run Database Migration

Generate the Prisma client and create the `JobRecord` table.

**npm**
```bash
npm run db:generate     # generates Prisma client from schema
npm run db:migrate      # creates the table in PostgreSQL
```

**yarn**
```bash
yarn db:generate
yarn db:migrate
```

When prompted `Enter a name for the new migration`, type:

```
init
```

**What this creates in PostgreSQL:**

```sql
CREATE TABLE "JobRecord" (
    id             TEXT PRIMARY KEY,
    "jobId"        TEXT UNIQUE NOT NULL,   -- BullMQ job ID
    "queueName"    TEXT NOT NULL,          -- e.g. 'email-queue'
    "jobName"      TEXT NOT NULL,          -- e.g. 'send-welcome-email'
    status         "JobStatus" NOT NULL,   -- QUEUED | PROCESSING | COMPLETED | FAILED | DEAD
    payload        JSONB NOT NULL,         -- input data passed to the job
    result         JSONB,                  -- return value from processor on success
    error          TEXT,                   -- error message on failure
    "attemptsMade" INT DEFAULT 0,
    "createdAt"    TIMESTAMP DEFAULT NOW(),
    "updatedAt"    TIMESTAMP,
    "startedAt"    TIMESTAMP,              -- when a worker picked it up
    "completedAt"  TIMESTAMP,              -- when it finished successfully
    "failedAt"     TIMESTAMP               -- when the last attempt failed
);
```

**Verify the table was created:**

```bash
psql -U postgres -d bullmq_server -c "\dt"
# Output:
#  Schema |    Name    | Type  |  Owner
# --------+------------+-------+----------
#  public | JobRecord  | table | postgres
```

---

## Step 5 — Start Redis

Redis must be running before starting the server or worker.

```bash
# Option A — if Redis is installed locally
redis-server

# Option B — via Docker (recommended, no installation needed)
docker run -d --name redis -p 6379:6379 redis

# Verify Redis is running
redis-cli ping
# Output: PONG
```

---

## Step 6 — Run the Application

The server and worker are **two separate processes**. Open two terminals.

**Terminal 1 — Web Server**

npm:
```bash
npm run dev
```

yarn:
```bash
yarn dev
```

Expected output:
```
Server running on http://localhost:3000
```

---

**Terminal 2 — Background Worker**

npm:
```bash
npm run worker
```

yarn:
```bash
yarn worker
```

Expected output:
```
[worker] Running — listening on "email-queue"
[worker] Press Ctrl+C to stop gracefully
```

> Both processes must run at the same time.
> The **server** receives HTTP requests and queues jobs.
> The **worker** picks up those jobs and processes them.

---

## Step 7 — Test the API

Use curl, Postman, Thunder Client, or any HTTP client.

### POST — Send a welcome email

```bash
curl -X POST http://localhost:3000/api/email/welcome \
  -H "Content-Type: application/json" \
  -d '{ "name": "Alice", "to": "alice@example.com" }'
```

Response `202 Accepted` — job is queued immediately, not yet processed:

```json
{ "jobId": "1", "status": "queued" }
```

Worker terminal output:

```
[email-queue] [waiting]      job 1 — queued
[email-queue] [active]       job 1 — processing
  [processor] Welcome email → alice@example.com
  [processor]   delivered: "Welcome to the app, Alice!" to alice@example.com
[email-queue] [completed]    job 1 — result: {"sent":true,...}
```

---

### POST — Send a password reset email

```bash
curl -X POST http://localhost:3000/api/email/reset \
  -H "Content-Type: application/json" \
  -d '{ "to": "bob@example.com", "token": "tok_abc123" }'
```

Response:

```json
{ "jobId": "2", "status": "queued" }
```

---

### GET — View all job history from the database

```bash
curl http://localhost:3000/api/email/history
```

Returns every `JobRecord` row written by the tracker:

```json
[
  {
    "jobId": "2",
    "jobName": "send-reset-email",
    "status": "COMPLETED",
    "payload": { "to": "bob@example.com", "token": "tok_abc123", "subject": "Reset your password" },
    "result": { "sent": true, "to": "bob@example.com", "sentAt": "2024-01-15T10:30:00.000Z" },
    "error": null,
    "attemptsMade": 0,
    "createdAt": "2024-01-15T10:29:59.000Z",
    "startedAt": "2024-01-15T10:29:59.200Z",
    "completedAt": "2024-01-15T10:30:00.000Z"
  }
]
```

---

### GET — View only failed jobs

```bash
curl http://localhost:3000/api/email/failed
```

---

## Full Request-to-Database Flow

What happens inside when you call `POST /api/email/welcome`:

```
POST /api/email/welcome
        │
        ▼
email.routes.ts              receives HTTP request, calls controller
        │
        ▼
email.controller.ts          sendWelcomeEmail()
  ├── producer.addJob()   →   job enters Redis          { status: waiting }
  └── prisma.create()     →   DB row created            { status: QUEUED   }
        │
        │    (worker picks it up via Redis polling)
        ▼
tracker.ts                   QueueEvents fires 'active'
  prisma.update()         →   DB row updated            { status: PROCESSING, startedAt }
        │
        ▼
email.processor.ts           emailProcessor() runs the actual logic
  simulateEmailSend()         replace with Resend / SendGrid / SES
  return { sent: true }
        │
        ▼
tracker.ts                   QueueEvents fires 'completed'
  prisma.update()         →   DB row updated            { status: COMPLETED, result, completedAt }
        │
        ▼
HTTP response already sent:  202 { jobId: "1", status: "queued" }
```

> The HTTP response is sent **immediately** after the job is queued — not after
> the email is delivered. The worker runs in the background independently.

---

## Scripts Reference

| Script | npm | yarn | What it does |
|---|---|---|---|
| Start server | `npm run dev` | `yarn dev` | Express web server on port 3000 |
| Start worker | `npm run worker` | `yarn worker` | Background job processor |
| Run migration | `npm run db:migrate` | `yarn db:migrate` | Create / update DB tables |
| Generate client | `npm run db:generate` | `yarn db:generate` | Rebuild Prisma client after schema change |
| Open DB browser | `npm run db:studio` | `yarn db:studio` | Prisma Studio at localhost:5555 |

---

## Database Browser (Optional)

Prisma Studio gives you a visual table editor in the browser:

**npm**
```bash
npm run db:studio
```

**yarn**
```bash
yarn db:studio
```

Open `http://localhost:5555` — browse and edit every `JobRecord` row directly.

---

## Common Errors and Fixes

| Error | Cause | Fix |
|---|---|---|
| `ECONNREFUSED 127.0.0.1:6379` | Redis not running | `redis-server` or `docker run -d -p 6379:6379 redis` |
| `ECONNREFUSED 127.0.0.1:5432` | PostgreSQL not running | Start the PostgreSQL service |
| `database "bullmq_server" does not exist` | Database not created | `psql -U postgres -c "CREATE DATABASE bullmq_server;"` |
| `Cannot find module '@prisma/client'` | Client not generated | `npm run db:generate` or `yarn db:generate` |
| `Table JobRecord does not exist` | Migration not run | `npm run db:migrate` or `yarn db:migrate` |
| `Invalid DATABASE_URL` | Wrong credentials in `.env` | Check username/password match your PostgreSQL setup |
| `maxRetriesPerRequest must be null` | Wrong Redis config | Ensure `maxRetriesPerRequest: null` in `connection.ts` |

---

## What to Add to `.gitignore`

Make sure these are excluded before pushing to GitHub:

```gitignore
# Environment variables — never commit secrets
.env

# Dependencies — reinstalled from package.json
node_modules/

# TypeScript build output
dist/

# Prisma generated client — regenerated from schema
node_modules/.prisma/
```
