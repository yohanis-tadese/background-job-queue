import { Job } from 'bullmq';

/**
 * emailProcessor
 *
 * Business logic for every email job type.
 * Imported by worker.ts (production) and index.ts (dev demo) — defined once.
 *
 * To add a new email job: add a new `if` block here.
 */
export async function emailProcessor(job: Job) {
    await job.updateProgress(0);

    if (job.name === 'send-welcome-email') {
        const { to, name, subject } = job.data;
        console.log(`  [processor] Welcome email → ${to}`);
        await job.updateProgress(50);
        await simulateEmailSend({ to, subject, body: `Hi ${name}, welcome to the app!` });
        await job.updateProgress(100);
        return { sent: true, to, sentAt: new Date().toISOString() };
    }

    if (job.name === 'send-reset-email') {
        const { to, token, subject } = job.data;
        console.log(`  [processor] Reset email   → ${to}`);
        await simulateEmailSend({
            to,
            subject,
            body: `Your password reset token: ${token}. Expires in 15 minutes.`,
        });
        return { sent: true, to, sentAt: new Date().toISOString() };
    }

    throw new Error(`Unknown job type: "${job.name}". Add a handler in email.processor.ts.`);
}

// Replace with your real email provider (Resend, SendGrid, AWS SES, etc.)
// e.g. await resend.emails.send({ from: 'no-reply@app.com', to, subject, html: body })
async function simulateEmailSend(opts: { to: string; subject: string; body: string }) {
    await new Promise((r) => setTimeout(r, 400));
    console.log(`  [processor]   delivered: "${opts.subject}" to ${opts.to}`);
}
