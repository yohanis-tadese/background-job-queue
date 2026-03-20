import { Router, Request, Response } from 'express';
import {
    sendWelcomeEmail,
    sendPasswordReset,
    getJobHistory,
    getFailedJobs,
} from '../controllers/email.controller.js';

export const emailRoutes = Router();

// POST /api/email/welcome
// Body: { name: string, to: string }
emailRoutes.post('/welcome', async (req: Request, res: Response) => {
    const { name, to } = req.body;
    const job = await sendWelcomeEmail(name, to);
    res.status(202).json({ jobId: job.id, status: 'queued' });
});

// POST /api/email/reset
// Body: { to: string, token: string }
emailRoutes.post('/reset', async (req: Request, res: Response) => {
    const { to, token } = req.body;
    const job = await sendPasswordReset(to, token);
    res.status(202).json({ jobId: job.id, status: 'queued' });
});

// GET /api/email/history
emailRoutes.get('/history', async (_req: Request, res: Response) => {
    const records = await getJobHistory();
    res.json(records);
});

// GET /api/email/failed
emailRoutes.get('/failed', async (_req: Request, res: Response) => {
    const records = await getFailedJobs();
    res.json(records);
});
