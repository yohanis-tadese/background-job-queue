import { PrismaClient } from '@prisma/client';

// Singleton pattern — one Prisma client for the entire process.
// Without this, each import would open a new database connection pool,
// eventually exhausting PostgreSQL's connection limit.
const globalForPrisma = globalThis as unknown as { prisma: PrismaClient };

export const prisma =
    globalForPrisma.prisma ??
    new PrismaClient({
        log: process.env.NODE_ENV === 'development'
            ? ['error', 'warn']
            : ['error'],
    });

if (process.env.NODE_ENV !== 'production') {
    globalForPrisma.prisma = prisma;
}
