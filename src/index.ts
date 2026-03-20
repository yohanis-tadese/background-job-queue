import 'dotenv/config';
import express           from 'express';
import { emailRoutes }   from './routes/email.routes.js';

const app  = express();
const PORT = process.env.PORT ?? 3000;

app.use(express.json());

// Routes
app.use('/api/email', emailRoutes);

app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});