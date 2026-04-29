import express, { Application, Request, Response } from 'express';
import cors from 'cors';
import helmet from 'helmet';
import dotenv from 'dotenv';
import { testDatabaseConnection } from './infrastructure/database';
import executeRoutes from './routes/execute';

dotenv.config();

const app: Application = express();
const PORT = process.env.PORT || 3001;

// Middleware
app.use(cors());
app.use(helmet());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Routes
app.use('/api', executeRoutes);

// Health Check
app.get('/health', (req: Request, res: Response) => {
  res.status(200).json({
    status: 'OK',
    message: 'Backend is running',
    timestamp: new Date().toISOString(),
  });
});

// Welcome Route
app.get('/api', (req: Request, res: Response) => {
  res.status(200).json({
    message: 'Welcome to Botón Pago API',
    version: '1.0.0',
    endpoints: {
      health: '/health',
      articles: '/api/articles',
      users: '/api/users',
      categories: '/api/categories',
    },
  });
});

// 404 Handler
app.use((req: Request, res: Response) => {
  res.status(404).json({
    error: 'Not Found',
    path: req.path,
    method: req.method,
  });
});

// Error Handler
app.use((err: any, req: Request, res: Response) => {
  console.error('Error:', err);
  res.status(err.status || 500).json({
    error: err.message || 'Internal Server Error',
  });
});

// Start Server
app.listen(PORT, async () => {
  const nodeEnv = process.env.NODE_ENV || 'development';
  console.log(`\nStarting server on port ${PORT}...`);

  const connected = await testDatabaseConnection();
  if (!connected) {
    console.warn('⚠ No se pudo conectar a la base de datos. Revisa DATABASE_URL y estado de PostgreSQL.');
  }

  console.log(`\n╔════════════════════════════════════════╗\n║  🔌 BOTÓN PAGO API - Backend           ║\n╚════════════════════════════════════════╝\n\n📍 Server:   http://localhost:${PORT}\n🌍 API:      http://localhost:${PORT}/api\n💓 Health:   http://localhost:${PORT}/health\n🔧 Env:      ${nodeEnv}\n⏰ Started:   ${new Date().toLocaleTimeString()}\n\n✅ Ready to accept requests...\n`);
});

export default app;
