import { Pool } from 'pg';

const connectionString = process.env.DATABASE_URL;

if (!connectionString) {
  throw new Error('DATABASE_URL no está definida. Configura .env.local con PostgreSQL.');
}

// Pool único para evitar abrir conexiones múltiples en dev/Hot reload
const pool = new Pool({
  connectionString,
});

export default pool;
