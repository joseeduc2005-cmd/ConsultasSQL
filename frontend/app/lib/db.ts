import { Pool } from 'pg';

// Lazy pool — only throws at runtime when a query is made, not at import/build time
let _pool: Pool | null = null;

function getPool(): Pool {
  if (!_pool) {
    const connectionString = process.env.DATABASE_URL;
    if (!connectionString) {
      throw new Error('DATABASE_URL no está definida. Configura .env.local con PostgreSQL.');
    }
    _pool = new Pool({ connectionString });
  }
  return _pool;
}

// Proxy that delegates all Pool calls to the lazy instance
const pool = new Proxy({} as Pool, {
  get(_target, prop) {
    return (getPool() as unknown as Record<string | symbol, unknown>)[prop];
  },
});

export default pool;
