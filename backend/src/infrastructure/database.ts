// src/infrastructure/database.ts

import { Pool } from 'pg';
import fs from 'fs';
import path from 'path';

let pool: Pool | null = null;

function resolvePrimaryDatabaseUrl(): string {
  const configFile = String(
    process.env.MULTI_DB_CONFIG_FILE
    || process.env.DATABASES_CONFIG_FILE
    || './config/multidb.databases.json'
    || ''
  ).trim();

  if (configFile) {
    try {
      const resolvedPath = path.isAbsolute(configFile)
        ? configFile
        : path.resolve(process.cwd(), configFile);

      if (fs.existsSync(resolvedPath)) {
        const parsed = JSON.parse(fs.readFileSync(resolvedPath, 'utf8') || '{}');
        const databases = Array.isArray(parsed?.databases) ? parsed.databases : [];
        const primary = databases.find((entry) => entry?.enabled !== false && (entry?.primary === true || entry?.isPrimary === true || String(entry?.role || '').toLowerCase() === 'primary'))
          || (databases.length === 1 ? databases[0] : null);

        // Buscar la primera entrada postgres disponible (preferir primary:true, sino cualquier postgres)
        const postgresEntry = databases.find(
          (entry) => entry?.enabled !== false
            && (entry?.primary === true || entry?.isPrimary === true)
            && String(entry?.type || '').trim().toLowerCase() === 'postgres'
        ) || databases.find(
          (entry) => entry?.enabled !== false
            && String(entry?.type || '').trim().toLowerCase() === 'postgres'
        ) || primary;

        if (postgresEntry && String(postgresEntry.type || 'postgres').trim().toLowerCase() === 'postgres') {
          const direct = String(postgresEntry.connectionString || postgresEntry.url || '').trim();
          if (direct) {
            return direct;
          }

          const host = String(postgresEntry.host || '').trim();
          const database = String(postgresEntry.database || '').trim();
          const user = String(postgresEntry.user || postgresEntry.username || '').trim();
          const password = String(postgresEntry.password || '').trim();
          const port = Number(postgresEntry.port) || 5432;

          if (host && database && user) {
            const auth = password
              ? `${encodeURIComponent(user)}:${encodeURIComponent(password)}`
              : encodeURIComponent(user);
            return `postgresql://${auth}@${host}:${port}/${database}`;
          }
        }
      }
    } catch (error) {
      console.error('⚠ No se pudo resolver la base primaria desde MULTI_DB_CONFIG_FILE:', error);
    }
  }

  return String(process.env.DATABASE_URL || '').trim();
}

export function initializeDatabase(): Pool {
  if (pool) {
    return pool;
  }

  const dbUrl = resolvePrimaryDatabaseUrl();
  if (!dbUrl) {
    throw new Error('No se encontró una base primaria en MULTI_DB_CONFIG_FILE ni DATABASE_URL como fallback');
  }

  pool = new Pool({
    connectionString: dbUrl,
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
  });

  pool.on('error', (err) => {
    console.error('Error inesperado en pool de conexiones:', err);
  });

  return pool;
}

export function getDatabase(): Pool {
  if (!pool) {
    return initializeDatabase();
  }
  return pool;
}

export async function testDatabaseConnection(): Promise<boolean> {
  try {
    const db = getDatabase();
    await db.query('SELECT NOW()');
    console.log('✓ Conexión a PostgreSQL exitosa');
    return true;
  } catch (error) {
    console.error('✗ Error conectando a PostgreSQL:', error);
    return false;
  }
}

export async function closeDatabase(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
  }
}
