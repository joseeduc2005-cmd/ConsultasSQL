/**
 * Cómo se llega a PostgreSQL y si se llega.
 *
 * Cuatro cosas que eran puro «hablar con la infraestructura» y vivían en app.js: de
 * dónde sale la cadena de conexión, cómo se construye el pool, si la base responde, y
 * el sondeo de las seis bases configuradas al arrancar.
 *
 * Están juntas porque son la misma pregunta mirada desde cuatro sitios, y fuera de
 * app.js porque no deciden nada del negocio: solo abren conexiones y cuentan si
 * salió bien. El sondeo es el que más se nota en un arranque — es lo que imprime esa
 * lista de OK y FAIL por cada base.
 */
import { Pool } from 'pg';

import { connectDatabase as probeUniversalConnection, closeConnection as closeUniversalConnection } from '../../infrastructure/universal-db/index.js';
import logger from '../../infrastructure/logger/logger.js';

/**
 * @param {object} deps
 * @param {Function} deps.obtenerPool      el pool de ahora; por getter, se reasigna
 * @param {object}   deps.multiDbRegistry  el registro de bases configuradas
 * @param {Function} deps.refrescarHuella   rehace la huella de conexión del esquema
 */
export function crearConexionPostgres({ obtenerPool, multiDbRegistry, refrescarHuella }) {
  function obtenerCadenaDeConexion() {
    try {
      // Busca postgres específicamente (para el pool interno del backend)
      // Si no hay postgres, intenta cualquier base disponible como fallback
      return multiDbRegistry.resolvePrimaryConnection({ expectedType: 'postgres', allowEnvFallback: true }).connectionString;
    } catch {
      try {
        return multiDbRegistry.resolvePrimaryConnection({ allowEnvFallback: true }).connectionString;
      } catch {
        return String(process.env.DATABASE_URL || '').trim() || null;
      }
    }
  }

  function createPgPool() {
    const connectionString = obtenerCadenaDeConexion();
    if (!connectionString) {
      console.warn('[DB] No se encontró postgres en el registro — el pool interno del backend no está disponible.');
      return null;
    }

    return new Pool({
      connectionString,
      max: 20,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 5000,
      // Generous default (2 min) so long / complex admin SQL is never cut short —
      // "if DBeaver can run it, we can too". Override with SQL_QUERY_TIMEOUT_MS.
      statement_timeout: Math.max(1000, Number(process.env.SQL_QUERY_TIMEOUT_MS) || 120000),
      query_timeout: Math.max(1000, Number(process.env.SQL_QUERY_TIMEOUT_MS) || 120000),
    });
  }

  async function testDatabaseConnection() {
    const pool = obtenerPool();
      if (!pool || typeof pool.query !== 'function') {
      console.warn('⚠ No hay pool PostgreSQL interno disponible. Continuando en modo multibase.');
      return false;
    }

    try {
      const result = await obtenerPool().query('SELECT NOW()');
      await refrescarHuella();
      logger.info('Conexión a PostgreSQL exitosa', { now: result.rows[0]?.now });
      return true;
    } catch (err) {
      logger.error('Error conectando a PostgreSQL', { error: err.message || String(err) });
      return false;
    }
  }

  async function probeConfiguredDatabasesConnectivity(reason = 'runtime') {
    const configured = multiDbRegistry.getConfigSnapshot()?.databases || [];

    if (configured.length === 0) {
      console.warn(`⚠ MultiDB probe (${reason}): no hay bases configuradas`);
      return [];
    }

    console.log(`🔌 MultiDB probe (${reason}): verificando ${configured.length} base(s)...`);

    // Ejecutar todas las pruebas de conectividad en paralelo (antes era secuencial)
    const summary = await Promise.all(
      configured.map(async (database) => {
        const id = String(database?.id || 'unknown').trim() || 'unknown';
        const type = String(database?.type || 'unknown').trim() || 'unknown';

        if (database?.enabled === false) {
          logger.debug('DB probe: SKIP (disabled)', { dbId: id, type, reason });
          return { id, type, status: 'skipped-disabled' };
        }

        const missing = ['host', 'port', 'database', 'user', 'password']
          .filter((key) => database?.[key] === undefined || database?.[key] === null || String(database?.[key]).trim() === '');

        if (missing.length > 0) {
          logger.warn('DB probe: SKIP (faltan campos)', { dbId: id, type, reason, missing });
          return { id, type, status: 'skipped-missing-fields', missing };
        }

        try {
          // MongoDB y MSSQL no usan el conector universal SQL — usan ensureClient directamente
          if (type === 'mongodb' || type === 'mssql') {
            await multiDbRegistry.ensureClient(database);
            console.log(`[DB:${id}|${type}] ✅ OK (${database.host}:${database.port}/${database.database})`);
            multiDbRegistry.setConnectionStatus(id, 'ok');
            return { id, type, status: 'ok' };
          }
          const result = await probeUniversalConnection(database);
          console.log(`[DB:${id}|${type}] ✅ OK (${database.host}:${database.port}/${database.database})`);
          multiDbRegistry.setConnectionStatus(id, 'ok');
          if (result?.dbId && !result?.reused) {
            await closeUniversalConnection(result.dbId).catch(() => {});
          }
          return { id, type, status: 'ok' };
        } catch (error) {
          console.error(`[DB:${id}|${type}] ❌ FAIL: ${error?.message || error}`);
          multiDbRegistry.setConnectionStatus(id, 'fail');
          return { id, type, status: 'fail', error: String(error?.message || error) };
        }
      }),
    );

    const okCount = summary.filter((item) => item.status === 'ok').length;
    const failCount = summary.filter((item) => item.status === 'fail').length;
    const skippedCount = summary.length - okCount - failCount;
    console.log(`🔌 MultiDB probe (${reason}) resumen: OK=${okCount}, FAIL=${failCount}, SKIP=${skippedCount}`);

    return summary;
  }
  return {
    obtenerCadenaDeConexion,
    createPgPool,
    testDatabaseConnection,
    probeConfiguredDatabasesConnectivity,
  };
}
