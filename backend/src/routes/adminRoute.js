// ─── Admin + DB schema routes: /api/admin/*, /api/db/* ───────────────────────
import { Router } from 'express';

/**
 * @param {{
 *   pool: import('pg').Pool,
 *   getUserFromRequest: Function,
 *   ensureDynamicSchemaReady: Function,
 *   normalizeSchemaIdentifier: Function,
 *   schemaDetector: object,
 *   schemaCache: object,
 * }} deps
 */
export function createAdminRouter({
  pool,
  getUserFromRequest,
  ensureDynamicSchemaReady,
  normalizeSchemaIdentifier,
  schemaDetector,
  schemaCache,
}) {
  const router = Router();

  // ── /api/admin/* ──────────────────────────────────────────────────────────

  // GET /api/admin/tables
  router.get('/tables', async (req, res) => {
    try {
      const user = getUserFromRequest(req);
      const role = req.headers['x-user-role'] || user?.role;
      if (!role || role !== 'admin') {
        return res.status(403).json({ success: false, error: 'Acceso denegado: se requiere rol admin' });
      }

      const schemaSnapshot = await ensureDynamicSchemaReady(false);
      const tables = Object.keys(schemaSnapshot.tablas || {}).sort((a, b) => a.localeCompare(b));

      return res.status(200).json({ success: true, data: tables });
    } catch (error) {
      console.error('[ADMIN] ❌ Error listando tablas:', error);
      return res.status(500).json({ success: false, error: 'Error interno del servidor', details: error.message });
    }
  });

  // GET /api/admin/tables/:tableName/columns
  router.get('/tables/:tableName/columns', async (req, res) => {
    try {
      const user = getUserFromRequest(req);
      const role = req.headers['x-user-role'] || user?.role;
      if (!role || role !== 'admin') {
        return res.status(403).json({ success: false, error: 'Acceso denegado: se requiere rol admin' });
      }

      const { tableName } = req.params;
      const schemaSnapshot = await ensureDynamicSchemaReady(false);
      const normalizedTable = normalizeSchemaIdentifier(tableName);
      const tableColumns = schemaSnapshot.tablas?.[normalizedTable] || null;

      if (!normalizedTable || !tableColumns) {
        return res.status(400).json({ success: false, error: 'Tabla no encontrada en schema dinámico' });
      }

      const result = await pool.query(
        `SELECT column_name, data_type, is_nullable, column_default
         FROM information_schema.columns
         WHERE table_schema = 'public' AND table_name = $1
         ORDER BY ordinal_position`,
        [normalizedTable]
      );

      const columns = result.rows.map((col) => ({
        name: col.column_name,
        type: col.data_type,
        nullable: col.is_nullable === 'YES',
        default: col.column_default,
      }));

      return res.status(200).json({ success: true, data: { tableName: normalizedTable, columns } });
    } catch (error) {
      console.error('[ADMIN] ❌ Error obteniendo columnas:', error);
      return res.status(500).json({ success: false, error: 'Error interno del servidor', details: error.message });
    }
  });

  return router;
}

/**
 * @param {{
 *   getUserFromRequest: Function,
 *   ensureDynamicSchemaReady: Function,
 *   schemaDetector: object,
 *   schemaCache: object,
 * }} deps
 */
export function createDbSchemaRouter({
  getUserFromRequest,
  ensureDynamicSchemaReady,
  schemaDetector,
  schemaCache,
}) {
  const router = Router();

  // GET /api/db/schema
  router.get('/schema', async (req, res) => {
    try {
      const user = getUserFromRequest(req);
      const role = req.headers['x-user-role'] || user?.role;
      if (!role || role !== 'admin') {
        return res.status(403).json({ success: false, error: 'Acceso denegado: se requiere rol admin' });
      }

      const forceRefresh = String(req.query.refresh || '').trim().toLowerCase() === 'true';
      const schemaSnapshot = await ensureDynamicSchemaReady(forceRefresh);

      const schemaPayload = {
        tablas: schemaSnapshot.tablas,
        relaciones: schemaSnapshot.relaciones,
        loadedAt: schemaSnapshot.loadedAt,
      };

      return res.status(200).json({ success: true, source: 'database', schema: schemaPayload, data: schemaPayload });
    } catch (error) {
      console.error('[DB SCHEMA] ❌ Error obteniendo schema:', error);
      return res.status(200).json({
        success: false,
        source: 'fallback',
        schema: null,
        data: null,
        error: 'No se pudo detectar el schema desde la base de datos',
        details: error.message,
      });
    }
  });

  // GET /api/db/schema-full
  router.get('/schema-full', async (req, res) => {
    try {
      const forceRefresh = String(req.query.refresh || '').trim().toLowerCase() === 'true';
      if (forceRefresh) schemaCache.invalidate();

      const connectionFingerprint = schemaDetector.getConnectionFingerprint();
      const schemaCacheKey = `full-schema:${connectionFingerprint}`;

      const fullSchema = await schemaCache.getOrFetch(schemaCacheKey, async () => {
        console.log('🔍 Detectando schema con SchemaDetector...');
        return schemaDetector.getFullSchema();
      });

      return res.status(200).json({
        success: true,
        source: 'database-auto-detected',
        schema: fullSchema,
        tables: fullSchema.tables || fullSchema.tablas || [],
        columns: fullSchema.columns || {},
        foreignKeys: fullSchema.foreignKeys || [],
        cacheStats: schemaCache.getStats(),
      });
    } catch (error) {
      console.error('[DB SCHEMA-FULL] ❌ Error:', error.message);
      return res.status(500).json({ success: false, schema: null, error: 'Error detectando schema automáticamente', details: error.message });
    }
  });

  return router;
}
