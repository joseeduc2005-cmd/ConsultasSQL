// ─── Query history routes: /api/query-history ────────────────────────────────
import { Router } from 'express';

/**
 * @param {{
 *   pool: import('pg').Pool,
 *   getUserFromRequest: Function,
 *   getRoleFromRequest: Function,
 *   hasRequiredRole: Function,
 *   bumpQueryCacheDataVersion: Function,
 * }} deps
 */
export function createHistoryRouter({
  pool,
  getUserFromRequest,
  getRoleFromRequest,
  hasRequiredRole,
  bumpQueryCacheDataVersion,
}) {
  const router = Router();

  // GET /api/query-history
  router.get('/', async (req, res) => {
    const user = getUserFromRequest(req);
    const role = getRoleFromRequest(req, user);
    if (!hasRequiredRole(role, 'admin')) {
      return res.status(403).json({ success: false, error: 'Acceso denegado: se requiere ADMIN o SUPERADMIN.' });
    }

    try {
      const limit = Math.max(1, Math.min(200, Number(req.query.limit) || 50));
      const result = await pool.query(
        `SELECT id, username, user_role, query_text, generated_sql, execution_ms, row_count, was_cached, status, created_at
         FROM query_history
         ORDER BY created_at DESC
         LIMIT $1`,
        [limit]
      );
      return res.status(200).json({ success: true, data: result.rows, count: result.rowCount || 0 });
    } catch (error) {
      console.error('[QUERY_HISTORY] ❌ Error:', error);
      return res.status(500).json({ success: false, error: 'Error interno del servidor' });
    }
  });

  // DELETE /api/query-history
  router.delete('/', async (req, res) => {
    const user = getUserFromRequest(req);
    const role = getRoleFromRequest(req, user);
    if (!hasRequiredRole(role, 'admin')) {
      return res.status(403).json({ success: false, error: 'Acceso denegado: se requiere ADMIN o SUPERADMIN.' });
    }

    try {
      const result = await pool.query('DELETE FROM query_history');
      bumpQueryCacheDataVersion();
      return res.status(200).json({
        success: true,
        cleared: Number(result.rowCount || 0),
        cacheInvalidated: true,
        timestamp: Date.now(),
      });
    } catch (error) {
      console.error('[QUERY_HISTORY] ❌ Error limpiando historial:', error);
      return res.status(500).json({ success: false, error: 'Error interno del servidor' });
    }
  });

  return router;
}
