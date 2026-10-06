// ─── Process routes: /api/process/:processName ───────────────────────────────
import { Router } from 'express';

/**
 * @param {{
 *   pool: import('pg').Pool,
 *   slugifyProcessName: Function,
 *   getQueryCacheDataVersion: Function,
 *   getQueryCacheEntry: Function,
 *   setQueryCacheEntry: Function,
 *   QUERY_CACHE_TTL_MS: number,
 *   executeArticleById: Function,
 *   getUserFromRequest: Function,
 *   getRoleFromRequest: Function,
 *   logQueryHistory: Function,
 * }} deps
 */
export function createProcessRouter({
  pool,
  slugifyProcessName,
  getQueryCacheDataVersion,
  getQueryCacheEntry,
  setQueryCacheEntry,
  QUERY_CACHE_TTL_MS,
  executeArticleById,
  getUserFromRequest,
  getRoleFromRequest,
  logQueryHistory,
}) {
  const router = Router();

  // GET /api/process/:processName
  router.get('/:processName', async (req, res) => {
    try {
      const { processName } = req.params;
      const normalizedProcess = slugifyProcessName(processName);

      if (!normalizedProcess) {
        return res.status(400).json({ success: false, error: 'Nombre de proceso inválido' });
      }

      const articleResult = await pool.query(
        `SELECT id, titulo
         FROM knowledge_base
         WHERE LOWER(REGEXP_REPLACE(unaccent(titulo), '[^a-zA-Z0-9]+', '_', 'g')) = $1
         LIMIT 1`,
        [normalizedProcess]
      ).catch(async () => {
        return pool.query('SELECT id, titulo FROM knowledge_base LIMIT 200');
      });

      let article = articleResult.rows?.[0] || null;
      if (!article || !article.id) {
        const fallbackResult = await pool.query('SELECT id, titulo FROM knowledge_base ORDER BY id DESC LIMIT 200');
        article = (fallbackResult.rows || []).find((row) => slugifyProcessName(row.titulo) === normalizedProcess) || null;
      }

      if (!article || !article.id) {
        return res.status(404).json({ success: false, error: 'Proceso no encontrado' });
      }

      const rawParams = { ...req.query };
      delete rawParams.processName;

      const cacheKey = JSON.stringify({
        type: 'process_endpoint',
        process: normalizedProcess,
        params: rawParams,
        dv: getQueryCacheDataVersion(),
      });

      const cached = getQueryCacheEntry(cacheKey);
      if (cached) {
        return res.status(200).json({ success: true, process: normalizedProcess, data: cached.data, count: cached.count, cached: true });
      }

      const execution = await executeArticleById(String(article.id), rawParams, () => {});
      if (!execution.success) {
        return res.status(400).json({ success: false, error: execution.message || 'Error ejecutando proceso' });
      }

      const rows = Array.isArray(execution.rows)
        ? execution.rows
        : Array.isArray(execution.data?.rows)
          ? execution.data.rows
          : Array.isArray(execution.data)
            ? execution.data
            : [];

      const payload = { data: rows, count: Number(execution.rowCount || rows.length || 0) };
      setQueryCacheEntry(cacheKey, payload, QUERY_CACHE_TTL_MS);

      await logQueryHistory({
        username: getUserFromRequest(req)?.username || 'anonymous',
        role: getRoleFromRequest(req, getUserFromRequest(req)),
        queryText: `process ${normalizedProcess}`,
        generatedSql: 'workflow execution by process endpoint',
        executionMs: 0,
        rowCount: payload.count,
        wasCached: false,
        status: 'ok',
      });

      return res.status(200).json({ success: true, process: normalizedProcess, ...payload, cached: false });
    } catch (error) {
      console.error('[PROCESS] ❌ Error ejecutando proceso dinámico:', error);
      return res.status(500).json({ success: false, error: error instanceof Error ? error.message : 'Error interno del servidor' });
    }
  });

  return router;
}
