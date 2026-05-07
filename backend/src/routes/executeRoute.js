// ─── Execute routes: /api/execute/*, /api/repair ─────────────────────────────
import { Router } from 'express';

/**
 * @param {{
 *   executeArticleById: Function,
 *   enforceStrictProductionSqlPayload: Function,
 *   getUserFromRequest: Function,
 *   getRoleFromRequest: Function,
 *   logQueryHistory: Function,
 *   bumpQueryCacheDataVersion: Function,
 * }} deps
 */
export function createExecuteRouter({
  executeArticleById,
  enforceStrictProductionSqlPayload,
  getUserFromRequest,
  getRoleFromRequest,
  logQueryHistory,
  bumpQueryCacheDataVersion,
}) {
  const router = Router();

  // GET /api/execute/stream/:articleId — SSE streaming endpoint
  router.get('/stream/:articleId', async (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders();

    const emit = (message, type = 'progress') => {
      const eventName = type === 'error' ? 'execution-error' : type;
      res.write(`event: ${eventName}\ndata: ${JSON.stringify({ message })}\n\n`);
    };

    const done = (success, message, result = null) => {
      const sanitized = enforceStrictProductionSqlPayload(result || { message });
      res.write(`event: done\ndata: ${JSON.stringify({ success: Boolean(success), message: String(message || ''), result: sanitized })}\n\n`);
      res.end();
    };

    try {
      const startedAt = Date.now();
      const { articleId } = req.params;

      if (!articleId || articleId === 'undefined' || articleId === 'null') {
        emit('❌ ID de artículo inválido', 'error');
        return done(false, 'ID inválido', null);
      }

      let rawParams = {};
      if (req.query.params) {
        try {
          rawParams = JSON.parse(decodeURIComponent(req.query.params));
        } catch {
          emit('❌ Parámetros inválidos', 'error');
          return done(false, 'Parámetros inválidos', null);
        }
      }

      const requestedDatabaseId = String(req.query?.databaseId || '').trim();
      const requestedDatabaseHint = String(req.query?.databaseHint || '').trim();
      const result = await executeArticleById(articleId, rawParams, emit, {
        databaseId: requestedDatabaseId,
        databaseHint: requestedDatabaseHint,
      });

      const enrichedResult = {
        ...result,
        executionMs: result?.executionMs ?? (Date.now() - startedAt),
      };
      return done(Boolean(result.success), result.message || 'Sin mensaje', enrichedResult);
    } catch (error) {
      console.error('[EXECUTE] ❌ Error:', error);
      try {
        emit('❌ Error interno del servidor', 'error');
        done(false, 'Error interno', null);
      } catch {
        // Response may already be closed
      }
    }
  });

  // POST /api/execute — JSON endpoint
  router.post('/', async (req, res) => {
    try {
      const startedAt = Date.now();
      const { articleId, formData } = req.body || {};

      if (!articleId || articleId === 'undefined' || articleId === 'null') {
        return res.status(400).json({ success: false, message: 'ID inválido' });
      }

      const rawParams = formData && typeof formData === 'object' ? formData : {};
      const requestedDatabaseId = String(req.body?.databaseId || '').trim();
      const requestedDatabaseHint = String(req.body?.databaseHint || '').trim();
      const result = await executeArticleById(String(articleId), rawParams, () => {}, {
        databaseId: requestedDatabaseId,
        databaseHint: requestedDatabaseHint,
      });

      if (!result.success) {
        await logQueryHistory({
          username: getUserFromRequest(req)?.username || 'anonymous',
          role: getRoleFromRequest(req, getUserFromRequest(req)),
          queryText: `execute article ${articleId}`,
          generatedSql: String(result?.executedQuery || 'workflow execution'),
          queryParams: result?.queryParams,
          placeholderOrder: result?.placeholderOrder,
          executionMs: Date.now() - startedAt,
          rowCount: 0,
          wasCached: false,
          status: 'error',
        });
        return res.status(400).json({ success: false, message: result.message });
      }

      bumpQueryCacheDataVersion();

      await logQueryHistory({
        username: getUserFromRequest(req)?.username || 'anonymous',
        role: getRoleFromRequest(req, getUserFromRequest(req)),
        queryText: `execute article ${articleId}`,
        generatedSql: String(result?.executedQuery || 'workflow execution'),
        queryParams: result?.queryParams,
        placeholderOrder: result?.placeholderOrder,
        executionMs: Date.now() - startedAt,
        rowCount: Number(result?.rowCount || 0),
        wasCached: false,
        status: 'ok',
      });

      const sanitized = enforceStrictProductionSqlPayload(result || {});
      return res.status(200).json({
        success: true,
        message: result?.message || 'Ejecución completada',
        result: sanitized,
      });
    } catch (error) {
      console.error('[EXECUTE] ❌ Error POST /api/execute:', error);
      return res.status(500).json({
        success: false,
        message: error instanceof Error ? error.message : 'Error interno',
      });
    }
  });

  return router;
}

// ─── Repair route: /api/repair ────────────────────────────────────────────────

/**
 * @param {{ executeArticleById: Function, enforceStrictProductionSqlPayload: Function }} deps
 */
export function createRepairRouter({ executeArticleById, enforceStrictProductionSqlPayload }) {
  const router = Router();

  router.post('/', async (req, res) => {
    try {
      const { articleId, formData } = req.body || {};

      if (!articleId || articleId === 'undefined' || articleId === 'null') {
        return res.status(400).json({ success: false, message: 'ID inválido' });
      }

      const rawParams = formData && typeof formData === 'object' ? formData : {};
      const requestedDatabaseId = String(req.body?.databaseId || '').trim();
      const requestedDatabaseHint = String(req.body?.databaseHint || '').trim();
      const result = await executeArticleById(String(articleId), rawParams, () => {}, {
        databaseId: requestedDatabaseId,
        databaseHint: requestedDatabaseHint,
      });

      if (!result.success) {
        return res.status(400).json({ success: false, message: result.message });
      }

      const sanitized = enforceStrictProductionSqlPayload(result || {});
      return res.status(200).json({
        success: true,
        message: result?.message || 'Ejecución completada',
        result: sanitized,
      });
    } catch (error) {
      console.error('[REPAIR] ❌ Error POST /api/repair:', error);
      return res.status(500).json({
        success: false,
        message: error instanceof Error ? error.message : 'Error interno',
      });
    }
  });

  return router;
}
