// ─── Execute routes: /api/execute/*, /api/repair ─────────────────────────────
import { Router } from 'express';
import logger from '../../infrastructure/logger/logger.js';

/**
 * @param {{
 *   executeArticleById: Function,
 *   enforceStrictProductionSqlPayload: Function,
 *   getUserFromRequest: Function,
 *   getUserFromRequestAsync: Function,
 *   getRoleFromRequest: Function,
 *   logQueryHistory: Function,
 *   logArticleExecution: Function,
 *   bumpQueryCacheDataVersion: Function,
 * }} deps
 */
export function createExecuteRouter({
  executeArticleById,
  enforceStrictProductionSqlPayload,
  getUserFromRequest,
  getUserFromRequestAsync,
  getRoleFromRequest,
  logQueryHistory,
  logArticleExecution,
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

    const reqLog = req.logger || logger;

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

      // Identidad del usuario: query param primero (EventSource no envía headers),
      // luego sesión restaurada desde BD (funciona tras reinicios del backend).
      const sessionUser = await (getUserFromRequestAsync || getUserFromRequest)(req);
      const username = String(
        req.query?.username || sessionUser?.username || 'anonymous'
      ).trim();
      const userRole = String(
        req.query?.userRole || req.headers['x-user-role'] || getRoleFromRequest(req, sessionUser) || 'user'
      ).trim();

      reqLog.info('SSE execute start', { articleId, username, userRole });

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

      const ms = Date.now() - startedAt;
      const enrichedResult = { ...result, executionMs: result?.executionMs ?? ms };

      reqLog.info('SSE execute done', {
        articleId, username, userRole,
        success: Boolean(result.success),
        ms, rowCount: result?.rowCount,
        databaseId: requestedDatabaseId || 'auto',
      });

      // ── Auditoría completa de la ejecución ───────────────────────────────
      if (typeof logArticleExecution === 'function') {
        logArticleExecution({
          articleId,
          articleTitulo: result?.articleTitulo || '',
          ejecutadoPor: username,
          ejecutadoRol: userRole,
          parametrosUsados: rawParams,
          filasResultado: Number(result?.rowCount || 0),
          duracionMs: ms,
          estado: result?.success ? 'ok' : 'error',
          mensajeError: result?.success ? null : (result?.message || null),
        }).catch(() => {});
      }

      return done(Boolean(result.success), result.message || 'Sin mensaje', enrichedResult);
    } catch (error) {
      const ms = Date.now ? 0 : 0;
      reqLog.error('SSE execute error', {
        articleId: req.params?.articleId,
        error: error?.message || String(error),
        stack: error?.stack,
      });
      if (typeof logArticleExecution === 'function') {
        logArticleExecution({
          articleId: req.params?.articleId,
          articleTitulo: '',
          ejecutadoPor: String(req.query?.username || 'anonymous'),
          ejecutadoRol: String(req.query?.userRole || 'user'),
          parametrosUsados: null,
          filasResultado: 0,
          duracionMs: 0,
          estado: 'error',
          mensajeError: error?.message || 'Error interno',
        }).catch(() => {});
      }
      try {
        emit('❌ Error interno del servidor', 'error');
        done(false, 'Error interno', null);
      } catch {
        // La respuesta SSE ya fue cerrada — ignorar
      }
    }
  });

  // POST /api/execute — JSON endpoint
  router.post('/', async (req, res) => {
    const reqLog = req.logger || logger;
    const startedAt = Date.now();
    try {
      const { articleId, formData } = req.body || {};

      if (!articleId || articleId === 'undefined' || articleId === 'null') {
        return res.status(400).json({ success: false, message: 'ID inválido' });
      }

      const rawParams = formData && typeof formData === 'object' ? formData : {};
      const requestedDatabaseId = String(req.body?.databaseId || '').trim();
      const requestedDatabaseHint = String(req.body?.databaseHint || '').trim();
      const username = getUserFromRequest(req)?.username || 'anonymous';
      const role = getRoleFromRequest(req, getUserFromRequest(req));

      reqLog.info('execute POST start', { articleId, databaseId: requestedDatabaseId || 'auto', username });

      const result = await executeArticleById(String(articleId), rawParams, () => {}, {
        databaseId: requestedDatabaseId,
        databaseHint: requestedDatabaseHint,
      });

      const execMs = Date.now() - startedAt;

      if (!result.success) {
        // Audit log de error — fire-and-forget (no bloquea la respuesta)
        logQueryHistory({
          username, role,
          queryText: `execute article ${articleId}`,
          generatedSql: String(result?.executedQuery || 'workflow execution'),
          queryParams: result?.queryParams, placeholderOrder: result?.placeholderOrder,
          executionMs: execMs, rowCount: 0, wasCached: false, status: 'error',
        }).catch((e) => logger.warn('logQueryHistory error (execute error)', { error: e?.message }));

        if (typeof logArticleExecution === 'function') {
          logArticleExecution({
            articleId, articleTitulo: result?.articleTitulo || '',
            ejecutadoPor: username, ejecutadoRol: role,
            parametrosUsados: rawParams, filasResultado: 0,
            duracionMs: execMs, estado: 'error', mensajeError: result.message,
          }).catch(() => {});
        }

        reqLog.warn('execute POST: article failed', { articleId, message: result.message, ms: execMs });
        return res.status(400).json({ success: false, message: result.message });
      }

      bumpQueryCacheDataVersion();

      // Audit log de éxito — fire-and-forget
      logQueryHistory({
        username, role,
        queryText: `execute article ${articleId}`,
        generatedSql: String(result?.executedQuery || 'workflow execution'),
        queryParams: result?.queryParams, placeholderOrder: result?.placeholderOrder,
        executionMs: execMs, rowCount: Number(result?.rowCount || 0), wasCached: false, status: 'ok',
      }).catch((e) => logger.warn('logQueryHistory error (execute ok)', { error: e?.message }));

      if (typeof logArticleExecution === 'function') {
        logArticleExecution({
          articleId, articleTitulo: result?.articleTitulo || '',
          ejecutadoPor: username, ejecutadoRol: role,
          parametrosUsados: rawParams, filasResultado: Number(result?.rowCount || 0),
          duracionMs: execMs, estado: 'ok', mensajeError: null,
        }).catch(() => {});
      }

      reqLog.info('execute POST: OK', { articleId, ms: execMs, rowCount: result?.rowCount });

      const sanitized = enforceStrictProductionSqlPayload(result || {});
      return res.status(200).json({
        success: true,
        message: result?.message || 'Ejecución completada',
        result: sanitized,
      });
    } catch (error) {
      (req.logger || logger).error('execute POST: error interno', {
        error: error?.message || String(error),
        stack: error?.stack,
        ms: Date.now() - startedAt,
      });
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
 * @param {{
 *   executeArticleById: Function,
 *   enforceStrictProductionSqlPayload: Function,
 *   getUserFromRequest: Function,
 *   getRoleFromRequest: Function,
 *   logQueryHistory: Function,
 *   bumpQueryCacheDataVersion: Function,
 * }} deps
 */
export function createRepairRouter({
  executeArticleById,
  enforceStrictProductionSqlPayload,
  getUserFromRequest,
  getRoleFromRequest,
  logQueryHistory,
  bumpQueryCacheDataVersion,
}) {
  const router = Router();

  router.post('/', async (req, res) => {
    const reqLog = req.logger || logger;
    const startedAt = Date.now();
    try {
      const { articleId, formData } = req.body || {};

      if (!articleId || articleId === 'undefined' || articleId === 'null') {
        return res.status(400).json({ success: false, message: 'ID inválido' });
      }

      const rawParams = formData && typeof formData === 'object' ? formData : {};
      const requestedDatabaseId = String(req.body?.databaseId || '').trim();
      const requestedDatabaseHint = String(req.body?.databaseHint || '').trim();
      const username = getUserFromRequest(req)?.username || 'anonymous';
      const role = getRoleFromRequest(req, getUserFromRequest(req));

      reqLog.info('repair POST start', { articleId, databaseId: requestedDatabaseId || 'auto', username });

      const result = await executeArticleById(String(articleId), rawParams, () => {}, {
        databaseId: requestedDatabaseId,
        databaseHint: requestedDatabaseHint,
      });

      if (!result.success) {
        reqLog.warn('repair POST: article failed', { articleId, message: result.message, ms: Date.now() - startedAt });
        return res.status(400).json({ success: false, message: result.message });
      }

      // Audit log de repair — fire-and-forget (igual que execute)
      if (typeof logQueryHistory === 'function') {
        logQueryHistory({
          username,
          role,
          queryText: `repair article ${articleId}`,
          generatedSql: String(result?.executedQuery || 'repair execution'),
          queryParams: result?.queryParams,
          placeholderOrder: result?.placeholderOrder,
          executionMs: Date.now() - startedAt,
          rowCount: Number(result?.rowCount || 0),
          wasCached: false,
          status: 'ok',
        }).catch((e) => logger.warn('logQueryHistory error (repair ok)', { error: e?.message }));
      }

      if (typeof bumpQueryCacheDataVersion === 'function') {
        bumpQueryCacheDataVersion();
      }

      reqLog.info('repair POST: OK', { articleId, ms: Date.now() - startedAt, rowCount: result?.rowCount });

      const sanitized = enforceStrictProductionSqlPayload(result || {});
      return res.status(200).json({
        success: true,
        message: result?.message || 'Ejecución completada',
        result: sanitized,
      });
    } catch (error) {
      (req.logger || logger).error('repair POST: error interno', {
        error: error?.message || String(error),
        stack: error?.stack,
        ms: Date.now() - startedAt,
      });
      return res.status(500).json({
        success: false,
        message: error instanceof Error ? error.message : 'Error interno',
      });
    }
  });

  return router;
}
