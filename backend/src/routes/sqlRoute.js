// ─── SQL manual route: /api/sql/manual ───────────────────────────────────────
import { Router } from 'express';

/**
 * @param {{
 *   getUserFromRequest: Function,
 *   getRoleFromRequest: Function,
 *   hasRequiredRole: Function,
 *   ensureDynamicSchemaReady: Function,
 *   resolveErrorAnalysisResponse: Function,
 *   shouldForceProceduralMode: Function,
 *   isComplexSqlScript: Function,
 *   executeManualSqlScript: Function,
 *   logQueryHistory: Function,
 *   buildProductionSqlResponse: Function,
 *   buildHumanSqlExplanation: Function,
 *   pool: import('pg').Pool,
 * }} deps
 */
export function createSqlRouter({
  getUserFromRequest,
  getRoleFromRequest,
  hasRequiredRole,
  ensureDynamicSchemaReady,
  resolveErrorAnalysisResponse,
  shouldForceProceduralMode,
  isComplexSqlScript,
  executeManualSqlScript,
  logQueryHistory,
  buildProductionSqlResponse,
  buildHumanSqlExplanation,
  pool,
}) {
  const router = Router();

  // POST /api/sql/manual
  router.post('/manual', async (req, res) => {
    const user = getUserFromRequest(req);
    const role = getRoleFromRequest(req, user);
    if (!hasRequiredRole(role, 'admin')) {
      return res.status(403).json({ success: false, error: 'Acceso denegado: se requiere ADMIN o SUPERADMIN para ejecutar SQL manual.' });
    }

    const startedAt = Date.now();

    try {
      await ensureDynamicSchemaReady(false);
      const sql = String(req.body?.sql || '').trim();
      const rawBodyParams = Array.isArray(req.body?.params)
        ? req.body.params
        : req.body?.params && typeof req.body.params === 'object'
          ? req.body.params
          : {};
      const rawParams = req.body?.writeConfirmed !== undefined
        ? Array.isArray(rawBodyParams)
          ? Object.assign([...rawBodyParams], { __write_confirmed: req.body.writeConfirmed })
          : { ...rawBodyParams, __write_confirmed: req.body.writeConfirmed }
        : rawBodyParams;

      if (!sql) {
        return res.status(400).json({ success: false, error: 'SQL requerido' });
      }

      const errorAnalysisResponse = await resolveErrorAnalysisResponse(sql, pool);
      if (errorAnalysisResponse) {
        return res.status(200).json(errorAnalysisResponse);
      }

      const progressEvents = [];
      const proceduralMode = shouldForceProceduralMode(sql) || isComplexSqlScript(sql);

      if (proceduralMode) {
        progressEvents.push(
          '🔍 Paso 1: Analizando script',
          '📊 Paso 2: Procesando lógica',
          '📈 Paso 3: Generando resultado',
        );
      }

      const result = await executeManualSqlScript(
        { sql, databaseId: String(req.body?.databaseId || '').trim() },
        rawParams,
        (msg) => { const clean = String(msg || '').trim(); if (clean) progressEvents.push(clean); }
      );

      await logQueryHistory({
        username: user?.username || 'anonymous',
        role,
        queryText: sql,
        generatedSql: result.executedQuery || sql,
        queryParams: result.queryParams,
        placeholderOrder: result.placeholderOrder,
        executionMs: result.executionMs ?? (Date.now() - startedAt),
        rowCount: result.affectedRows || 0,
        wasCached: false,
        status: 'ok',
      });

      const cleanResponse = buildProductionSqlResponse({
        success: true,
        rows: result.data || [],
        notices: result.notices || progressEvents,
        semanticCandidate: result.resultadoSemantico || (Array.isArray(result.data) ? result.data[0] : {}),
        explicacion: result.resumenHumano || result.explicacion || buildHumanSqlExplanation(result.data || []),
        message: result.message || '',
      });

      return res.status(200).json(cleanResponse);
    } catch (error) {
      await logQueryHistory({
        username: user?.username || 'anonymous',
        role,
        queryText: String(req.body?.sql || ''),
        generatedSql: String(req.body?.sql || ''),
        queryParams: Array.isArray(req.body?.params)
          ? req.body.params
          : req.body?.params && typeof req.body.params === 'object'
            ? req.body.params
            : [],
        executionMs: Date.now() - startedAt,
        rowCount: 0,
        wasCached: false,
        status: 'error',
      });

      console.error('[SQL_MANUAL] ❌ Error:', error);
      return res.status(400).json({
        success: false,
        error: error instanceof Error ? error.message : 'Error ejecutando SQL',
      });
    }
  });

  return router;
}
