// ─── Distributed routes: /api/distributed/*, /api/query/distributed ──────────
import { Router } from 'express';

/**
 * @param {{
 *   multiDbRegistry: object,
 *   multiDatabaseEngine: object,
 *   ensureMultiDbReady: Function,
 *   resolveErrorAnalysisResponse: Function,
 *   pool: import('pg').Pool,
 *   buildProductionSqlResponse: Function,
 *   resultExplainer: object,
 *   logQueryHistory: Function,
 * }} deps
 */
export function createDistributedRouter({
  multiDbRegistry,
  multiDatabaseEngine,
  ensureMultiDbReady,
  resolveErrorAnalysisResponse,
  pool,
  buildProductionSqlResponse,
  resultExplainer,
  logQueryHistory,
}) {
  const router = Router();

  function buildDistributedSyntheticSql(scripts = []) {
    return (scripts || [])
      .map((script) => {
        const db = String(script?.databaseId || '').trim() || 'unknown-db';
        const sql = String(script?.sql || '').trim();
        return `-- ${db}\n${sql}`;
      })
      .filter(Boolean)
      .join('\n\n');
  }

  async function executeDistributedWithExplanation({ text, parameterValue = null, limit = 50, databases = [], user, role }) {
    const startTime = Date.now();
    const distributedResult = await multiDatabaseEngine.execute(text, { parameterValue, limit, databases });

    if (distributedResult.executionType === 'unresolved') {
      return {
        statusCode: 400,
        payload: {
          success: false,
          message: distributedResult.message || 'Tu consulta es ambigua',
          suggestions: distributedResult.suggestions || [],
          data: [],
          rowCount: 0,
        },
      };
    }

    const mergedRows = distributedResult.mergedResults || distributedResult.data || [];
    const syntheticSql = buildDistributedSyntheticSql(distributedResult.scripts);
    const explanationResult = resultExplainer.explain(mergedRows, syntheticSql || '/* distributed */');
    const executionMs = Date.now() - startTime;

    await logQueryHistory({
      username: user?.username || 'anonymous',
      role,
      queryText: text,
      generatedSql: syntheticSql || '/* distributed */',
      executionMs,
      rowCount: mergedRows.length,
      wasCached: false,
      status: distributedResult.success ? 'ok' : 'error',
    });

    return {
      statusCode: distributedResult.success ? 200 : 207,
      payload: buildProductionSqlResponse({
        success: distributedResult.success,
        rows: mergedRows,
        notices: distributedResult.warnings || [],
        semanticCandidate: mergedRows[0] || {},
        explicacion: explanationResult.explicacionCompleta,
        message: mergedRows.length === 0 ? 'No se encontraron resultados para este criterio' : '',
      }),
    };
  }

  // GET /api/distributed/databases
  // Devuelve solo las BDs que conectaron exitosamente y no son la BD primaria del sistema.
  // La BD primaria es interna (artículos, usuarios, sesiones) y no debe exponerse a consultas de usuario.
  router.get('/databases', (req, res) => {
    try {
      const all = multiDbRegistry.getDatabaseSummaries();
      // showAll=true permite ver todas (para admin/debug); por defecto solo conectadas y no primarias
      const showAll = req.query.showAll === 'true';
      const databases = showAll
        ? all
        : all.filter((db) => db.connected === true && db.primary !== true);
      return res.status(200).json({
        success: true,
        databases,
      });
    } catch (error) {
      return res.status(500).json({ success: false, error: error?.message || 'No se pudo leer el registro distribuido' });
    }
  });

  // POST /api/query/distributed (mounted under /api/query)
  router.post('/distributed', async (req, res) => {
    try {
      await ensureMultiDbReady({ reason: 'api-query-distributed' });
      const { text, parameterValue = null, limit = 50, databases = [] } = req.body || {};
      const inputText = String(text || '').trim();

      if (!inputText) {
        return res.status(400).json({ success: false, error: 'El campo text es requerido' });
      }

      const errorAnalysisResponse = await resolveErrorAnalysisResponse(inputText, pool);
      if (errorAnalysisResponse) return res.status(200).json(errorAnalysisResponse);

      const safeLimit = Math.max(1, Math.min(Number(limit) || 50, 100));
      const result = await multiDatabaseEngine.execute(inputText, { parameterValue, limit: safeLimit, databases });

      const statusCode = result.success ? 200 : 207;
      return res.status(statusCode).json({
        success: result.success,
        data: result.data,
        source: result.source,
        executionType: result.executionType,
        databases: result.source,
        scripts: result.scripts,
        mergedResults: result.mergedResults,
        partialResults: result.partialResults,
        errors: result.errors,
        warnings: result.warnings || [],
        responseMode: 'distributed-multi-db',
      });
    } catch (error) {
      return res.status(400).json({
        success: false,
        error: error?.message || 'No se pudo ejecutar la consulta distribuida',
        responseMode: 'distributed-multi-db',
      });
    }
  });

  return { router, executeDistributedWithExplanation, buildDistributedSyntheticSql };
}
