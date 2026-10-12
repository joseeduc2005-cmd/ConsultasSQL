// ─── Distributed routes: /api/distributed/*, /api/query/distributed ──────────
import { Router } from 'express';
import fs from 'fs';
import path from 'path';
import logger from '../../infrastructure/logger/logger.js';

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
 *   getUserFromRequest: Function,
 *   getRoleFromRequest: Function,
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
  getUserFromRequest,
  getRoleFromRequest,
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

    // Fire-and-forget: el logging no debe bloquear la respuesta
    logQueryHistory({
      username: user?.username || 'anonymous',
      role,
      queryText: text,
      generatedSql: syntheticSql || '/* distributed */',
      executionMs,
      rowCount: mergedRows.length,
      wasCached: false,
      status: distributedResult.success ? 'ok' : 'error',
    }).catch((e) => logger.warn('logQueryHistory error (distributed)', { error: e?.message }));

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
  router.get('/databases', (req, res) => {
    const reqLog = req.logger || logger;
    try {
      const all = multiDbRegistry.getDatabaseSummaries();
      const showAll = req.query.showAll === 'true';
      const databases = showAll
        ? all
        : all.filter((db) => db.connected === true && db.primary !== true);
      reqLog.debug('GET /distributed/databases', { total: all.length, returned: databases.length, showAll });
      return res.status(200).json({ success: true, databases });
    } catch (error) {
      reqLog.error('GET /distributed/databases error', { error: error?.message });
      return res.status(500).json({ success: false, error: error?.message || 'No se pudo leer el registro distribuido' });
    }
  });

  // ── Helpers para leer/escribir el JSON de configuración ──────────────────────
  function getConfigFilePath() {
    const configFile = String(
      process.env.MULTI_DB_CONFIG_FILE || process.env.DATABASES_CONFIG_FILE || './config/multidb.databases.json'
    ).trim();
    return path.isAbsolute(configFile) ? configFile : path.resolve(process.cwd(), configFile);
  }

  async function readConfigFileAsync() {
    const filePath = getConfigFilePath();
    const raw = await fs.promises.readFile(filePath, 'utf8');
    return { filePath, config: JSON.parse(raw) };
  }

  async function writeConfigFileAsync(filePath, config) {
    await fs.promises.writeFile(filePath, JSON.stringify(config, null, 2), 'utf8');
  }

  /**
   * Valida que el request tenga rol admin.
   * El rol se lee desde la sesión (getUserFromRequest) — NO desde headers raw.
   * Un cliente no puede auto-elevarse poniendo x-user-role: admin.
   */
  function requireAdmin(req, res) {
    if (!getUserFromRequest || !getRoleFromRequest) {
      // Si no se inyectaron las funciones de sesión, bloquear por seguridad
      res.status(403).json({ success: false, error: 'Acceso restringido' });
      return false;
    }
    const user = getUserFromRequest(req);
    const role = getRoleFromRequest(req, user);
    if (String(role || '').toLowerCase() !== 'admin') {
      (req.logger || logger).warn('Acceso denegado a operación admin', { role, path: req.path });
      res.status(403).json({ success: false, error: 'Solo admin puede realizar esta operación' });
      return false;
    }
    return true;
  }

  // POST /api/distributed/databases — agregar nueva BD (solo admin)
  router.post('/databases', async (req, res) => {
    const reqLog = req.logger || logger;
    if (!requireAdmin(req, res)) return;

    try {
      const { id, label, type, host, port, database, user, password, enabled = true } = req.body || {};

      if (!id || !type || !host || !port || !database || !user || !password) {
        return res.status(400).json({ success: false, error: 'Faltan campos: id, type, host, port, database, user, password' });
      }

      const { filePath, config } = await readConfigFileAsync();

      if ((config.databases || []).some((db) => db.id === id)) {
        return res.status(409).json({ success: false, error: `Ya existe una base con id "${id}"` });
      }

      config.databases = config.databases || [];
      config.databases.push({ id, label: label || id, type, host, port: Number(port), database, user, password, enabled, schema: { tables: [] } });
      await writeConfigFileAsync(filePath, config);

      reqLog.info('Nueva base de datos agregada', { dbId: id, type, host });
      // El file watcher detectará el cambio en ~2s y hará reload+probe automático
      return res.status(201).json({ success: true, message: `Base "${id}" agregada. El sistema la conectará en segundos.` });
    } catch (error) {
      reqLog.error('POST /distributed/databases error', { error: error?.message });
      return res.status(500).json({ success: false, error: error?.message || 'Error al guardar la configuración' });
    }
  });

  // DELETE /api/distributed/databases/:id — eliminar BD (solo admin)
  router.delete('/databases/:id', async (req, res) => {
    const reqLog = req.logger || logger;
    if (!requireAdmin(req, res)) return;

    try {
      const targetId = String(req.params.id || '').trim();
      if (!targetId) return res.status(400).json({ success: false, error: 'Se requiere el id de la base' });

      const { filePath, config } = await readConfigFileAsync();
      const existing = (config.databases || []).find((db) => db.id === targetId);
      if (!existing) return res.status(404).json({ success: false, error: `No existe base con id "${targetId}"` });
      if (existing.primary === true) return res.status(400).json({ success: false, error: 'No se puede eliminar la base primaria del sistema' });

      config.databases = config.databases.filter((db) => db.id !== targetId);
      await writeConfigFileAsync(filePath, config);

      reqLog.info('Base de datos eliminada', { dbId: targetId });
      return res.status(200).json({ success: true, message: `Base "${targetId}" eliminada.` });
    } catch (error) {
      reqLog.error('DELETE /distributed/databases error', { error: error?.message });
      return res.status(500).json({ success: false, error: error?.message || 'Error al guardar la configuración' });
    }
  });

  // POST /api/query/distributed (mounted under /api/query)
  router.post('/distributed', async (req, res) => {
    const reqLog = req.logger || logger;
    const startedAt = Date.now();
    try {
      await ensureMultiDbReady({ reason: 'api-query-distributed' });
      const { text, parameterValue = null, limit = 50, databases = [] } = req.body || {};
      const inputText = String(text || '').trim();

      if (!inputText) {
        return res.status(400).json({ success: false, error: 'El campo text es requerido' });
      }

      reqLog.info('distributed query start', { textLen: inputText.length, limit, databases });

      const errorAnalysisResponse = await resolveErrorAnalysisResponse(inputText, pool);
      if (errorAnalysisResponse) return res.status(200).json(errorAnalysisResponse);

      const safeLimit = Math.max(1, Math.min(Number(limit) || 50, 100));
      const result = await multiDatabaseEngine.execute(inputText, { parameterValue, limit: safeLimit, databases });

      reqLog.info('distributed query done', {
        success: result.success,
        executionType: result.executionType,
        rowCount: (result.mergedResults || result.data || []).length,
        ms: Date.now() - startedAt,
      });

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
      reqLog.error('distributed query error', { error: error?.message, ms: Date.now() - startedAt });
      return res.status(400).json({
        success: false,
        error: error?.message || 'No se pudo ejecutar la consulta distribuida',
        responseMode: 'distributed-multi-db',
      });
    }
  });

  return { router, executeDistributedWithExplanation, buildDistributedSyntheticSql };
}
