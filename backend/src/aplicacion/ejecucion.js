/**
 * Ejecutar un script: la capa de aplicación.
 *
 * Esto es lo que ocurre cuando alguien aplasta «ejecutar» en un artículo. Son
 * veintiséis funciones que orquestan: deciden a qué base va la sentencia, piden al
 * dominio que valide lo que se va a tocar, abren la conexión, lanzan, guardan una foto
 * de las filas si la cosa escribe, y devuelven el resultado contado en castellano.
 *
 * No se pueden volver puras, y no es un defecto: su trabajo es precisamente abrir una
 * conexión y lanzar algo. Lo que sí se puede es dejar escrito de qué dependen, y eso
 * es lo que cambia. Antes había que deducirlo leyendo cuatro mil líneas de app.js, y
 * cualquiera de esas líneas podía llamarlas. Ahora las doce dependencias están en la
 * primera línea de la fábrica.
 *
 * Dos de ellas llegan como getter y no por valor —el pool y el registro de acciones—
 * porque se reasignan al recargar la configuración multi-base. Guardar el valor de
 * ahora significaría ejecutar contra una conexión cerrada en cuanto alguien toque
 * multidb.databases.json, y lo haría en silencio.
 *
 * De las veintiséis, solo cinco salen de aquí:
 *
 *   executeArticleById                 ejecutar el script de un artículo
 *   executeManualSqlScript             un SQL escrito a mano, por pasos
 *   executeSafeSelectQuery             una consulta de solo lectura
 *   executeDistributedWithExplanation  la misma consulta contra varias bases
 *   executeErrorAnalysis               analizar un error ocurrido
 *
 * Las otras veintiuna eran detalle interno repartido por app.js. Que ahora no se vean
 * desde fuera es la mitad del punto de haberlas traído aquí.
 */
import { getBusinessActionMetadata, getRequiredParamsForActions } from '../dominio/acciones/catalogo.js';
import { detectErrorAnalysisMode } from '../dominio/consultas/decidir.js';
import { evaluateProceduralExpression, evaluateSimpleCondition, evaluateValidationCondition, evaluateWorkflowValidationCondition, extractVariables, formatRaiseNoticeMessage, getManualSqlStepTitle, normalizeWorkflowStoredValue, replaceVariables, resolveTemplateVariable } from '../dominio/flujos/pasos.js';
import { buildAggregatedResultFromStepPayload, buildComplexWorkflowExplanation, buildDistributedSyntheticSql, buildPerStepWriteSummary } from '../dominio/presentacion/agregacion.js';
import { buildHumanSqlExplanation, buildProductionSqlResponse, extractSemanticMetricsFromNotices } from '../dominio/presentacion/explicacion.js';
import { EXECUTABLE_SOLUTION_TYPES, MANUAL_SQL_TIMEOUT_MS, normalizeAndValidateStructuredParams, normalizeStructuredParams, parseStructuredScript } from '../dominio/scripts/estructurado.js';
import { WORKFLOW_ALLOWED_STEP_TYPES, assertDynamicScriptAllowed, detectSqlOperation, ensureDynamicColumnAllowed, isPlainObject, normalizeBusinessStep, normalizeDynamicScriptType, parseQualifiedFieldRef, resolveWhereEntry, validateRelationalMutationScriptDefinition, validateRelationalScriptDefinition } from '../dominio/scripts/validacion.js';
import { buildAnalyticSelectQuery, buildManualSqlQuery, getValueByPath, isWriteConfirmed, normalizeLimitValue, resolveDynamicScriptNode, resolveValueMultiSource, sanitizeStructuredParamValue } from '../dominio/sql/construccion.js';
import { containsBlockedSql, extractSqlReferencedTables, getDatabaseSchemaTableNames, normalizeSchemaIdentifier, splitManualSqlSteps } from '../dominio/sql/guardas.js';
import { buildWhereClause, detectMongoOperation, parseWriteTarget, renumberWhereParams, scriptContainsMutation } from '../dominio/sql/mutaciones.js';
import { buildRelationalMutationQuery, buildRelationalSelectQuery, buildWriteDetailExplanation } from '../dominio/sql/relacional.js';
import MultiDbRegistry from '../infrastructure/distributed/MultiDbRegistry.js';
import logger from '../infrastructure/logger/logger.js';
import { analyzeErrorInput, buildErrorAnalysisResponse, enrichErrorAnalysisWithAi, searchErrorLogs } from '../infrastructure/query/ErrorAnalysisEngine.js';
import { isComplexSqlScript, parsePlSqlScript, shouldForceProceduralMode } from '../dominio/sql/plsql.js';
import { buildEngineLog, detectQueryType, detectSqlSyntaxEngine, extractDatabaseDirective, isNativePlSql } from '../dominio/sql/SqlEngineDetector.js';
import path from 'path';

export function crearEjecucionDeScripts({
  // Estos cuatro eran imports directos al adaptador de PostgreSQL. Entran por aqui
  // porque la capa de aplicacion no debe saber que detras hay PostgreSQL: solo que
  // hay donde registrar y donde preguntar por el esquema.
  logQueryHistory,
  ensureDynamicSchemaReady,
  refreshDynamicSchemaCache,
  tablasPermitidas,
  obtenerPool,
  obtenerRegistroAcciones,
  esquema,
  accionesPermitidas,
  accionesEstructuradas,
  resolverBasesDeConsulta,
  multiDatabaseEngine,
  multiDbRegistry,
  queryBuilder,
  resultExplainer,
  schemaCache,
  schemaDetector,
}) {
  function resolveSqlTargetDatabase(sqlText, options = {}) {
    const requestedDatabaseId = String(options?.databaseId || '').trim();
    if (requestedDatabaseId) {
      return multiDbRegistry.getDatabaseById(requestedDatabaseId) || null;
    }

    // Detect engine from syntax markers BEFORE schema table lookup
    const syntaxEngine = detectSqlSyntaxEngine(sqlText);
    if (syntaxEngine) {
      const allDbs = multiDbRegistry.getDatabases();
      const syntaxMatch = allDbs.find((db) => String(db.type || '').toLowerCase() === syntaxEngine);
      if (syntaxMatch) {
        console.log(buildEngineLog({
          engine: syntaxEngine,
          queryType: detectQueryType(sqlText),
          databaseId: syntaxMatch.id,
          reason: 'syntax markers detected',
          sql: sqlText,
        }));
        return syntaxMatch;
      }
      console.warn(`[ENGINE DETECTED] ${syntaxEngine} | [ROUTING] no registered database of type '${syntaxEngine}' — falling back to schema lookup`);
    }

    const referencedTables = extractSqlReferencedTables(sqlText);
    if (referencedTables.length === 0) {
      return null;
    }

    const databases = multiDbRegistry.getDatabases();
    const candidates = databases.filter((db) => {
      const tableSet = new Set(getDatabaseSchemaTableNames(db));
      return referencedTables.every((table) => tableSet.has(table));
    });

    if (candidates.length === 0) {
      if (options?.preferReadOnlyDatabases === true) {
        const readOnlyCandidates = databases.filter((db) => db.primary !== true);
        if (readOnlyCandidates.length === 1) {
          return readOnlyCandidates[0];
        }
      }
      return null;
    }
    if (candidates.length === 1) return candidates[0];

    if (options?.preferReadOnlyDatabases === true) {
      const readOnlyCandidate = candidates.find((db) => db.primary !== true);
      if (readOnlyCandidate) {
        return readOnlyCandidate;
      }
    }

    return candidates.find((db) => db.primary === true) || candidates[0];
  }

  async function ensureSqlUsesWhitelistedTables(sqlText, options = {}) {
    const providedAllowedTables = Array.isArray(options?.allowedTables)
      ? options.allowedTables.map((table) => normalizeSchemaIdentifier(table)).filter(Boolean)
      : null;

    if (!providedAllowedTables || providedAllowedTables.length === 0) {
      await refreshDynamicSchemaCache();
    }

    const allowedTables = new Set((providedAllowedTables && providedAllowedTables.length > 0)
      ? providedAllowedTables
      : tablasPermitidas().map((table) => normalizeSchemaIdentifier(table)).filter(Boolean));

    // Accept both fully-qualified and plain table references
    // (e.g. system.comments and comments) for multi-DB SQL passthrough.
    for (const allowed of [...allowedTables]) {
      const plainAllowed = String(allowed || '').split('.').pop() || '';
      if (plainAllowed) {
        allowedTables.add(plainAllowed);
      }
    }

    const tableRefs = extractSqlReferencedTables(sqlText);
    for (const tableName of tableRefs) {
      const plainTableName = String(tableName || '').split('.').pop() || '';
      if (!allowedTables.has(tableName) && !allowedTables.has(plainTableName)) {
        throw new Error(`Tabla no permitida en SQL: ${tableName}`);
      }
    }
  }

  async function executeSafeSelectQuery(queryText, queryParams = [], timeoutMs = 5000, options = {}) {
    if (containsBlockedSql(queryText)) {
      throw new Error('SQL bloqueado por políticas de seguridad. Solo SELECT sin sentencias peligrosas.');
    }

    let targetDatabase = null;
    if (options?.allowCrossDatabase === true) {
      targetDatabase = resolveSqlTargetDatabase(queryText, {
        databaseId: options?.databaseId,
        preferReadOnlyDatabases: true,
      });
    }

    const targetAllowedTables = targetDatabase
      ? getDatabaseSchemaTableNames(targetDatabase)
      : null;

    const skipWhitelistForUnknownTargetTables = Boolean(options?.allowUnknownTablesForTarget)
      && targetDatabase;

    if (!skipWhitelistForUnknownTargetTables) {
      await ensureSqlUsesWhitelistedTables(queryText, {
        allowedTables: targetAllowedTables,
      });
    }

    if (targetDatabase && targetDatabase.primary !== true && detectSqlOperation(queryText) !== 'select') {
      throw new Error(`La base ${targetDatabase.id} es de solo consulta (primary=false). Solo se permite SELECT.`);
    }

    if (targetDatabase && targetDatabase.type !== 'postgres') {
      // Adaptar placeholders $N al formato nativo del motor destino
      // MySQL usa ? posicional, Oracle usa :N posicional, MSSQL usa parámetros nominales
      let sqlForTarget = queryText;
      let paramsForTarget = Array.isArray(queryParams) && queryParams.length > 0 ? queryParams : [];

      if (paramsForTarget.length > 0) {
        if (targetDatabase.type === 'mysql') {
          // PostgreSQL $1,$2,... → MySQL ?,?,... (expandir repetidos: $4 dos veces = dos ? y dos valores)
          const expandedMysqlParams = [];
          sqlForTarget = queryText.replace(/\$(\d+)/g, (_, n) => {
            const idx = parseInt(n, 10) - 1;
            expandedMysqlParams.push(paramsForTarget[idx] ?? null);
            return '?';
          });
          paramsForTarget = expandedMysqlParams;
        } else if (targetDatabase.type === 'oracle') {
          // PostgreSQL $1,$2,... → Oracle NAMED binds :p1,:p2,...
          // Los named binds pueden repetirse en el SQL (e.g. :p1 IS NULL OR col = :p1)
          // sin necesidad de duplicar valores. oracledb acepta params como objeto {p1:val,...}.
          const namedOracleParams = {};
          sqlForTarget = queryText.replace(/\$(\d+)/g, (_, n) => {
            const idx = parseInt(n, 10) - 1;
            namedOracleParams[`p${n}`] = paramsForTarget[idx] ?? null;
            return `:p${n}`;
          });
          paramsForTarget = namedOracleParams;
        } else if (targetDatabase.type === 'mssql') {
          // SQL Server no soporta params posicionales por esta vía; sustituir como literales seguros
          // (solo strings y números — oracledb/mssql tienen sus propios bindings)
          let idx = 0;
          sqlForTarget = queryText.replace(/\$\d+/g, () => {
            const val = paramsForTarget[idx++];
            if (val === null || val === undefined) return 'NULL';
            if (typeof val === 'number' && Number.isFinite(val)) return String(val);
            return `'${String(val).replace(/'/g, "''")}'`;
          });
          paramsForTarget = [];
        }
      }

      const rows = await multiDbRegistry.executeCompiledQuery({
        databaseId: targetDatabase.id,
        sql: sqlForTarget,
        params: paramsForTarget,
      });

      return {
        rows,
        rowCount: rows.length,
      };
    }

    const primaryDb = multiDbRegistry.getPrimaryDatabase();
    if (targetDatabase && targetDatabase.type === 'postgres' && primaryDb && targetDatabase.id !== primaryDb.id) {
      const rows = await multiDbRegistry.executeCompiledQuery({
        databaseId: targetDatabase.id,
        sql: queryText,
        params: Array.isArray(queryParams) ? queryParams : [],
      });

      return {
        rows,
        rowCount: rows.length,
      };
    }

    const client = await obtenerPool().connect();
    try {
      await client.query('BEGIN');
      await client.query(`SET LOCAL statement_timeout = ${Math.max(1000, Number(timeoutMs) || 5000)}`);
      const result = await client.query(queryText, queryParams);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  function resolveArticleExecutionDatabaseId(article = {}, structuredScript = null, options = {}) {
    // ── DISEÑO: el "Base de datos destino" configurado en el artículo es SIEMPRE la autoridad ──
    //
    // ORDEN DE PRIORIDAD:
    //   1. target_database  — campo canónico guardado por el modal de admin (la selección explícita del admin)
    //   2. databaseId       — alias camelCase (puede estar desactualizado si hubo save parcial)
    //   3. database_id      — alias snake_case (seed original)
    //   4. target_database_id — alias legacy
    //   5. options.databaseId — override del request (solo se usa si el artículo NO tiene BD configurada,
    //                           porque puede venir del caché del frontend con un valor desactualizado)
    //
    // RAZÓN: options.databaseId viene de inferArticleExecutionTarget() en el frontend, que lee el
    // script_json LOCAL (puede estar en caché). Si target_database ya está en el artículo de la BD,
    // ese valor SIEMPRE debe ganar sobre cualquier cosa que el frontend calcule.

    const articleConfiguredDb =
      String(structuredScript?.target_database || '').trim() ||
      String(structuredScript?.databaseId || '').trim() ||
      String(structuredScript?.database_id || '').trim() ||
      String(structuredScript?.target_database_id || '').trim();

    // El artículo tiene BD explícita configurada → usar siempre, ignorar el param del request
    if (articleConfiguredDb) {
      console.log(`[resolveArticleExecutionDatabaseId] BD del artículo: "${articleConfiguredDb}" (artículo ${article?.id || 'unknown'})`);
      return articleConfiguredDb;
    }

    // El artículo NO tiene BD configurada → usar el override del request como fallback
    const requestOverride = String(options?.databaseId || '').trim();
    if (requestOverride) {
      console.log(`[resolveArticleExecutionDatabaseId] BD del request (fallback): "${requestOverride}" (artículo ${article?.id || 'unknown'})`);
      return requestOverride;
    }

    const explicitDatabaseId = ''; // ninguno encontrado, continuar con detección automática
    if (explicitDatabaseId) {
      // rama never-reached — mantenida para no romper el flujo del return abajo
      return explicitDatabaseId;
    }

    // Syntax-based detection on the SQL
    const sqlText = String(structuredScript?.sql || '').trim();
    if (sqlText) {
      // 1. Directiva explícita: -- database: boton
      const directive = extractDatabaseDirective(sqlText);
      if (directive) return directive;

      // 2. Schema-based: detecta "bpc_ordenes.tabla" y busca qué BD tiene ese schema
      //    Tiene prioridad sobre el tipo de motor para no confundir bpc_base con pg_main
      const schemaMatches = (sqlText.match(/\b([a-z_][a-z0-9_]*)\.([a-z_][a-z0-9_]*)/gi) || [])
        .map((m) => m.split('.')[0].toLowerCase());
      if (schemaMatches.length > 0) {
        const schemaSet = new Set(schemaMatches);
        for (const db of multiDbRegistry.getDatabases()) {
          if (db.primary === true || db.enabled === false) continue;
          const dbSchemas = new Set((db.schema?.tables || []).map((t) => String(t?.schema || 'public').toLowerCase()));
          if ([...schemaSet].some((s) => dbSchemas.has(s))) {
            console.log(`[resolveArticleExecutionDatabaseId] schema match "${[...schemaSet].join(',')}" → ${db.id}`);
            return db.id;
          }
        }
      }

      // 3. Tipo de motor (Oracle/MySQL/MSSQL son únicos — pero evitar asignar postgres a pg_main)
      const engine = detectSqlSyntaxEngine(sqlText);
      if (engine && engine !== 'postgres') {
        const db = multiDbRegistry.findDatabaseByType(engine);
        if (db?.id) return db.id;
      }
    }

    const hintText = [
      String(options?.databaseHint || '').trim(),
      String(article?.titulo || '').trim(),
      String(article?.categoria || '').trim(),
      String(article?.subcategoria || '').trim(),
      String(article?.descripcion || '').trim(),
      sqlText,
    ].filter(Boolean).join(' ');

    const requested = resolverBasesDeConsulta(hintText, []);
    return requested[0] || '';
  }

  async function ejecutarAccionEmpresarial(nombre, params, context, emit) {
    const actionName = String(nombre || '').trim();
    if (!actionName || !obtenerRegistroAcciones()[actionName]) {
      throw new Error(`Acción no permitida: ${actionName || 'desconocida'}`);
    }

    return obtenerRegistroAcciones()[actionName]({ params, context, emit });
  }

  async function executeBusinessWorkflow(script, rawParams, emit) {
    const scriptName = String(script.script || '').trim();
    const rawSteps = Array.isArray(script.pasos) ? script.pasos : [];
    if (rawSteps.length === 0) {
      throw new Error('Script empresarial sin pasos');
    }

    const params = Object.entries(rawParams || {}).reduce((acc, [key, value]) => {
      acc[key] = sanitizeStructuredParamValue(value);
      return acc;
    }, {});

    const context = {};
    const orderedSteps = rawSteps.map((step, index) => normalizeBusinessStep(step, index)).sort((a, b) => a.orden - b.orden);
    const actionNames = orderedSteps.map((step) => step.accion);
    const requiredParamsFromActions = getRequiredParamsForActions(actionNames);
    const requiredParamsFromScript = Array.isArray(script.parametros_requeridos)
      ? script.parametros_requeridos.map((item) => String(item || '').trim()).filter(Boolean)
      : [];
    const requiredParams = Array.from(new Set([...requiredParamsFromActions, ...requiredParamsFromScript]));

    for (const actionName of actionNames) {
      const metadata = getBusinessActionMetadata(actionName);
      if (!metadata) {
        throw new Error(`Acción no permitida: ${actionName}`);
      }
    }

    for (const paramName of requiredParams) {
      const value = params[paramName];
      if (value === undefined || value === null || String(value).trim() === '') {
        throw new Error(`Falta parámetro requerido para workflow empresarial: ${paramName}`);
      }
    }

    const stepLogs = [];
    let finalResult = null;

    if (typeof emit === 'function') {
      emit(`🏢 Script empresarial detectado${scriptName ? `: ${scriptName}` : ''}`);
      emit(`[PARAMS] ${JSON.stringify(params)}`);
    }

    for (const step of orderedSteps) {
      const stepLabel = `[STEP ${step.orden}] ${step.descripcion}`;
      stepLogs.push(stepLabel);
      if (typeof emit === 'function') emit(stepLabel);

      try {
        if (typeof emit === 'function') emit(`⏳ Ejecutando acción: ${step.accion}`);
        const actionResult = await ejecutarAccionEmpresarial(step.accion, params, context, emit || (() => {}));
        finalResult = actionResult;
        context[`step_${step.orden}`] = actionResult;
        const okLog = `[STEP ${step.orden}] ✔ Acción ${step.accion} completada`;
        stepLogs.push(okLog);
        if (typeof emit === 'function') emit(okLog);
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Error desconocido';
        const failLog = `[STEP ${step.orden}] ❌ ${message}`;
        stepLogs.push(failLog);
        if (typeof emit === 'function') emit(failLog, 'error');
        throw new Error(`Paso ${step.orden} falló: ${message}`);
      }
    }

    return {
      success: true,
      steps: stepLogs,
      message: 'Script ejecutado correctamente',
      context,
      resultado_final: finalResult,
    };
  }

  async function executeErrorAnalysis(text, dbPool) {
    const analysis = analyzeErrorInput(text);
    const matches = await searchErrorLogs(dbPool, analysis, 20);
    const enrichedAnalysis = await enrichErrorAnalysisWithAi(analysis, matches);
    return buildErrorAnalysisResponse(enrichedAnalysis, matches);
  }

  /**
   * Executes a write (DML) query on the specified target database, converting
   * placeholder syntax as needed for MySQL, Oracle, MSSQL, and MongoDB.
   * Falls back to the primary Postgres pool when no targetDatabaseId is given.
   */
  async function executeWriteToTargetDb(sqlText, queryParams, targetDatabaseId) {
    const params = Array.isArray(queryParams) ? queryParams : [];

    if (!targetDatabaseId) {
      return await obtenerPool().query(sqlText, params);
    }

    const targetDb = multiDbRegistry.getDatabaseById(targetDatabaseId);
    if (!targetDb) {
      // DB not in registry — fall back to primary pool (postgres)
      return await obtenerPool().query(sqlText, params);
    }

    if (targetDb.type === 'postgres') {
      const primaryDb = multiDbRegistry.getPrimaryDatabase();
      if (!primaryDb || targetDb.id === primaryDb.id) {
        return await obtenerPool().query(sqlText, params);
      }
      const rows = await multiDbRegistry.executeCompiledQuery({ databaseId: targetDb.id, sql: sqlText, params });
      return { rows: rows || [], rowCount: rows?.rowCount ?? (rows || []).length };
    }

    if (targetDb.type === 'mongodb') {
      const rows = await multiDbRegistry.executeCompiledQuery({ databaseId: targetDb.id, sql: sqlText, params: {} });
      return { rows: rows || [], rowCount: rows?.rowCount ?? (rows || []).length };
    }

    // MySQL / Oracle / MSSQL — convert $N placeholders to native format
    let sqlForTarget = sqlText;
    let paramsForTarget = [...params];

    if (paramsForTarget.length > 0) {
      if (targetDb.type === 'mysql') {
        // Expandir repetidos: si $4 aparece 2 veces → 2 '?' y 2 copias del valor en paramsForTarget
        const expandedMysqlWriteParams = [];
        sqlForTarget = sqlText.replace(/\$(\d+)/g, (_, n) => {
          const idx = parseInt(n, 10) - 1;
          expandedMysqlWriteParams.push(params[idx] ?? null);
          return '?';
        });
        paramsForTarget = expandedMysqlWriteParams;
      } else if (targetDb.type === 'oracle') {
        // Oracle NAMED binds: :p1, :p2,... — pueden repetirse sin duplicar valores
        const namedOracleWriteParams = {};
        sqlForTarget = sqlText.replace(/\$(\d+)/g, (_, n) => {
          const idx = parseInt(n, 10) - 1;
          namedOracleWriteParams[`p${n}`] = params[idx] ?? null;
          return `:p${n}`;
        });
        paramsForTarget = namedOracleWriteParams;
      } else if (targetDb.type === 'mssql') {
        // Convertir $N → @pN; los valores se pasan como parámetros vinculados (no concatenación)
        sqlForTarget = sqlText.replace(/\$(\d+)/g, '@p$1');
        // paramsForTarget se mantiene; MultiDbRegistry usará request.input('p1', val)
      }
    }

    const rows = await multiDbRegistry.executeCompiledQuery({ databaseId: targetDb.id, sql: sqlForTarget, params: paramsForTarget });
    return { rows: rows || [], rowCount: rows?.rowCount ?? (rows || []).length };
  }

  /** Runs a bounded SELECT against the target DB to snapshot rows. Never throws. */
  async function snapshotRowsForDetail(table, whereClause, values, targetDatabaseId, dbType, limit = 100) {
    try {
      const safeTable = String(table || '').replace(/[^\w.$]/g, '');
      if (!safeTable) return [];
      const limitClause = dbType === 'oracle' ? ` FETCH FIRST ${limit} ROWS ONLY` : ` LIMIT ${limit}`;
      let snapSql = `SELECT * FROM ${safeTable}`;
      let snapValues = [];
      if (whereClause && whereClause.trim()) {
        const renum = renumberWhereParams(whereClause, values);
        snapSql += ` WHERE ${renum.clause}`;
        snapValues = renum.values;
      }
      snapSql += limitClause;
      const res = await executeSafeSelectQuery(snapSql, snapValues, 5000, {
        allowCrossDatabase: true,
        databaseId: targetDatabaseId,
        allowUnknownTablesForTarget: true,
      });
      return Array.isArray(res?.rows) ? res.rows : [];
    } catch (err) {
      console.warn('[WRITE_DETAIL] snapshot falló:', err?.message || err);
      return [];
    }
  }

  async function executeSingleManualSqlStep(sqlText, rawParams, emit, workflowContext = {}, silent = false, options = {}) {
    const startedAt = Date.now();
    const _stepDatabaseId = String(options?.databaseId || '').trim();
    const _stepTargetDb = _stepDatabaseId ? multiDbRegistry.getDatabaseById(_stepDatabaseId) : null;
    const _stepDbType = String(_stepTargetDb?.type || 'postgres').toLowerCase();
    const isMongoDb = _stepDbType === 'mongodb';

    let text, values, placeholderOrder, placeholderOccurrences, operation;

    if (isMongoDb) {
      // MongoDB: resolve {{variable}} placeholders via string substitution,
      // then pass the resulting JSON directly to executeCompiledQuery.
      // A placeholder may appear two ways inside the JSON:
      //   "{{dept}}"  → user already quoted it  → replace incl. quotes with JSON value
      //   {{min}}     → bare placeholder        → replace with JSON value
      // Both must end up as a single valid JSON token (no double-quoting).
      let resolvedQuery = String(sqlText || '').trim();
      const allContext = { ...(workflowContext || {}), ...(rawParams || {}) };
      for (const [key, val] of Object.entries(allContext)) {
        const jsonValue = val === null || val === undefined ? 'null'
          : typeof val === 'string' ? JSON.stringify(val)
          : (typeof val === 'number' || typeof val === 'boolean') ? String(val)
          : JSON.stringify(val);
        const keyPattern = `\\{\\{\\s*${key}\\s*\\}\\}`;
        // 1) quoted form: "{{key}}" → jsonValue (strip the surrounding quotes)
        resolvedQuery = resolvedQuery.replace(new RegExp(`"${keyPattern}"`, 'g'), jsonValue);
        // 2) bare form: {{key}} → jsonValue
        resolvedQuery = resolvedQuery.replace(new RegExp(keyPattern, 'g'), jsonValue);
      }
      text = resolvedQuery;
      values = [];
      placeholderOrder = [];
      placeholderOccurrences = [];
      operation = detectMongoOperation(resolvedQuery);
    } else {
      // Standard SQL path
      ({ text, values, placeholderOrder, placeholderOccurrences } = buildManualSqlQuery(
        sqlText, rawParams, workflowContext, { ...options, dbType: _stepDbType },
      ));
      operation = detectSqlOperation(sqlText);
    }

    if (!silent && typeof emit === 'function') {
      emit(isMongoDb ? '🍃 Motor MongoDB activado' : '🧠 Motor SQL manual avanzado activado');
      emit(`[OPERATION] ${operation.toUpperCase()}`);
      emit(`[QUERY] ${text.slice(0, 500)}`);
      if (placeholderOrder.length > 0) emit(`[SQL PLACEHOLDERS] ${JSON.stringify(placeholderOrder)}`);
      if (values.length > 0) emit(`[SQL PARAMS] ${JSON.stringify(values)}`);
    }

    logger.info('Query manual ejecutada', {
      operation, dbType: _stepDbType,
      query: text.slice(0, 300),
      paramCount: values.length,
      databaseId: _stepDatabaseId || 'primary',
    });

    const targetDatabaseId = _stepDatabaseId;

    let result;
    let writeDetail = null; // human before/after diff for UPDATE/DELETE/INSERT
    if (isMongoDb) {
      // MongoDB: all operations go through executeCompiledQuery
      const rows = await multiDbRegistry.executeCompiledQuery({ databaseId: targetDatabaseId, sql: text, params: {} });
      result = { rows: rows || [], rowCount: rows?.rowCount ?? (rows || []).length };
    } else if (operation === 'select') {
      // Generous timeout (2 min, env-overridable) so heavy analytical SELECTs run
      result = await executeSafeSelectQuery(text, values, MANUAL_SQL_TIMEOUT_MS, {
        allowCrossDatabase: true,
        databaseId: targetDatabaseId,
        allowUnknownTablesForTarget: true,
      });
    } else {
      // DML, DDL, MERGE, CALL, EXEC, TRUNCATE, DROP, CREATE, ALTER...
      // Todo pasa al motor del target — la seguridad es la visibilidad del artículo.
      //
      // Para UPDATE/DELETE/INSERT capturamos un "antes" y un "después" de las filas
      // afectadas para mostrar al usuario el cambio real (campo: viejo → nuevo).
      const parsedTarget = parseWriteTarget(text);
      let beforeRows = [];
      if (parsedTarget && (parsedTarget.operation === 'update' || parsedTarget.operation === 'delete')) {
        beforeRows = await snapshotRowsForDetail(
          parsedTarget.table, parsedTarget.whereClause, values, targetDatabaseId, _stepDbType,
        );
      }

      result = await executeWriteToTargetDb(text, values, targetDatabaseId);

      if (parsedTarget) {
        let afterRows = [];
        if (parsedTarget.operation === 'update') {
          afterRows = await snapshotRowsForDetail(
            parsedTarget.table, parsedTarget.whereClause, values, targetDatabaseId, _stepDbType,
          );
        } else if (parsedTarget.operation === 'insert' && Array.isArray(result?.rows) && result.rows.length > 0) {
          // Postgres con RETURNING devuelve las filas insertadas
          afterRows = result.rows;
        }
        try {
          writeDetail = buildWriteDetailExplanation({
            operation: parsedTarget.operation,
            table: parsedTarget.table,
            affectedRows: Number(result.rowCount || 0),
            beforeRows,
            afterRows,
            text,
          });
        } catch (detailErr) {
          console.warn('[WRITE_DETAIL] no se pudo construir el detalle:', detailErr?.message || detailErr);
        }
      }
    }

    const executionMs = Date.now() - startedAt;
    const affectedRows = Number(result.rowCount || 0);

    if (operation !== 'select') {
      console.info('[WRITE_AUDIT]', {
        operation, dbType: _stepDbType,
        affectedRows,
        databaseId: targetDatabaseId || 'primary',
        query: text.slice(0, 200),
      });
    }

    // Build a meaningful explanation even when no rows are returned
    const rows = result.rows || [];
    let explicacion;
    if (writeDetail) {
      // Detailed before/after diff for write operations
      explicacion = writeDetail;
    } else if (rows.length > 0) {
      explicacion = buildHumanSqlExplanation(rows);
    } else if (operation !== 'select') {
      // Write operation we couldn't parse for detail — describe what was executed
      const opUpper = String(operation || 'write').toUpperCase();
      const queryPreview = text.slice(0, 120).replace(/\s+/g, ' ').trim();
      const ellipsis = text.length > 120 ? '…' : '';
      if (affectedRows > 0) {
        explicacion = `✅ ${opUpper} ejecutado correctamente. ${affectedRows} fila${affectedRows !== 1 ? 's' : ''} afectada${affectedRows !== 1 ? 's' : ''}.\n📝 SQL: ${queryPreview}${ellipsis}`;
      } else {
        explicacion = `✅ ${opUpper} ejecutado correctamente (0 filas afectadas).\n📝 SQL: ${queryPreview}${ellipsis}`;
      }
    } else {
      explicacion = 'La consulta se ejecutó correctamente pero no devolvió registros.';
    }

    return {
      success: true,
      data: rows,
      resultado: rows,
      resultado_final: rows[0] || null,
      affectedRows,
      message: 'Consulta ejecutada correctamente',
      explicacion,
      executedQuery: text,
      placeholderOrder,
      placeholderOccurrences,
      queryParams: values,
      executionMs,
    };
  }

  async function executeProceduralScriptWithNotices(sqlText, emit) {
    const startedAt = Date.now();
    const text = String(sqlText || '').trim();
    if (!text) {
      throw new Error('Script procedural vacío');
    }

    const client = await obtenerPool().connect();
    const notices = [];
    const onNotice = (msg) => {
      const noticeText = String(msg?.message || '').trim();
      if (!noticeText) return;
      notices.push(noticeText);
      if (typeof emit === 'function') {
        emit(`NOTICE: ${noticeText}`);
      }
    };

    try {
      client.on('notice', onNotice);
      const result = await client.query(text);
      const rows = Array.isArray(result?.rows) ? result.rows : [];
      const affectedRows = Number(result?.rowCount || 0);
      const executionMs = Date.now() - startedAt;
      const resumenHumano = notices.length > 0
        ? notices.join('\n')
        : buildHumanSqlExplanation(rows);
      const semanticRow = rows[0] && typeof rows[0] === 'object' ? rows[0] : {};

      const stepExecution = {
        step: 'step1',
        title: 'Script procedural (PostgreSQL)',
        executedQuery: text,
        executionMs,
        affectedRows,
      };

      return {
        success: true,
        message: 'Script procedural ejecutado correctamente',
        explicacion: buildHumanSqlExplanation(rows),
        resumenHumano,
        notices,
        resultadoSemantico: semanticRow,
        data: rows,
        resultado_final: rows[0] || null,
        affectedRows,
        workflow: {
          steps: [stepExecution],
          context: {},
          final: rows,
          interpreted: null,
        },
        workflowContext: {},
        executedQueries: [stepExecution],
        executedQuery: text,
        placeholderOrder: [],
        queryParams: [],
        executionMs,
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error || 'Error ejecutando script procedural');
      const suffix = notices.length > 0 ? ` Notices capturados: ${notices.join(' | ')}` : '';
      throw new Error(`${message}${suffix}`.trim());
    } finally {
      client.removeListener('notice', onNotice);
      client.release();
    }
  }

  async function executeOneSqlStep(sqlText, rawParams, workflowContext, stepNum, emit) {
    if (typeof emit === 'function') {
      emit(`[STEP ${stepNum}] ${sqlText.trim().split(/\s+/).slice(0, 4).join(' ')}...`);
    }
    // Always silent=true so no technical param/placeholder noise is emitted
    return executeSingleManualSqlStep(sqlText, rawParams, null, workflowContext, true, {
      assumeWriteConfirmed: true,
    });
  }

  async function executeNoticeStep(step, workflowContext, stepNum, emit) {
    const args = Array.isArray(step?.noticeArgs)
      ? step.noticeArgs.map((expr) => evaluateProceduralExpression(expr, workflowContext))
      : [];
    const message = formatRaiseNoticeMessage(step?.noticeTemplate || '', args);

    if (typeof emit === 'function' && message) {
      emit(`NOTICE: ${message}`);
    }

    return {
      success: true,
      data: [],
      resultado_final: null,
      affectedRows: 0,
      message,
      explicacion: message,
      notice: message,
      executionMs: 0,
    };
  }

  async function executeStepList(steps, rawParams, workflowContext, startIndex, emit) {
    const executions = [];
    const notices = [];
    let finalResult = null;
    for (let i = 0; i < steps.length; i++) {
      const step = steps[i];
      const stepNum = startIndex + i + 1;
      if (typeof emit === 'function') emit(`[STEP ${stepNum}] ${step.title}`);

      if (step.type === 'notice') {
        const noticeResult = await executeNoticeStep(step, workflowContext, stepNum, emit);
        finalResult = finalResult || noticeResult;
        if (noticeResult?.notice) notices.push(String(noticeResult.notice));
        executions.push({
          step: `step${stepNum}`,
          executedQuery: `RAISE NOTICE '${String(step.noticeTemplate || '').replace(/'/g, "''")}'`,
          executionMs: 0,
          title: step.title,
          affectedRows: 0,
          notice: noticeResult?.notice || null,
        });
      } else if (step.type === 'for_loop') {
        const loopResult = await executeForLoopStep(step, rawParams, workflowContext, stepNum, emit);
        finalResult = loopResult;
        workflowContext[`step${stepNum}`] = normalizeWorkflowStoredValue(loopResult);
        executions.push({ step: `step${stepNum}`, executedQuery: step.sql, executionMs: loopResult.executionMs || 0, title: step.title, affectedRows: loopResult.affectedRows || 0 });
        if (Array.isArray(loopResult?.notices)) notices.push(...loopResult.notices.map((n) => String(n || '').trim()).filter(Boolean));
      } else if (step.type === 'conditional') {
        const condResult = await executeConditionalStep(step, rawParams, workflowContext, stepNum, emit);
        finalResult = condResult;
        workflowContext[`step${stepNum}`] = normalizeWorkflowStoredValue(condResult);
        executions.push({ step: `step${stepNum}`, executedQuery: step.conditionSql || '-- conditional', executionMs: condResult.executionMs || 0, title: step.title, affectedRows: condResult.affectedRows || 0 });
        if (Array.isArray(condResult?.notices)) notices.push(...condResult.notices.map((n) => String(n || '').trim()).filter(Boolean));
      } else {
        const stepResult = await executeOneSqlStep(step.sql, rawParams, workflowContext, stepNum, emit);
        finalResult = stepResult;
        workflowContext[`step${stepNum}`] = normalizeWorkflowStoredValue(stepResult);
        executions.push({
          step: `step${stepNum}`,
          executedQuery: stepResult.executedQuery,
          executionMs: stepResult.executionMs,
          title: step.title,
          affectedRows: stepResult.affectedRows || 0,
          explicacion: stepResult.explicacion || '',
        });
      }
    }
    return { executions, finalResult, notices };
  }

  async function executeForLoopStep(step, rawParams, workflowContext, stepNum, emit) {
    const startedAt = Date.now();
    const selectResult = await executeOneSqlStep(step.sql, rawParams, workflowContext, stepNum, emit);
    const rows = selectResult.data || [];
    const allResults = [];
    const loopNotices = [];
    let affected = 0;

    for (let rowIdx = 0; rowIdx < rows.length; rowIdx++) {
      const row = rows[rowIdx];
      // Inject loopRow into workflowContext so {{loopRow.field}} resolves via parameterized path
      const loopContext = { ...workflowContext, loopRow: row };
      const { executions: bodyExecs, finalResult: bodyFinal, notices: bodyNotices } = await executeStepList(
        step.bodySteps || [],
        rawParams,
        loopContext,
        stepNum,
        emit,
      );

      if (Array.isArray(bodyFinal?.data)) {
        allResults.push(...bodyFinal.data);
      }

      affected += (bodyExecs || []).reduce((sum, entry) => sum + Number(entry?.affectedRows || 0), 0);

      if (Array.isArray(bodyNotices) && bodyNotices.length > 0) {
        loopNotices.push(...bodyNotices);
      }
    }

    return {
      success: true,
      data: allResults,
      resultado_final: allResults[0] || null,
      affectedRows: affected,
      loopIterations: rows.length,
      notices: loopNotices,
      message: `Loop procesó ${rows.length} registros`,
      explicacion: `Se procesaron ${rows.length} registros en el paso ${stepNum}. ${affected > 0 ? `${affected} filas afectadas.` : ''}`,
      executionMs: Date.now() - startedAt,
    };
  }

  async function executeConditionalStep(step, rawParams, workflowContext, stepNum, emit) {
    const startedAt = Date.now();
    let conditionMet = true;

    if (step.conditionSql) {
      try {
        const condResult = await executeOneSqlStep(step.conditionSql, rawParams, workflowContext, stepNum, null);
        const firstRow = condResult.data?.[0];
        const firstVal = firstRow ? Object.values(firstRow)[0] : null;
        conditionMet = Number(firstVal) > 0 || firstVal === true || firstVal === 't';
      } catch {
        conditionMet = false;
      }
    } else {
      // Numeric/variable condition: evaluate against workflowContext
      conditionMet = evaluateSimpleCondition(step.conditionExpr, workflowContext);
    }

    const branchSteps = conditionMet ? (step.thenSteps || []) : (step.elseSteps || []);
    const branchTag = conditionMet ? 'THEN' : 'ELSE';
    if (typeof emit === 'function' && branchSteps.length > 0) {
      emit(`[STEP ${stepNum}] Condición evaluada → ${branchTag} (${branchSteps.length} pasos)`);
    }

    const { executions, finalResult, notices } = await executeStepList(branchSteps, rawParams, workflowContext, stepNum, emit);

    if (finalResult) {
      return {
        ...finalResult,
        notices: Array.isArray(notices) ? notices : [],
      };
    }

    return {
      success: true,
      data: [],
      resultado_final: null,
      affectedRows: 0,
      notices: notices || [],
      message: `Condicional evaluado: ${branchTag}`,
      explicacion: `La condición resultó en rama ${branchTag}.`,
      executionMs: Date.now() - startedAt,
    };
  }

  async function executeComplexSqlWorkflow(sqlText, rawParams, emit) {
    const startedAt = Date.now();

    let parsed;
    try {
      parsed = parsePlSqlScript(sqlText);
    } catch (parseErr) {
      throw new Error(`Error interpretando script: ${parseErr.message}`);
    }

    const { steps } = parsed;
    if (steps.length === 0) {
      throw new Error('El script procedural no generó pasos ejecutables');
    }

    if (typeof emit === 'function') {
      emit(`🧠 Script interpretado: ${steps.length} pasos detectados`);
    }

    const workflowContext = {};
    const stepPayload = {};
    const { executions, finalResult, notices } = await executeStepList(steps, rawParams, workflowContext, 0, emit);

    executions.forEach((ex) => { stepPayload[ex.step] = workflowContext[ex.step]; });

    const aggregatedResult = buildAggregatedResultFromStepPayload(stepPayload);
    const consolidatedData = aggregatedResult ? [aggregatedResult] : (finalResult?.data ?? []);
    const consolidatedFinal = aggregatedResult ?? finalResult?.resultado_final ?? (consolidatedData[0] || null);

    const explanation = buildComplexWorkflowExplanation(
      steps,
      executions,
      {
        ...(finalResult || {}),
        data: consolidatedData,
        resultado_final: consolidatedFinal,
      },
    );

    const summaryLines = Array.isArray(notices)
      ? notices.map((line) => String(line || '').trim()).filter(Boolean)
      : [];

    const semanticFromNotices = extractSemanticMetricsFromNotices(summaryLines);
    const semanticFromData = (consolidatedData && consolidatedData[0] && typeof consolidatedData[0] === 'object')
      ? consolidatedData[0]
      : {};
    const structuredSemanticResult = {
      ...semanticFromData,
      ...semanticFromNotices,
    };

    return {
      success: true,
      message: 'Script procedural ejecutado correctamente',
      explicacion: explanation,
      notices: summaryLines,
      // Per-step write detail (antes→después) takes priority over RAISE NOTICE lines
      resumenHumano: /▸|✅\s+(UPDATE|DELETE|INSERT)\b/.test(explanation)
        ? explanation
        : (summaryLines.length > 0 ? summaryLines.join('\n') : explanation),
      resultadoSemantico: structuredSemanticResult,
      ...stepPayload,
      data: consolidatedData,
      resultado_final: consolidatedFinal,
      workflow: {
        steps: executions,
        context: stepPayload,
        final: consolidatedData,
        interpreted: { stepCount: steps.length, inputVars: parsed.inputVars },
      },
      workflowContext: stepPayload,
      executedQueries: executions,
      placeholderOrder: [],
      queryParams: [],
      executionMs: Date.now() - startedAt,
    };
  }

  async function executeManualSqlScript(script, rawParams, emit) {
    // ── Construir sqlText ─────────────────────────────────────────────────────
    // Para artículos SQL estándar: script.sql contiene el texto de la consulta.
    // Para artículos MongoDB: el script guarda pipeline/collection/operation por separado
    // y hay que ensamblar el JSON { collection, aggregate/find/... } que espera executeCompiledQuery.
    const sqlText = (() => {
      const rawSql = String(script?.sql || '').trim();
      if (rawSql) return rawSql;

      const collection = String(script?.collection || '').trim();
      const pipelineRaw = String(script?.pipeline || '').trim();
      const filterRaw   = String(script?.filter   || '').trim();
      const sortRaw     = String(script?.sort      || '').trim();
      const limitVal    = script?.limit != null ? Number(script.limit) : null;
      const operation   = String(script?.operation || 'aggregate').trim().toLowerCase();

      if (!collection) return rawSql;

      // ── BSON type normalizer (shared by pipeline and filter parsers) ────────
      const normalizeBson = (str) => str
        .replace(/\bObjectId\s*\(\s*['"]([^'"]*)['"]\s*\)/g, '"$1"')
        .replace(/\bNumberInt\s*\(\s*['"]?(-?\d+)['"]?\s*\)/g, '$1')
        .replace(/\bNumberLong\s*\(\s*['"]?(-?\d+)['"]?\s*\)/g, '$1')
        .replace(/\bNumberDecimal\s*\(\s*['"]?(-?[\d.]+)['"]?\s*\)/g, '$1')
        .replace(/\bDouble\s*\(\s*['"]?(-?[\d.eE+\-]+)['"]?\s*\)/g, '$1')
        .replace(/\bISODate\s*\(\s*['"]([^'"]*)['"]\s*\)/g, '"$1"')
        .replace(/\bnew\s+Date\s*\(\s*['"]([^'"]*)['"]\s*\)/g, '"$1"')
        .replace(/\bBinData\s*\([^)]*\)/g, '"[BinData]"')
        .replace(/\bTimestamp\s*\([^)]*\)/g, '0')
        .replace(/\bUUID\s*\(\s*['"]([^'"]*)['"]\s*\)/g, '"$1"');

      const relaxMongo = (str) => normalizeBson(str)
        .replace(/([{,]\s*)([a-zA-Z_$][a-zA-Z0-9_$]*)\s*:/g, '$1"$2":')  // quote unquoted keys
        .replace(/:\s*'([^'\\]*(\\.[^'\\]*)*)'/g, ': "$1"');              // single→double quotes

      // ── Aggregate pipeline (e.g. MongoDB Pipeline con $group, $addFields) ───
      if (pipelineRaw) {
        try {
          // Try strict JSON first; fall back to relaxed parser for MongoDB shell / BSON syntax
          let parsedPipeline;
          try {
            parsedPipeline = JSON.parse(pipelineRaw);
          } catch {
            parsedPipeline = JSON.parse(relaxMongo(pipelineRaw));
          }
          const mongoOp = { collection };
          if (operation === 'aggregate') mongoOp.aggregate = parsedPipeline;
          else mongoOp[operation] = parsedPipeline;
          return JSON.stringify(mongoOp);
        } catch {
          // Will fall through and fail with clear message at sqlSteps check
        }
      }

      // ── Find / findOne con filter (puede contener {{params}} no resueltos aún) ──
      // Construimos el JSON como string para que executeSingleManualSqlStep pueda
      // reemplazar los {{placeholders}} antes de parsear.
      if (operation === 'find' || operation === 'findone' || filterRaw) {
        const collectionJson = JSON.stringify(collection);
        // Normalize BSON types in filter (e.g. from MongoDB Compass copy-paste)
        const findFilter = relaxMongo(filterRaw || '{}');
        let jsonStr = `{"collection":${collectionJson},"find":${findFilter}`;
        if (sortRaw)  jsonStr += `,"sort":${relaxMongo(sortRaw)}`;
        if (limitVal) jsonStr += `,"limit":${limitVal}`;
        jsonStr += '}';
        return jsonStr;
      }

      return rawSql;
    })();

    // ── Resolve target database ───────────────────────────────────────────────
    // target_database es el campo canónico guardado por el admin. databaseId es alias.
    // target_database SIEMPRE tiene prioridad: es lo que el admin seleccionó en el modal.
    const resolvedDatabaseId =
      String(script?.target_database || '').trim() ||
      String(script?.databaseId || '').trim() ||
      String(script?.database_id || '').trim() ||
      null;

    console.log(buildEngineLog({
      engine: null,
      queryType: detectQueryType(sqlText),
      databaseId: resolvedDatabaseId || 'primary (fallback)',
      reason: resolvedDatabaseId ? 'script.databaseId (Base de datos destino)' : 'primary (fallback)',
      sql: sqlText,
    }));

    // ── Native Oracle PL/SQL path ─────────────────────────────────────────────
    // DO $$ blocks are unambiguously PostgreSQL — never route to Oracle regardless of other markers
    const isPostgresAnonymousBlock = /\bDO\s+\$\$/i.test(sqlText);
    const resolvedDb = resolvedDatabaseId ? multiDbRegistry.getDatabaseById(resolvedDatabaseId) : null;
    // Sin base destino elegida, el motor se deduce del propio SQL.
    //
    // Esta línea decía `detectedEngine`, un nombre que no existe en ninguna parte del
    // backend —el único parecido, `_detectedEngine`, vive en otra función— así que
    // lanzaba ReferenceError siempre que un artículo no traía base destino y el script
    // no era un bloque DO $$. Venía de antes de este refactor, en main también está.
    //
    // Se arregla con lo que evidentemente se quería: detectar el motor por la sintaxis,
    // que es para lo que existe detectSqlSyntaxEngine y lo que ya se hace en la ruta
    // de consultas. Está bien acotado, porque la rama de Oracle solo entra si además
    // el script parece PL/SQL. La alternativa conservadora sería poner `false` y
    // mandarlo siempre a la base primaria, pero entonces un PL/SQL sin base destino
    // iría a PostgreSQL, donde no puede funcionar.
    const isOracleTarget = !isPostgresAnonymousBlock && (
      resolvedDb?.type === 'oracle'
      || (!resolvedDatabaseId && detectSqlSyntaxEngine(sqlText) === 'oracle')
    );

    if (isOracleTarget && (isNativePlSql(sqlText) || shouldForceProceduralMode(sqlText) || isComplexSqlScript(sqlText))) {
      const oracleTarget = resolvedDb || multiDbRegistry.findDatabaseByType('oracle');
      if (oracleTarget?.type === 'oracle') {
        if (typeof emit === 'function') emit('🔍 Oracle PL/SQL detectado. Ejecutando nativamente en Oracle...');
        const rows = await multiDbRegistry.executeOraclePlSqlBlock(oracleTarget.id, sqlText);
        return {
          success: true,
          message: 'Script PL/SQL ejecutado en Oracle correctamente',
          explicacion: `Se obtuvieron ${rows.length} línea(s) de salida de Oracle.`,
          data: rows,
          resultado: rows,
          resultado_final: rows[0] || null,
          executedQuery: sqlText,
          placeholderOrder: [],
          queryParams: [],
          executionMs: 0,
        };
      }
    }

    // ── Complex PostgreSQL procedural scripts ─────────────────────────────────
    // Only apply for PostgreSQL targets (or fallback primary). MySQL/Oracle/MongoDB/MSSQL
    // articles must NOT be intercepted here — INSERT INTO triggers \bINTO\b in isComplexSqlScript.
    const isPostgresOrDefaultTarget = !resolvedDatabaseId || !resolvedDb || resolvedDb.type === 'postgres';
    if (isPostgresOrDefaultTarget && (shouldForceProceduralMode(sqlText) || isComplexSqlScript(sqlText))) {
      if (typeof emit === 'function') {
        emit('🔍 Paso 1: Analizando script');
        emit('📊 Paso 2: Procesando lógica');
        emit('📈 Paso 3: Generando resultado');
        emit('🧠 Script procedural detectado. Ejecutando en PostgreSQL real...');
      }
      return executeProceduralScriptWithNotices(sqlText, emit);
    }

    // Guard: reject raw MongoDB BSON documents (ObjectId/NumberInt/ISODate without db. prefix)
    // These cause confusing JS parser errors downstream. Users must use db.collection.op() syntax.
    const BSON_CONSTRUCTOR_RE = /\b(ObjectId|NumberInt|NumberLong|NumberDecimal|Double|ISODate)\s*\(/;
    if (BSON_CONSTRUCTOR_RE.test(sqlText) && !/^\s*db\./.test(sqlText)) {
      throw new Error(
        'Se detectó un documento MongoDB (ObjectId/NumberInt/ISODate). '
        + 'Para consultar usa: db.conciliacion.find({institutionCode:4}) '
        + 'o db.conciliacion.aggregate([{$match:{institutionCode:4}}])'
      );
    }

    const sqlSteps = splitManualSqlSteps(sqlText);
    if (sqlSteps.length === 0) {
      throw new Error('SQL manual vacío');
    }

    if (sqlSteps.length > 1) {
      const startedAt = Date.now();
      const workflowContext = {};
      const stepPayload = {};
      const stepExecutions = [];
      let finalResult = null;

      for (let index = 0; index < sqlSteps.length; index += 1) {
        const stepNumber = index + 1;
        const stepSql = sqlSteps[index];
        const stepKey = `step${stepNumber}`;
        const stepTitle = getManualSqlStepTitle(stepSql, stepNumber);

        if (typeof emit === 'function') {
          emit(`[STEP ${stepNumber}] ${stepTitle}`);
        }

        let stepResult;
        try {
          stepResult = await executeSingleManualSqlStep(stepSql, rawParams, null, workflowContext, true, {
            databaseId: resolvedDatabaseId,
          });
        } catch (error) {
          const message = error instanceof Error ? error.message : 'Error de ejecución';
          throw new Error(`Paso ${stepNumber} (${stepTitle}) falló: ${message}`);
        }

        finalResult = stepResult;
        workflowContext[stepKey] = normalizeWorkflowStoredValue(stepResult);
        stepPayload[stepKey] = workflowContext[stepKey];
        stepExecutions.push({
          step: stepKey,
          title: stepTitle,
          executedQuery: stepResult.executedQuery,
          queryParams: stepResult.queryParams,
          placeholderOrder: stepResult.placeholderOrder,
          affectedRows: stepResult.affectedRows || 0,
          executionMs: stepResult.executionMs,
          explicacion: stepResult.explicacion || '',
        });
      }

      const aggregatedResult = buildAggregatedResultFromStepPayload(stepPayload);
      const consolidatedData = aggregatedResult ? [aggregatedResult] : (finalResult?.data ?? []);
      const consolidatedFinal = aggregatedResult ?? finalResult?.resultado_final ?? (consolidatedData[0] || null);

      // Consolidate per-step write detail (antes→después) when present;
      // otherwise fall back to the last step's explanation.
      const perStepSummary = buildPerStepWriteSummary(stepExecutions);

      return {
        success: true,
        message: 'Proceso SQL ejecutado correctamente',
        explicacion: perStepSummary || finalResult?.explicacion || buildHumanSqlExplanation(consolidatedData),
        ...stepPayload,
        final: consolidatedData,
        data: consolidatedData,
        resultado_final: consolidatedFinal,
        workflow: {
          steps: stepExecutions,
          context: stepPayload,
          final: consolidatedData,
        },
        workflowContext: stepPayload,
        executedQueries: stepExecutions,
        placeholderOrder: stepExecutions.flatMap((item) => item.placeholderOrder || []),
        queryParams: stepExecutions.flatMap((item) => item.queryParams || []),
        executionMs: Date.now() - startedAt,
      };
    }

    return executeSingleManualSqlStep(sqlText, rawParams, emit, {}, false, {
      databaseId: resolvedDatabaseId,
    });
  }

  async function runPostProcessValidations(script, tableName, normalizedInputParams, sourceRows, sourceWhere, emit) {
    const validations = Array.isArray(script?.post_proceso?.validaciones) ? script.post_proceso.validaciones : [];
    if (validations.length === 0) {
      return;
    }

    let rows = Array.isArray(sourceRows) ? sourceRows : [];

    if (rows.length === 0) {
      const whereForValidation = isPlainObject(sourceWhere) ? sourceWhere : {};
      if (Object.keys(whereForValidation).length > 0) {
        for (const fieldRef of Object.keys(whereForValidation)) {
          const { table, column } = parseQualifiedFieldRef(fieldRef, tableName);
          ensureDynamicColumnAllowed(table, [column], esquema);
        }
      }

      const validationLimit = normalizeLimitValue(script?.post_proceso?.limit ?? script?.limit, 1);
      const { whereValues, whereClause } = buildWhereClause(whereForValidation, 1);
      const query = `SELECT * FROM "${tableName}"${whereClause} LIMIT ${validationLimit}`;

      console.log('[QUERY] Validación post-proceso ejecutada');
      const result = await obtenerPool().query(query, whereValues);
      rows = result.rows;
      console.log(`[RESULTADO] Filas obtenidas para validación: ${result.rowCount}`);
    }

    if (rows.length === 0) {
      throw new Error('No se encontraron registros para validación');
    }

    for (const validation of validations) {
      if (!validation || typeof validation !== 'object') {
        throw new Error('Validación post_proceso inválida');
      }

      const condition = validation.condicion || 'igual';
      const validationContext = {
        ...normalizedInputParams,
        resultado: rows,
        first_row: rows[0] || null,
        resultado_final: rows[0] || null,
      };

      const field = String(validation.campo || '').trim();
      if (field) {
        ensureDynamicColumnAllowed(tableName, [field], esquema);

        const expected = resolveDynamicScriptNode(validation.valor, validationContext);
        const valid = rows.some((row) => evaluateValidationCondition(row[field], condition, expected));

        if (!valid) {
          throw new Error(validation.mensaje || `Validación fallida para ${field}`);
        }

        if (typeof emit === 'function') {
          emit(`✅ Validación post proceso OK: ${field}`);
        }
        continue;
      }

      const variablePath = String(validation.variable || '').trim();
      const actualValue = variablePath
        ? resolveTemplateVariable(variablePath, validationContext)
        : rows;

      const expected = Object.prototype.hasOwnProperty.call(validation, 'valor')
        ? resolveDynamicScriptNode(validation.valor, validationContext)
        : undefined;

      const valid = evaluateWorkflowValidationCondition(actualValue, condition, expected);
      if (!valid) {
        throw new Error(validation.mensaje || validation.mensaje_error || 'Validación post_proceso fallida');
      }

      if (typeof emit === 'function') {
        emit(`✅ Validación post proceso OK: ${variablePath || 'resultado'}`);
      }
    }
  }

  async function executeDynamicTypedScript(script, rawParams, emit, workflowContext = {}) {
    // rawParams = entrada del usuario (parámetros de entrada)
    // workflowContext = datos internos del workflow (resultados de pasos previos)
  
    const normalizedInputParams = Object.entries(rawParams || {}).reduce((acc, [key, value]) => {
      // Solo sanitizar parámetros de entrada que NO vienen del workflow
      if (!Object.prototype.hasOwnProperty.call(workflowContext, key)) {
        acc[key] = sanitizeStructuredParamValue(value);
      } else {
        // Parámetros que vienen del workflow no se sanitizan (pueden ser objetos)
        acc[key] = value;
      }
      return acc;
    }, {});

    const validated = assertDynamicScriptAllowed(script, esquema);
    console.log('[SCRIPT] Ejecutando script dinámico validado');

    if (validated.type === 'update') {
      if (Array.isArray(script.join) && script.join.length > 0) {
        const relationalScript = {
          ...script,
          tipo: 'update',
          tabla: validated.tableName,
        };

        validateRelationalMutationScriptDefinition(relationalScript, 'update', esquema);

        const resolvedSet = resolveDynamicScriptNode(validated.setObject, normalizedInputParams, workflowContext);
        const setFields = Object.keys(resolvedSet);
        for (const field of setFields) {
          esquema.validarTipo(validated.tableName, field, resolvedSet[field]);
        }

        const setValues = setFields.map((field) => resolvedSet[field]);
        const relationalParts = buildRelationalMutationQuery(
          relationalScript,
          normalizedInputParams,
          workflowContext,
          setFields.length + 1,
          esquema
        );

        const query = `UPDATE "${validated.tableName}" AS "b" SET ${setFields
          .map((field, index) => `"${field}" = $${index + 1}`)
          .join(', ')} WHERE EXISTS (SELECT 1 FROM "${validated.tableName}" AS "${relationalParts.baseAlias}" ${relationalParts.joinSql}${relationalParts.whereClause})`;

        console.log('[RELATIONAL UPDATE QUERY] Ejecutando update relacional parametrizado');

        const result = await obtenerPool().query(query, [...setValues, ...relationalParts.whereValues]);
        await runPostProcessValidations(script, validated.tableName, normalizedInputParams, [], relationalParts.resolvedWhere, emit);

        if (typeof emit === 'function') {
          emit(`🧩 Script relacional tipo update sobre tabla ${validated.tableName}`);
          emit(`✅ Filas afectadas: ${result.rowCount}`);
        }

        return {
          success: true,
          message: 'UPDATE relacional ejecutado correctamente',
          affectedRows: result.rowCount,
        };
      }

      const resolvedSet = resolveDynamicScriptNode(validated.setObject, normalizedInputParams, workflowContext);
      const resolvedWhere = resolveDynamicScriptNode(validated.whereObject, normalizedInputParams, workflowContext);

      const setFields = Object.keys(resolvedSet);
      const whereFields = Object.keys(resolvedWhere);

      for (const field of setFields) {
        esquema.validarTipo(validated.tableName, field, resolvedSet[field]);
      }
      for (const field of whereFields) {
        esquema.validarTipo(validated.tableName, field, resolvedWhere[field]);
      }

      const setValues = setFields.map((field) => resolvedSet[field]);
      const { whereValues, whereClause } = buildWhereClause(resolvedWhere, setFields.length + 1);

      const query = `UPDATE "${validated.tableName}" SET ${setFields
        .map((field, index) => `"${field}" = $${index + 1}`)
        .join(', ')}${whereClause}`;

      console.log('[QUERY] Ejecutando update parametrizado');

      const result = await obtenerPool().query(query, [...setValues, ...whereValues]);
      console.log(`[RESULTADO] Filas afectadas por update: ${result.rowCount}`);

      await runPostProcessValidations(script, validated.tableName, normalizedInputParams, [], resolvedWhere, emit);

      if (typeof emit === 'function') {
        emit(`🧩 Script dinámico tipo update sobre tabla ${validated.tableName}`);
        emit(`✅ Filas afectadas: ${result.rowCount}`);
      }

      return {
        success: true,
        message: 'UPDATE ejecutado correctamente',
        affectedRows: result.rowCount,
      };
    }

    if (validated.type === 'insert') {
      const resolvedData = resolveDynamicScriptNode(validated.dataObject, normalizedInputParams, workflowContext);
      const dataFields = Object.keys(resolvedData);
      const dataValues = dataFields.map((field) => resolvedData[field]);

      for (const field of dataFields) {
        esquema.validarTipo(validated.tableName, field, resolvedData[field]);
      }

      const query = `INSERT INTO "${validated.tableName}" (${dataFields
        .map((field) => `"${field}"`)
        .join(', ')}) VALUES (${dataFields.map((_, index) => `$${index + 1}`).join(', ')})`;

      console.log('[QUERY] Ejecutando insert parametrizado');

      const result = await obtenerPool().query(query, dataValues);
      console.log(`[RESULTADO] Filas afectadas por insert: ${result.rowCount}`);

      const whereForValidation = isPlainObject(script.where)
        ? resolveDynamicScriptNode(script.where, normalizedInputParams, workflowContext)
        : resolvedData;
      await runPostProcessValidations(script, validated.tableName, normalizedInputParams, [], whereForValidation, emit);

      if (typeof emit === 'function') {
        emit(`🧩 Script dinámico tipo insert sobre tabla ${validated.tableName}`);
        emit(`✅ Filas insertadas: ${result.rowCount}`);
      }

      return {
        success: true,
        message: 'INSERT ejecutado correctamente',
        affectedRows: result.rowCount,
      };
    }

    if (validated.type === 'delete') {
      if (Array.isArray(script.join) && script.join.length > 0) {
        const relationalScript = {
          ...script,
          tipo: 'delete',
          tabla: validated.tableName,
        };

        validateRelationalMutationScriptDefinition(relationalScript, 'delete', esquema);

        const relationalParts = buildRelationalMutationQuery(relationalScript, normalizedInputParams, workflowContext, 1, esquema);

        const query = `DELETE FROM "${validated.tableName}" AS "b" WHERE EXISTS (SELECT 1 FROM "${validated.tableName}" AS "${relationalParts.baseAlias}" ${relationalParts.joinSql}${relationalParts.whereClause})`;

        console.log('[RELATIONAL DELETE QUERY] Ejecutando delete relacional parametrizado');

        const result = await obtenerPool().query(query, relationalParts.whereValues);

        await runPostProcessValidations(script, validated.tableName, normalizedInputParams, [], relationalParts.resolvedWhere, emit);

        if (typeof emit === 'function') {
          emit(`🧩 Script relacional tipo delete sobre tabla ${validated.tableName}`);
          emit(`✅ Filas eliminadas: ${result.rowCount}`);
        }

        return {
          success: true,
          message: 'DELETE relacional ejecutado correctamente',
          affectedRows: result.rowCount,
        };
      }

      const resolvedWhere = resolveDynamicScriptNode(validated.whereObject, normalizedInputParams, workflowContext);
      const whereFields = Object.keys(resolvedWhere);

      for (const field of whereFields) {
        esquema.validarTipo(validated.tableName, field, resolvedWhere[field]);
      }

      const { whereValues, whereClause } = buildWhereClause(resolvedWhere, 1);
      const query = `DELETE FROM "${validated.tableName}"${whereClause}`;

      console.log('[QUERY] Ejecutando delete parametrizado');

      const result = await obtenerPool().query(query, whereValues);
      console.log(`[RESULTADO] Filas afectadas por delete: ${result.rowCount}`);

      await runPostProcessValidations(script, validated.tableName, normalizedInputParams, [], resolvedWhere, emit);

      if (typeof emit === 'function') {
        emit(`🧩 Script dinámico tipo delete sobre tabla ${validated.tableName}`);
        emit(`✅ Filas eliminadas: ${result.rowCount}`);
      }

      return {
        success: true,
        message: 'DELETE ejecutado correctamente',
        affectedRows: result.rowCount,
      };
    }

    if (validated.type === 'select') {
      // Analytic (GROUP BY) mode
      if (script.query_mode === 'analitico') {
        const analyticScript = { ...script, tabla: validated.tableName };
        const analyticQuery = buildAnalyticSelectQuery(analyticScript, normalizedInputParams, workflowContext);

        console.log('[ANALYTIC QUERY] Ejecutando consulta analítica parametrizada');

        const result = await obtenerPool().query(analyticQuery.query, analyticQuery.whereValues);

        if (typeof emit === 'function') {
          emit(`📊 Consulta analítica sobre tabla ${validated.tableName}`);
          emit(`✅ Filas resultado: ${result.rowCount}`);
        }

        return {
          success: true,
          message: 'Consulta analítica ejecutada correctamente',
          affectedRows: result.rowCount,
          data: result.rows,
        };
      }

      if (Array.isArray(script.join) && script.join.length > 0) {
        const relationalScript = {
          ...script,
          tipo: 'select',
          tabla: validated.tableName,
        };

        validateRelationalScriptDefinition(relationalScript, esquema);
        const relationalQuery = buildRelationalSelectQuery(relationalScript, normalizedInputParams, workflowContext, esquema);

        console.log('[RELATIONAL QUERY] Ejecutando select relacional parametrizado');

        const result = await obtenerPool().query(relationalQuery.query, relationalQuery.whereValues);
        console.log(`[RESULTADO] Filas encontradas en select relacional: ${result.rowCount}`);

        await runPostProcessValidations(
          script,
          validated.tableName,
          normalizedInputParams,
          result.rows,
          relationalQuery.resolvedWhere,
          emit
        );

        if (typeof emit === 'function') {
          emit(`🧩 Script relacional tipo select sobre tabla ${validated.tableName}`);
          emit(`✅ Filas encontradas: ${result.rowCount}`);
        }

        return {
          success: true,
          message: 'SELECT relacional ejecutado correctamente',
          affectedRows: result.rowCount,
          data: result.rows,
        };
      }

      const resolvedWhere = validated.whereObject
        ? resolveDynamicScriptNode(validated.whereObject, normalizedInputParams, workflowContext)
        : {};
      const whereLogic = ['AND', 'OR'].includes(String(script.logic || '').toUpperCase())
        ? String(script.logic).toUpperCase()
        : 'AND';
      const { whereFields, whereValues, whereClause } = buildWhereClause(resolvedWhere, 1, whereLogic);

      // Validate simple values; for field-vs-field comparisons validate referenced
      // right-hand field allow-list instead of scalar type.
      for (const field of whereFields) {
        const { value, fieldRef } = resolveWhereEntry(resolvedWhere[field]);
        if (fieldRef) {
          const right = parseQualifiedFieldRef(fieldRef, validated.tableName);
          if (!tablasPermitidas().includes(right.table)) {
            throw new Error(`WHERE: tabla no permitida (${right.table})`);
          }
          ensureDynamicColumnAllowed(right.table, [right.column], esquema);
        } else {
          esquema.validarTipo(validated.tableName, field, value);
        }
      }

      const limitValue = normalizeLimitValue(script.limit, 0);
      const limitClause = limitValue > 0 ? ` LIMIT ${limitValue}` : '';
      const projectionSql = Array.isArray(script.columnas) && script.columnas.length > 0
        ? script.columnas
            .map((entry) => parseQualifiedFieldRef(String(entry || '').trim(), validated.tableName))
            .map(({ table, column }) => `"${table}"."${column}"`)
            .join(', ')
        : '*';

      const query = `SELECT ${projectionSql} FROM "${validated.tableName}"${whereClause}${limitClause}`;

      console.log('[QUERY] Ejecutando select parametrizado');

      const result = await obtenerPool().query(query, whereValues);
      console.log(`[RESULTADO] Filas encontradas en select: ${result.rowCount}`);

      await runPostProcessValidations(script, validated.tableName, normalizedInputParams, result.rows, resolvedWhere, emit);

      if (typeof emit === 'function') {
        emit(`🧩 Script dinámico tipo select sobre tabla ${validated.tableName}`);
        emit(`✅ Filas encontradas: ${result.rowCount}`);
      }

      return {
        success: true,
        message: 'SELECT ejecutado correctamente',
        affectedRows: result.rowCount,
        data: result.rows,
      };
    }

    throw new Error('Operación no permitida');
  }

  async function executeRelationalScript(script, rawParams, emit) {
    validateRelationalScriptDefinition(script, esquema);

    const normalizedInputParams = Object.entries(rawParams || {}).reduce((acc, [key, value]) => {
      acc[key] = sanitizeStructuredParamValue(value);
      return acc;
    }, {});

    const relationalQuery = buildRelationalSelectQuery(script, normalizedInputParams, {}, esquema);
    console.log('[RELATIONAL QUERY]', relationalQuery.query, relationalQuery.whereValues);

    const result = await obtenerPool().query(relationalQuery.query, relationalQuery.whereValues);

    if (typeof emit === 'function') {
      emit(`🧩 Script relacional sobre tabla ${relationalQuery.baseTable}`);
      emit(`✅ Filas encontradas: ${result.rowCount}`);
    }

    return {
      success: true,
      message: 'SELECT relacional ejecutado correctamente',
      affectedRows: result.rowCount,
      data: result.rows,
    };
  }

  function executeWorkflowValidationStep(step, workflowContext, userParams = {}) {
    const variablePath = String(step.variable || '').trim();
    if (!variablePath) {
      throw new Error('Validación sin variable');
    }

    let actualValue = resolveValueMultiSource(variablePath, workflowContext, userParams);
  
    // Si hay un campo especificado, extrae ese campo del valor obtenido
    const fieldName = String(step.campo || '').trim();
    if (fieldName) {
      actualValue = getValueByPath(actualValue, fieldName);
    }
  
    const expectedValue = Object.prototype.hasOwnProperty.call(step, 'valor')
      ? resolveDynamicScriptNode(step.valor, userParams, workflowContext)
      : undefined;

    const condition = String(step.condicion || 'existe').trim().toLowerCase();
    const isValid = evaluateWorkflowValidationCondition(actualValue, condition, expectedValue);
    if (!isValid) {
      throw new Error(step.mensaje_error || `Validación fallida: ${variablePath}`);
    }
  }

  async function executeWorkflowScript(script, rawParams, emit) {
    const workflow = Array.isArray(script.workflow) ? script.workflow : [];
    if (workflow.length === 0) {
      throw new Error('Workflow vacío o inválido');
    }

    // Separación clara: params = entrada del usuario, context = datos internos del workflow
    const userParams = Object.entries(rawParams || {}).reduce((acc, [key, value]) => {
      acc[key] = sanitizeStructuredParamValue(value);
      return acc;
    }, {});
  
    const workflowContext = {}; // Almacena resultados de pasos previos (guardar_en)
    const stepLogs = [];
    let finalResult = null;

    console.log('[CONTEXT] Inicializado para ejecución de workflow');

    for (let index = 0; index < workflow.length; index++) {
      const stepNumber = index + 1;
      const step = workflow[index];

      if (!isPlainObject(step)) {
        throw new Error(`Paso ${stepNumber} inválido`);
      }

      const stepType = normalizeDynamicScriptType(step.tipo);
      if (!WORKFLOW_ALLOWED_STEP_TYPES.includes(stepType)) {
        throw new Error(`Operación no soportada: ${stepType || 'desconocida'}`);
      }

      if (stepType === 'validacion') {
        executeWorkflowValidationStep(step, workflowContext, userParams);
        const okLog = `[STEP ${stepNumber}] VALIDACION OK`;
        stepLogs.push(okLog);
        if (typeof emit === 'function') emit(okLog);
        continue;
      }

      const tableName = String(step.tabla || '').trim().toLowerCase();
      const startLog = `[STEP ${stepNumber}] ${stepType.toUpperCase()} ${tableName}`;
      stepLogs.push(startLog);
      if (typeof emit === 'function') emit(startLog);

      // Pasa userParams como entrada y workflowContext como datos internos
      const stepResult = await executeDynamicTypedScript(step, userParams, emit, workflowContext);
      finalResult = stepResult;

      // Guarda resultado en context (NO en params)
      if (typeof step.guardar_en === 'string' && step.guardar_en.trim()) {
        workflowContext[step.guardar_en.trim()] = normalizeWorkflowStoredValue(stepResult);
        console.log(`[STEP ${stepNumber}] Resultado de paso guardado en contexto interno`);
      }

      const doneLog = `[STEP ${stepNumber}] ${stepType.toUpperCase()} ejecutado`;
      stepLogs.push(doneLog);
      if (typeof emit === 'function') emit(doneLog);
    }

    return {
      success: true,
      message: 'Workflow ejecutado correctamente',
      steps: stepLogs,
      resultado_final: finalResult,
    };
  }

  async function executeStructuredScript(script, rawParams, emit) {
    if (!script || typeof script !== 'object' || Array.isArray(script)) {
      emit('❌ El script_json configurado no es válido', 'error');
      return { success: false, message: 'script_json inválido' };
    }

    logger.debug('Script estructurado validado', { modo: script.modo, origen: script.origen, databaseId: script.databaseId });

    let validatedRuntimeParams = {};
    try {
      validatedRuntimeParams = normalizeAndValidateStructuredParams(script, rawParams || {});
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Parámetros inválidos';
      emit(`❌ ${message}`, 'error');
      return { success: false, message };
    }

    const requiresWriteConfirmation = scriptContainsMutation(script);
    const isWorkflowScript = Array.isArray(script.workflow);
    const isManualSqlScript =
      String(script.modo || '').trim().toLowerCase() === 'script'
      && String(script.origen || '').trim().toLowerCase() === 'manual-sql';
    const isProceduralManualSql = isManualSqlScript && isComplexSqlScript(String(script.sql || ''));
    const requiresExplicitWriteConfirmation = requiresWriteConfirmation
      && !isWorkflowScript
      && !isProceduralManualSql;

    // El sistema ya no escribe, asi que esto no pide confirmacion: se niega.
    //
    // Antes decia «confirma para continuar» y la escritura seguia sin ejecutarse
    // —estaba cerrada mas abajo—, asi que quien lo intentaba confirmaba una y otra vez
    // sin entender por que no pasaba nada. El mensaje tiene que decir lo que ocurre.
    if (requiresWriteConfirmation) {
      void requiresExplicitWriteConfirmation;
      void isWriteConfirmed;
      const message = 'Este articulo modifica datos y el sistema ya no ejecuta escrituras: '
        + 'un articulo solo consulta. Conviertalo en una consulta con los campos que haya que pedir.';
      emit(`❌ ${message}`, 'error');
      return { success: false, message };
    }

    if (Array.isArray(script.workflow)) {
      logger.debug('Motor: workflow visual');
      try {
        return await executeWorkflowScript(script, validatedRuntimeParams, emit);
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Error en workflow';
        emit(`❌ ${message}`, 'error');
        return { success: false, message };
      }
    }

    if (Array.isArray(script.join)) {
      logger.debug('Motor: relacional (JOIN)');
      try {
        return await executeRelationalScript(script, validatedRuntimeParams, emit);
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Error en script relacional';
        emit(`❌ ${message}`, 'error');
        return { success: false, message };
      }
    }

    if (
      String(script.modo || '').trim().toLowerCase() === 'script'
      && String(script.origen || '').trim().toLowerCase() === 'manual-sql'
    ) {
      logger.debug('Motor: SQL manual', { databaseId: script.databaseId || 'auto' });
      try {
        return await executeManualSqlScript(script, validatedRuntimeParams, emit);
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Error en SQL manual';
        emit(`❌ ${message}`, 'error');
        return { success: false, message };
      }
    }

    if (String(script.modo || '').trim().toLowerCase() === 'script') {
      logger.debug('Motor: modo empresarial');
      try {
        return await executeBusinessWorkflow(script, validatedRuntimeParams, emit);
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Error en script empresarial';
        emit(`❌ ${message}`, 'error');
        return { success: false, message };
      }
    }

    const dynamicType = normalizeDynamicScriptType(script.tipo);
    if (dynamicType) {
      logger.debug('Motor: dinámico', { tipo: script.tipo });
      try {
        return await executeDynamicTypedScript(script, validatedRuntimeParams, emit);
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Operación no permitida';
        emit(`❌ ${message}`, 'error');
        return { success: false, message };
      }
    }

    const actionName = typeof script.accion === 'string' ? script.accion.trim() : '';
    if (actionName) {
      logger.debug('Motor: modo antiguo (accion)');
      if (!accionesEstructuradas[actionName]) {
        emit('❌ La acción indicada en script_json no está permitida', 'error');
        return { success: false, message: 'Acción de script_json no permitida' };
      }

      const variables = extractVariables(script.parametros || {});
      for (const variableName of variables) {
        const value = validatedRuntimeParams[variableName];
        if (value === undefined || value === null || String(value).trim() === '') {
          emit(`❌ Falta el parámetro: ${variableName}`, 'error');
          return { success: false, message: `Falta el parámetro: ${variableName}` };
        }
      }

      const interpolatedParams = replaceVariables(script.parametros || {}, validatedRuntimeParams);
      const normalizedParams = normalizeStructuredParams(actionName, interpolatedParams);

      emit(`🧩 Script dinámico detectado: ${actionName}`);
      return accionesEstructuradas[actionName].execute(normalizedParams, obtenerPool(), emit);
    }

    emit('❌ Script JSON sin acción soportada', 'error');
    return { success: false, message: 'Script JSON sin acción soportada' };
  }

  async function executeArticleById(articleId, rawParams, emit, options = {}) {
    await ensureDynamicSchemaReady(false);

    const articleResult = await obtenerPool().query(
      'SELECT id, titulo, script, script_json, tipo_solucion, campos_formulario FROM knowledge_base WHERE id = $1',
      [articleId]
    );

    if (articleResult.rowCount === 0) {
      if (typeof emit === 'function') emit('❌ Artículo no encontrado', 'error');
      return { success: false, message: 'Artículo no encontrado' };
    }

    const article = articleResult.rows[0];
    if (!EXECUTABLE_SOLUTION_TYPES.has(String(article.tipo_solucion || '').trim().toLowerCase())) {
      if (typeof emit === 'function') emit('❌ Este artículo no tiene una acción ejecutable configurada', 'error');
      return { success: false, message: 'No ejecutable' };
    }

    const structuredScript = parseStructuredScript(article.script_json);
    const requestedDatabaseId = resolveArticleExecutionDatabaseId(article, structuredScript, options);
    if (structuredScript) {
      if (typeof emit === 'function') emit(`🔍 Iniciando: ${article.titulo}`);
      const structuredScriptWithDatabase = requestedDatabaseId && !String(structuredScript?.databaseId || '').trim()
        ? { ...structuredScript, databaseId: requestedDatabaseId }
        : structuredScript;
      return executeStructuredScript(structuredScriptWithDatabase, rawParams || {}, emit || (() => {}));
    }

    const actionKey = article.script;
    if (!actionKey) {
      if (typeof emit === 'function') emit('❌ Acción no configurada. Contacta al administrador.', 'error');
      return { success: false, message: 'Acción no permitida' };
    }

    const isDynamicAction = actionKey.startsWith('table:');
    let action = null;

    if (isDynamicAction) {
      const parts = actionKey.split(':');
      if (parts.length < 2) {
        if (typeof emit === 'function') emit('❌ Configuración de acción dinámica inválida', 'error');
        return { success: false, message: 'Configuración inválida' };
      }

      const tableName = parts[1];
      const normalizedTableName = normalizeSchemaIdentifier(tableName);
      if (!tablasPermitidas().includes(normalizedTableName)) {
        if (typeof emit === 'function') emit('❌ Tabla no permitida', 'error');
        console.warn(`[EXECUTE] ⚠️ Tabla no permitida: "${tableName}" en artículo ${articleId}`);
        return { success: false, message: 'Tabla no permitida' };
      }

      const selectedColumns = parts.length > 2 ? parts[2].split(',').map(c => c.trim()) : [];
      if (selectedColumns.length === 0) {
        if (typeof emit === 'function') emit('❌ No hay columnas configuradas para la búsqueda', 'error');
        return { success: false, message: 'Sin columnas' };
      }

      try {
        ensureDynamicColumnAllowed(normalizedTableName, selectedColumns, esquema);
      } catch (validationError) {
        const message = validationError instanceof Error ? validationError.message : 'Columnas inválidas';
        if (typeof emit === 'function') emit(`❌ ${message}`, 'error');
        return { success: false, message };
      }

      action = {
        isDynamic: true,
        tableName: normalizedTableName,
        selectedColumns: selectedColumns,
      };
    } else if (accionesPermitidas[actionKey]) {
      action = accionesPermitidas[actionKey];
    } else {
      if (typeof emit === 'function') emit('❌ Acción no configurada. Contacta al administrador.', 'error');
      console.warn(`[EXECUTE] ⚠️ Acción no permitida: "${actionKey}" en artículo ${articleId}`);
      return { success: false, message: 'Acción no permitida' };
    }

    if (action.isDynamic) {
      const { tableName, selectedColumns } = action;
      const configuredFields = Array.isArray(article.campos_formulario) ? article.campos_formulario : [];
      const fieldConfigByName = new Map(configuredFields.map((field) => [field.name, field]));
      if (typeof emit === 'function') emit(`🔍 Consultando tabla ${tableName}...`);

      const columnList = selectedColumns.map(col => `"${col}"`).join(', ');
      const inputColumns = selectedColumns.filter((col) => {
        const mode = fieldConfigByName.get(col)?.mode;
        return mode !== 'output';
      });

      for (const col of inputColumns) {
        const config = fieldConfigByName.get(col);
        if (config?.required) {
          const value = rawParams[col];
          if (value === undefined || value === null || String(value).trim() === '') {
            if (typeof emit === 'function') emit(`❌ Campo requerido: ${config.label || col}`, 'error');
            return { success: false, message: `Falta el campo: ${config.label || col}` };
          }
        }
      }

      const filterColumns = inputColumns.filter((col) => {
        const value = rawParams[col];
        return value !== undefined && value !== null && String(value).trim() !== '';
      });

      const filterValues = filterColumns.map((col) => {
        const value = rawParams[col];
        return typeof value === 'boolean' ? value : String(value).trim();
      });

      const whereClause = filterColumns.length > 0
        ? ` WHERE ${filterColumns.map((col, idx) => `"${col}" = $${idx + 1}`).join(' AND ')}`
        : '';

      const query = `SELECT ${columnList} FROM ${tableName}${whereClause} LIMIT 50`;

      try {
        const result = await obtenerPool().query(query, filterValues);
        if (typeof emit === 'function') {
          emit(`✅ Encontrados ${result.rowCount} registros en ${tableName}`);
          if (result.rowCount > 0) {
            emit(`📊 Mostrando primeros ${Math.min(result.rowCount, 10)} registros:`);
            result.rows.slice(0, 10).forEach((row, idx) => {
              const rowStr = selectedColumns.map(col => `${col}: ${row[col] || '(vacío)'}`).join(' | ');
              emit(`  ${idx + 1}. ${rowStr}`);
            });
          } else {
            emit('ℹ️ No hay registros en esta tabla');
          }
        }

        return {
          success: true,
          message: `Consulta completada: ${result.rowCount} registros encontrados`,
          rowCount: result.rowCount,
          rows: result.rows,
        };
      } catch (error) {
        if (typeof emit === 'function') emit(`❌ Error ejecutando consulta: ${error.message}`);
        console.error('[EXECUTE] ❌ Dynamic query error:', error);
        return { success: false, message: 'Error en la consulta' };
      }
    }

    const safeParams = {};
    for (const field of action.fields) {
      const val = rawParams[field.name];
      if (field.required && (!val || !String(val).trim())) {
        if (typeof emit === 'function') emit(`❌ Campo requerido: ${field.label}`, 'error');
        return { success: false, message: `Falta el campo: ${field.label}` };
      }
      if (val !== undefined && val !== null && String(val).trim()) {
        safeParams[field.name] = String(val).substring(0, 255).replace(/[\x00-\x1F\x7F]/g, '').trim();
      }
    }

    if (typeof emit === 'function') emit(`🔍 Iniciando: ${article.titulo}`);
    logger.info('Ejecutando acción', { actionKey, articleId });

    const result = await action.execute(safeParams, obtenerPool(), emit || (() => {}));
    logger.info('Acción completada', { actionKey, articleId, success: result.success, message: result.message });
    return result;
  }

  async function executeDistributedWithExplanation({
    text,
    parameterValue = null,
    limit = 50,
    databases = [],
    user,
    role,
    sourceRoute,
  }) {
    const startTime = Date.now();
    const distributedResult = await multiDatabaseEngine.execute(text, {
      parameterValue,
      limit,
      databases,
    });

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


  // ── Por dónde va una consulta y cuándo se analiza un error ──────────────────
  // Las dos deciden a quién llamar, no ejecutan nada por su cuenta. Estaban en
  // app.js, que es el único sitio del que no tenían por qué depender.
  function decideQueryExecutionMode(text, requestedDatabases = []) {
    const inputText = String(text || '').trim();
    if (!inputText) {
      return {
        mode: 'normal',
        reason: 'empty-text',
        matchedDatabases: [],
        matchedEntities: [],
      };
    }

    try {
      const effectiveRequestedDatabases = resolverBasesDeConsulta(inputText, requestedDatabases);
      const configuredDatabases = multiDbRegistry.getDatabases();
      if (configuredDatabases.length <= 1) {
        return {
          mode: 'normal',
          reason: 'single-database-config',
          matchedDatabases: configuredDatabases.map((database) => database.id),
          matchedEntities: [],
        };
      }

      const resolution = multiDatabaseEngine.resolveEntitiesAcrossDatabases(inputText, effectiveRequestedDatabases);
      const decision = multiDatabaseEngine.decideExecution(resolution);

      return {
        mode: decision.mode,
        reason: decision.executionType.toLowerCase(),
        matchedDatabases: decision.source || [],
        matchedEntities: (resolution.entities || []).map((entity) => entity.primary?.tableName || entity.entity).filter(Boolean),
        requestedDatabases: effectiveRequestedDatabases,
        resolution,
        executionType: decision.executionType,
      };
    } catch (error) {
      return {
        mode: 'normal',
        reason: 'decision-fallback-error',
        matchedDatabases: [],
        matchedEntities: [],
        error: error?.message || String(error),
      };
    }
  }

  async function resolveErrorAnalysisResponse(text, dbPool) {
    const inputText = String(text || '').trim();
    if (!detectErrorAnalysisMode(inputText)) {
      return null;
    }

    try {
      return await executeErrorAnalysis(inputText, dbPool);
    } catch (error) {
      console.error('[ERROR_ANALYSIS] ❌ Error en análisis:', error?.message || error);
      return null;
    }
  }

  return {
    decideQueryExecutionMode,
    resolveErrorAnalysisResponse,
    executeSafeSelectQuery,
    executeErrorAnalysis,
    executeManualSqlScript,
    executeArticleById,
    executeDistributedWithExplanation,
  };
}
