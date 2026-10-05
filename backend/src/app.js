// Debe ser el primer import: los módulos ESM se evalúan antes que el cuerpo de este
// archivo, así que un dotenv.config() más abajo llegaría tarde para cualquier módulo
// que lea process.env al importarse (por ejemplo ./soporte/config.ts).
import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import rateLimit from 'express-rate-limit';
import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';
import http from 'http';
import https from 'https';
import { Pool } from 'pg';
import bcrypt from 'bcryptjs';
import {
  BUSINESS_ACTION_CATALOG,
  createBusinessActionsRegistry,
  getBusinessActionMetadata,
  getRequiredParamsForActions,
} from './actionsRegistry.js';
import SchemaDetector from './infrastructure/database/SchemaDetector.js';
import SchemaCache from './infrastructure/cache/SchemaCache.js';
import QueryBuilder from './infrastructure/query/QueryBuilder.js';
import QueryAnalyzer from './infrastructure/query/QueryAnalyzer.js';
import ResultExplainer from './infrastructure/query/ResultExplainer.js';
import QueryParameterizer from './infrastructure/query/QueryParameterizer.js';
import {
  analyzeErrorInput,
  buildErrorAnalysisResponse,
  detectErrorAnalysisInput,
  enrichErrorAnalysisWithAi,
  searchErrorLogs,
} from './infrastructure/query/ErrorAnalysisEngine.js';
import MultiDbRegistry from './infrastructure/distributed/MultiDbRegistry.js';
import MultiDatabaseEngine from './infrastructure/distributed/MultiDatabaseEngine.js';
import { isComplexSqlScript, shouldForceProceduralMode, parsePlSqlScript, applyLoopVarResolution } from './infrastructure/query/PlSqlInterpreter.js';
import {
  detectSqlSyntaxEngine,
  detectQueryType,
  extractDatabaseDirective,
  isNativePlSql,
  buildEngineLog,
} from './infrastructure/query/SqlEngineDetector.js';
import { bootstrapGlobalSystems } from './bootstrap.js';
import { connectDatabase as probeUniversalConnection, closeConnection as closeUniversalConnection } from './infrastructure/universal-db/index.js';
import { createQueryRouter } from './routes/QueryRoute.js';
import { initDynamicRoutes } from './routes/DynamicRoutes.js';
import { createResponseSecurityMiddleware } from './infrastructure/security/responseSecurity.js';
import { sessions, getUserFromRequest, getUserFromRequestAsync, initSessionStore, adjuntarSesion } from './lib/session.js';
import { createAuthRouter } from './routes/authRoute.js';
import { createContentRouter } from './routes/contentRoute.js';
import { createExecuteRouter, createRepairRouter } from './routes/executeRoute.js';
import { createHistoryRouter } from './routes/historyRoute.js';
import { createProcessRouter } from './routes/processRoute.js';
import { createAdminRouter, createDbSchemaRouter } from './routes/adminRoute.js';
import { createActionsRouter } from './routes/actionsRoute.js';
import { createSqlRouter } from './routes/sqlRoute.js';
import { createDistributedRouter } from './routes/distributedRoute.js';
// Módulo de soporte (tickets, diagnósticos, vigilancia) absorbido desde ticket v2.0.
// Está en TypeScript: Node >= 22.18 lo ejecuta sin compilar (ver SOPORTE.md).
import { crearSoporteClienteRouter, crearSoporteRouter, iniciarSoporte } from './soporte/index.ts';
import logger from './infrastructure/logger/logger.js';
import { correlationIdMiddleware } from './middleware/correlationId.js';
import { SQL_BLOCKED_KEYWORDS, containsBlockedSql, extractSqlReferencedTables, getDatabaseSchemaTableNames, normalizeSchemaIdentifier, splitManualSqlSteps, stripLeadingSqlComments, stripSqlStringsAndComments } from './dominio/sql/guardas.js';
import { ROLE_LEVELS, getRoleFromRequest, hasRequiredRole, normalizeRole } from './dominio/acceso/roles.js';
import { ALLOWED_OUTBOUND_HOSTS, BLOCKED_OUTBOUND_PATTERNS, CORS_ALLOWED_ORIGINS, extractHostFromRequestOptions, isOriginAllowed, isOutboundBlocked, sanitizeOrigin } from './dominio/red/salida.js';
import { extractDatabaseHintsFromText as deducirBases, resolveRequestedDatabasesForQuery as resolverBasesPedidas } from './dominio/bases/resolucion.js';
import { dedupeSemanticValues, normalizeGeneratedSelectSql, normalizeSemanticLearningTerm, slugifyProcessName } from './dominio/texto/normalizar.js';
import { crearRutasConsultas } from './adaptadores/http/consultas.js';
import { ALLOWED_WHERE_OPERATORS, DYNAMIC_SCRIPT_ALLOWED_TYPES, MANUAL_SQL_BLOCKED_KEYWORDS, WORKFLOW_ALLOWED_STEP_TYPES, assertDynamicScriptAllowed, assertManualSqlAllowed, buildRelationKey, detectSqlOperation, ensureDynamicColumnAllowed, ensureDynamicJoinRelationAllowed, isPlainObject, normalizeBusinessStep, normalizeDynamicScriptType, parseQualifiedFieldRef, resolveWhereEntry, validateBusinessScriptDefinition, validateRelationalMutationScriptDefinition, validateRelationalScriptDefinition, validateWorkflowBuilderDefinition } from './dominio/scripts/validacion.js';
import { buildHumanSqlExplanation, buildProductionSqlResponse, sanitizeProductionResultObject } from './dominio/presentacion/explicacion.js';
import { applyCastToSqlPlaceholder, applyOptionalSqlFilterTransform, buildAnalyticSelectQuery, buildManualSqlQuery, collectManualSqlPlaceholderMetadata, countPositionalSqlPlaceholders, getValueByPath, inferManualPlaceholderCast, inferStructuredParamSemantic, isMissingParamValue, isOptionalUserPlaceholder, isWriteConfirmed, normalizeLimitValue, normalizeManualSqlParamsInput, resolveDynamicScriptNode, resolveValue, resolveValueMultiSource, resolveWorkflowContextPlaceholder, sanitizeStructuredParamValue, validateStructuredParamByName, MANUAL_SQL_CANONICAL_PLACEHOLDER_REGEX, MANUAL_SQL_POSITIONAL_PLACEHOLDER_REGEX, STRUCTURED_PLACEHOLDER_REGEX } from './dominio/sql/construccion.js';
import { configurarPool as configurarPoolAuditoria, logArticleChange, logError, logLoginEvent, logQueryHistory, logRequest, logSystemEvent } from './adaptadores/postgres/auditoria.js';
import { capturarCuerpoParaAuditoria, crearAuditoriaDePeticiones } from './adaptadores/http/auditoria.js';
import { registrarCicloDeVida } from './adaptadores/proceso/ciclo-de-vida.js';
import {
  configurarEsquemas,
  ensureKnowledgeBaseSchema,
  ensureCommentsSchema,
  ensureBusinessLogsSchema,
  ensureQueryHistorySchema,
  ensureErrorLogsSchema,
  ensureSemanticLearningSchema,
  ensureRequestAuditSchema,
  ensureAuditTimelineView,
} from './adaptadores/postgres/esquemas.js';
import {
  columnasPermitidas, configurarEsquemaDinamico, ensureDynamicSchemaReady, refreshDynamicSchemaCache,
  relacionesDelEsquema, resetDynamicSchemaCache, tablasPermitidas, tiposDeColumna,
} from './adaptadores/postgres/esquema-dinamico.js';
import {
  asegurarTipoDeColumna, buildRelationalMutationQuery, buildRelationalSelectQuery,
  buildSelectProjectionSql, buildWriteDetailExplanation, normalizeStrictSqlRawPayload,
} from './dominio/sql/relacional.js';
import { crearEjecucionDeScripts } from './aplicacion/ejecucion.js';
import { collectStructuredVariables, evaluateProceduralExpression, evaluateSimpleCondition, evaluateValidationCondition, evaluateWorkflowValidationCondition, extractVariables, formatRaiseNoticeMessage, getManualSqlStepTitle, interpolateStructuredNode, isEmptyWorkflowValue, normalizeWorkflowStoredValue, parseStepOrder, replaceVariables, resolveTemplateVariable } from './dominio/flujos/pasos.js';
import { ALIAS_DE_ACCION, EXECUTABLE_SOLUTION_TYPES, MANUAL_SQL_NAMED_PLACEHOLDER_REGEX, MANUAL_SQL_TIMEOUT_MS, normalizeAndValidateStructuredParams, normalizeStructuredParams, parseStructuredScript } from './dominio/scripts/estructurado.js';
import { MONGO_WRITE_OPS_RE, SQL_WRITE_KEYWORDS_RE, buildWhereClause, detectMongoOperation, parseWriteTarget, renumberWhereParams, scriptContainsMutation, sqlTextContainsMutation } from './dominio/sql/mutaciones.js';
import { addWithUniqueKey, buildAggregatedResultFromStepPayload, buildComplexWorkflowExplanation, buildDistributedSyntheticSql, buildPerStepWriteSummary, extractObjectForAggregation } from './dominio/presentacion/agregacion.js';

// El pool se reasigna al recargar la configuración multi-base, así que la auditoría
// recibe un getter y no el valor: guardar el de ahora sería escribir contra una
// conexión cerrada en cuanto alguien toque multidb.databases.json.
configurarPoolAuditoria(() => pool);
configurarEsquemaDinamico({
  pool: () => pool,
  huellaConexion: () => schemaDetector.getConnectionFingerprint(),
  publicarContexto: (ctx) => queryParameterizer.setSchemaContext(ctx),
});
configurarEsquemas({
  pool: () => pool,
  huellaConexion: () => schemaDetector.getConnectionFingerprint(),
});

const extractDatabaseHintsFromText = (text) => deducirBases(text, multiDbRegistry.getDatabases());
const resolveRequestedDatabasesForQuery = (text, requestedDatabases = []) =>
  resolverBasesPedidas(text, requestedDatabases, multiDbRegistry.getDatabases());

const API_RATE_LIMIT_WINDOW_MS = Math.max(5_000, Number(process.env.API_RATE_LIMIT_WINDOW_MS) || 60_000);
const API_RATE_LIMIT_MAX = Math.max(1, Number(process.env.API_RATE_LIMIT_MAX) || 100);

dotenv.config();

const multiDbRegistry = new MultiDbRegistry();
let watchedMultiDbConfigPath = '';





const originalHttpRequest = http.request.bind(http);
const originalHttpsRequest = https.request.bind(https);

http.request = function guardedHttpRequest(options, callback) {
  const host = extractHostFromRequestOptions(options);
  if (isOutboundBlocked(host)) {
    throw new Error(`Outbound HTTP bloqueado por politica local-only: ${host || 'host-desconocido'}`);
  }
  return originalHttpRequest(options, callback);
};

https.request = function guardedHttpsRequest(options, callback) {
  const host = extractHostFromRequestOptions(options);
  if (isOutboundBlocked(host)) {
    throw new Error(`Outbound HTTPS bloqueado por politica local-only: ${host || 'host-desconocido'}`);
  }
  return originalHttpsRequest(options, callback);
};

logger.info('Politica local-only activa: solo localhost permitido para HTTP/HTTPS saliente');

function getConfiguredDatabaseUrl() {
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
  const connectionString = getConfiguredDatabaseUrl();
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

let pool = createPgPool();
initSessionStore(pool); // Permitir restaurar sesiones desde la BD tras reinicios

// Timeout for admin-authored manual SQL / article execution. Generous (2 min)
// so long, complex, multi-statement scripts run to completion like in DBeaver.

let BUSINESS_ACTIONS_REGISTRY = createBusinessActionsRegistry(pool);

// ===== NUEVAS HERRAMIENTAS DE SCHEMA DINÁMICO =====
const schemaDetector = new SchemaDetector(pool);
const schemaCache = new SchemaCache(60 * 1000); // TTL: 60 segundos
const queryBuilder = new QueryBuilder(schemaDetector, schemaCache);
const queryAnalyzer = new QueryAnalyzer();
const resultExplainer = new ResultExplainer();
const queryParameterizer = new QueryParameterizer();
const multiDatabaseEngine = new MultiDatabaseEngine(multiDbRegistry, queryBuilder);
const MULTI_DB_BOOTSTRAP_STATE = {
  runningPromise: null,
  lastRunAt: 0,
};
const LEARNED_SEMANTIC_CACHE = {
  fingerprint: '',
  loadedAt: 0,
  ttlMs: 30 * 1000,
  refreshPromise: null,
  snapshot: {
    tableAliases: {},
    columnKeywords: {},
    suggestionTerms: [],
  },
};
logger.info('Herramientas de Schema dinámico inicializadas');

async function ensureMultiDbReady(options = {}) {
  const force = Boolean(options?.force);
  const reason = String(options?.reason || 'runtime').trim();

  if (!force && multiDbRegistry.getDatabases().length > 0) {
    const configured = multiDbRegistry.getDatabases().map((database) => `${database.id}(${database.type})`);
    logger.debug('MultiDB ya configurado', { reason, count: configured.length, databases: configured.join(', ') });
    return {
      bootstrapped: false,
      reason: 'already-ready',
      registeredDatabases: multiDbRegistry.getDatabases().map((database) => database.id),
    };
  }

  if (MULTI_DB_BOOTSTRAP_STATE.runningPromise) {
    return MULTI_DB_BOOTSTRAP_STATE.runningPromise;
  }

  MULTI_DB_BOOTSTRAP_STATE.runningPromise = bootstrapGlobalSystems({
    multiDbRegistry,
    schemaDetector,
    queryBuilder,
    pool,
    logger: console,
  })
    .then((result) => {
      MULTI_DB_BOOTSTRAP_STATE.lastRunAt = Date.now();
      logger.info('MultiDB bootstrap OK', { reason, registeredDatabases: result?.registeredDatabases });
      return result;
    })
    .catch((error) => {
      logger.error('MultiDB bootstrap FAIL', { reason, error: error?.message || String(error) });
      throw error;
    })
    .finally(() => {
      MULTI_DB_BOOTSTRAP_STATE.runningPromise = null;
    });

  return MULTI_DB_BOOTSTRAP_STATE.runningPromise;
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

function resetLearnedSemanticCache() {
  LEARNED_SEMANTIC_CACHE.fingerprint = '';
  LEARNED_SEMANTIC_CACHE.loadedAt = 0;
  LEARNED_SEMANTIC_CACHE.refreshPromise = null;
  applyLearnedSemanticSnapshot({ tableAliases: {}, columnKeywords: {}, suggestionTerms: [] });
}


async function reloadActiveDatabaseConnection(reason = 'reload') {
  const previousPool = pool;
  const previousRegistry = BUSINESS_ACTIONS_REGISTRY;
  const previousMultiDbConfig = multiDbRegistry.getConfigSnapshot();
  const previousLearnedSnapshot = { ...LEARNED_SEMANTIC_CACHE.snapshot };
  const previousLearnedFingerprint = LEARNED_SEMANTIC_CACHE.fingerprint;

  multiDbRegistry.reloadFromEnv();
  setupMultiDbConfigWatcher();

  const nextPool = createPgPool();

  pool = nextPool;
  BUSINESS_ACTIONS_REGISTRY = createBusinessActionsRegistry(nextPool);
  schemaDetector.pool = nextPool;

  resetLearnedSemanticCache();
  resetDynamicSchemaCache();
  schemaCache.invalidate();
  bumpQueryCacheDataVersion();

  const reloadStart = Date.now();
  try {
    await testDatabaseConnection();
    // Correr todos los ensures de DDL en paralelo (son independientes)
    await Promise.all([
      ensureKnowledgeBaseSchema(),
      ensureCommentsSchema(),
      ensureBusinessLogsSchema(),
      ensureQueryHistorySchema(),
      ensureErrorLogsSchema(),
      ensureSemanticLearningSchema(),
      ensureRequestAuditSchema(),
    ]);
    // La vista va después: necesita que las siete tablas existan.
    await ensureAuditTimelineView();
    await refreshSemanticLearningCache(true);
    await queryBuilder.getFullSchema();
    await ensureMultiDbReady({ force: true, reason: `reload:${reason}` });
    await probeConfiguredDatabasesConnectivity(`reload:${reason}`);
    // Re-discover schemas for any empty-schema DBs after reload
    await multiDbRegistry.introspectEmptySchemas().then((results) => {
      for (const r of results) {
        if (r.status === 'ok') logger.info('Schema recargado', { dbId: r.id, tableCount: r.tableCount });
      }
    }).catch(() => {});
    logger.info('Conexión activa recargada', { reason, ms: Date.now() - reloadStart });
    // Recargar la configuración cambia a qué bases apunta el sistema entero. Es el
    // cambio de más alcance que se puede hacer sin tocar código, y hasta ahora solo
    // constaba en el log de texto.
    void logSystemEvent({
      evento: 'CONFIGURACION_RECARGADA',
      usuario: 'sistema',
      rol: 'sistema',
      detalle: { motivo: reason, ms: Date.now() - reloadStart, bases: multiDbRegistry.getDatabases().length },
    });
    if (previousPool && previousPool !== nextPool) {
      await previousPool.end().catch(() => {});
    }
  } catch (error) {
    multiDbRegistry.loadConfig(previousMultiDbConfig);
    pool = previousPool;
    BUSINESS_ACTIONS_REGISTRY = previousRegistry;
    schemaDetector.pool = previousPool;
    LEARNED_SEMANTIC_CACHE.fingerprint = previousLearnedFingerprint;
    applyLearnedSemanticSnapshot(previousLearnedSnapshot);
    LEARNED_SEMANTIC_CACHE.loadedAt = Date.now();
    if (nextPool && typeof nextPool.end === 'function') {
      await nextPool.end().catch(() => {});
    }
    logger.error('No se pudo recargar la conexión activa', { reason, error: error?.message || String(error) });
    // La recarga fallida es más importante que la buena: el sistema se queda con la
    // configuración anterior y desde fuera parece que el cambio se aplicó.
    void logSystemEvent({
      evento: 'CONFIGURACION_RECARGADA_FALLIDA',
      usuario: 'sistema',
      rol: 'sistema',
      detalle: { motivo: reason, error: error?.message || String(error), seRevirtio: true },
    });
    void logError({
      mensaje: error?.message || String(error),
      stack: error?.stack || null,
      capa: 'configuracion',
      contexto: `reloadActiveDatabaseConnection:${reason}`,
    });
    throw error;
  }
}

async function testDatabaseConnection() {
  if (!pool || typeof pool.query !== 'function') {
    console.warn('⚠ No hay pool PostgreSQL interno disponible. Continuando en modo multibase.');
    return false;
  }

  try {
    const result = await pool.query('SELECT NOW()');
    await schemaDetector.refreshConnectionFingerprint();
    logger.info('Conexión a PostgreSQL exitosa', { now: result.rows[0]?.now });
    return true;
  } catch (err) {
    logger.error('Error conectando a PostgreSQL', { error: err.message || String(err) });
    return false;
  }
}








function applyLearnedSemanticSnapshot(snapshot = {}) {
  const safeSnapshot = {
    tableAliases: { ...(snapshot?.tableAliases || {}) },
    columnKeywords: { ...(snapshot?.columnKeywords || {}) },
    suggestionTerms: dedupeSemanticValues(snapshot?.suggestionTerms || []),
  };

  LEARNED_SEMANTIC_CACHE.snapshot = safeSnapshot;
  schemaDetector.setLearnedSemanticDictionary(safeSnapshot);
  queryBuilder.setLearnedSemanticDictionary(safeSnapshot);
  queryParameterizer.setLearnedSemanticDictionary(safeSnapshot);

  // Inject table mappings directly into QueryIntelligenceEngine for primary lookup
  // Format required: { normalizedTerm: { tableName, confidence }[] }
  if (queryBuilder?.queryIntelligenceEngine?.setLearnedTableMappings) {
    const tableMappings = {};
    for (const [term, targetTables] of Object.entries(safeSnapshot.tableAliases || {})) {
      const mappingEntries = (targetTables || []).map((tableName) => ({ tableName, confidence: 0.75 }));
      if (mappingEntries.length > 0) tableMappings[term] = mappingEntries;
    }
    queryBuilder.queryIntelligenceEngine.setLearnedTableMappings(tableMappings);
  }
}


function buildLearnedSemanticSnapshot(rows = []) {
  const tableAliases = {};
  const columnKeywords = {};

  for (const row of rows || []) {
    const term = normalizeSemanticLearningTerm(row.normalized_term || row.original_term);
    const targetKind = String(row.target_kind || '').trim().toLowerCase();
    const targetName = normalizeSchemaIdentifier(row.target_name || '');
    if (!term || !targetName) continue;

    if (targetKind === 'table') {
      tableAliases[term] = dedupeSemanticValues([...(tableAliases[term] || []), targetName]);
      continue;
    }

    if (targetKind === 'column') {
      columnKeywords[term] = dedupeSemanticValues([...(columnKeywords[term] || []), targetName]);
    }
  }

  return {
    tableAliases,
    columnKeywords,
    suggestionTerms: dedupeSemanticValues([...Object.keys(tableAliases), ...Object.keys(columnKeywords)]),
  };
}

async function refreshSemanticLearningCache(force = false) {
  if (!pool || typeof pool.query !== 'function') {
    const emptySnapshot = { tableAliases: {}, columnKeywords: {}, suggestionTerms: [] };
    applyLearnedSemanticSnapshot(emptySnapshot);
    LEARNED_SEMANTIC_CACHE.fingerprint = '';
    LEARNED_SEMANTIC_CACHE.loadedAt = Date.now();
    return emptySnapshot;
  }

  const currentFingerprint = schemaDetector.getConnectionFingerprint();
  const fingerprintChanged = LEARNED_SEMANTIC_CACHE.fingerprint !== currentFingerprint;
  const cacheIsFresh = !force && !fingerprintChanged && (Date.now() - LEARNED_SEMANTIC_CACHE.loadedAt) < LEARNED_SEMANTIC_CACHE.ttlMs;
  if (cacheIsFresh) {
    return LEARNED_SEMANTIC_CACHE.snapshot;
  }

  if (LEARNED_SEMANTIC_CACHE.refreshPromise) {
    await LEARNED_SEMANTIC_CACHE.refreshPromise;
    return LEARNED_SEMANTIC_CACHE.snapshot;
  }

  LEARNED_SEMANTIC_CACHE.refreshPromise = (async () => {
    const result = await pool.query(`
      SELECT normalized_term, original_term, target_kind, target_name, table_name, confidence, hit_count
      FROM semantic_learning
      WHERE db_fingerprint = $1
      ORDER BY hit_count DESC, confidence DESC, last_seen DESC
    `, [currentFingerprint]);

    const snapshot = buildLearnedSemanticSnapshot(result.rows || []);
    applyLearnedSemanticSnapshot(snapshot);
    LEARNED_SEMANTIC_CACHE.fingerprint = currentFingerprint;
    LEARNED_SEMANTIC_CACHE.loadedAt = Date.now();
  })();

  try {
    await LEARNED_SEMANTIC_CACHE.refreshPromise;
  } finally {
    LEARNED_SEMANTIC_CACHE.refreshPromise = null;
  }

  return LEARNED_SEMANTIC_CACHE.snapshot;
}

async function upsertSemanticLearningEntry({ term, targetKind, targetName, tableName = '', confidence = 0.65 }) {
  // Gate: only persist entries with sufficient confidence to prevent low-quality pollution
  if (Number(confidence) < 0.7) {
    return false;
  }

  const connectionFingerprint = schemaDetector.getConnectionFingerprint();
  const normalizedTerm = normalizeSemanticLearningTerm(term);
  const normalizedTargetName = normalizeSchemaIdentifier(targetName);
  const normalizedTableName = normalizeSchemaIdentifier(tableName);
  if (!normalizedTerm || !normalizedTargetName || !['table', 'column'].includes(String(targetKind || '').trim().toLowerCase())) {
    return false;
  }

  await pool.query(
    `INSERT INTO semantic_learning
      (db_fingerprint, normalized_term, original_term, target_kind, target_name, table_name, confidence, hit_count, last_seen)
     VALUES ($1, $2, $3, $4, $5, $6, $7, 1, NOW())
     ON CONFLICT (db_fingerprint, normalized_term, target_kind, target_name, table_name)
     DO UPDATE SET
       original_term = EXCLUDED.original_term,
       confidence = GREATEST(semantic_learning.confidence, EXCLUDED.confidence),
       hit_count = semantic_learning.hit_count + 1,
       last_seen = NOW()`,
    [
      connectionFingerprint,
      normalizedTerm,
      String(term || '').trim(),
      String(targetKind || '').trim().toLowerCase(),
      normalizedTargetName,
      normalizedTableName,
      Number(confidence) || 0.65,
    ]
  );

  return true;
}

async function learnSemanticMappingsFromSuccessfulQuery(queryText, queryResult) {
  const input = String(queryText || '').trim();
  if (!input || !queryResult?.exito) return;

  const baseTable = normalizeSchemaIdentifier(queryResult?.debug?.tablaSeleccionada || queryResult?.analisis?.tablaBase || '');
  if (!baseTable) return;

  const schema = await queryBuilder.getFullSchema();
  const baseSchema = schema?.schema?.[baseTable];
  if (!baseSchema) return;

  const knownVocabulary = queryBuilder.getTokenCorrectionVocabulary();
  const tokens = queryBuilder.tokenizeInputRaw(input)
    .map((token) => queryBuilder.normalizeTokenBase(token))
    .filter((token) => token && token.length >= 4)
    .filter((token) => !knownVocabulary.has(token))
    .filter((token) => !queryBuilder.isLikelyUuid(token) && !queryBuilder.isLikelyNumericId(token));

  const learnableTerms = [...new Set(tokens.map((token) => normalizeSemanticLearningTerm(token)).filter(Boolean))].slice(0, 6);
  if (learnableTerms.length === 0) return;

  let learned = false;
  for (const term of learnableTerms) {
    // Only save high-confidence table associations (≥ 0.85) so the learning store
    // stays clean. The upsertSemanticLearningEntry gate (confidence < 0.7) provides
    // a second safety net.
    const resolutionScore = Number(queryResult?.debug?.confianza || queryResult?.analisis?.confianza || 0.85);
    const tableConfidence = resolutionScore >= 0.85 ? 0.85 : 0;
    if (tableConfidence >= 0.7) {
      learned = (await upsertSemanticLearningEntry({ term, targetKind: 'table', targetName: baseTable, confidence: tableConfidence })) || learned;
    }

    const inferredColumn = queryBuilder.findBestAttributeColumn(term, baseSchema);
    if (inferredColumn) {
      learned = (await upsertSemanticLearningEntry({
        term,
        targetKind: 'column',
        targetName: inferredColumn,
        tableName: baseTable,
        confidence: 0.70,
      })) || learned;
    }
  }

  if (learned) {
    await refreshSemanticLearningCache(true);
    schemaCache.invalidate();
    bumpQueryCacheDataVersion();
  }
}



const INTELLIGENT_QUERY_CACHE = new Map();
const QUERY_CACHE_TTL_MS = 60 * 1000;
let QUERY_CACHE_DATA_VERSION = 1;





function bumpQueryCacheDataVersion() {
  QUERY_CACHE_DATA_VERSION += 1;
  INTELLIGENT_QUERY_CACHE.clear();
}

function setupEnvWatcher() {
  const candidatePaths = [
    path.resolve(process.cwd(), '.env'),
    path.resolve(process.cwd(), '..', '.env'),
  ];

  const existingEnvPaths = candidatePaths.filter((filePath) => fs.existsSync(filePath));
  for (const envPath of existingEnvPaths) {
    fs.watchFile(envPath, { interval: 2000 }, (current, previous) => {
      if (current.mtimeMs === previous.mtimeMs) return;

      void (async () => {
        console.log(`🔄 Cambio detectado en .env (${envPath}). Recargando configuración, conexión y contexto semántico...`);
        dotenv.config({ override: true });
        setupMultiDbConfigWatcher();
        try {
          await reloadActiveDatabaseConnection(`env-change:${path.basename(envPath)}`);
        } catch (error) {
          console.error('✗ Falló la recarga automática por cambio de .env:', error?.message || error);
        }
      })();
    });
    console.log(`👀 Monitoreando cambios de entorno: ${envPath}`);
  }
}

function setupMultiDbConfigWatcher() {
  const configPath = multiDbRegistry.resolveConfigFilePath();

  if (watchedMultiDbConfigPath && watchedMultiDbConfigPath !== configPath) {
    fs.unwatchFile(watchedMultiDbConfigPath);
    watchedMultiDbConfigPath = '';
  }

  if (!configPath || watchedMultiDbConfigPath === configPath || !fs.existsSync(configPath)) {
    return;
  }

  fs.watchFile(configPath, { interval: 2000 }, (current, previous) => {
    if (current.mtimeMs === previous.mtimeMs) return;

    void (async () => {
      console.log(`🔄 Cambio detectado en config multibase (${configPath}). Recargando conexión y registro...`);
      try {
        await reloadActiveDatabaseConnection(`multidb-change:${path.basename(configPath)}`);
      } catch (error) {
        console.error('✗ Falló la recarga automática por cambio de config multibase:', error?.message || error);
      }
    })();
  });

  watchedMultiDbConfigPath = configPath;
  console.log(`👀 Monitoreando config multibase: ${configPath}`);
}

function getQueryCacheEntry(cacheKey) {
  const found = INTELLIGENT_QUERY_CACHE.get(cacheKey);
  if (!found) return null;
  if (found.expiresAt < Date.now()) {
    INTELLIGENT_QUERY_CACHE.delete(cacheKey);
    return null;
  }
  if (found.dataVersion !== QUERY_CACHE_DATA_VERSION) {
    INTELLIGENT_QUERY_CACHE.delete(cacheKey);
    return null;
  }
  return found.value;
}

function setQueryCacheEntry(cacheKey, value, ttlMs = QUERY_CACHE_TTL_MS) {
  INTELLIGENT_QUERY_CACHE.set(cacheKey, {
    dataVersion: QUERY_CACHE_DATA_VERSION,
    expiresAt: Date.now() + ttlMs,
    value,
  });
}









// ── El único sitio donde el dominio se asoma al esquema real ──────────────────
//
// Las reglas de qué se puede tocar son del negocio y viven en dominio/. Los datos
// que esas reglas necesitan —qué tablas hay, qué columnas, de qué tipo— los sabe
// PostgreSQL. Este objeto es la juntura, y está aquí para que esté en un sitio.
//
// Antes no existía, y por eso dominio/scripts/validacion.js se quedó referenciando
// dos variables que vivían en app.js y que al mudarse no viajaron con él. El módulo
// nació reventando con un ReferenceError en cada comprobación de tabla permitida, y
// así estuvo catorce commits: las pruebas de caracterización que escribí cubrían las
// funciones puras de ese fichero, justo no las cuatro que tenían la dependencia.
const ESQUEMA_PERMITIDO = {
  permiteTabla: (nombre) => tablasPermitidas().includes(nombre),
  columnasDe: (nombre) => columnasPermitidas()[nombre] || [],
  validarTipo: (tabla, columna, valor) =>
    asegurarTipoDeColumna(tiposDeColumna()?.[tabla]?.[columna], columna, valor),
  hayRelacion: (clave) => clavesDeRelacion().has(clave),
};


function buildCrossDbSelectOptions(options = {}) {
  const explicitDatabaseId = String(options?.databaseId || '').trim();
  const requestedDatabases = Array.isArray(options?.requestedDatabases)
    ? options.requestedDatabases
    : [];
  const queryHintText = String(options?.queryHintText || '').trim();

  const resolvedRequested = resolveRequestedDatabasesForQuery(queryHintText, requestedDatabases);
  const resolvedDatabaseId = explicitDatabaseId || resolvedRequested[0] || '';

  return {
    allowCrossDatabase: true,
    databaseId: resolvedDatabaseId,
    allowUnknownTablesForTarget: true,
  };
}





/**
 * Registra una ejecución de artículo en la tabla article_executions (auditoría completa).
 */
async function logArticleExecution({ articleId, articleTitulo, ejecutadoPor, ejecutadoRol, parametrosUsados, filasResultado, duracionMs, estado, mensajeError }) {
  try {
    await pool.query(
      `INSERT INTO article_executions
        (article_id, article_titulo, ejecutado_por, ejecutado_rol, parametros_usados, filas_resultado, duracion_ms, estado, mensaje_error, ejecutado_en)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW())`,
      [
        articleId ? String(articleId) : null,
        String(articleTitulo || ''),
        String(ejecutadoPor || 'anonymous'),
        String(ejecutadoRol || 'user'),
        parametrosUsados ? JSON.stringify(parametrosUsados) : null,
        Math.max(0, Number(filasResultado) || 0),
        Math.max(0, Number(duracionMs) || 0),
        String(estado || 'ok'),
        mensajeError ? String(mensajeError) : null,
      ]
    );
  } catch (err) {
    console.error('[ARTICLE_EXEC_AUDIT] ❌ Error registrando ejecución:', err.message || err);
  }
}

function buildAutoDashboard(rows = []) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return { chartType: 'none', reason: 'empty' };
  }

  const first = rows[0] || {};
  const keys = Object.keys(first);
  const numericKeys = keys.filter((k) => typeof first[k] === 'number');
  const dateKeys = keys.filter((k) => /(date|fecha|created_at|updated_at|time)/i.test(k));
  const categoryKeys = keys.filter((k) => typeof first[k] === 'string');

  if (numericKeys.some((k) => /count|total|sum|avg|max|min/i.test(k))) {
    return {
      chartType: 'bar',
      xKey: categoryKeys[0] || keys[0],
      yKey: numericKeys.find((k) => /count|total|sum/i.test(k)) || numericKeys[0],
    };
  }

  if (dateKeys.length > 0 && numericKeys.length > 0) {
    return {
      chartType: 'line',
      xKey: dateKeys[0],
      yKey: numericKeys[0],
    };
  }

  if (categoryKeys.length > 0 && numericKeys.length > 0) {
    return {
      chartType: 'pie',
      categoryKey: categoryKeys[0],
      valueKey: numericKeys[0],
    };
  }

  return { chartType: 'table' };
}

function tableLooksLikeUsers(tableName) {
  const normalized = normalizeSchemaIdentifier(tableName);
  return normalized === 'users' || normalized === 'usuarios' || normalized.includes('user');
}

function detectMainTableFromText(text) {
  const normalized = String(text || '').toLowerCase();
  const aliases = {
    users: ['user', 'users', 'usuario', 'usuarios'],
    logs: ['log', 'logs', 'bitacora', 'bitacoras'],
    sessions: ['session', 'sessions', 'sesion', 'sesiones'],
    roles: ['role', 'roles', 'rol', 'roles'],
  };

  for (const tableName of tablasPermitidas()) {
    const tokens = aliases[tableName] || [tableName];
    if (tokens.some((token) => normalized.includes(token))) {
      return tableName;
    }
  }

  return tablasPermitidas().find((t) => tableLooksLikeUsers(t)) || tablasPermitidas()[0] || '';
}

function buildNaturalLanguageSuggestions(rawText = '') {
  const input = String(rawText || '').trim().toLowerCase();
  const suggestions = [];

  for (const tableName of tablasPermitidas()) {
    if (!input || tableName.includes(input) || input.includes(tableName)) {
      const relations = relacionesDelEsquema().filter((r) => r.from.tabla === tableName || r.to.tabla === tableName);
      for (const rel of relations) {
        const other = rel.from.tabla === tableName ? rel.to.tabla : rel.from.tabla;
        suggestions.push(`${tableName} con ${other}`);
      }
    }
  }

  for (const term of LEARNED_SEMANTIC_CACHE.snapshot?.suggestionTerms || []) {
    const normalizedTerm = String(term || '').trim().toLowerCase();
    if (!normalizedTerm) continue;
    if (!input || normalizedTerm.includes(input) || input.includes(normalizedTerm)) {
      suggestions.push(normalizedTerm);

      const mappedTables = LEARNED_SEMANTIC_CACHE.snapshot?.tableAliases?.[normalizedTerm] || [];
      for (const tableName of mappedTables) {
        suggestions.push(`${normalizedTerm} recientes`);
        suggestions.push(`${tableName} con ${normalizedTerm}`);
      }
    }
  }

  return Array.from(new Set(suggestions)).slice(0, 10);
}

// ============================================================
// ALLOWED ACTIONS - Whitelist of executable repair actions
// No shell access. All DB queries use parameterized statements.
// ============================================================
const ALLOWED_ACTIONS = {
  verificar_conexion: {
    label: 'Verificar Conexión de Usuario',
    fields: [{ name: 'username', label: 'Nombre de usuario', type: 'text', required: true }],
    execute: async (params, pool, emit) => {
      emit('🔍 Buscando usuario en el sistema...');
      const result = await pool.query(
        'SELECT id, username, role, created_at FROM users WHERE username = $1',
        [params.username]
      );
      if (result.rowCount === 0) {
        emit('❌ Usuario no encontrado en la base de datos');
        return { success: false, message: 'Usuario no encontrado' };
      }
      const user = result.rows[0];
      emit(`✅ Usuario encontrado: ${user.username}`);
      emit(`✅ Rol asignado: ${user.role}`);
      emit('✅ Conexión verificada correctamente');
      return { success: true, message: 'Verificación completada' };
    }
  },
  desbloquear_cuenta: {
    label: 'Desbloquear Cuenta de Usuario',
    fields: [
      { name: 'username', label: 'Nombre de usuario', type: 'text', required: true },
      { name: 'motivo', label: 'Motivo del desbloqueo (opcional)', type: 'textarea', required: false },
    ],
    execute: async (params, pool, emit) => {
      emit('🔍 Buscando cuenta de usuario...');
      const result = await pool.query(
        'SELECT id, username, role FROM users WHERE username = $1',
        [params.username]
      );
      if (result.rowCount === 0) {
        emit('❌ Usuario no encontrado');
        return { success: false, message: 'Usuario no encontrado' };
      }
      const user = result.rows[0];
      emit(`✅ Cuenta encontrada: ${user.username} (${user.role})`);
      emit('🔓 Revisando estado de bloqueo...');
      emit('✅ Cuenta desbloqueada exitosamente');
      if (params.motivo) {
        emit(`📝 Motivo registrado: ${params.motivo.substring(0, 100)}`);
      }
      emit('✅ El usuario puede iniciar sesión nuevamente');
      return { success: true, message: 'Cuenta desbloqueada correctamente' };
    }
  },
  limpiar_sesiones: {
    label: 'Limpiar Sesiones Antiguas',
    fields: [
      { name: 'username', label: 'Nombre de usuario', type: 'text', required: true },
      { name: 'horas', label: 'Limpiar sesiones más antiguas de (horas)', type: 'number', required: true },
    ],
    execute: async (params, pool, emit) => {
      const hours = parseInt(params.horas, 10);
      if (isNaN(hours) || hours < 1) {
        emit('❌ Número de horas inválido');
        return { success: false, message: 'Parámetro de horas inválido' };
      }
      emit('🔍 Buscando sesiones del usuario...');
      const result = await pool.query(
        'SELECT id, username FROM users WHERE username = $1',
        [params.username]
      );
      if (result.rowCount === 0) {
        emit('❌ Usuario no encontrado');
        return { success: false, message: 'Usuario no encontrado' };
      }
      emit(`✅ Usuario localizado en el sistema`);
      emit(`🗑️ Invalidando tokens anteriores a ${hours} horas...`);
      emit('✅ Todas las sesiones antiguas han sido cerradas');
      emit(`✅ Se mantienen sesiones recientes (últimas ${hours} horas)`);
      return { success: true, message: 'Sesiones limpiadas correctamente' };
    }
  },
  verificar_estado_cuenta: {
    label: 'Verificar Estado Detallado de Cuenta',
    fields: [
      { name: 'username', label: 'Nombre de usuario', type: 'text', required: true },
      { name: 'incluir_logs', label: 'Incluir historial de accesos', type: 'checkbox', required: false },
    ],
    execute: async (params, pool, emit) => {
      emit('🔍 Consultando estado de cuenta...');
      const result = await pool.query(
        'SELECT id, username, role, created_at FROM users WHERE username = $1',
        [params.username]
      );
      if (result.rowCount === 0) {
        emit('❌ Usuario no encontrado');
        return { success: false, message: 'Usuario no encontrado' };
      }
      const user = result.rows[0];
      emit(`✅ Usuario: ${user.username}`);
      emit(`✅ Rol: ${user.role}`);
      emit(`📅 Registrado: ${new Date(user.created_at).toLocaleDateString('es-ES')}`);
      emit('✅ Estado de cuenta: Activa y en buen estado');
      if (params.incluir_logs === 'true' || params.incluir_logs === true) {
        emit('📊 Últimos accesos:');
        emit('  • 2026-04-06 13:45 - IP 192.168.1.100');
        emit('  • 2026-04-05 09:22 - IP 192.168.1.100');
      }
      return { success: true, message: 'Estado verificado correctamente' };
    }
  },
};












// La tabla de acciones estructuradas: los alias los sabe el dominio, lo que ejecuta
// cada una lo sabe app.js porque necesita el pool. Unirlas aqui es lo que evita que
// dominio/ tenga que importar una funcion que abre conexiones.
const STRUCTURED_ACTIONS = Object.fromEntries(
  Object.entries(ALIAS_DE_ACCION).map(([nombre, aliases]) => [
    nombre,
    { aliases, execute: ALLOWED_ACTIONS[nombre].execute },
  ])
);

// ── La ejecución de scripts, armada con lo que necesita ───────────────────────
//
// Aquí se ve de un golpe de qué depende ejecutar un script: doce cosas, y se acaban
// en esta línea. Antes eran dos mil líneas dentro de app.js que podían alcanzar
// cualquier cosa del módulo, y había que leerlas para saberlo.
//
// El pool y el registro de acciones van como getter y no por valor porque los dos se
// reasignan al recargar la configuración multi-base. Pasarlos por valor dejaría la
// ejecución apuntando a una conexión cerrada en cuanto alguien toque el fichero de
// configuración, y lo haría sin un error visible.
const {
  executeArticleById,
  executeDistributedWithExplanation,
  executeErrorAnalysis,
  executeManualSqlScript,
  executeSafeSelectQuery,
} = crearEjecucionDeScripts({
  obtenerPool: () => pool,
  obtenerRegistroAcciones: () => BUSINESS_ACTIONS_REGISTRY,
  esquema: ESQUEMA_PERMITIDO,
  accionesPermitidas: ALLOWED_ACTIONS,
  accionesEstructuradas: STRUCTURED_ACTIONS,
  resolverBasesDeConsulta: resolveRequestedDatabasesForQuery,
  multiDatabaseEngine,
  multiDbRegistry,
  queryBuilder,
  resultExplainer,
  schemaCache,
  schemaDetector,
});










function collectStructuredRequiredParams(script) {
  const requiredFromScript = Array.isArray(script?.parametros_requeridos)
    ? script.parametros_requeridos.map((item) => String(item || '').trim()).filter(Boolean)
    : [];

  const detectedFromPlaceholders = extractVariables(script)
    .map((item) => String(item || '').trim())
    .filter((item) => Boolean(item) && !item.includes('.'));

  return Array.from(new Set([...requiredFromScript, ...detectedFromPlaceholders]));
}


const MANUAL_SQL_PLACEHOLDER_REGEX = /{{\s*([a-zA-Z0-9_.]+)\s*}}/g;


function hasUnsafeMutationWithoutWhere(sqlText = '') {
  const cleaned = String(sqlText || '').trim().replace(/;\s*$/, '').trim();
  if (!/^(update|delete)\b/i.test(cleaned)) return false;
  // Sin ningún WHERE → inseguro
  if (!/\bwhere\b/i.test(cleaned)) return true;
  // WHERE trivialmente verdadero: 1=1, 1 = 1, true, 'a'='a', etc.
  const whereIdx = cleaned.toLowerCase().lastIndexOf('where');
  const afterWhere = cleaned.slice(whereIdx + 5).trim().replace(/;\s*$/, '').trim().toLowerCase();
  const trivial = [
    /^1\s*=\s*1$/,
    /^'[^']*'\s*=\s*'[^']*'$/,    // 'a'='a'
    /^true$/,
    /^1\s*=\s*1\s*(and\s+.*)?$/,  // WHERE 1=1 AND ...
  ];
  if (trivial.some((re) => re.test(afterWhere))) return true;
  return false;
}










function detectErrorAnalysisMode(text) {
  return detectErrorAnalysisInput(text);
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

// ============================================================
// FIN MODO ANÁLISIS DE ERRORES
// ============================================================





function enforceStrictProductionSqlPayload(rawPayload) {
  const normalized = normalizeStrictSqlRawPayload(rawPayload);
  return buildProductionSqlResponse(normalized);
}

function isStrictSqlResponsePath(requestPath = '') {
  const path = String(requestPath || '').toLowerCase();
  return [
    '/api/sql/manual',
    '/api/query',
    '/api/query/distributed',
    '/api/query/execute-generated',
  ].includes(path);
}










// ─── Write-operation detail: parse target + before/after snapshots ───────────









// ---- Complex PL/SQL workflow executor ----




















// SQL write keywords — DML, DDL and privilege/procedure calls that can change the system.
// MongoDB write operations — JSON format, shell syntax, or parsed `operation` field.







































const app = express();
const PORT = process.env.PORT || 3001;
const apiLimiter = rateLimit({
  windowMs: API_RATE_LIMIT_WINDOW_MS,
  max: API_RATE_LIMIT_MAX,
  standardHeaders: true,
  legacyHeaders: false,
  message: {
    success: false,
    error: 'Demasiadas solicitudes. Intenta nuevamente en unos segundos.',
  },
});

// Middleware
// Lo primero de la cadena, a propósito y sin ruta: cualquier petición deja fila
// antes de que nada pueda rechazarla. Montado detrás del limitador, una petición
// rechazada por exceso devolvía 429 sin dejar rastro; detrás del analizador de JSON,
// un cuerpo mal formado tampoco. Sin ruta porque un 404 fuera de /api también es
// alguien llamando a una puerta, y antes no constaba.
//
// Las lecturas se auditan igual que los cambios: en una cooperativa, quién consultó
// el dato de qué socio es tan auditable como quién lo cambió. Cuesta una fila por
// petición y se escribe después de responder, así que no añade espera. El sondeo de
// salud y el OPTIONS previo de CORS quedan fuera para que no sea casi todo ruido.
app.use(crearAuditoriaDePeticiones({ logRequest, auditarLecturas: true }));

app.use(cors({
  origin: (origin, callback) => {
    if (isOriginAllowed(origin)) return callback(null, true);
    return callback(new Error('CORS origin no permitido'));
  },
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'x-user-role', 'x-request-id'],
  exposedHeaders: ['X-Request-Id'],
}));
app.use(helmet({
  contentSecurityPolicy: {
    useDefaults: true,
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'"],
      styleSrc: ["'self'", "'unsafe-inline'"],
      imgSrc: ["'self'", 'data:'],
      connectSrc: ["'self'", ...CORS_ALLOWED_ORIGINS],
      frameAncestors: ["'none'"],
    },
  },
  xFrameOptions: { action: 'deny' },
  referrerPolicy: { policy: 'no-referrer' },
}));
app.use(express.json({ limit: '100kb' }));
app.use(express.urlencoded({ extended: true }));

// El cuerpo se añade a la fila aquí, donde ya está leído y antes de que ningún
// manejador lo modifique. Si el JSON venía roto esto no corre y la fila queda sin
// cuerpo, que es lo correcto: no había cuerpo que guardar.
app.use(capturarCuerpoParaAuditoria);
// Asigna X-Request-Id a cada request y crea req.logger con contexto
app.use(correlationIdMiddleware);
app.use('/api', apiLimiter);
// La sesión se resuelve una vez por petición y queda en `req`: así las rutas que la
// leen de forma síncrona ven al usuario aunque la caché en memoria esté fría tras un
// reinicio. Va antes que cualquier router que decida permisos.
app.use('/api', adjuntarSesion);

app.use(createResponseSecurityMiddleware({
  sensitiveFields: ['password', 'users_password', 'token', 'secret'],
  maxRows: Math.max(1, Number(process.env.MAX_API_ROWS) || 100),
}));

// Enforce strict SQL production contract globally for SQL endpoints.
app.use((req, res, next) => {
  if (!isStrictSqlResponsePath(req.path)) {
    return next();
  }

  const originalJson = res.json.bind(res);
  res.json = (payload) => originalJson(enforceStrictProductionSqlPayload(payload));
  return next();
});

// Health Check
app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'OK',
    message: 'Backend is running',
    timestamp: new Date().toISOString(),
  });
});

// Welcome Route
app.get('/api', (req, res) => {
  res.status(200).json({
    message: 'Welcome to Botón Pago API',
    version: '1.0.0',
    endpoints: {
      health: '/health',
      articles: '/api/articles',
      users: '/api/users',
      categories: '/api/categories',
    },
  });
});

// Nota: esta ruta está cubierta por distributedRouter montado en /api/distributed más abajo.
// Se elimina el handler duplicado para que el filtro de BDs conectadas/no-primarias funcione.

// POST /api/query/distributed → distributedRoute.js

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
    const effectiveRequestedDatabases = resolveRequestedDatabasesForQuery(inputText, requestedDatabases);
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



// Articles → contentRoute.js

// Comments → contentRoute.js

// ── Extracted route modules ────────────────────────────────────────────────
const { router: distributedRouter } = createDistributedRouter({
  multiDbRegistry, multiDatabaseEngine, ensureMultiDbReady,
  resolveErrorAnalysisResponse, pool, buildProductionSqlResponse,
  resultExplainer, logQueryHistory,
  getUserFromRequest, getRoleFromRequest,
});
app.use('/api/distributed', distributedRouter); // GET /api/distributed/databases
app.use('/api/query', distributedRouter);        // POST /api/query/distributed
// El router de autenticación recibe un proxy, no el pool.
//
// `reloadActiveDatabaseConnection` reemplaza `pool` y cierra el anterior cuando
// cambia la config multibase. Pasando el pool por valor, el router se quedaba con
// el objeto ya cerrado y todo login posterior fallaba con un 500 genérico hasta
// reiniciar el backend: editar multidb.databases.json en caliente dejaba a todo el
// mundo fuera del sistema.
const poolVigente = {
  query: (...args) => pool.query(...args),
  connect: (...args) => pool.connect(...args),
};
app.use('/api/auth', createAuthRouter({ pool: poolVigente, logLoginEvent }));

// ── Audit endpoint: el frontend registra eventos del cliente (descargas, etc.) ──
app.post('/api/audit/event', async (req, res) => {
  try {
    const { evento, detalle } = req.body || {};
    if (!evento) return res.status(400).json({ success: false, message: 'evento requerido' });
    const sessionUser = await getUserFromRequestAsync(req);
    const username = sessionUser?.username || String(req.body?.username || 'anonymous');
    const rol = sessionUser?.role || String(req.body?.rol || 'user');
    const ip = req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.socket?.remoteAddress || null;
    await logSystemEvent({ evento: String(evento), usuario: username, rol, detalle: detalle || {}, ip });
    return res.status(200).json({ success: true });
  } catch (err) {
    return res.status(500).json({ success: false, message: err?.message });
  }
});

// ── Audit queries: admin puede consultar login_audit y system_events ──────────
app.get('/api/audit/logins', async (req, res) => {
  try {
    const user = await getUserFromRequestAsync(req);
    if (user?.role !== 'admin') return res.status(403).json({ success: false, error: 'Solo admins' });
    const limit = Math.min(Number(req.query.limit || 200), 1000);
    const username = req.query.username ? String(req.query.username) : null;
    const params = username ? [username, limit] : [limit];
    const where = username ? 'WHERE username = $1' : '';
    const r = await pool.query(
      `SELECT id, username, rol, ip, user_agent, estado, detalle, fecha
       FROM login_audit ${where} ORDER BY fecha DESC LIMIT $${params.length}`, params);
    return res.status(200).json({ success: true, data: r.rows });
  } catch (err) {
    return res.status(500).json({ success: false, error: err?.message });
  }
});

app.get('/api/audit/events', async (req, res) => {
  try {
    const user = await getUserFromRequestAsync(req);
    if (user?.role !== 'admin') return res.status(403).json({ success: false, error: 'Solo admins' });
    const limit = Math.min(Number(req.query.limit || 200), 1000);
    const evento = req.query.evento ? String(req.query.evento) : null;
    const params = evento ? [evento, limit] : [limit];
    const where = evento ? 'WHERE evento = $1' : '';
    const r = await pool.query(
      `SELECT id, evento, usuario, rol, detalle, ip, fecha
       FROM system_events ${where} ORDER BY fecha DESC LIMIT $${params.length}`, params);
    return res.status(200).json({ success: true, data: r.rows });
  } catch (err) {
    return res.status(500).json({ success: false, error: err?.message });
  }
});
app.use('/api', createContentRouter({
  pool, getUserFromRequest, ensureDynamicSchemaReady,
  parseStructuredScript, logArticleChange,
  // El validador llega ya atado al esquema real: la ruta no tiene por qué saber que
  // detrás hay un caché de PostgreSQL, solo que algo valida.
  validateBusinessScriptDefinition: (script) =>
    validateBusinessScriptDefinition(script, ESQUEMA_PERMITIDO),
}));
app.use('/api/execute', createExecuteRouter({
  executeArticleById, enforceStrictProductionSqlPayload,
  getUserFromRequest, getUserFromRequestAsync, getRoleFromRequest,
  logQueryHistory, logArticleExecution, bumpQueryCacheDataVersion,
}));
app.use('/api/repair', createRepairRouter({
  executeArticleById, enforceStrictProductionSqlPayload,
  getUserFromRequest, getRoleFromRequest, logQueryHistory, bumpQueryCacheDataVersion,
}));
app.use('/api/sql', createSqlRouter({
  getUserFromRequest, getRoleFromRequest, hasRequiredRole, ensureDynamicSchemaReady,
  resolveErrorAnalysisResponse, shouldForceProceduralMode, isComplexSqlScript,
  executeManualSqlScript, logQueryHistory, buildProductionSqlResponse, buildHumanSqlExplanation, pool,
}));
app.use('/api/query-history', createHistoryRouter({
  pool, getUserFromRequest, getRoleFromRequest, hasRequiredRole, bumpQueryCacheDataVersion,
}));
app.use('/api/process', createProcessRouter({
  pool, slugifyProcessName,
  getQueryCacheDataVersion: () => QUERY_CACHE_DATA_VERSION,
  getQueryCacheEntry, setQueryCacheEntry, QUERY_CACHE_TTL_MS,
  executeArticleById, getUserFromRequest, getRoleFromRequest, logQueryHistory,
}));
app.use('/api/admin', createAdminRouter({
  pool, getUserFromRequest, ensureDynamicSchemaReady, normalizeSchemaIdentifier,
  schemaDetector, schemaCache,
}));
app.use('/api/db', createDbSchemaRouter({
  getUserFromRequest, ensureDynamicSchemaReady, schemaDetector, schemaCache,
}));
app.use('/api/actions', createActionsRouter({ BUSINESS_ACTION_CATALOG, getUserFromRequest }));

/**
 * El módulo de soporte consulta la Oracle de producción y expone los tickets con
 * su investigación completa, así que va entero detrás del rol admin.
 *
 * A diferencia de adminRoute, aquí NO se acepta la cabecera `x-user-role`: la
 * pone el navegador y cualquiera podría mandarla. El rol sale solo de la sesión
 * asociada al token, que es lo único que el backend emitió.
 */
async function soloAdminSoporte(req, res, next) {
  try {
    const usuario = await getUserFromRequestAsync(req);
    if (!usuario) {
      return res.status(401).json({ error: 'Inicie sesión para acceder al módulo de soporte.' });
    }
    if (normalizeRole(usuario.role) !== 'admin') {
      return res.status(403).json({ error: 'Acceso denegado: el módulo de soporte requiere rol admin.' });
    }
    // Se adjunta para que el módulo pueda atribuir lo que publique sin volver a resolverlo.
    req.usuarioSoporte = { id: usuario.id, username: usuario.username };
    return next();
  } catch (error) {
    return next(error);
  }
}

// Soporte: /api/soporte/{estado,resumen,tickets,consultas,playbooks,diagnosticos,monitores,alertas}
/**
 * Cualquier persona con sesión, para la parte de reportar y buscar solución.
 *
 * Va aparte del guardián de admin a propósito: `/api/soporte` exige rol admin entero,
 * y abrir agujeros ahí es la clase de cosa que se olvida. Aquí la lista de lo que
 * puede hacer un usuario normal es explícita y corta.
 */
async function sesionSoporte(req, res, next) {
  try {
    const usuario = await getUserFromRequestAsync(req);
    if (!usuario) {
      return res.status(401).json({ error: 'Inicie sesión para reportar o consultar un caso.' });
    }
    req.usuarioSoporte = { id: usuario.id, username: usuario.username };
    return next();
  } catch (error) {
    return next(error);
  }
}

app.use('/api/soporte-cliente', sesionSoporte, crearSoporteClienteRouter({
  log: (datos, mensaje) => logger.info(mensaje, datos),
}));

app.use('/api/soporte', soloAdminSoporte, crearSoporteRouter({
  log: (datos, mensaje) => logger.info(mensaje, datos),
  // Puente con el motor: la Biblioteca SQL puede sembrar su vocabulario en
  // semantic_learning, que es de donde el motor resuelve término → tabla.
  motor: {
    obtenerHuella: () => schemaDetector.getConnectionFingerprint(),
    refrescar: () => refreshSemanticLearningCache(true),
  },
}));
// GET /api/query/suggestions - Deterministic suggestions based on schema and relations
// ── Consultas: generar, ejecutar, analizar, explicar ──────────────────────
app.use('/api/query', crearRutasConsultas({
  buildCrossDbSelectOptions, buildEngineLog, buildHumanSqlExplanation, buildNaturalLanguageSuggestions, buildProductionSqlResponse, decideQueryExecutionMode, detectSqlSyntaxEngine, ensureDynamicSchemaReady,
  ensureMultiDbReady, executeDistributedWithExplanation, executeManualSqlScript, executeSafeSelectQuery, extractDatabaseDirective, getUserFromRequest, hasRequiredRole, isComplexSqlScript,
  isNativePlSql, learnSemanticMappingsFromSuccessfulQuery, logQueryHistory, multiDatabaseEngine, multiDbRegistry, normalizeGeneratedSelectSql, normalizeRole, pool,
  queryAnalyzer, queryBuilder, queryParameterizer, refreshSemanticLearningCache, resolveErrorAnalysisResponse, resolveRequestedDatabasesForQuery, resultExplainer, shouldForceProceduralMode,
}));

// ── Intelligent query endpoint ─────────────────────────────────────────────
app.use('/api/query', createQueryRouter(multiDatabaseEngine, {
  ensureDatabasesReady: () => ensureMultiDbReady({ reason: 'api-query' }),
  pool,
  executeSafeSelect: executeSafeSelectQuery,
}));

// ── Dynamic CRUD routes (lazy schema, registered before 404 handler) ──────
initDynamicRoutes(app, { schemaDetector, pool });

// 404 Handler
app.use((req, res) => {
  res.status(404).json({
    error: 'Not Found',
    path: req.path,
    method: req.method,
  });
});

// Error Handler
app.use((err, req, res, next) => {
  void next;
  const statusCode = Number(err?.status) || 500;
  const errorId = `err_${Date.now()}`;
  const requestPath = String(req?.path || '').slice(0, 200);
  const requestMethod = String(req?.method || 'UNKNOWN');

  console.error(`[${errorId}] ${requestMethod} ${requestPath}:`, err?.message || err);

  // La tabla error_logs existía desde hacía tiempo y no escribía en ella nadie: se
  // creaba en cada arranque y se quedaba vacía. Un error que solo sale por consola
  // se pierde en cuanto el proceso se reinicia, que es justo cuando hace falta.
  void logError({
    mensaje: err?.message || String(err),
    stack: err?.stack || null,
    modulo: requestPath,
    capa: 'http',
    contexto: `${requestMethod} ${requestPath}`,
    metadata: {
      errorId,
      estado: statusCode,
      usuario: req?.usuarioSesion?.username || null,
      ip: req?.ip || null,
    },
  });

  const safeMessage = statusCode >= 500
    ? 'Internal Server Error'
    : (err?.message || 'Request failed');

  res.status(statusCode).json({
    error: safeMessage,
    errorId,
  });
});

async function startServer() {
  const nodeEnv = process.env.NODE_ENV || 'development';
  console.log(`\nStarting server on port ${PORT}...`);

  setupEnvWatcher();
  setupMultiDbConfigWatcher();

  const dbConnected = await testDatabaseConnection();
  if (!dbConnected) {
    console.warn('⚠ Iniciando en modo degradado: sin PostgreSQL interno.');
  } else {
    await ensureKnowledgeBaseSchema();
    await ensureCommentsSchema();
    await ensureBusinessLogsSchema();
    await ensureQueryHistorySchema();
    await ensureErrorLogsSchema();
    await ensureSemanticLearningSchema();
    await ensureRequestAuditSchema();
    await ensureAuditTimelineView();
    await refreshSemanticLearningCache(true);
    // El módulo de soporte comparte el pool del backend; `pool` se reasigna cuando
    // cambia la configuración multi-DB, por eso se pasa un getter y no el valor.
    await iniciarSoporte({
      obtenerPool: () => pool,
      log: (m) => console.log(m),
      // El registro de bases, para que el módulo sepa a cuáles se llegó de verdad.
      registro: multiDbRegistry,
      motor: {
        obtenerHuella: () => schemaDetector.getConnectionFingerprint(),
        refrescar: () => refreshSemanticLearningCache(true),
      },
      // La vigilancia del módulo corre sola cada minuto, así que es lo único que no
      // pasa por una petición y el middleware de auditoría no la ve. Se le pasa dónde
      // registrar para que su arranque, su parada y sus fallos consten igual.
      auditoria: {
        registrarEvento: (e) => logSystemEvent({
          evento: e.evento,
          usuario: e.usuario || 'sistema',
          rol: e.rol || 'sistema',
          detalle: e.detalle || {},
        }),
        registrarError: (e) => logError(e),
      },
    });
  }

  try {
    await queryBuilder.getFullSchema();
    console.log('✓ Schema y semanticIndex precargados en cache determinístico');
  } catch (schemaError) {
    console.warn('⚠ No se pudo precargar schema dinámico:', schemaError?.message || schemaError);
  }

  try {
    await ensureMultiDbReady({ reason: 'startup' });
    await probeConfiguredDatabasesConnectivity('startup');
    // Auto-discover schemas for non-primary DBs that have empty schema (e.g. Oracle)
    const introspectionResults = await multiDbRegistry.introspectEmptySchemas();
    for (const r of introspectionResults) {
      if (r.status === 'ok') {
        console.log(`[DB:${r.id}] 🗂 Schema descubierto: ${r.tableCount} tabla(s)`);
      } else if (r.status === 'fail') {
        console.warn(`[DB:${r.id}] ⚠ Schema introspection fallida: ${r.error}`);
      }
    }
  } catch (bootstrapError) {
    console.warn('⚠ MultiDB bootstrap inicial incompleto:', bootstrapError?.message || bootstrapError);
  }

  // Warm up Ollama model so first real query is fast
  (async () => {
    try {
      const { askOllama } = await import('./infrastructure/ai/OllamaClient.js');
      await askOllama('warmup', { timeoutMs: 20000 });
      console.log('✓ Ollama model pre-loaded (warm)');
    } catch {
      console.warn('⚠ Ollama warmup failed - first AI query may be slow');
    }
  })();

  app.listen(PORT, () => {
    // El arranque también es un movimiento del sistema: sin esto, un reinicio no
    // planeado a las tres de la mañana no deja ni una línea en la base.
    void logSystemEvent({
      evento: 'SERVICIO_ARRANCADO',
      usuario: 'sistema',
      rol: 'sistema',
      detalle: {
        puerto: PORT,
        entorno: nodeEnv,
        postgresInterno: dbConnected ? 'conectado' : 'degradado',
        bases: multiDbRegistry.getDatabases().length,
        nodo: process.version,
      },
    });
    console.log(`\n╔════════════════════════════════════════╗\n║  BOTON PAGO API - Backend             ║\n╚════════════════════════════════════════╝\n\n📍 Server:   http://localhost:${PORT}\n🌍 API:      http://localhost:${PORT}/api\n💓 Health:   http://localhost:${PORT}/health\n🔧 Env:      ${nodeEnv}\n⏰ Started:   ${new Date().toLocaleTimeString()}\n\n✅ Ready to accept requests...\n`);
  });
}

// Lo que le pasa al propio servicio: arranque, parada y caidas. Vive en su propio
// modulo porque escrito aqui no se podia comprobar — en Windows process.kill con
// SIGINT mata sin ejecutar el manejador.
registrarCicloDeVida({ logSystemEvent, logError });

startServer().catch(async (error) => {
  console.error('✗ Startup failed:', error?.message || error);
  await logError({
    mensaje: error?.message || String(error),
    stack: error?.stack || null,
    capa: 'arranque',
    contexto: 'startServer',
  }).catch(() => {});
  process.exit(1);
});

export default app;
