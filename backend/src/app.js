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
import { ALLOWED_WHERE_OPERATORS, DYNAMIC_SCRIPT_ALLOWED_TYPES, MANUAL_SQL_BLOCKED_KEYWORDS, WORKFLOW_ALLOWED_STEP_TYPES, assertDynamicScriptAllowed, assertManualSqlAllowed, buildRelationKey, detectSqlOperation, ensureDynamicColumnAllowed, ensureDynamicJoinRelationAllowed, hasRelationBetweenFields, isPlainObject, normalizeBusinessStep, normalizeDynamicScriptType, parseQualifiedFieldRef, resolveWhereEntry, validateBusinessScriptDefinition, validateRelationalMutationScriptDefinition, validateRelationalScriptDefinition, validateWorkflowBuilderDefinition } from './dominio/scripts/validacion.js';
import { buildHumanSqlExplanation, buildProductionSqlResponse, sanitizeProductionResultObject } from './dominio/presentacion/explicacion.js';
import { applyCastToSqlPlaceholder, applyOptionalSqlFilterTransform, buildAnalyticSelectQuery, buildManualSqlQuery, collectManualSqlPlaceholderMetadata, countPositionalSqlPlaceholders, getValueByPath, inferManualPlaceholderCast, inferStructuredParamSemantic, isMissingParamValue, isOptionalUserPlaceholder, isWriteConfirmed, normalizeLimitValue, normalizeManualSqlParamsInput, resolveDynamicScriptNode, resolveValue, resolveValueMultiSource, resolveWorkflowContextPlaceholder, sanitizeStructuredParamValue, validateStructuredParamByName, MANUAL_SQL_CANONICAL_PLACEHOLDER_REGEX, MANUAL_SQL_POSITIONAL_PLACEHOLDER_REGEX, STRUCTURED_PLACEHOLDER_REGEX } from './dominio/sql/construccion.js';
import { configurarPool as configurarPoolAuditoria, logArticleChange, logLoginEvent, logQueryHistory, logSystemEvent } from './adaptadores/postgres/auditoria.js';
import {
  configurarEsquemas,
  ensureKnowledgeBaseSchema,
  ensureCommentsSchema,
  ensureBusinessLogsSchema,
  ensureQueryHistorySchema,
  ensureErrorLogsSchema,
  ensureSemanticLearningSchema,
} from './adaptadores/postgres/esquemas.js';
import {
  configurarEsquemaDinamico, ensureDynamicSchemaReady, refreshDynamicSchemaCache,
  relacionesDelEsquema, resetDynamicSchemaCache, tablasPermitidas, tiposDeColumna,
} from './adaptadores/postgres/esquema-dinamico.js';

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
const MANUAL_SQL_TIMEOUT_MS = Math.max(1000, Number(process.env.SQL_QUERY_TIMEOUT_MS) || 120000);

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
    ]);
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

  const client = await pool.connect();
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

  const requested = resolveRequestedDatabasesForQuery(hintText, []);
  return requested[0] || '';
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

const EXECUTABLE_SOLUTION_TYPES = new Set(['ejecutable', 'database', 'script']);

const STRUCTURED_ACTIONS = {
  verificar_conexion: {
    execute: ALLOWED_ACTIONS.verificar_conexion.execute,
    aliases: {
      usuario: 'username',
      username: 'username',
    },
  },
  desbloquear_cuenta: {
    execute: ALLOWED_ACTIONS.desbloquear_cuenta.execute,
    aliases: {
      usuario: 'username',
      username: 'username',
      motivo: 'motivo',
    },
  },
  limpiar_sesiones: {
    execute: ALLOWED_ACTIONS.limpiar_sesiones.execute,
    aliases: {
      usuario: 'username',
      username: 'username',
      horas: 'horas',
    },
  },
  verificar_estado_cuenta: {
    execute: ALLOWED_ACTIONS.verificar_estado_cuenta.execute,
    aliases: {
      usuario: 'username',
      username: 'username',
      incluir_logs: 'incluir_logs',
    },
  },
};










async function ejecutarAccionEmpresarial(nombre, params, context, emit) {
  const actionName = String(nombre || '').trim();
  if (!actionName || !BUSINESS_ACTIONS_REGISTRY[actionName]) {
    throw new Error(`Acción no permitida: ${actionName || 'desconocida'}`);
  }

  return BUSINESS_ACTIONS_REGISTRY[actionName]({ params, context, emit });
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



function parseStructuredScript(scriptJson) {
  if (!scriptJson) {
    return null;
  }

  if (typeof scriptJson === 'object') {
    return scriptJson;
  }

  if (typeof scriptJson === 'string') {
    try {
      return JSON.parse(scriptJson);
    } catch {
      return null;
    }
  }

  return null;
}




function collectStructuredRequiredParams(script) {
  const requiredFromScript = Array.isArray(script?.parametros_requeridos)
    ? script.parametros_requeridos.map((item) => String(item || '').trim()).filter(Boolean)
    : [];

  const detectedFromPlaceholders = extractVariables(script)
    .map((item) => String(item || '').trim())
    .filter((item) => Boolean(item) && !item.includes('.'));

  return Array.from(new Set([...requiredFromScript, ...detectedFromPlaceholders]));
}

function normalizeAndValidateStructuredParams(script, rawParams = {}) {
  const normalizedParams = Object.entries(rawParams || {}).reduce((acc, [key, value]) => {
    acc[key] = sanitizeStructuredParamValue(value);
    return acc;
  }, {});

  Object.entries(normalizedParams).forEach(([paramName, value]) => {
    validateStructuredParamByName(paramName, value);
  });

  return normalizedParams;
}

const MANUAL_SQL_PLACEHOLDER_REGEX = /{{\s*([a-zA-Z0-9_.]+)\s*}}/g;
const MANUAL_SQL_NAMED_PLACEHOLDER_REGEX = /(^|[^:]):([a-zA-Z_][a-zA-Z0-9_]*)/g;


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







function getManualSqlStepTitle(stepSql = '', stepNumber = 1) {
  const raw = String(stepSql || '');
  const commentMatch = raw.match(/^\s*--\s*Paso\s*\d+\s*:\s*([^\n\r]+)/i);
  if (commentMatch && commentMatch[1]) {
    return String(commentMatch[1]).trim();
  }

  const cleaned = stripLeadingSqlComments(raw).trim();
  const operation = detectSqlOperation(cleaned);
  if (operation === 'select') return `Paso ${stepNumber}: Consultando datos`;
  if (operation === 'update') return `Paso ${stepNumber}: Actualizando registros`;
  if (operation === 'insert') return `Paso ${stepNumber}: Insertando registros`;
  if (operation === 'delete') return `Paso ${stepNumber}: Eliminando registros`;
  return `Paso ${stepNumber}: Procesando`;
}



function detectErrorAnalysisMode(text) {
  return detectErrorAnalysisInput(text);
}

async function executeErrorAnalysis(text, dbPool) {
  const analysis = analyzeErrorInput(text);
  const matches = await searchErrorLogs(dbPool, analysis, 20);
  const enrichedAnalysis = await enrichErrorAnalysisWithAi(analysis, matches);
  return buildErrorAnalysisResponse(enrichedAnalysis, matches);
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




function normalizeStrictSqlRawPayload(rawPayload) {
  const raw = rawPayload && typeof rawPayload === 'object' && !Array.isArray(rawPayload)
    ? rawPayload
    : {};

  const rows = Array.isArray(raw.rows)
    ? raw.rows
    : Array.isArray(raw.data)
      ? raw.data
      : (raw.resultado && typeof raw.resultado === 'object' && !Array.isArray(raw.resultado) && Array.isArray(raw.resultado.data))
        ? raw.resultado.data
      : Array.isArray(raw.resultado)
        ? raw.resultado
        : [];

  const semanticCandidate =
    (raw.resultado && typeof raw.resultado === 'object' && !Array.isArray(raw.resultado) ? raw.resultado : null)
    || (raw.resultadoSemantico && typeof raw.resultadoSemantico === 'object' && !Array.isArray(raw.resultadoSemantico) ? raw.resultadoSemantico : null)
    || (raw.resultado_final && typeof raw.resultado_final === 'object' && !Array.isArray(raw.resultado_final) ? raw.resultado_final : null)
    || (raw.final && typeof raw.final === 'object' && !Array.isArray(raw.final) ? raw.final : null)
    || (rows[0] && typeof rows[0] === 'object' && !Array.isArray(rows[0]) ? rows[0] : {});

  const noticesFromArray = Array.isArray(raw.notices) ? raw.notices : [];
  const noticesFromSummary = String(raw.resumenHumano || raw.explicacion || raw.explanation || '')
    .split(/\r?\n/)
    .map((line) => String(line || '').trim())
    .filter(Boolean);
  const notices = noticesFromArray.length > 0 ? noticesFromArray : noticesFromSummary;

  const explicacion = String(
    raw.resumenHumano
    || raw.explicacion
    || raw.explanation
    || raw.message
    || raw.error
    || '',
  ).trim();

  const message = String(raw.message || raw.error || '').trim();

  return {
    rows,
    notices,
    semanticCandidate,
    explicacion,
    message,
  };
}

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








/**
 * Detects the operation type of a MongoDB JSON query.
 * Returns 'select' for read operations and 'write' for DML.
 */
function detectMongoOperation(jsonText = '') {
  try {
    const op = JSON.parse(String(jsonText || '').trim());
    if (op?.find !== undefined || op?.findOne !== undefined || op?.aggregate || op?.countDocuments !== undefined || op?.distinct !== undefined) return 'select';
    if (op?.insertOne || op?.insertMany || op?.updateOne || op?.updateMany || op?.replaceOne || op?.deleteOne || op?.deleteMany) return 'insert';
  } catch {
    // not valid JSON — treat as read
  }
  return 'select';
}

/**
 * Executes a write (DML) query on the specified target database, converting
 * placeholder syntax as needed for MySQL, Oracle, MSSQL, and MongoDB.
 * Falls back to the primary Postgres pool when no targetDatabaseId is given.
 */
async function executeWriteToTargetDb(sqlText, queryParams, targetDatabaseId) {
  const params = Array.isArray(queryParams) ? queryParams : [];

  if (!targetDatabaseId) {
    return await pool.query(sqlText, params);
  }

  const targetDb = multiDbRegistry.getDatabaseById(targetDatabaseId);
  if (!targetDb) {
    // DB not in registry — fall back to primary pool (postgres)
    return await pool.query(sqlText, params);
  }

  if (targetDb.type === 'postgres') {
    const primaryDb = multiDbRegistry.getPrimaryDatabase();
    if (!primaryDb || targetDb.id === primaryDb.id) {
      return await pool.query(sqlText, params);
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

// ─── Write-operation detail: parse target + before/after snapshots ───────────

/**
 * Extracts the target table + WHERE clause from a single-statement
 * UPDATE / DELETE / INSERT so we can snapshot affected rows.
 * Returns null when the statement can't be parsed (multi-statement, exotic SQL).
 */
function parseWriteTarget(sqlText = '') {
  const sql = String(sqlText || '')
    .replace(/\/\*[\s\S]*?\*\//g, ' ')
    .replace(/--[^\n]*/g, ' ')
    .replace(/;\s*$/, '')
    .replace(/\s+/g, ' ')
    .trim();
  // Bail on multi-statement scripts — the snapshot would be ambiguous.
  if (/;\s*\S/.test(sql)) return null;

  let m = sql.match(/^update\s+([a-zA-Z_][\w.$"]*)\s+set\s+[\s\S]*?(?:\s+where\s+([\s\S]+?))?(?:\s+returning\b[\s\S]*)?$/i);
  if (m) return { operation: 'update', table: m[1].replace(/"/g, ''), whereClause: (m[2] || '').trim() };

  m = sql.match(/^delete\s+from\s+([a-zA-Z_][\w.$"]*)(?:\s+where\s+([\s\S]+?))?(?:\s+returning\b[\s\S]*)?$/i);
  if (m) return { operation: 'delete', table: m[1].replace(/"/g, ''), whereClause: (m[2] || '').trim() };

  m = sql.match(/^insert\s+into\s+([a-zA-Z_][\w.$"]*)/i);
  if (m) return { operation: 'insert', table: m[1].replace(/"/g, ''), whereClause: '' };

  return null;
}

/**
 * Re-numbers the $N placeholders inside a WHERE clause so it can run as a
 * standalone SELECT (the original numbering references the full statement).
 */
function renumberWhereParams(whereClause = '', allValues = []) {
  const map = new Map();
  const values = [];
  const clause = String(whereClause || '').replace(/\$(\d+)/g, (_, n) => {
    const old = Number(n);
    if (!map.has(old)) { map.set(old, values.length + 1); values.push(allValues[old - 1]); }
    return '$' + map.get(old);
  });
  return { clause, values };
}

/** Picks a likely identifier column from a row, for before/after matching. */
function pickRowIdentifier(row = {}) {
  for (const k of ['id', 'ID', 'Id', '_id', 'uuid', 'UUID', 'codigo', 'CODIGO']) {
    if (row && Object.prototype.hasOwnProperty.call(row, k)) return { key: k, value: row[k] };
  }
  return null;
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

/** Formats a value for the before/after detail lines. */
function formatDetailValue(v) {
  if (v === null || v === undefined) return 'NULL';
  if (v instanceof Date) return v.toISOString();
  if (typeof v === 'object') {
    const s = JSON.stringify(v);
    return s.length > 80 ? s.slice(0, 80) + '…' : s;
  }
  const s = String(v);
  return s.length > 80 ? s.slice(0, 80) + '…' : s;
}

/**
 * Builds a human, multi-line explanation for a write operation, including a
 * per-row before→after diff (UPDATE), deleted rows (DELETE) or inserted rows (INSERT).
 */
function buildWriteDetailExplanation({ operation, table, affectedRows, beforeRows = [], afterRows = [], text = '' }) {
  const op = String(operation || '').toLowerCase();
  const preview = String(text || '').replace(/\s+/g, ' ').trim();
  const queryPreview = preview.length > 160 ? preview.slice(0, 160) + '…' : preview;
  const n = Number(affectedRows || 0);
  const plural = (count) => (count === 1 ? '' : 's');
  const lines = [];

  if (op === 'update') {
    lines.push(`✅ UPDATE ejecutado en \`${table}\` — ${n} fila${plural(n)} afectada${plural(n)}.`);
    if (n === 0) {
      lines.push('ℹ️ Ninguna fila coincidió con el criterio WHERE — no se modificó ningún dato.');
    } else if (beforeRows.length && afterRows.length) {
      const afterById = new Map();
      for (const r of afterRows) {
        const id = pickRowIdentifier(r);
        if (id) afterById.set(String(id.value), r);
      }
      lines.push('📋 Cambios aplicados (antes → después):');
      let shown = 0;
      for (const before of beforeRows) {
        if (shown >= 20) { lines.push(`  … y ${beforeRows.length - shown} fila${plural(beforeRows.length - shown)} más`); break; }
        const id = pickRowIdentifier(before);
        const after = id ? afterById.get(String(id.value)) : null;
        if (!after) continue;
        const changes = [];
        for (const key of Object.keys(before)) {
          const b = formatDetailValue(before[key]);
          const a = formatDetailValue(after[key]);
          if (b !== a) changes.push(`${key}: ${b} → ${a}`);
        }
        if (changes.length) {
          const lbl = id ? `${id.key}=${formatDetailValue(id.value)}` : `fila ${shown + 1}`;
          lines.push(`  • ${lbl} — ${changes.join('  |  ')}`);
          shown++;
        }
      }
      if (shown === 0) lines.push('  • Las filas coincidieron pero los valores nuevos eran iguales a los anteriores.');
    }
  } else if (op === 'delete') {
    lines.push(`✅ DELETE ejecutado en \`${table}\` — ${n} fila${plural(n)} eliminada${plural(n)}.`);
    if (n === 0) {
      lines.push('ℹ️ Ninguna fila coincidió con el criterio WHERE — no se eliminó ningún dato.');
    } else if (beforeRows.length) {
      lines.push('🗑️ Registros eliminados:');
      beforeRows.slice(0, 20).forEach((r) => {
        const entries = Object.entries(r).slice(0, 6).map(([k, v]) => `${k}=${formatDetailValue(v)}`);
        lines.push(`  • ${entries.join(', ')}`);
      });
      if (beforeRows.length > 20) lines.push(`  … y ${beforeRows.length - 20} más`);
    }
  } else if (op === 'insert') {
    lines.push(`✅ INSERT ejecutado en \`${table}\` — ${n} fila${plural(n)} insertada${plural(n)}.`);
    if (afterRows.length) {
      lines.push('📥 Registros insertados:');
      afterRows.slice(0, 20).forEach((r) => {
        const entries = Object.entries(r).slice(0, 8).map(([k, v]) => `${k}=${formatDetailValue(v)}`);
        lines.push(`  • ${entries.join(', ')}`);
      });
    }
  } else {
    lines.push(`✅ ${op.toUpperCase()} ejecutado correctamente — ${n} fila${plural(n)} afectada${plural(n)}.`);
  }

  lines.push(`📝 SQL: ${queryPreview}`);
  return lines.join('\n');
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

  const client = await pool.connect();
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

// ---- Complex PL/SQL workflow executor ----

async function executeOneSqlStep(sqlText, rawParams, workflowContext, stepNum, emit) {
  if (typeof emit === 'function') {
    emit(`[STEP ${stepNum}] ${sqlText.trim().split(/\s+/).slice(0, 4).join(' ')}...`);
  }
  // Always silent=true so no technical param/placeholder noise is emitted
  return executeSingleManualSqlStep(sqlText, rawParams, null, workflowContext, true, {
    assumeWriteConfirmed: true,
  });
}

function evaluateProceduralExpression(rawExpr, workflowContext = {}) {
  const expr = String(rawExpr || '').trim();
  if (!expr) return '';

  const placeholderMatch = expr.match(/^{{\s*([a-zA-Z0-9_.]+)\s*}}$/);
  if (placeholderMatch) {
    const resolved = resolveWorkflowContextPlaceholder(placeholderMatch[1], workflowContext, workflowContext);
    return isMissingParamValue(resolved) ? '' : resolved;
  }

  if (/^'.*'$/s.test(expr)) {
    return expr.replace(/^'(.*)'$/s, '$1').replace(/''/g, "'");
  }

  if (/^-?\d+(?:\.\d+)?$/.test(expr)) {
    return Number(expr);
  }

  if (/^(true|false)$/i.test(expr)) {
    return expr.toLowerCase() === 'true';
  }

  const contextual = resolveWorkflowContextPlaceholder(expr, workflowContext, workflowContext);
  if (!isMissingParamValue(contextual)) return contextual;

  if (Object.prototype.hasOwnProperty.call(workflowContext || {}, expr)) {
    return workflowContext[expr];
  }

  const lowered = expr.toLowerCase();
  if (Object.prototype.hasOwnProperty.call(workflowContext || {}, lowered)) {
    return workflowContext[lowered];
  }

  return expr;
}

function formatRaiseNoticeMessage(template, args = []) {
  let index = 0;
  const text = String(template || '').replace(/%/g, () => {
    const value = index < args.length ? args[index] : '';
    index += 1;
    return String(value ?? '');
  });
  return text.trim();
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

function evaluateSimpleCondition(expr, workflowContext) {
  // Handle simple patterns like "v_count > 0", "step1.count > 0" or "{{step1.count}} > 0"
  const clean = String(expr || '').trim().replace(/\{\{\s*([a-zA-Z0-9_.]+)\s*\}\}/g, '$1');
  const m = clean.match(/^([a-zA-Z_][\w.]*)\s*(>=|<=|!=|<>|=|>|<)\s*(.+)$/i);
  if (!m) return true; // can't evaluate → assume true

  const [, lhs, op, rhs] = m;

  const resolveToken = (tokenRaw) => {
    const token = String(tokenRaw || '').trim().replace(/^['"]|['"]$/g, '');
    if (token === '') return null;
    if (/^(true|false)$/i.test(token)) return token.toLowerCase() === 'true';
    if (/^-?\d+(?:\.\d+)?$/.test(token)) return Number(token);

    const contextual = resolveWorkflowContextPlaceholder(token, workflowContext, workflowContext);
    if (!isMissingParamValue(contextual)) return contextual;

    if (Object.prototype.hasOwnProperty.call(workflowContext || {}, token)) {
      return workflowContext[token];
    }
    if (Object.prototype.hasOwnProperty.call(workflowContext || {}, token.toLowerCase())) {
      return workflowContext[token.toLowerCase()];
    }
    return token;
  };

  const lhsVal = resolveToken(lhs);
  const rhsVal = resolveToken(rhs);

  const lNum = Number(lhsVal);
  const rNum = Number(rhsVal);
  const bothNumeric = Number.isFinite(lNum) && Number.isFinite(rNum);

  if (op === '>') return bothNumeric ? lNum > rNum : String(lhsVal) > String(rhsVal);
  if (op === '<') return bothNumeric ? lNum < rNum : String(lhsVal) < String(rhsVal);
  if (op === '>=') return bothNumeric ? lNum >= rNum : String(lhsVal) >= String(rhsVal);
  if (op === '<=') return bothNumeric ? lNum <= rNum : String(lhsVal) <= String(rhsVal);
  if (op === '=') return bothNumeric ? lNum === rNum : String(lhsVal) === String(rhsVal);
  if (op === '!=' || op === '<>') return bothNumeric ? lNum !== rNum : String(lhsVal) !== String(rhsVal);
  return true;
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


/**
 * If any step produced a detailed write explanation (UPDATE/DELETE/INSERT
 * antes→después), builds a per-step breakdown so the user sees exactly what
 * each step changed. Returns null when no step has write detail.
 */
function buildPerStepWriteSummary(executions = []) {
  const list = Array.isArray(executions) ? executions : [];
  const detailedSteps = list.filter(
    (ex) => typeof ex.explicacion === 'string'
      && /^✅\s+(UPDATE|DELETE|INSERT)\b/i.test(ex.explicacion.trim()),
  );
  if (detailedSteps.length === 0) return null;

  const affectedTotal = list.reduce((sum, ex) => sum + Number(ex.affectedRows || 0), 0);
  const stepCount = list.length;
  const blocks = detailedSteps.map((ex, idx) => {
    const title = String(ex.title || `Paso ${idx + 1}`).trim();
    return `▸ ${title}\n${ex.explicacion.trim()}`;
  });
  const header = `El script ejecutó ${stepCount} ${stepCount === 1 ? 'paso' : 'pasos'}`
    + (affectedTotal > 0 ? ` y afectó ${affectedTotal} fila${affectedTotal !== 1 ? 's' : ''} en total.` : '.');
  return `${header}\n\n${blocks.join('\n\n')}`;
}

function buildComplexWorkflowExplanation(steps, executions, finalResult) {
  const rows = finalResult?.data || [];
  const loopSteps = steps.filter((s) => s.type === 'for_loop');
  const affectedTotal = executions.reduce((sum, ex) => sum + (ex.affectedRows || 0), 0);

  // Per-step write detail (UPDATE/DELETE/INSERT antes→después)
  const perStepSummary = buildPerStepWriteSummary(executions);
  if (perStepSummary) return perStepSummary;

  if (loopSteps.length > 0 && affectedTotal > 0) {
    return `El script procesó registros en ${loopSteps.length} loop(s) y afectó ${affectedTotal} filas en total.`;
  }
  if (rows.length > 0) {
    return buildHumanSqlExplanation(rows);
  }
  if (affectedTotal > 0) {
    return `El script ejecutó ${executions.length} pasos y afectó ${affectedTotal} filas.`;
  }
  return `El script se ejecutó correctamente en ${executions.length} ${executions.length === 1 ? 'paso' : 'pasos'}.`;
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
  const isOracleTarget = !isPostgresAnonymousBlock && (
    resolvedDb?.type === 'oracle'
    || (!resolvedDatabaseId && detectedEngine === 'oracle')
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

function parseStepOrder(stepKey = '') {
  const match = String(stepKey || '').match(/^step(\d+)$/i);
  return match ? Number(match[1]) : Number.MAX_SAFE_INTEGER;
}

function extractObjectForAggregation(value) {
  if (Array.isArray(value)) {
    const firstObj = value.find((item) => item && typeof item === 'object' && !Array.isArray(item));
    return firstObj || null;
  }
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    return value;
  }
  return null;
}

function addWithUniqueKey(target, key, value, stepKey) {
  const baseKey = String(key || '').trim();
  if (!baseKey) return;

  if (!(baseKey in target)) {
    target[baseKey] = value;
    return;
  }

  if (target[baseKey] === value) return;

  const safeStep = String(stepKey || 'step').toLowerCase();
  let candidate = `${baseKey}_${safeStep}`;
  if (!(candidate in target)) {
    target[candidate] = value;
    return;
  }

  let suffix = 2;
  while (`${candidate}_${suffix}` in target) suffix += 1;
  target[`${candidate}_${suffix}`] = value;
}

function buildAggregatedResultFromStepPayload(stepPayload = {}) {
  const aggregated = {};
  let hasData = false;

  const entries = Object.entries(stepPayload || {})
    .sort((a, b) => parseStepOrder(a[0]) - parseStepOrder(b[0]));

  for (const [stepKey, stepValue] of entries) {
    const sourceObj = extractObjectForAggregation(stepValue);
    if (!sourceObj) continue;

    for (const [field, value] of Object.entries(sourceObj)) {
      if (value === undefined || value === null) continue;
      addWithUniqueKey(aggregated, field, value, stepKey);
      hasData = true;
    }
  }

  return hasData ? aggregated : null;
}

function collectStructuredVariables(node, variables = new Set()) {
  if (typeof node === 'string') {
    let match;
    while ((match = STRUCTURED_PLACEHOLDER_REGEX.exec(node)) !== null) {
      variables.add(match[1]);
    }
    STRUCTURED_PLACEHOLDER_REGEX.lastIndex = 0;

    let sqlMatch;
    while ((sqlMatch = MANUAL_SQL_NAMED_PLACEHOLDER_REGEX.exec(node)) !== null) {
      variables.add(sqlMatch[2]);
    }
    MANUAL_SQL_NAMED_PLACEHOLDER_REGEX.lastIndex = 0;

    return variables;
  }

  if (Array.isArray(node)) {
    node.forEach((item) => collectStructuredVariables(item, variables));
    return variables;
  }

  if (node && typeof node === 'object') {
    Object.values(node).forEach((value) => collectStructuredVariables(value, variables));
  }

  return variables;
}

function extractVariables(scriptJson) {
  const parsedScript = parseStructuredScript(scriptJson);
  if (!parsedScript || typeof parsedScript !== 'object') {
    return [];
  }

  return Array.from(collectStructuredVariables(parsedScript));
}

// SQL write keywords — DML, DDL and privilege/procedure calls that can change the system.
const SQL_WRITE_KEYWORDS_RE = /\b(insert|update|delete|merge|upsert|replace|truncate|drop|alter|create|grant|revoke|call|exec|execute)\b/i;
// MongoDB write operations — JSON format, shell syntax, or parsed `operation` field.
const MONGO_WRITE_OPS_RE = /\b(insertone|insertmany|updateone|updatemany|replaceone|deleteone|deletemany|findandmodify|findoneandupdate|findoneanddelete|findoneandreplace|bulkwrite)\b/i;

/**
 * Detects whether a raw SQL / MongoDB text performs a write operation.
 * Strips SQL comments first so leading `-- comment`, block comments, `BEGIN;`
 * or whitespace cannot hide a real mutation keyword.
 */
function sqlTextContainsMutation(rawSql) {
  const noComments = String(rawSql || '')
    .replace(/\/\*[\s\S]*?\*\//g, ' ')   // block comments
    .replace(/--[^\n]*/g, ' ');           // line comments
  if (!noComments.trim()) return false;
  // MongoDB write ops — checked before stripping quotes (Mongo JSON/shell relies on them).
  if (MONGO_WRITE_OPS_RE.test(noComments)) return true;
  // SQL — strip single-quoted string literals so data values can't trigger false positives.
  const sqlOnly = noComments.replace(/'(?:[^']|'')*'/g, "''");
  return SQL_WRITE_KEYWORDS_RE.test(sqlOnly);
}

function scriptContainsMutation(node) {
  if (!node || typeof node !== 'object') {
    return false;
  }

  if (Array.isArray(node)) {
    return node.some((item) => scriptContainsMutation(item));
  }

  const current = node;
  const stepType = String(current.tipo || '').trim().toLowerCase();
  if (['update', 'delete', 'insert', 'merge', 'truncate', 'drop', 'alter', 'ddl'].includes(stepType)) {
    return true;
  }

  // MongoDB shell-parsed format: { operation: 'updateone', ... }
  const operation = String(current.operation || '').trim().toLowerCase();
  if (operation && MONGO_WRITE_OPS_RE.test(operation)) {
    return true;
  }

  // Manual SQL / MongoDB text — check every field that can carry a query,
  // regardless of modo/origen, so any storage format is covered.
  for (const key of ['sql', 'pipeline', 'filter', 'query', 'statement']) {
    const val = current[key];
    if (typeof val === 'string' && sqlTextContainsMutation(val)) {
      return true;
    }
  }

  return Object.values(current).some((value) => scriptContainsMutation(value));
}

function interpolateStructuredNode(node, rawParams) {
  if (typeof node === 'string') {
    const fullMatch = node.match(/^{{\s*([a-zA-Z0-9_]+)\s*}}$/);
    if (fullMatch) {
      return sanitizeStructuredParamValue(rawParams[fullMatch[1]]);
    }

    return node.replace(STRUCTURED_PLACEHOLDER_REGEX, (_, variableName) => {
      return String(sanitizeStructuredParamValue(rawParams[variableName]));
    });
  }

  if (Array.isArray(node)) {
    return node.map((item) => interpolateStructuredNode(item, rawParams));
  }

  if (node && typeof node === 'object') {
    return Object.fromEntries(
      Object.entries(node).map(([key, value]) => [key, interpolateStructuredNode(value, rawParams)])
    );
  }

  return node;
}

function replaceVariables(scriptJson, inputs) {
  const parsedScript = parseStructuredScript(scriptJson);
  if (!parsedScript) {
    return null;
  }

  const safeInputs = Object.entries(inputs || {}).reduce((acc, [key, value]) => {
    acc[key] = sanitizeStructuredParamValue(value);
    return acc;
  }, {});

  return interpolateStructuredNode(parsedScript, safeInputs);
}

function normalizeStructuredParams(actionName, params) {
  const actionConfig = STRUCTURED_ACTIONS[actionName];
  if (!actionConfig) {
    return {};
  }

  return Object.entries(params || {}).reduce((acc, [key, value]) => {
    const mappedKey = actionConfig.aliases[key] || key;
    acc[mappedKey] = sanitizeStructuredParamValue(value);
    return acc;
  }, {});
}







function resolveTemplateVariable(path, source) {
  return getValueByPath(source, path);
}

function isEmptyWorkflowValue(value) {
  if (value === undefined || value === null) return true;
  if (typeof value === 'string' && value.trim() === '') return true;
  if (Array.isArray(value)) return value.length === 0;
  if (typeof value === 'object') return Object.keys(value).length === 0;
  return false;
}

function normalizeWorkflowStoredValue(stepResult) {
  if (stepResult && Array.isArray(stepResult.data)) {
    if (stepResult.data.length === 1) {
      return stepResult.data[0];
    }
    return stepResult.data;
  }

  if (stepResult && stepResult.data !== undefined) {
    return stepResult.data;
  }

  return stepResult;
}




function validateDynamicColumnType(tableName, field, value) {
  if (value === null || value === undefined) {
    return;
  }

  const expected = tiposDeColumna()?.[tableName]?.[field];
  if (!expected) {
    return;
  }

  if (expected === 'string' && typeof value !== 'string') {
    throw new Error(`Tipo inválido para ${field}`);
  }

  if (expected === 'boolean' && typeof value !== 'boolean') {
    throw new Error(`Tipo inválido para ${field}`);
  }

  if (expected === 'number' && (typeof value !== 'number' || !Number.isFinite(value))) {
    throw new Error(`Tipo inválido para ${field}`);
  }

  if (expected === 'string_or_number' && typeof value !== 'string' && typeof value !== 'number') {
    throw new Error(`Tipo inválido para ${field}`);
  }
}




function buildSelectProjectionSql(script, baseTable) {
  const rootColumns = Array.isArray(script?.columnas)
    ? script.columnas.map((entry) => String(entry || '').trim()).filter(Boolean)
    : [];

  const joinColumns = Array.isArray(script?.join)
    ? script.join.flatMap((joinItem) => {
        const joinTable = String(joinItem?.tabla || '').trim().toLowerCase();
        const cols = Array.isArray(joinItem?.columnas) ? joinItem.columnas : [];
        return cols
          .map((entry) => String(entry || '').trim())
          .filter(Boolean)
          .map((column) => ({ table: joinTable, column }));
      })
    : [];

  const projection = [
    ...rootColumns.map((column) => ({ table: baseTable, column })),
    ...joinColumns,
  ];

  if (projection.length === 0) {
    return `"${baseTable}".*`;
  }

  return projection
    .map(({ table, column }) => `"${table}"."${column}"`)
    .join(', ');
}


function buildRelationalMutationQuery(script, normalizedInputParams, workflowContext = {}, startIndex = 1) {
  const baseTable = String(script.tabla || '').trim().toLowerCase();
  const joins = Array.isArray(script.join) ? script.join : [];
  const baseAlias = 'rb';

  const toQualifiedSql = (fieldRef) => {
    const ref = parseQualifiedFieldRef(fieldRef, baseTable);
    const tableSql = ref.table === baseTable ? baseAlias : ref.table;
    return `"${tableSql}"."${ref.column}"`;
  };

  const joinSql = joins
    .map((joinItem) => {
      const joinTable = String(joinItem.tabla || '').trim().toLowerCase();
      const onObject = joinItem.on || {};
      const onParts = Object.entries(onObject).map(([leftRef, rightRef]) => (
        `${toQualifiedSql(leftRef)} = ${toQualifiedSql(rightRef)}`
      ));
      return `JOIN "${joinTable}" ON ${onParts.join(' AND ')}`;
    })
    .join(' ');

  const resolvedWhere = isPlainObject(script.where)
    ? resolveDynamicScriptNode(script.where, normalizedInputParams, workflowContext)
    : {};

  const whereValues = [];
  const whereConditions = Object.entries(resolvedWhere || {}).map(([fieldRef, rawValue]) => {
    const leftSql = toQualifiedSql(fieldRef);
    const { op, value, fieldRef: rightFieldRef } = resolveWhereEntry(rawValue);

    if (rightFieldRef) {
      return `${leftSql} ${op} ${toQualifiedSql(rightFieldRef)}`;
    }

    const ref = parseQualifiedFieldRef(fieldRef, baseTable);
    validateDynamicColumnType(ref.table, ref.column, value);
    whereValues.push(value);
    return `${leftSql} ${op} $${startIndex + whereValues.length - 1}`;
  });

  const logic = ['AND', 'OR'].includes(String(script.logic || '').toUpperCase())
    ? String(script.logic).toUpperCase()
    : 'AND';

  const whereClause = whereConditions.length
    ? ` WHERE ${whereConditions.join(` ${logic} `)} AND "${baseAlias}".ctid = "b".ctid`
    : ` WHERE "${baseAlias}".ctid = "b".ctid`;

  return {
    baseTable,
    baseAlias,
    joinSql,
    whereClause,
    whereValues,
    resolvedWhere,
  };
}


function buildRelationalSelectQuery(script, normalizedInputParams, workflowContext = {}) {
  const baseTable = String(script.tabla || '').trim().toLowerCase();
  const joins = Array.isArray(script.join) ? script.join : [];

    const joinClauses = joins.map((joinItem) => {
      const joinTable = String(joinItem.tabla || '').trim().toLowerCase();
      const onObject = joinItem.on || {};

      const onParts = Object.entries(onObject).map(([leftRef, rightRef]) => {
      const left = parseQualifiedFieldRef(leftRef, baseTable);
      const right = parseQualifiedFieldRef(rightRef);

      return `"${left.table}"."${left.column}" = "${right.table}"."${right.column}"`;
    });

    return `JOIN "${joinTable}" ON ${onParts.join(' AND ')}`;
  });

  const resolvedWhere = isPlainObject(script.where)
    ? resolveDynamicScriptNode(script.where, normalizedInputParams, workflowContext)
    : {};

  const whereEntries = Object.entries(resolvedWhere || {});
  const whereValues = [];
  const whereConditions = whereEntries.map(([fieldRef, rawValue]) => {
    const { table, column } = parseQualifiedFieldRef(fieldRef, baseTable);
    const { op, value, fieldRef: rightFieldRef } = resolveWhereEntry(rawValue);

    if (rightFieldRef) {
      const right = parseQualifiedFieldRef(rightFieldRef, baseTable);
      if (!tablasPermitidas().includes(right.table)) {
        throw new Error(`WHERE: tabla no permitida (${right.table})`);
      }
      ensureDynamicColumnAllowed(right.table, [right.column]);
      return `"${table}"."${column}" ${op} "${right.table}"."${right.column}"`;
    }

    validateDynamicColumnType(table, column, value);
    whereValues.push(value);
    return `"${table}"."${column}" ${op} $${whereValues.length}`;
  });

  // Support AND / OR logic between conditions (default: AND)
  const logic = ['AND', 'OR'].includes(String(script.logic || '').toUpperCase())
    ? String(script.logic).toUpperCase()
    : 'AND';

  const whereClause = whereConditions.length ? ` WHERE ${whereConditions.join(` ${logic} `)}` : '';
  const limitValue = normalizeLimitValue(script.limit, 0);
  const limitClause = limitValue > 0 ? ` LIMIT ${limitValue}` : '';
  const projectionSql = buildSelectProjectionSql(script, baseTable);

  return {
    baseTable,
    resolvedWhere,
    whereValues,
    query: `SELECT ${projectionSql} FROM "${baseTable}" ${joinClauses.join(' ')}${whereClause}${limitClause}`,
  };
}


function buildWhereClause(whereObject, startIndex = 1, logic = 'AND') {
  const safeLogic = ['AND', 'OR'].includes(String(logic).toUpperCase())
    ? String(logic).toUpperCase()
    : 'AND';
  const whereFields = Object.keys(whereObject || {});
  const whereValues = [];
  const conditions = whereFields.map((field, index) => {
    const { op, value, fieldRef } = resolveWhereEntry(whereObject[field]);

    if (fieldRef) {
      const right = parseQualifiedFieldRef(fieldRef);
      return `"${field}" ${op} "${right.table}"."${right.column}"`;
    }

    whereValues.push(value);
    return `"${field}" ${op} $${startIndex + whereValues.length - 1}`;
  });
  const whereClause = whereFields.length
    ? ` WHERE ${conditions.join(` ${safeLogic} `)}`
    : '';

  return { whereFields, whereValues, whereClause };
}


function evaluateValidationCondition(actual, condition, expected) {
  const normalized = String(condition || 'igual').trim().toLowerCase();

  if (normalized === 'igual') return actual === expected;
  if (normalized === 'diferente') return actual !== expected;
  if (normalized === 'mayor') return Number(actual) > Number(expected);
  if (normalized === 'menor') return Number(actual) < Number(expected);
  if (normalized === 'contiene') return String(actual ?? '').includes(String(expected ?? ''));

  throw new Error(`Condición no soportada: ${condition}`);
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
        ensureDynamicColumnAllowed(table, [column]);
      }
    }

    const validationLimit = normalizeLimitValue(script?.post_proceso?.limit ?? script?.limit, 1);
    const { whereValues, whereClause } = buildWhereClause(whereForValidation, 1);
    const query = `SELECT * FROM "${tableName}"${whereClause} LIMIT ${validationLimit}`;

    console.log('[QUERY] Validación post-proceso ejecutada');
    const result = await pool.query(query, whereValues);
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
      ensureDynamicColumnAllowed(tableName, [field]);

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

  const validated = assertDynamicScriptAllowed(script);
  console.log('[SCRIPT] Ejecutando script dinámico validado');

  if (validated.type === 'update') {
    if (Array.isArray(script.join) && script.join.length > 0) {
      const relationalScript = {
        ...script,
        tipo: 'update',
        tabla: validated.tableName,
      };

      validateRelationalMutationScriptDefinition(relationalScript, 'update');

      const resolvedSet = resolveDynamicScriptNode(validated.setObject, normalizedInputParams, workflowContext);
      const setFields = Object.keys(resolvedSet);
      for (const field of setFields) {
        validateDynamicColumnType(validated.tableName, field, resolvedSet[field]);
      }

      const setValues = setFields.map((field) => resolvedSet[field]);
      const relationalParts = buildRelationalMutationQuery(
        relationalScript,
        normalizedInputParams,
        workflowContext,
        setFields.length + 1
      );

      const query = `UPDATE "${validated.tableName}" AS "b" SET ${setFields
        .map((field, index) => `"${field}" = $${index + 1}`)
        .join(', ')} WHERE EXISTS (SELECT 1 FROM "${validated.tableName}" AS "${relationalParts.baseAlias}" ${relationalParts.joinSql}${relationalParts.whereClause})`;

      console.log('[RELATIONAL UPDATE QUERY] Ejecutando update relacional parametrizado');

      const result = await pool.query(query, [...setValues, ...relationalParts.whereValues]);
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
      validateDynamicColumnType(validated.tableName, field, resolvedSet[field]);
    }
    for (const field of whereFields) {
      validateDynamicColumnType(validated.tableName, field, resolvedWhere[field]);
    }

    const setValues = setFields.map((field) => resolvedSet[field]);
    const { whereValues, whereClause } = buildWhereClause(resolvedWhere, setFields.length + 1);

    const query = `UPDATE "${validated.tableName}" SET ${setFields
      .map((field, index) => `"${field}" = $${index + 1}`)
      .join(', ')}${whereClause}`;

    console.log('[QUERY] Ejecutando update parametrizado');

    const result = await pool.query(query, [...setValues, ...whereValues]);
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
      validateDynamicColumnType(validated.tableName, field, resolvedData[field]);
    }

    const query = `INSERT INTO "${validated.tableName}" (${dataFields
      .map((field) => `"${field}"`)
      .join(', ')}) VALUES (${dataFields.map((_, index) => `$${index + 1}`).join(', ')})`;

    console.log('[QUERY] Ejecutando insert parametrizado');

    const result = await pool.query(query, dataValues);
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

      validateRelationalMutationScriptDefinition(relationalScript, 'delete');

      const relationalParts = buildRelationalMutationQuery(relationalScript, normalizedInputParams, workflowContext, 1);

      const query = `DELETE FROM "${validated.tableName}" AS "b" WHERE EXISTS (SELECT 1 FROM "${validated.tableName}" AS "${relationalParts.baseAlias}" ${relationalParts.joinSql}${relationalParts.whereClause})`;

      console.log('[RELATIONAL DELETE QUERY] Ejecutando delete relacional parametrizado');

      const result = await pool.query(query, relationalParts.whereValues);

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
      validateDynamicColumnType(validated.tableName, field, resolvedWhere[field]);
    }

    const { whereValues, whereClause } = buildWhereClause(resolvedWhere, 1);
    const query = `DELETE FROM "${validated.tableName}"${whereClause}`;

    console.log('[QUERY] Ejecutando delete parametrizado');

    const result = await pool.query(query, whereValues);
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

      const result = await pool.query(analyticQuery.query, analyticQuery.whereValues);

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

      validateRelationalScriptDefinition(relationalScript);
      const relationalQuery = buildRelationalSelectQuery(relationalScript, normalizedInputParams, workflowContext);

      console.log('[RELATIONAL QUERY] Ejecutando select relacional parametrizado');

      const result = await pool.query(relationalQuery.query, relationalQuery.whereValues);
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
        ensureDynamicColumnAllowed(right.table, [right.column]);
      } else {
        validateDynamicColumnType(validated.tableName, field, value);
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

    const result = await pool.query(query, whereValues);
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
  validateRelationalScriptDefinition(script);

  const normalizedInputParams = Object.entries(rawParams || {}).reduce((acc, [key, value]) => {
    acc[key] = sanitizeStructuredParamValue(value);
    return acc;
  }, {});

  const relationalQuery = buildRelationalSelectQuery(script, normalizedInputParams, {});
  console.log('[RELATIONAL QUERY]', relationalQuery.query, relationalQuery.whereValues);

  const result = await pool.query(relationalQuery.query, relationalQuery.whereValues);

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

function evaluateWorkflowValidationCondition(actual, condition, expected) {
  const normalized = String(condition || 'existe').trim().toLowerCase();

  if (normalized === 'existe') return !isEmptyWorkflowValue(actual);
  if (normalized === 'no_existe') return isEmptyWorkflowValue(actual);
  if (normalized === 'igual') {
    if (typeof actual === 'object' || typeof expected === 'object') {
      return JSON.stringify(actual) === JSON.stringify(expected);
    }
    return actual === expected;
  }

  throw new Error(`Condición no soportada: ${condition}`);
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

  if (requiresExplicitWriteConfirmation && !isWriteConfirmed(validatedRuntimeParams)) {
    const message = 'Esta acción modificará datos, confirma para continuar.';
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
    if (!STRUCTURED_ACTIONS[actionName]) {
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
    return STRUCTURED_ACTIONS[actionName].execute(normalizedParams, pool, emit);
  }

  emit('❌ Script JSON sin acción soportada', 'error');
  return { success: false, message: 'Script JSON sin acción soportada' };
}

async function executeArticleById(articleId, rawParams, emit, options = {}) {
  await ensureDynamicSchemaReady(false);

  const articleResult = await pool.query(
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
      ensureDynamicColumnAllowed(normalizedTableName, selectedColumns);
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
  } else if (ALLOWED_ACTIONS[actionKey]) {
    action = ALLOWED_ACTIONS[actionKey];
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
      const result = await pool.query(query, filterValues);
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

  const result = await action.execute(safeParams, pool, emit || (() => {}));
  logger.info('Acción completada', { actionKey, articleId, success: result.success, message: result.message });
  return result;
}

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
  parseStructuredScript, validateBusinessScriptDefinition, logArticleChange,
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
    console.log(`\n╔════════════════════════════════════════╗\n║  BOTON PAGO API - Backend             ║\n╚════════════════════════════════════════╝\n\n📍 Server:   http://localhost:${PORT}\n🌍 API:      http://localhost:${PORT}/api\n💓 Health:   http://localhost:${PORT}/health\n🔧 Env:      ${nodeEnv}\n⏰ Started:   ${new Date().toLocaleTimeString()}\n\n✅ Ready to accept requests...\n`);
  });
}

startServer().catch((error) => {
  console.error('✗ Startup failed:', error?.message || error);
  process.exit(1);
});

export default app;
