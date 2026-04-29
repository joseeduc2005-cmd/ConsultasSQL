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

dotenv.config();

const multiDbRegistry = new MultiDbRegistry();
let watchedMultiDbConfigPath = '';

const ALLOWED_OUTBOUND_HOSTS = new Set(['localhost', '127.0.0.1', '::1']);
const BLOCKED_OUTBOUND_PATTERNS = ['openai', 'anthropic', 'googleapis', 'huggingface', 'ai'];
const CORS_ALLOWED_ORIGINS = (process.env.CORS_ALLOWED_ORIGINS || 'http://localhost:3000,http://localhost:3001,http://localhost:3002')
  .split(',')
  .map((origin) => String(origin || '').trim())
  .filter(Boolean);
const API_RATE_LIMIT_WINDOW_MS = Math.max(5_000, Number(process.env.API_RATE_LIMIT_WINDOW_MS) || 60_000);
const API_RATE_LIMIT_MAX = Math.max(1, Number(process.env.API_RATE_LIMIT_MAX) || 100);

function sanitizeOrigin(origin) {
  return String(origin || '').trim();
}

function isOriginAllowed(origin) {
  if (!origin) return true;
  const normalizedOrigin = sanitizeOrigin(origin);
  return CORS_ALLOWED_ORIGINS.includes(normalizedOrigin);
}

function extractHostFromRequestOptions(options, maybeUrl) {
  if (typeof options === 'string') {
    try {
      return new URL(options).hostname || '';
    } catch {
      return '';
    }
  }

  if (options instanceof URL) {
    return options.hostname || '';
  }

  if (options && typeof options === 'object') {
    return String(options.hostname || options.host || '').toLowerCase();
  }

  if (typeof maybeUrl === 'string') {
    try {
      return new URL(maybeUrl).hostname || '';
    } catch {
      return '';
    }
  }

  return '';
}

function isOutboundBlocked(hostnameRaw) {
  const hostname = String(hostnameRaw || '').toLowerCase().trim();
  if (!hostname) return false;

  if (ALLOWED_OUTBOUND_HOSTS.has(hostname)) return false;

  if (BLOCKED_OUTBOUND_PATTERNS.some((pattern) => hostname.includes(pattern))) {
    return true;
  }

  return true;
}

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

console.log('✓ Politica local-only activa: solo localhost permitido para HTTP/HTTPS saliente');

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
    statement_timeout: Math.max(1000, Number(process.env.SQL_QUERY_TIMEOUT_MS) || 8000),
    query_timeout: Math.max(1000, Number(process.env.SQL_QUERY_TIMEOUT_MS) || 8000),
  });
}

let pool = createPgPool();

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
console.log('✓ Herramientas de Schema dinámico inicializadas');

async function ensureMultiDbReady(options = {}) {
  const force = Boolean(options?.force);
  const reason = String(options?.reason || 'runtime').trim();

  if (!force && multiDbRegistry.getDatabases().length > 0) {
    const configured = multiDbRegistry.getDatabases().map((database) => `${database.id}(${database.type})`);
    console.log(`✓ MultiDB configurado (${reason}): ${configured.length} base(s) -> ${configured.join(', ')}`);
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
      console.log(`✓ MultiDB ready (${reason})`);
      return result;
    })
    .catch((error) => {
      console.error(`✗ MultiDB bootstrap failed (${reason}):`, error?.message || error);
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

  const summary = [];

  for (const database of configured) {
    const id = String(database?.id || 'unknown').trim() || 'unknown';
    const type = String(database?.type || 'unknown').trim() || 'unknown';
    const label = `[DB:${id}|${type}]`;

    if (database?.enabled === false) {
      console.log(`${label} ⏭ SKIP (disabled)`);
      summary.push({ id, type, status: 'skipped-disabled' });
      continue;
    }

    const missing = ['host', 'port', 'database', 'user', 'password']
      .filter((key) => database?.[key] === undefined || database?.[key] === null || String(database?.[key]).trim() === '');

    if (missing.length > 0) {
      console.warn(`${label} ⏭ SKIP (faltan campos: ${missing.join(', ')})`);
      summary.push({ id, type, status: 'skipped-missing-fields', missing });
      continue;
    }

    try {
      const result = await probeUniversalConnection(database);
      console.log(`${label} ✅ OK (${database.host}:${database.port}/${database.database})`);
      summary.push({ id, type, status: 'ok' });

      if (result?.dbId && !result?.reused) {
        await closeUniversalConnection(result.dbId).catch(() => {});
      }
    } catch (error) {
      console.error(`${label} ❌ FAIL:`, error?.message || error);
      summary.push({ id, type, status: 'fail', error: String(error?.message || error) });
    }
  }

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

function resetDynamicSchemaCache() {
  DYNAMIC_SCRIPT_ALLOWED_TABLES = [];
  DYNAMIC_SCRIPT_ALLOWED_COLUMNS = {};
  DYNAMIC_SCRIPT_COLUMN_TYPES = {};
  DYNAMIC_SCRIPT_TABLE_PKS = {};
  DYNAMIC_SCHEMA_RELATIONSHIPS = [];
  DYNAMIC_SCHEMA_RELATION_KEYS = new Set();
  DYNAMIC_SCHEMA_CACHE.loadedAt = 0;
  DYNAMIC_SCHEMA_CACHE.fingerprint = '';
  DYNAMIC_SCHEMA_CACHE.refreshPromise = null;
  queryParameterizer.setSchemaContext({ tables: [] });
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

  try {
    await testDatabaseConnection();
    await ensureKnowledgeBaseSchema();
    await ensureCommentsSchema();
    await ensureBusinessLogsSchema();
    await ensureQueryHistorySchema();
    await ensureErrorLogsSchema();
    await ensureSemanticLearningSchema();
    await refreshSemanticLearningCache(true);
    await queryBuilder.getFullSchema();
    await ensureMultiDbReady({ force: true, reason: `reload:${reason}` });
    await probeConfiguredDatabasesConnectivity(`reload:${reason}`);
    // Re-discover schemas for any empty-schema DBs after reload
    await multiDbRegistry.introspectEmptySchemas().then((results) => {
      for (const r of results) {
        if (r.status === 'ok') console.log(`[DB:${r.id}] 🗂 Schema recargado: ${r.tableCount} tabla(s)`);
      }
    }).catch(() => {});
    console.log(`✓ Conexión activa recargada (${reason})`);
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
    console.error(`✗ No se pudo recargar la conexión activa (${reason}):`, error?.message || error);
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
    console.log('✓ Conexión a PostgreSQL exitosa:', result.rows[0]);
    return true;
  } catch (err) {
    console.error('✗ Error conectando a PostgreSQL:', err.message || err);
    return false;
  }
}

async function ensureKnowledgeBaseSchema() {
  try {
    await pool.query(`ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS descripcion TEXT`);
    await pool.query(`ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS contenido_md TEXT`);
    await pool.query(`ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS tipo_solucion VARCHAR(20) DEFAULT 'lectura'`);
    await pool.query(`ALTER TABLE knowledge_base DROP CONSTRAINT IF EXISTS knowledge_base_tipo_solucion_check`);
    await pool.query(`
      DO $$
      DECLARE c record;
      BEGIN
        FOR c IN
          SELECT con.conname
          FROM pg_constraint con
          JOIN pg_class rel ON rel.oid = con.conrelid
          JOIN pg_namespace nsp ON nsp.oid = con.connamespace
          WHERE rel.relname = 'knowledge_base'
            AND nsp.nspname = 'public'
            AND con.contype = 'c'
            AND pg_get_constraintdef(con.oid) ILIKE '%tipo_solucion%'
        LOOP
          EXECUTE format('ALTER TABLE knowledge_base DROP CONSTRAINT %I', c.conname);
        END LOOP;
      END $$;
    `);
    await pool.query(`
      ALTER TABLE knowledge_base
      ADD CONSTRAINT knowledge_base_tipo_solucion_check
      CHECK (tipo_solucion IN ('lectura', 'ejecutable', 'database', 'script'))
    `);
    await pool.query(`ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS pasos JSONB`);
    await pool.query(`ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS campos_formulario JSONB`);
    await pool.query(`ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS script TEXT`);
    await pool.query(`ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS script_json JSONB`);
    console.log('✓ Esquema de knowledge_base asegurado: columnas descripcion, contenido_md, tipo_solucion, pasos, campos_formulario, script y script_json presentes');
  } catch (err) {
    console.error('✗ Error asegurando esquema knowledge_base:', err.message || err);
  }
}

async function ensureCommentsSchema() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS comments (
        id SERIAL PRIMARY KEY,
        article_id VARCHAR(255) NOT NULL,
        parent_id INTEGER REFERENCES comments(id) ON DELETE CASCADE,
        author_username VARCHAR(255) NOT NULL,
        author_role VARCHAR(50) NOT NULL DEFAULT 'user',
        content TEXT NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_comments_article_id ON comments(article_id)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_comments_parent_id ON comments(parent_id)`);
    console.log('✓ Esquema comments asegurado: tabla comments lista para uso');
  } catch (err) {
    console.error('✗ Error asegurando esquema comments:', err.message || err);
  }
}

async function ensureBusinessLogsSchema() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS logs (
        id SERIAL PRIMARY KEY,
        accion VARCHAR(120) NOT NULL,
        usuario VARCHAR(255) NOT NULL,
        detalle TEXT,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_logs_usuario ON logs(usuario)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_logs_created_at ON logs(created_at)`);
    console.log('✓ Esquema logs asegurado: tabla logs lista para auditoría empresarial');
  } catch (err) {
    console.error('✗ Error asegurando esquema logs:', err.message || err);
  }
}

async function ensureQueryHistorySchema() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS query_history (
        id SERIAL PRIMARY KEY,
        username VARCHAR(255) NOT NULL,
        user_role VARCHAR(50) NOT NULL,
        query_text TEXT,
        generated_sql TEXT NOT NULL,
        query_params JSONB,
        placeholder_order JSONB,
        execution_ms INTEGER NOT NULL DEFAULT 0,
        row_count INTEGER NOT NULL DEFAULT 0,
        was_cached BOOLEAN NOT NULL DEFAULT FALSE,
        status VARCHAR(20) NOT NULL DEFAULT 'ok',
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
      )
    `);
    await pool.query('ALTER TABLE query_history ADD COLUMN IF NOT EXISTS query_params JSONB');
    await pool.query('ALTER TABLE query_history ADD COLUMN IF NOT EXISTS placeholder_order JSONB');
    await pool.query('CREATE INDEX IF NOT EXISTS idx_query_history_created_at ON query_history(created_at DESC)');
    await pool.query('CREATE INDEX IF NOT EXISTS idx_query_history_username ON query_history(username)');
    console.log('✓ Esquema query_history asegurado');
  } catch (err) {
    console.error('✗ Error asegurando esquema query_history:', err.message || err);
  }
}

async function ensureErrorLogsSchema() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS error_logs (
        id SERIAL PRIMARY KEY,
        mensaje TEXT NOT NULL,
        stack TEXT,
        modulo VARCHAR(255),
        capa VARCHAR(120),
        contexto VARCHAR(255),
        archivo VARCHAR(255),
        linea INTEGER,
        metadata JSONB,
        fecha TIMESTAMP WITH TIME ZONE DEFAULT NOW()
      )
    `);
    await pool.query('CREATE INDEX IF NOT EXISTS idx_error_logs_fecha ON error_logs(fecha DESC)');
    await pool.query('CREATE INDEX IF NOT EXISTS idx_error_logs_modulo ON error_logs(modulo)');
    console.log('✓ Esquema error_logs asegurado');
  } catch (err) {
    console.error('✗ Error asegurando esquema error_logs:', err.message || err);
  }
}

function normalizeSemanticLearningTerm(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9_]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

function dedupeSemanticValues(values = []) {
  return [...new Set((values || []).map((item) => String(item || '').trim()).filter(Boolean))];
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

async function ensureSemanticLearningSchema() {
  try {
    const connectionFingerprint = schemaDetector.getConnectionFingerprint();
    await pool.query(`
      CREATE TABLE IF NOT EXISTS semantic_learning (
        id SERIAL PRIMARY KEY,
        db_fingerprint TEXT NOT NULL DEFAULT '',
        normalized_term VARCHAR(120) NOT NULL,
        original_term VARCHAR(120) NOT NULL,
        target_kind VARCHAR(20) NOT NULL,
        target_name VARCHAR(120) NOT NULL,
        table_name VARCHAR(120) NOT NULL DEFAULT '',
        confidence NUMERIC(4,3) NOT NULL DEFAULT 0.650,
        hit_count INTEGER NOT NULL DEFAULT 1,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        last_seen TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        CONSTRAINT semantic_learning_target_kind_check CHECK (target_kind IN ('table', 'column'))
      )
    `);
    await pool.query('ALTER TABLE semantic_learning ADD COLUMN IF NOT EXISTS db_fingerprint TEXT NOT NULL DEFAULT \'\'');
    await pool.query('UPDATE semantic_learning SET db_fingerprint = $1 WHERE COALESCE(db_fingerprint, \'\') = \'\'', [connectionFingerprint]);
    await pool.query('ALTER TABLE semantic_learning DROP CONSTRAINT IF EXISTS semantic_learning_unique');
    await pool.query('CREATE INDEX IF NOT EXISTS idx_semantic_learning_term ON semantic_learning(normalized_term)');
    await pool.query('CREATE INDEX IF NOT EXISTS idx_semantic_learning_last_seen ON semantic_learning(last_seen DESC)');
    await pool.query('CREATE INDEX IF NOT EXISTS idx_semantic_learning_fingerprint ON semantic_learning(db_fingerprint)');
    await pool.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_semantic_learning_unique_scope
      ON semantic_learning (db_fingerprint, normalized_term, target_kind, target_name, table_name)
    `);
    console.log('✓ Esquema semantic_learning asegurado');
  } catch (err) {
    console.error('✗ Error asegurando esquema semantic_learning:', err.message || err);
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

const ROLE_LEVELS = {
  user: 1,
  admin: 2,
  superadmin: 3,
};

const SQL_BLOCKED_KEYWORDS = [
  'drop',
  'truncate',
  'alter',
  'create',
  'grant',
  'revoke',
  'comment',
];

const INTELLIGENT_QUERY_CACHE = new Map();
const QUERY_CACHE_TTL_MS = 60 * 1000;
let QUERY_CACHE_DATA_VERSION = 1;

function normalizeRole(roleValue) {
  const role = String(roleValue || '').trim().toLowerCase();
  return ROLE_LEVELS[role] ? role : 'user';
}

function hasRequiredRole(userRole, requiredRole) {
  const current = ROLE_LEVELS[normalizeRole(userRole)] || 0;
  const required = ROLE_LEVELS[normalizeRole(requiredRole)] || 0;
  return current >= required;
}

function getRoleFromRequest(req, user) {
  return normalizeRole(req.headers['x-user-role'] || user?.role || 'user');
}

function slugifyProcessName(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

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

function containsBlockedSql(sqlText) {
  const stripped = String(sqlText || '').trimEnd().replace(/;\s*$/, '');
  const normalized = stripLeadingSqlComments(stripped.trim()).toLowerCase();
  if (!normalized.startsWith('select') && !normalized.startsWith('with')) return true;
  if (normalized.includes(';')) return true;
  return SQL_BLOCKED_KEYWORDS.some((word) => normalized.includes(`${word} `));
}

function extractSqlReferencedTables(sqlText) {
  const sourceSql = String(sqlText || '');
  const refs = Array.from(sourceSql.matchAll(
    /\b(?:from|join)\s+((?:"[^"]+"|[a-zA-Z0-9_]+)(?:\s*\.\s*(?:"[^"]+"|[a-zA-Z0-9_]+))?)/gi,
  ));

  const resolvedRefs = new Set();

  refs.forEach((match) => {
    const qualified = String(match?.[1] || '').trim();
    if (!qualified) return;

    const parts = qualified
      .split('.')
      .map((part) => part.trim())
      .filter(Boolean)
      .map((part) => part.replace(/^"|"$/g, ''));

    if (parts.length === 0) return;

    // Preserve schema-qualified names for multi-database allow-lists
    // (e.g. system.comments), and include unqualified fallback (comments).
    const qualifiedName = normalizeSchemaIdentifier(parts.join('.'));
    const tableName = normalizeSchemaIdentifier(parts[parts.length - 1]);

    if (qualifiedName) resolvedRefs.add(qualifiedName);
    if (tableName) resolvedRefs.add(tableName);
  });

  return [...resolvedRefs];
}

function getDatabaseSchemaTableNames(database) {
  return (database?.schema?.tables || [])
    .map((table) => normalizeSchemaIdentifier(table?.name || table))
    .filter(Boolean);
}

function extractDatabaseHintsFromText(text) {
  const input = String(text || '').trim().toLowerCase();
  if (!input) return [];

  const configured = multiDbRegistry.getDatabases();
  if (configured.length === 0) return [];

  const ids = new Set();
  const mentionsOracle = /\boracle\b/.test(input);
  const mentionsPostgres = /\bpostgres(?:ql)?\b|\bpg\b/.test(input);
  const mentionsMySql = /\bmysql\b/.test(input);

  for (const db of configured) {
    const dbId = String(db.id || '').trim();
    const dbType = String(db.type || '').trim().toLowerCase();
    if (!dbId) continue;

    if (input.includes(dbId.toLowerCase())) {
      ids.add(dbId);
      continue;
    }

    if (mentionsOracle && dbType === 'oracle') ids.add(dbId);
    if (mentionsPostgres && dbType === 'postgres') ids.add(dbId);
    if (mentionsMySql && dbType === 'mysql') ids.add(dbId);
  }

  return [...ids];
}

function resolveRequestedDatabasesForQuery(text, requestedDatabases = []) {
  const explicit = Array.isArray(requestedDatabases)
    ? requestedDatabases.map((item) => String(item || '').trim()).filter(Boolean)
    : [];

  if (explicit.length > 0) {
    return [...new Set(explicit)];
  }

  const inferred = extractDatabaseHintsFromText(text);
  return [...new Set(inferred)];
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
    : DYNAMIC_SCRIPT_ALLOWED_TABLES.map((table) => normalizeSchemaIdentifier(table)).filter(Boolean));

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
    if (Array.isArray(queryParams) && queryParams.length > 0) {
      throw new Error(`La base ${targetDatabase.id} (${targetDatabase.type}) no soporta placeholders posicionales en SQL manual. Usa SQL literal o selecciona la base principal.`);
    }

    const rows = await multiDbRegistry.executeCompiledQuery({
      databaseId: targetDatabase.id,
      sql: queryText,
      params: [],
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
  const explicitDatabaseId = String(options?.databaseId || '').trim()
    || String(structuredScript?.databaseId || '').trim();
  if (explicitDatabaseId) {
    return explicitDatabaseId;
  }

  // Syntax-based detection on the SQL (most reliable)
  const sqlText = String(structuredScript?.sql || '').trim();
  if (sqlText) {
    const directive = extractDatabaseDirective(sqlText);
    if (directive) return directive;
    const engine = detectSqlSyntaxEngine(sqlText);
    if (engine) {
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

function normalizeGeneratedSelectSql(sqlText) {
  const input = String(sqlText || '');
  if (!input) return input;

  // Repair common generator drift: ILIKE over non-text columns without explicit cast.
  // Example: "t1"."activo" ILIKE '%x%' -> "t1"."activo"::text ILIKE '%x%'
  return input.replace(/((?:"[^"]+"|[a-zA-Z_][\w]*)\.(?:"[^"]+"|[a-zA-Z_][\w]*))(?!::text)\s+ILIKE/gi, '$1::text ILIKE');
}

async function logQueryHistory({
  username,
  role,
  queryText,
  generatedSql,
  queryParams,
  placeholderOrder,
  executionMs,
  rowCount,
  wasCached,
  status,
}) {
  try {
    await pool.query(
      `INSERT INTO query_history
        (username, user_role, query_text, generated_sql, query_params, placeholder_order, execution_ms, row_count, was_cached, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
      [
        String(username || 'anonymous'),
        normalizeRole(role),
        queryText || null,
        String(generatedSql || ''),
        queryParams === undefined ? null : JSON.stringify(queryParams),
        placeholderOrder === undefined ? null : JSON.stringify(placeholderOrder),
        Math.max(0, Number(executionMs) || 0),
        Math.max(0, Number(rowCount) || 0),
        Boolean(wasCached),
        String(status || 'ok'),
      ]
    );
  } catch (error) {
    console.error('[QUERY_HISTORY] ❌ Error registrando historial:', error.message || error);
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

  for (const tableName of DYNAMIC_SCRIPT_ALLOWED_TABLES) {
    const tokens = aliases[tableName] || [tableName];
    if (tokens.some((token) => normalized.includes(token))) {
      return tableName;
    }
  }

  return DYNAMIC_SCRIPT_ALLOWED_TABLES.find((t) => tableLooksLikeUsers(t)) || DYNAMIC_SCRIPT_ALLOWED_TABLES[0] || '';
}

function buildNaturalLanguageSuggestions(rawText = '') {
  const input = String(rawText || '').trim().toLowerCase();
  const suggestions = [];

  for (const tableName of DYNAMIC_SCRIPT_ALLOWED_TABLES) {
    if (!input || tableName.includes(input) || input.includes(tableName)) {
      const relations = DYNAMIC_SCHEMA_RELATIONSHIPS.filter((r) => r.from.tabla === tableName || r.to.tabla === tableName);
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

const STRUCTURED_PLACEHOLDER_REGEX = /{{\s*([a-zA-Z0-9_.]+)\s*}}/g;
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

const DYNAMIC_SCRIPT_ALLOWED_TYPES = ['update', 'insert', 'delete', 'select'];
const WORKFLOW_ALLOWED_STEP_TYPES = ['select', 'update', 'insert', 'delete', 'validacion'];
let DYNAMIC_SCRIPT_ALLOWED_TABLES = [];
// Safe whitelist of WHERE operators — prevents any form of SQL injection via operator field
const ALLOWED_WHERE_OPERATORS = new Set(['=', '!=', '>', '<', '>=', '<=', 'LIKE']);
let DYNAMIC_SCRIPT_ALLOWED_COLUMNS = {};
let DYNAMIC_SCRIPT_COLUMN_TYPES = {};
let DYNAMIC_SCRIPT_TABLE_PKS = {};
let DYNAMIC_SCHEMA_RELATIONSHIPS = [];
let DYNAMIC_SCHEMA_RELATION_KEYS = new Set();

const DYNAMIC_SCHEMA_CACHE = {
  loadedAt: 0,
  fingerprint: '',
  ttlMs: 60 * 1000,
  refreshPromise: null,
};

function normalizeSchemaIdentifier(value) {
  return String(value || '').trim().toLowerCase();
}

function inferRuntimeTypeFromPgType(dataType, udtName) {
  const normalized = normalizeSchemaIdentifier(dataType || udtName || '');

  if (['boolean', 'bool'].includes(normalized)) return 'boolean';
  if (
    normalized.includes('int')
    || normalized.includes('numeric')
    || normalized.includes('decimal')
    || normalized.includes('double')
    || normalized.includes('real')
  ) {
    return 'number';
  }

  if (normalized.includes('json')) return 'object';
  return 'string';
}

function buildRelationKey(fromTable, fromColumn, toTable, toColumn) {
  return `${normalizeSchemaIdentifier(fromTable)}.${normalizeSchemaIdentifier(fromColumn)}=>${normalizeSchemaIdentifier(toTable)}.${normalizeSchemaIdentifier(toColumn)}`;
}

function hasRelationBetweenFields(left, right) {
  const direct = buildRelationKey(left.table, left.column, right.table, right.column);
  const reverse = buildRelationKey(right.table, right.column, left.table, left.column);
  return DYNAMIC_SCHEMA_RELATION_KEYS.has(direct) || DYNAMIC_SCHEMA_RELATION_KEYS.has(reverse);
}

function getDynamicSchemaSnapshot() {
  return {
    tablas: Object.fromEntries(
      Object.entries(DYNAMIC_SCRIPT_ALLOWED_COLUMNS).map(([tableName, columns]) => [tableName, [...columns]])
    ),
    relaciones: [...DYNAMIC_SCHEMA_RELATIONSHIPS],
    fingerprint: DYNAMIC_SCHEMA_CACHE.fingerprint,
    loadedAt: DYNAMIC_SCHEMA_CACHE.loadedAt,
  };
}

function getDynamicSchemaFull() {
  const result = {};
  for (const tableName of DYNAMIC_SCRIPT_ALLOWED_TABLES) {
    const columns = DYNAMIC_SCRIPT_ALLOWED_COLUMNS[tableName] || [];
    const tipos = DYNAMIC_SCRIPT_COLUMN_TYPES[tableName] || {};
    const pk = DYNAMIC_SCRIPT_TABLE_PKS[tableName] || null;
    const fk = DYNAMIC_SCHEMA_RELATIONSHIPS
      .filter((r) => r.from.tabla === tableName)
      .map((r) => ({ columna: r.from.columna, referencia: `${r.to.tabla}.${r.to.columna}` }));
    result[tableName] = { columnas: columns, tipos, pk, fk };
  }
  return result;
}

async function refreshDynamicSchemaCache(force = false) {
  const currentFingerprint = schemaDetector.getConnectionFingerprint();
  const cacheIsFresh =
    !force
    && DYNAMIC_SCHEMA_CACHE.fingerprint === currentFingerprint
    && DYNAMIC_SCRIPT_ALLOWED_TABLES.length > 0
    && (Date.now() - DYNAMIC_SCHEMA_CACHE.loadedAt) < DYNAMIC_SCHEMA_CACHE.ttlMs;

  if (cacheIsFresh) {
    return getDynamicSchemaSnapshot();
  }

  if (DYNAMIC_SCHEMA_CACHE.refreshPromise) {
    await DYNAMIC_SCHEMA_CACHE.refreshPromise;
    return getDynamicSchemaSnapshot();
  }

  DYNAMIC_SCHEMA_CACHE.refreshPromise = (async () => {
    const tablesResult = await pool.query(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE'
      ORDER BY table_name
    `);

    const columnsResult = await pool.query(`
      SELECT table_name, column_name, data_type, udt_name
      FROM information_schema.columns
      WHERE table_schema = 'public'
      ORDER BY table_name, ordinal_position
    `);

    const fkResult = await pool.query(`
      SELECT
        tc.table_name AS tabla_origen,
        kcu.column_name AS columna_origen,
        ccu.table_name AS tabla_destino,
        ccu.column_name AS columna_destino
      FROM information_schema.table_constraints AS tc
      JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
      JOIN information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
        AND ccu.table_schema = tc.table_schema
      WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = 'public'
      ORDER BY tc.table_name, kcu.column_name
    `);

    const pkResult = await pool.query(`
      SELECT tc.table_name, kcu.column_name
      FROM information_schema.table_constraints AS tc
      JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
      WHERE tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_schema = 'public'
      ORDER BY tc.table_name
    `);

    const nextTables = tablesResult.rows
      .map((row) => normalizeSchemaIdentifier(row.table_name))
      .filter(Boolean);

    const nextColumnsByTable = {};
    const nextTypesByTable = {};

    for (const row of columnsResult.rows) {
      const tableName = normalizeSchemaIdentifier(row.table_name);
      const columnName = normalizeSchemaIdentifier(row.column_name);
      if (!tableName || !columnName) continue;

      if (!nextColumnsByTable[tableName]) nextColumnsByTable[tableName] = [];
      if (!nextTypesByTable[tableName]) nextTypesByTable[tableName] = {};

      nextColumnsByTable[tableName].push(columnName);
      nextTypesByTable[tableName][columnName] = inferRuntimeTypeFromPgType(row.data_type, row.udt_name);
    }

    const nextRelationships = fkResult.rows.map((row) => {
      const fromTable = normalizeSchemaIdentifier(row.tabla_origen);
      const fromColumn = normalizeSchemaIdentifier(row.columna_origen);
      const toTable = normalizeSchemaIdentifier(row.tabla_destino);
      const toColumn = normalizeSchemaIdentifier(row.columna_destino);

      return {
        from: { tabla: fromTable, columna: fromColumn },
        to: { tabla: toTable, columna: toColumn },
      };
    }).filter((item) => item.from.tabla && item.from.columna && item.to.tabla && item.to.columna);

    const relationKeys = new Set(
      nextRelationships.map((relation) => buildRelationKey(
        relation.from.tabla,
        relation.from.columna,
        relation.to.tabla,
        relation.to.columna
      ))
    );

    DYNAMIC_SCRIPT_ALLOWED_TABLES = nextTables;
    DYNAMIC_SCRIPT_ALLOWED_COLUMNS = nextColumnsByTable;
    DYNAMIC_SCRIPT_COLUMN_TYPES = nextTypesByTable;
    DYNAMIC_SCHEMA_RELATIONSHIPS = nextRelationships;
    DYNAMIC_SCHEMA_RELATION_KEYS = relationKeys;
    DYNAMIC_SCHEMA_CACHE.fingerprint = currentFingerprint;
    DYNAMIC_SCHEMA_CACHE.loadedAt = Date.now();
    queryParameterizer.setSchemaContext({ tables: nextTables });

      const nextPKs = {};
      for (const row of pkResult.rows) {
        const tableName = normalizeSchemaIdentifier(row.table_name);
        if (tableName) nextPKs[tableName] = normalizeSchemaIdentifier(row.column_name);
      }
      DYNAMIC_SCRIPT_TABLE_PKS = nextPKs;
  })();

  try {
    await DYNAMIC_SCHEMA_CACHE.refreshPromise;
  } finally {
    DYNAMIC_SCHEMA_CACHE.refreshPromise = null;
  }

  return getDynamicSchemaSnapshot();
}

async function ensureDynamicSchemaReady(force = false) {
  const snapshot = await refreshDynamicSchemaCache(force);
  if (!snapshot || !snapshot.tablas || Object.keys(snapshot.tablas).length === 0) {
    throw new Error('No se pudo detectar el schema dinámico de la base de datos');
  }
  return snapshot;
}

async function ejecutarAccionEmpresarial(nombre, params, context, emit) {
  const actionName = String(nombre || '').trim();
  if (!actionName || !BUSINESS_ACTIONS_REGISTRY[actionName]) {
    throw new Error(`Acción no permitida: ${actionName || 'desconocida'}`);
  }

  return BUSINESS_ACTIONS_REGISTRY[actionName]({ params, context, emit });
}

function normalizeBusinessStep(step, index) {
  if (!step || typeof step !== 'object') {
    throw new Error(`Paso ${index + 1} inválido`);
  }

  const ordenRaw = Number(step.orden);
  const orden = Number.isFinite(ordenRaw) && ordenRaw > 0 ? Math.floor(ordenRaw) : index + 1;
  const descripcion = String(step.descripcion || '').trim();
  const accion = String(step.accion || '').trim();

  if (!descripcion) {
    throw new Error(`Paso ${orden} sin descripción`);
  }

  if (!accion) {
    throw new Error(`Paso ${orden} sin acción`);
  }

  return { orden, descripcion, accion };
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

function validateWorkflowBuilderDefinition(script) {
  if (!script || typeof script !== 'object' || Array.isArray(script)) {
    throw new Error('script_json de workflow inválido');
  }

  const workflow = Array.isArray(script.workflow) ? script.workflow : [];
  if (workflow.length === 0) {
    throw new Error('El workflow requiere al menos un paso');
  }

  workflow.forEach((rawStep, index) => {
    if (!isPlainObject(rawStep)) {
      throw new Error(`Paso ${index + 1} inválido`);
    }

    const stepType = normalizeDynamicScriptType(rawStep.tipo);
    if (!WORKFLOW_ALLOWED_STEP_TYPES.includes(stepType)) {
      throw new Error(`Operación no soportada: ${stepType || 'desconocida'}`);
    }

    if (stepType === 'validacion') {
      const variable = String(rawStep.variable || '').trim();
      if (!variable) {
        throw new Error(`Paso ${index + 1}: la validación requiere variable`);
      }

      const condition = String(rawStep.condicion || 'existe').trim().toLowerCase();
      if (!['existe', 'no_existe', 'igual'].includes(condition)) {
        throw new Error(`Paso ${index + 1}: condición de validación no soportada`);
      }

      if (condition === 'igual' && !Object.prototype.hasOwnProperty.call(rawStep, 'valor')) {
        throw new Error(`Paso ${index + 1}: la condición igual requiere valor`);
      }
      return;
    }

    assertDynamicScriptAllowed(rawStep);
  });
}

function validateBusinessScriptDefinition(script) {
  if (!script || typeof script !== 'object' || Array.isArray(script)) {
    throw new Error('script_json empresarial inválido');
  }

  if (
    String(script.modo || '').trim().toLowerCase() === 'script'
    && String(script.origen || '').trim().toLowerCase() === 'manual-sql'
  ) {
    const sqlText = String(script.sql || '').trim();
    if (!sqlText) throw new Error('SQL manual vacío');

    // Complex PL/SQL scripts: validate via interpreter
    if (isComplexSqlScript(sqlText)) {
      const parsed = parsePlSqlScript(sqlText);
      if (parsed.steps.length === 0) {
        throw new Error('El script procedural no generó pasos ejecutables');
      }
      return;
    }

    // Standard multi-step SQL
    const sqlSteps = splitManualSqlSteps(sqlText);
    if (sqlSteps.length === 0) throw new Error('SQL manual vacío');
    sqlSteps.forEach((stepSql) => {
      assertManualSqlAllowed(stepSql, { allowMutations: true, writeConfirmed: true });
    });
    return;
  }

  if (Array.isArray(script.workflow)) {
    validateWorkflowBuilderDefinition(script);
    return;
  }

  if (Array.isArray(script.join)) {
    validateRelationalScriptDefinition(script);
    return;
  }

  const dynamicType = normalizeDynamicScriptType(script.tipo);
  if (dynamicType) {
    assertDynamicScriptAllowed(script);
    return;
  }

  if (String(script.modo || '').trim().toLowerCase() !== 'script') {
    throw new Error('El script empresarial debe incluir modo="script"');
  }

  const scriptName = String(script.script || '').trim();
  if (!scriptName) {
    throw new Error('El script empresarial requiere nombre de proceso');
  }

  const steps = Array.isArray(script.pasos) ? script.pasos : [];
  if (steps.length === 0) {
    throw new Error('El script empresarial requiere al menos un paso');
  }

  const normalizedSteps = steps.map((step, index) => normalizeBusinessStep(step, index));
  const actionNames = normalizedSteps.map((step) => step.accion);

  for (const actionName of actionNames) {
    const metadata = getBusinessActionMetadata(actionName);
    if (!metadata) {
      throw new Error(`Acción no permitida: ${actionName}`);
    }
  }

  const requiredFromActions = getRequiredParamsForActions(actionNames);
  const requiredFromScript = Array.isArray(script.parametros_requeridos)
    ? script.parametros_requeridos.map((item) => String(item || '').trim()).filter(Boolean)
    : [];

  for (const paramName of requiredFromActions) {
    if (!requiredFromScript.includes(paramName)) {
      throw new Error(`El script empresarial debe declarar el parámetro requerido: ${paramName}`);
    }
  }
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

function sanitizeStructuredParamValue(value) {
  if (Array.isArray(value)) {
    return value.map((item) => sanitizeStructuredParamValue(item));
  }

  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value).map(([key, nestedValue]) => [key, sanitizeStructuredParamValue(nestedValue)])
    );
  }

  if (typeof value === 'boolean' || typeof value === 'number') {
    return value;
  }

  return String(value ?? '')
    .substring(0, 255)
    .replace(/[\x00-\x1F\x7F]/g, '')
    .replace(/[\u200B-\u200D\uFEFF]/g, '')
    .trim();
}

function inferStructuredParamSemantic(name) {
  const normalized = String(name || '').trim().toLowerCase();
  if (!normalized) return 'text';
  if (normalized.includes('date') || normalized.includes('fecha')) return 'date';
  if (normalized.includes('role') || normalized.includes('rol')) return 'role';
  if (normalized.includes('codigo') || normalized.includes('code')) return 'code';
  if (normalized.includes('log')) return 'log';
  if (normalized.includes('id')) return 'id';
  if (normalized.includes('name') || normalized.includes('user') || normalized.includes('usuario')) return 'user_text';
  return 'text';
}

function validateStructuredParamByName(name, value) {
  if (isMissingParamValue(value)) {
    return;
  }

  const semantic = inferStructuredParamSemantic(name);
  const stringValue = typeof value === 'string' ? value : String(value ?? '');
  const trimmed = stringValue.trim();

  if (semantic === 'id' && /\s/.test(trimmed)) {
    throw new Error(`Parámetro inválido (${name}): no puede contener espacios`);
  }

  if (semantic === 'role') {
    if (!/^[\w\s-]{2,40}$/i.test(trimmed)) {
      throw new Error(`Parámetro inválido (${name}): formato de rol inválido`);
    }
  }

  if (semantic === 'code') {
    if (!/^[\w-]{1,60}$/i.test(trimmed)) {
      throw new Error(`Parámetro inválido (${name}): formato de código inválido`);
    }
  }

  if (semantic === 'date') {
    const parsed = new Date(trimmed);
    if (Number.isNaN(parsed.getTime())) {
      throw new Error(`Parámetro inválido (${name}): fecha inválida`);
    }
  }
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

const MANUAL_SQL_BLOCKED_KEYWORDS = /\b(insert|update|delete|drop|alter|truncate|create|grant|revoke)\b/i;
const MANUAL_SQL_PLACEHOLDER_REGEX = /{{\s*([a-zA-Z0-9_.]+)\s*}}/g;
const MANUAL_SQL_NAMED_PLACEHOLDER_REGEX = /(^|[^:]):([a-zA-Z_][a-zA-Z0-9_]*)/g;
const MANUAL_SQL_CANONICAL_PLACEHOLDER_REGEX = /{{\s*([a-zA-Z0-9_.]+)\s*}}|:([a-zA-Z_][a-zA-Z0-9_]*)/g;
const MANUAL_SQL_POSITIONAL_PLACEHOLDER_REGEX = /\$(\d+)/g;

function isWriteConfirmed(rawParams = {}) {
  const flag = rawParams?.__write_confirmed;
  return flag === true || String(flag || '').trim().toLowerCase() === 'true';
}

function hasUnsafeMutationWithoutWhere(sqlText = '') {
  const cleaned = String(sqlText || '').trim().replace(/;\s*$/, '').trim();
  if (/^(update|delete)\b/i.test(cleaned) && !/\bwhere\b/i.test(cleaned)) {
    return true;
  }
  return false;
}

function detectSqlOperation(sqlText = '') {
  const cleaned = stripLeadingSqlComments(String(sqlText || '').trim().replace(/;\s*$/, '').trim()).toLowerCase();
  if (cleaned.startsWith('select')) return 'select';
  if (cleaned.startsWith('update')) return 'update';
  if (cleaned.startsWith('delete')) return 'delete';
  if (cleaned.startsWith('insert')) return 'insert';
  return 'unknown';
}

function stripLeadingSqlComments(sqlText = '') {
  return String(sqlText || '')
    .replace(/^(?:\s*(?:--[^\n]*(?:\r?\n|$)|\/\*[\s\S]*?\*\/))+/, '')
    .trim();
}

function collectManualSqlPlaceholderMetadata(sqlText = '') {
  const source = String(sqlText || '');
  const placeholderOrder = [];
  const userPlaceholderOrder = [];
  const contextPlaceholderOrder = [];
  const placeholderOccurrences = [];
  const seen = new Set();
  const seenUser = new Set();
  const seenContext = new Set();

  const isContextPlaceholder = (name) => /^(step\d+\.|loopRow\.)/i.test(String(name || '').trim());

  for (const match of source.matchAll(MANUAL_SQL_CANONICAL_PLACEHOLDER_REGEX)) {
    const [rawMatch = '', curlyVariable, namedVariable] = match;
    const offset = Number(match.index || 0);
    if (rawMatch.startsWith(':')) {
      const previousChar = offset > 0 ? source[offset - 1] : '';
      if (previousChar === ':') {
        continue;
      }
    }

    const variableName = String(curlyVariable || namedVariable || '').trim();
    if (!variableName) continue;
    placeholderOccurrences.push(variableName);

    if (isContextPlaceholder(variableName)) {
      if (!seenContext.has(variableName)) {
        seenContext.add(variableName);
        contextPlaceholderOrder.push(variableName);
      }
    } else if (!seenUser.has(variableName)) {
      seenUser.add(variableName);
      userPlaceholderOrder.push(variableName);
    }

    if (!seen.has(variableName)) {
      seen.add(variableName);
      placeholderOrder.push(variableName);
    }
  }

  return {
    placeholderOrder,
    userPlaceholderOrder,
    contextPlaceholderOrder,
    placeholderOccurrences,
  };
}

function resolveWorkflowContextPlaceholder(path, workflowContext = {}, safeParams = {}) {
  const pathStr = String(path || '').trim();

  // loopRow.field → direct lookup in workflowContext.loopRow
  const loopRowMatch = pathStr.match(/^loopRow\.([a-zA-Z_][\w]*)$/i);
  if (loopRowMatch) {
    const loopRow = workflowContext?.loopRow;
    if (loopRow && typeof loopRow === 'object') {
      const val = loopRow[loopRowMatch[1]];
      if (!isMissingParamValue(val)) return val;
    }
    return undefined;
  }

  const resolved = resolveValueMultiSource(pathStr, workflowContext, safeParams);
  if (!isMissingParamValue(resolved)) {
    return resolved;
  }

  // If stepN stores an array, allow using first row field via stepN.field.
  const pathMatch = pathStr.match(/^(step\d+)\.([a-zA-Z_][\w]*)$/i);
  if (!pathMatch) {
    return undefined;
  }

  const [, stepKey, fieldName] = pathMatch;
  const stepValue = workflowContext?.[stepKey];
  if (Array.isArray(stepValue) && stepValue.length > 0) {
    const first = stepValue[0];
    if (first && typeof first === 'object') {
      return first[fieldName];
    }
  }

  return undefined;
}

function normalizeManualSqlParamsInput(rawParams) {
  if (Array.isArray(rawParams)) {
    return rawParams.map((value, index) => {
      const normalized = sanitizeStructuredParamValue(value);
      if (isMissingParamValue(normalized)) {
        throw new Error(`Parámetro posicional inválido en posición ${index + 1}`);
      }
      return normalized;
    });
  }

  if (rawParams && typeof rawParams === 'object') {
    return Object.entries(rawParams).reduce((acc, [key, value]) => {
      if (key === '__write_confirmed') {
        acc[key] = value;
        return acc;
      }

      const normalized = sanitizeStructuredParamValue(value);
      if (!isMissingParamValue(normalized)) {
        acc[key] = normalized;
      }
      return acc;
    }, {});
  }

  return {};
}

function splitManualSqlSteps(sqlText = '') {
  return String(sqlText || '')
    .split(';')
    .map((item) => item.trim())
    .filter(Boolean);
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

function buildHumanSqlExplanation(rows = []) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return 'La consulta se ejecutó correctamente pero no devolvió registros.';
  }

  const first = rows[0] || {};

  const normalizeLabel = (key) => String(key || '')
    .replace(/^[_\W]+|[_\W]+$/g, '')
    .replace(/_/g, ' ')
    .toLowerCase();

  const inferFieldRole = (key) => {
    const k = String(key || '').toLowerCase();
    if (/(_id$|^id$|uuid|guid)/.test(k)) return 'identifier';
    if (/(count|total|cantidad|qty|numero|num|sum|avg|max|min|monto|importe|valor|amount|price|precio|saldo)/.test(k)) return 'metric';
    if (/(status|estado|clasificacion|class|tipo|type|rol|role)/.test(k)) return 'status';
    if (/(name|username|nombre|titulo|title)/.test(k)) return 'entity';
    if (/(date|fecha|time|hora|timestamp|created|updated)/.test(k)) return 'temporal';
    if (typeof first[k] === 'number') return 'metric';
    return 'text';
  };

  const formatMetricSentence = (key, value) => {
    const k = String(key || '').toLowerCase();
    const n = Number(value);
    const hasNumber = Number.isFinite(n);
    if (!hasNumber) return `El valor de ${normalizeLabel(key)} es ${String(value)}.`;

    if (/(session|sesion)/.test(k)) {
      if (n <= 0) return 'No tiene sesiones registradas.';
      if (n === 1) return 'Tiene 1 sesión registrada.';
      return `Tiene ${n} sesiones registradas.`;
    }
    if (/(user|usuario)/.test(k)) {
      if (n <= 0) return 'No existen usuarios en el resultado.';
      if (n === 1) return 'Existe 1 usuario en el sistema.';
      return `Existen ${n} usuarios en el sistema.`;
    }
    if (/(log|actividad|evento|evento)/.test(k)) {
      if (n <= 0) return 'No hay actividad registrada.';
      if (n === 1) return 'Hay 1 actividad registrada.';
      return `Hay ${n} actividades registradas.`;
    }
    if (/(monto|importe|amount|price|precio|saldo|valor)/.test(k)) {
      return `El monto total es ${n}.`;
    }

    return `El total de ${normalizeLabel(key)} es ${n}.`;
  };

  const usableEntries = Object.entries(first)
    .filter(([key, value]) => value !== null && value !== undefined && String(value).trim?.() !== '')
    .map(([key, value]) => ({ key, value, role: inferFieldRole(key) }))
    .filter((item) => item.role !== 'identifier' && !/^(__|step\d+\.|v_|p_|l_)/i.test(item.key));

  if (usableEntries.length === 0) {
    return `La consulta se ejecutó correctamente y devolvió ${rows.length} ${rows.length === 1 ? 'registro' : 'registros'}.`;
  }

  const entityEntry = usableEntries.find((entry) => entry.role === 'entity' && typeof entry.value === 'string');
  const statusEntry = usableEntries.find((entry) => entry.role === 'status');
  const metricEntries = usableEntries.filter((entry) => entry.role === 'metric').slice(0, 2);
  const temporalEntry = usableEntries.find((entry) => entry.role === 'temporal');

  const parts = [];

  if (entityEntry) {
    parts.push(`Entidad principal: ${String(entityEntry.value)}.`);
  }

  if (statusEntry) {
    parts.push(`${normalizeLabel(statusEntry.key)}: ${String(statusEntry.value)}.`);
  }

  for (const metric of metricEntries) {
    parts.push(formatMetricSentence(metric.key, metric.value));
  }

  if (temporalEntry) {
    const parsed = new Date(String(temporalEntry.value));
    const formatted = Number.isNaN(parsed.getTime())
      ? String(temporalEntry.value)
      : parsed.toLocaleString('es-ES');
    parts.push(`${normalizeLabel(temporalEntry.key)}: ${formatted}.`);
  }

  if (parts.length === 0) {
    return `La consulta se ejecutó correctamente y devolvió ${rows.length} ${rows.length === 1 ? 'registro' : 'registros'}.`;
  }

  return parts.join(' ').replace(/\s+/g, ' ').trim();
}

function sanitizeProductionResultObject(input = {}) {
  const source = input && typeof input === 'object' && !Array.isArray(input) ? input : {};
  const blockedKey = /^(workflow|steps?|debug|script|sql|query|executed|placeholder|context|internal|step\d+|__|v_|p_|l_)/i;
  const blockedStepSuffix = /_step\d+$/i;
  const sensitiveKey = /(password|passwd|pwd|hash|token|secret|apikey|api_key|salt|refresh_token|access_token)/i;
  const compact = {};
  const canonicalMap = new Map();

  const normalizeCanonicalKey = (key = '') => String(key || '').trim().replace(/_step\d+$/i, '').toLowerCase();
  const isPrimitive = (value) => ['string', 'number', 'boolean'].includes(typeof value);
  const isUsefulArray = (value) => Array.isArray(value)
    && value.length > 0
    && value.length <= 5
    && value.every((item) => isPrimitive(item) && String(item).length <= 80);

  for (const [rawKey, rawValue] of Object.entries(source)) {
    const key = String(rawKey || '').trim();
    if (!key || blockedKey.test(key) || blockedStepSuffix.test(key) || sensitiveKey.test(key)) continue;
    if (rawValue === null || rawValue === undefined) continue;
    if (typeof rawValue === 'string' && rawValue.trim() === '') continue;

    const canonical = normalizeCanonicalKey(key);
    if (!canonical) continue;

    // Keep only useful primitives/small arrays; discard large nested payloads.
    if (!(isPrimitive(rawValue) || isUsefulArray(rawValue))) continue;
    if (typeof rawValue === 'string' && rawValue.length > 240) continue;

    const existingKey = canonicalMap.get(canonical);
    if (!existingKey) {
      canonicalMap.set(canonical, key);
      compact[key] = rawValue;
      continue;
    }

    // Prefer non-empty scalar values when duplicate semantic keys appear.
    const existingValue = compact[existingKey];
    const existingIsEmpty = existingValue === '' || existingValue === null || existingValue === undefined;
    if (existingIsEmpty && rawValue !== null && rawValue !== undefined && String(rawValue).trim() !== '') {
      compact[existingKey] = rawValue;
    }
  }

  const keys = Object.keys(compact);
  if (keys.length <= 14) {
    return compact;
  }

  // For very wide rows, project top-priority fields similar to what users expect in SQL grids.
  const priorityOrder = [
    'username', 'usuario', 'nombre', 'email',
    'role', 'rol', 'estado', 'activo', 'bloqueado',
    'total', 'total_logs', 'total_sesiones', 'sesiones_activas', 'comentarios_principales',
    'count', 'row_count',
    'id', 'user_id', 'role_id',
    'created_at', 'fecha', 'updated_at',
  ];

  const projected = {};
  const lowered = Object.entries(compact).map(([k, v]) => [k, String(k).toLowerCase(), v]);

  for (const preferred of priorityOrder) {
    const hit = lowered.find(([, lower]) => lower === preferred);
    if (hit) projected[hit[0]] = hit[2];
  }

  for (const [key, lowerKey, value] of lowered) {
    if (Object.prototype.hasOwnProperty.call(projected, key)) continue;
    if (/^(total_|count|promedio|max|min|avg|sum|sesiones|logs|comentarios)/i.test(lowerKey)) {
      projected[key] = value;
    }
    if (Object.keys(projected).length >= 12) break;
  }

  return Object.keys(projected).length > 0 ? projected : compact;
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

function filterRelevantNotices(notices = []) {
  const blocked = [
    /^=+.*=+$/i,
    /^\s*\[step\s*\d+\]/i,
    /^\s*paso\s*\d+\s*:/i,
    /^\s*\[sql/i,
    /^\s*\[sql operation\]/i,
    /^\s*\[sql placeholders\]/i,
    /^\s*\[sql params\]/i,
    /^\s*(inicio|fin)\b/i,
    /^\s*(debug|traza|trace)\b/i,
    /^\s*(motor|workflow)\b/i,
  ];

  const unique = new Set();
  const output = [];

  for (const raw of Array.isArray(notices) ? notices : []) {
    let msg = String(raw || '').trim();
    if (!msg) continue;
    msg = msg.replace(/^NOTICE:\s*/i, '').trim();
    if (!msg) continue;
    if (blocked.some((pattern) => pattern.test(msg))) continue;

    const dedupeKey = msg.toLowerCase();
    if (unique.has(dedupeKey)) continue;
    unique.add(dedupeKey);
    output.push(msg);
  }

  return output;
}

function buildProductionSummary({ rows = [], semantic = {}, notices = [], fallback = '' } = {}) {
  const cleanNotices = filterRelevantNotices(notices);
  const semanticFromNotices = extractSemanticMetricsFromNotices(cleanNotices);
  const mergedSemantic = {
    ...(semantic && typeof semantic === 'object' && !Array.isArray(semantic) ? semantic : {}),
    ...semanticFromNotices,
  };

  const s = mergedSemantic;
  const totalUsuarios = Number(s.total_usuarios);
  const totalLogs = Number(s.total_logs);
  const totalSesiones = Number(s.total_sesiones);
  const sesionesActivas = Number(s.sesiones_activas);
  const parts = [];

  if (Number.isFinite(totalUsuarios)) {
    parts.push(`Se analizaron ${totalUsuarios} ${totalUsuarios === 1 ? 'usuario' : 'usuarios'}.`);
  }

  if (Number.isFinite(totalLogs) || Number.isFinite(totalSesiones) || Number.isFinite(sesionesActivas)) {
    const logsText = Number.isFinite(totalLogs) ? `${totalLogs} ${totalLogs === 1 ? 'log' : 'logs'} registrados` : 'logs no disponibles';
    const sesionesText = Number.isFinite(sesionesActivas)
      ? `${sesionesActivas} ${sesionesActivas === 1 ? 'sesion activa' : 'sesiones activas'}`
      : Number.isFinite(totalSesiones)
        ? `${totalSesiones} ${totalSesiones === 1 ? 'sesion registrada' : 'sesiones registradas'}`
        : 'sesiones no disponibles';
    parts.push(`Hay ${logsText} y ${sesionesText}.`);
  }

  // Raw notices are already human-readable — return them directly before
  // attempting a synthetic sentence from partial metric extraction.
  if (cleanNotices.length > 0) {
    return cleanNotices.join('\n').trim();
  }

  if (parts.length > 0) {
    return parts.join(' ');
  }

  if (String(fallback || '').trim()) {
    return String(fallback || '').trim();
  }

  return buildHumanSqlExplanation(rows);
}

function buildProductionSqlResponse({
  success = true,
  rows = [],
  notices = [],
  semanticCandidate = null,
  explicacion = '',
  message = '',
} = {}) {
  void success;
  const data = Array.isArray(rows) ? rows : [];
  const semanticBase = semanticCandidate && typeof semanticCandidate === 'object' && !Array.isArray(semanticCandidate)
    ? semanticCandidate
    : (data[0] && typeof data[0] === 'object' ? data[0] : {});
  const semanticFromNotices = extractSemanticMetricsFromNotices(notices);
  const cleanedSemantic = sanitizeProductionResultObject({
    ...(semanticBase && typeof semanticBase === 'object' && !Array.isArray(semanticBase) ? semanticBase : {}),
    ...semanticFromNotices,
  });
  const cleanedRows = data.filter(
    (row) => row !== null && row !== undefined && typeof row === 'object' && !Array.isArray(row),
  );

  const semanticKeys = Object.keys(cleanedSemantic);
  void semanticKeys;
  const resultado = cleanedRows;
  const cleanNotices = filterRelevantNotices(notices);
  const resumenHumano = buildProductionSummary({
    rows: data,
    semantic: cleanedSemantic,
    notices: cleanNotices,
    fallback: explicacion || message,
  });

  const payload = {
    resumenHumano,
    resultado,
  };

  // Only fall back to generic message when no notice content was generated
  if (data.length === 0 && resultado.length === 0 && String(message || '').trim() && cleanNotices.length === 0) {
    payload.resumenHumano = String(message || '').trim();
  }

  return payload;
}

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

function countPositionalSqlPlaceholders(sqlText = '') {
  const indexes = Array.from(String(sqlText || '').matchAll(MANUAL_SQL_POSITIONAL_PLACEHOLDER_REGEX))
    .map((match) => Number(match[1]))
    .filter((value) => Number.isFinite(value) && value > 0);

  return indexes.length > 0 ? Math.max(...indexes) : 0;
}

function assertManualSqlAllowed(sqlText, options = {}) {
  const { allowMutations = false, writeConfirmed = false, assumeWriteConfirmed = false } = options;
  const raw = String(sqlText || '').trim();
  if (!raw) {
    throw new Error('SQL manual vacío');
  }

  const cleaned = stripLeadingSqlComments(raw.replace(/;\s*$/, '').trim());

  const operation = detectSqlOperation(cleaned);
  if (!['select', 'update', 'delete', 'insert'].includes(operation)) {
    throw new Error('Operación SQL no permitida. Usa SELECT/UPDATE/DELETE/INSERT');
  }

  if (operation !== 'select' && !allowMutations) {
    throw new Error('Solo se permite SQL SELECT en este contexto');
  }

  if (operation !== 'select' && !(writeConfirmed || assumeWriteConfirmed)) {
    throw new Error('Esta acción modificará datos. Confirma la ejecución para continuar.');
  }

  if (operation === 'select' && MANUAL_SQL_BLOCKED_KEYWORDS.test(cleaned)) {
    throw new Error('SQL contiene palabras bloqueadas (solo SELECT permitido)');
  }

  if (cleaned.includes(';')) {
    throw new Error('Solo se permite una consulta SQL por ejecución');
  }

  if (hasUnsafeMutationWithoutWhere(cleaned)) {
    throw new Error('UPDATE/DELETE requiere cláusula WHERE para seguridad');
  }
}

function isOptionalUserPlaceholder(varName = '') {
  const key = String(varName || '').trim();
  if (!key) return false;
  return !/^(step\d+\.|loopRow\.)/i.test(key);
}

function inferManualPlaceholderCast(sqlText = '', variableName = '', offset = 0, placeholderText = '') {
  const source = String(sqlText || '');
  const varName = String(variableName || '').trim().toLowerCase();
  const token = String(placeholderText || '').trim();
  const leftWindow = source.slice(Math.max(0, offset - 120), offset);
  const rightWindow = source.slice(offset + token.length, Math.min(source.length, offset + token.length + 120));
  const around = `${leftWindow}${token}${rightWindow}`.toLowerCase();

  if (/\bilike\b/.test(around)) {
    return 'text';
  }

  const idSemantic = /(^id$|_id$|uuid|guid)/.test(varName);
  const idComparison = /([\w".]+id|uuid|guid)\s*(=|!=|<>|in\s*\(|any\s*\()/i.test(around)
    || /\b(=|!=|<>)\s*\{\{\s*[^}]+\s*\}\}|\b(=|!=|<>)\s*:[a-zA-Z_][a-zA-Z0-9_]*/i.test(around);
  if (idSemantic || idComparison) {
    return 'uuid';
  }

  const numericSemantic = /(count|total|cantidad|numero|num|qty|sum|avg|max|min|edad|anio|year|month|day)/.test(varName);
  const numericComparison = /\b(count\s*\(|sum\s*\(|avg\s*\()/.test(around)
    || /(>=|<=|>|<)\s*(\{\{|:)/.test(around)
    || /\bbetween\b/.test(around);
  if (numericSemantic || numericComparison) {
    return 'int';
  }

  return null;
}

function applyCastToSqlPlaceholder(sqlPlaceholder = '', castType = null) {
  const placeholder = String(sqlPlaceholder || '').trim();
  const cast = String(castType || '').trim().toLowerCase();
  if (!placeholder) return placeholder;
  if (!cast) return placeholder;
  if (/(::\s*(text|uuid|int))$/i.test(placeholder)) return placeholder;
  if (cast === 'text' || cast === 'uuid' || cast === 'int') {
    return `${placeholder}::${cast}`;
  }
  return placeholder;
}

function applyOptionalSqlFilterTransform(sqlText = '') {
  let sql = String(sqlText || '');

  // campo = {{variable}}  -> ({{variable}} IS NULL OR campo = {{variable}})
  // campo = :variable     -> (:variable IS NULL OR campo = :variable)
  sql = sql.replace(
    /([\w".]+)\s*=\s*(\{\{\s*([a-zA-Z0-9_.]+)\s*\}\}|:([a-zA-Z_][a-zA-Z0-9_]*))(?!\s*\))/gi,
    (match, leftExpr, placeholderExpr, curlyName, namedName) => {
      const varName = String(curlyName || namedName || '').trim();
      if (!isOptionalUserPlaceholder(varName)) return match;
      return `(${placeholderExpr} IS NULL OR ${leftExpr} = ${placeholderExpr})`;
    },
  );

  // campo ILIKE {{variable}} -> ({{variable}} IS NULL OR campo ILIKE '%' || {{variable}} || '%')
  // campo ILIKE :variable    -> (:variable IS NULL OR campo ILIKE '%' || :variable || '%')
  sql = sql.replace(
    /([\w".]+)\s+ILIKE\s*(\{\{\s*([a-zA-Z0-9_.]+)\s*\}\}|:([a-zA-Z_][a-zA-Z0-9_]*))(?!\s*\|\|)/gi,
    (match, leftExpr, placeholderExpr, curlyName, namedName) => {
      const varName = String(curlyName || namedName || '').trim();
      if (!isOptionalUserPlaceholder(varName)) return match;
      return `(${placeholderExpr} IS NULL OR ${leftExpr} ILIKE '%' || ${placeholderExpr} || '%')`;
    },
  );

  return sql;
}

function buildManualSqlQuery(sqlText, rawParams = {}, workflowContext = {}, options = {}) {
  const writeConfirmed = isWriteConfirmed(rawParams);
  assertManualSqlAllowed(sqlText, {
    allowMutations: true,
    writeConfirmed,
    assumeWriteConfirmed: Boolean(options?.assumeWriteConfirmed),
  });

  const cleaned = applyOptionalSqlFilterTransform(String(sqlText || '').trim().replace(/;\s*$/, '').trim());
  const normalizedParams = normalizeManualSqlParamsInput(rawParams);
  const {
    placeholderOrder,
    userPlaceholderOrder,
    placeholderOccurrences,
  } = collectManualSqlPlaceholderMetadata(cleaned);

  if (placeholderOrder.length === 0) {
    // Flexible mode: if the query has no placeholders, run it as-is without binding params.
    // This allows multi-step workflows where some steps use params and others do not.
    const positionalCount = countPositionalSqlPlaceholders(cleaned);
    if (positionalCount === 0) {
      return {
        text: cleaned,
        values: [],
        placeholderOrder: [],
        placeholderOccurrences: [],
      };
    }

    if (Array.isArray(normalizedParams)) {
      if (positionalCount !== normalizedParams.length) {
        throw new Error(`La consulta espera ${positionalCount} parámetros posicionales y recibió ${normalizedParams.length}`);
      }
      return {
        text: cleaned,
        values: normalizedParams,
        placeholderOrder: [],
        placeholderOccurrences: [],
      };
    }

    throw new Error(`La consulta usa placeholders posicionales ($1..$N). Envía ${positionalCount} parámetros en arreglo.`);
  }

  const externalPlaceholderOrder = userPlaceholderOrder;
  const safeParams = Array.isArray(normalizedParams)
    ? externalPlaceholderOrder.reduce((acc, variableName, index) => {
        // Optional mode: missing user params become null instead of hard failure.
        acc[variableName] = index < normalizedParams.length ? normalizedParams[index] : null;
        return acc;
      }, {})
    : normalizedParams;

  const values = [];
  const variableIndexes = new Map();

  const registerPlaceholder = (variableName) => {
    const key = String(variableName || '').trim();
    if (!key) {
      throw new Error('Placeholder SQL inválido');
    }

    const resolvedValue = /^(step\d+\.|loopRow\.)/i.test(key)
      ? resolveWorkflowContextPlaceholder(key, workflowContext, safeParams)
      : safeParams[key];
    if (isMissingParamValue(resolvedValue)) {
      // Optional mode for user input placeholders: bind NULL and continue.
      if (isOptionalUserPlaceholder(key)) {
        if (!variableIndexes.has(key)) {
          values.push(null);
          variableIndexes.set(key, values.length);
        }
        return `$${variableIndexes.get(key)}`;
      }
      throw new Error(`Falta parámetro para SQL manual: ${key}`);
    }

    validateStructuredParamByName(key.split('.').pop(), resolvedValue);

    if (!variableIndexes.has(key)) {
      values.push(sanitizeStructuredParamValue(resolvedValue));
      variableIndexes.set(key, values.length);
    }

    return `$${variableIndexes.get(key)}`;
  };

  const text = cleaned.replace(MANUAL_SQL_CANONICAL_PLACEHOLDER_REGEX, (match, curlyVariable, namedVariable, offset, source) => {
    const variableName = String(curlyVariable || namedVariable || '').trim();
    if (!variableName) {
      throw new Error('Placeholder SQL inválido');
    }

    if (match.startsWith(':')) {
      const previousChar = offset > 0 ? String(source || '')[offset - 1] : '';
      if (previousChar === ':') {
        return match;
      }
    }

    const sqlPlaceholder = registerPlaceholder(variableName);
    const castType = inferManualPlaceholderCast(String(source || ''), variableName, Number(offset || 0), match);
    return applyCastToSqlPlaceholder(sqlPlaceholder, castType);
  });

  if (placeholderOccurrences.length > 0 && values.length === 0) {
    throw new Error('Consulta con placeholders pero sin parámetros utilizables');
  }

  if (placeholderOrder.length !== values.length) {
    throw new Error('Mismatch entre placeholders y parámetros preparados');
  }

  return { text, values, placeholderOrder, placeholderOccurrences };
}

async function executeSingleManualSqlStep(sqlText, rawParams, emit, workflowContext = {}, silent = false, options = {}) {
  const startedAt = Date.now();
  const { text, values, placeholderOrder, placeholderOccurrences } = buildManualSqlQuery(
    sqlText,
    rawParams,
    workflowContext,
    options,
  );
  const operation = detectSqlOperation(sqlText);

  if (!silent && typeof emit === 'function') {
    emit('🧠 Motor SQL manual avanzado activado');
    emit(`[SQL OPERATION] ${operation.toUpperCase()}`);
    emit(`[SQL] ${text}`);
    if (placeholderOrder.length > 0) {
      emit(`[SQL PLACEHOLDERS] ${JSON.stringify(placeholderOrder)}`);
    }
    if (values.length > 0) {
      emit(`[SQL PARAMS] ${JSON.stringify(values)}`);
    }
  }

  console.info('[SQL_MANUAL][AUDIT]', {
    operation,
    query: text,
    placeholderOrder,
    placeholderOccurrences,
    values,
  });

  const targetDatabaseId = String(options?.databaseId || '').trim();
  if (operation !== 'select' && targetDatabaseId) {
    const db = multiDbRegistry.getDatabaseById(targetDatabaseId);
    if (db && db.primary !== true) {
      throw new Error(`La base ${targetDatabaseId} es de solo consulta (primary=false). No se permiten escrituras.`);
    }
  }

  const result = operation === 'select'
    ? await executeSafeSelectQuery(text, values, 5000, {
      allowCrossDatabase: true,
      databaseId: targetDatabaseId,
      allowUnknownTablesForTarget: true,
    })
    : await pool.query(text, values);
  const executionMs = Date.now() - startedAt;
  const affectedRows = Number(result.rowCount || 0);

  if (operation !== 'select') {
    console.info('[SQL_MANUAL][WRITE_AUDIT]', {
      operation,
      affectedRows,
      query: text,
    });
  }

  return {
    success: true,
    data: result.rows || [],
    resultado: result.rows || [],
    resultado_final: (result.rows || [])[0] || null,
    affectedRows,
    message: 'Consulta ejecutada correctamente',
    explicacion: buildHumanSqlExplanation(result.rows || []),
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
      executions.push({ step: `step${stepNum}`, executedQuery: stepResult.executedQuery, executionMs: stepResult.executionMs, title: step.title, affectedRows: stepResult.affectedRows || 0 });
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
    resumenHumano: summaryLines.length > 0 ? summaryLines.join('\n') : explanation,
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

function extractSemanticMetricsFromNotices(lines = []) {
  const metrics = {};

  for (const lineRaw of lines || []) {
    const line = String(lineRaw || '').trim();
    if (!line) continue;

    // Examples: "Total logs: 2", "Total sesiones: 1", "Max logs: 5"
    const metricMatch = line.match(/^\s*([a-zA-Z_\s]+?)\s*:\s*(-?\d+(?:\.\d+)?)\s*$/i);
    if (!metricMatch) continue;

    const label = String(metricMatch[1] || '')
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[^a-z0-9\s_]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
    const value = Number(metricMatch[2]);
    if (!Number.isFinite(value)) continue;

    let key = label
      .replace(/\btotal\b/g, 'total')
      .replace(/\bpromedio\b/g, 'promedio')
      .replace(/\bmaximo\b|\bmax\b/g, 'max')
      .replace(/\bsesiones activas\b/g, 'sesiones_activas')
      .replace(/\s+/g, '_');

    if (key === 'total_usuarios' || key === 'usuarios_total') key = 'total_usuarios';
    if (key === 'total_logs' || key === 'logs_total') key = 'total_logs';
    if (key === 'total_sesiones' || key === 'sesiones_total') key = 'total_sesiones';
    if (key === 'total_activos' || key === 'activos_total') key = 'total_activos';
    if (key === 'max_logs' || key === 'logs_max') key = 'max_logs';
    if (key === 'promedio_logs' || key === 'logs_promedio' || key === 'avg_logs') key = 'promedio_logs';

    if (key) metrics[key] = value;
  }

  return metrics;
}

function buildComplexWorkflowExplanation(steps, executions, finalResult) {
  const rows = finalResult?.data || [];
  const loopSteps = steps.filter((s) => s.type === 'for_loop');
  const affectedTotal = executions.reduce((sum, ex) => sum + (ex.affectedRows || 0), 0);

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
  const sqlText = String(script?.sql || '').trim();

  // ── Resolve target database from SQL syntax markers ───────────────────────
  const directiveId = extractDatabaseDirective(sqlText);
  const detectedEngine = directiveId ? null : detectSqlSyntaxEngine(sqlText);
  const engineDb = detectedEngine ? multiDbRegistry.findDatabaseByType(detectedEngine) : null;
  const resolvedDatabaseId = directiveId
    || engineDb?.id
    || String(script?.databaseId || '').trim();

  console.log(buildEngineLog({
    engine: detectedEngine,
    queryType: detectQueryType(sqlText),
    databaseId: resolvedDatabaseId || 'primary (fallback)',
    reason: directiveId ? '-- database: directive' : (detectedEngine ? 'syntax markers detected' : 'script.databaseId or primary fallback'),
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
  if (shouldForceProceduralMode(sqlText) || isComplexSqlScript(sqlText)) {
    if (typeof emit === 'function') {
      emit('🔍 Paso 1: Analizando script');
      emit('📊 Paso 2: Procesando lógica');
      emit('📈 Paso 3: Generando resultado');
      emit('🧠 Script procedural detectado. Ejecutando en PostgreSQL real...');
    }
    return executeProceduralScriptWithNotices(sqlText, emit);
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
      });
    }

    const aggregatedResult = buildAggregatedResultFromStepPayload(stepPayload);
    const consolidatedData = aggregatedResult ? [aggregatedResult] : (finalResult?.data ?? []);
    const consolidatedFinal = aggregatedResult ?? finalResult?.resultado_final ?? (consolidatedData[0] || null);

    return {
      success: true,
      message: 'Proceso SQL ejecutado correctamente',
      explicacion: finalResult?.explicacion || buildHumanSqlExplanation(consolidatedData),
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

function scriptContainsMutation(node) {
  if (!node || typeof node !== 'object') {
    return false;
  }

  if (Array.isArray(node)) {
    return node.some((item) => scriptContainsMutation(item));
  }

  const current = node;
  const stepType = String(current.tipo || '').trim().toLowerCase();
  if (stepType === 'update' || stepType === 'delete' || stepType === 'insert') {
    return true;
  }

  const mode = String(current.modo || '').trim().toLowerCase();
  const source = String(current.origen || '').trim().toLowerCase();
  const sqlText = String(current.sql || '').trim().toLowerCase();
  if (mode === 'script' && source === 'manual-sql' && /^(update|delete|insert)\b/.test(sqlText)) {
    return true;
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

function normalizeDynamicScriptType(type) {
  return String(type || '').trim().toLowerCase();
}

function isPlainObject(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function isMissingParamValue(value) {
  return value === undefined || value === null || (typeof value === 'string' && value.trim() === '');
}

function getValueByPath(source, path) {
  if (!source || typeof source !== 'object') {
    return undefined;
  }

  if (Object.prototype.hasOwnProperty.call(source, path)) {
    return source[path];
  }

  return String(path)
    .split('.')
    .reduce((acc, key) => (acc !== undefined && acc !== null ? acc[key] : undefined), source);
}

function resolveValueMultiSource(path, context, params) {
  // Busca primero en context (datos internos del workflow), luego en params (entrada del usuario)
  const pathObj = getValueByPath(context, path);
  if (pathObj !== undefined) {
    return pathObj;
  }
  return getValueByPath(params, path);
}

function resolveValue(value, context, params) {
  if (typeof value !== 'string') {
    return value;
  }

  const fullMatch = value.match(/^{{\s*([a-zA-Z0-9_.]+)\s*}}$/);
  if (fullMatch) {
    const resolved = resolveValueMultiSource(fullMatch[1], context, params);
    if (isMissingParamValue(resolved)) {
      throw new Error(`Falta parámetro: ${fullMatch[1]}`);
    }
    return sanitizeStructuredParamValue(resolved);
  }

  return value.replace(STRUCTURED_PLACEHOLDER_REGEX, (_, rawPath) => {
    const path = String(rawPath || '').trim();
    const resolved = resolveValueMultiSource(path, context, params);
    if (isMissingParamValue(resolved)) {
      throw new Error(`Falta parámetro: ${path}`);
    }
    return String(sanitizeStructuredParamValue(resolved));
  });
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

/**
 * Normalize a WHERE value that may be either a plain scalar or an extended
 * operator object from the builder:
 *   { op: '!=', value: 5 }  or  { op: '=', field: 'employees.user_id' }
 * Returns { op, value, fieldRef } and rejects unknown operators.
 */
function resolveWhereEntry(rawValue) {
  if (
    rawValue !== null &&
    typeof rawValue === 'object' &&
    !Array.isArray(rawValue) &&
    rawValue.op != null
  ) {
    const op = String(rawValue.op).trim().toUpperCase();
    if (!ALLOWED_WHERE_OPERATORS.has(op)) {
      throw new Error(`Operador WHERE no permitido: ${rawValue.op}`);
    }

    const hasField = typeof rawValue.field === 'string' && rawValue.field.trim() !== '';
    if (hasField) {
      return { op, value: null, fieldRef: String(rawValue.field).trim() };
    }

    return { op, value: rawValue.value ?? null, fieldRef: null };
  }
  return { op: '=', value: rawValue, fieldRef: null };
}

function ensureDynamicColumnAllowed(tableName, columns) {
  const allowedColumns = DYNAMIC_SCRIPT_ALLOWED_COLUMNS[tableName] || [];
  for (const columnName of columns) {
    if (!allowedColumns.includes(columnName)) {
      throw new Error(`Columna no permitida: ${columnName}`);
    }
  }
}

function ensureDynamicJoinRelationAllowed(left, right, contextLabel = 'JOIN') {
  if (left.table === right.table && left.column === right.column) {
    return;
  }

  if (!hasRelationBetweenFields(left, right)) {
    throw new Error(
      `${contextLabel}: no existe relación FK entre ${left.table}.${left.column} y ${right.table}.${right.column}`
    );
  }
}

function validateDynamicColumnType(tableName, field, value) {
  if (value === null || value === undefined) {
    return;
  }

  const expected = DYNAMIC_SCRIPT_COLUMN_TYPES?.[tableName]?.[field];
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

function assertDynamicScriptAllowed(script) {
  const type = normalizeDynamicScriptType(script.tipo);
  const tableName = String(script.tabla || '').trim().toLowerCase();

  if (!DYNAMIC_SCRIPT_ALLOWED_TYPES.includes(type)) {
    throw new Error('Operación no permitida');
  }

  if (!DYNAMIC_SCRIPT_ALLOWED_TABLES.includes(tableName)) {
    throw new Error('Tabla no permitida');
  }

  const setObject = isPlainObject(script.set) ? script.set : null;
  const whereObject = isPlainObject(script.where) ? script.where : null;
  const dataObject = isPlainObject(script.data) ? script.data : null;

  if (type === 'update') {
    if (!setObject || Object.keys(setObject).length === 0) {
      throw new Error('UPDATE requiere un objeto set válido');
    }
    if (!whereObject || Object.keys(whereObject).length === 0) {
      throw new Error('UPDATE requiere un objeto where válido');
    }
    ensureDynamicColumnAllowed(tableName, Object.keys(setObject));
    for (const fieldRef of Object.keys(whereObject)) {
      const { table, column } = parseQualifiedFieldRef(fieldRef, tableName);
      if (!DYNAMIC_SCRIPT_ALLOWED_TABLES.includes(table)) {
        throw new Error(`WHERE: tabla no permitida (${table})`);
      }
      ensureDynamicColumnAllowed(table, [column]);
    }
  }

  if (type === 'insert') {
    if (!dataObject || Object.keys(dataObject).length === 0) {
      throw new Error('INSERT requiere un objeto data válido');
    }
    ensureDynamicColumnAllowed(tableName, Object.keys(dataObject));
  }

  if (type === 'delete' || type === 'select') {
    if (type === 'delete' && (!whereObject || Object.keys(whereObject).length === 0)) {
      throw new Error(`${type.toUpperCase()} requiere un objeto where válido`);
    }
    if (whereObject && Object.keys(whereObject).length > 0) {
      // Support both plain "columna" keys and qualified "tabla.columna" keys
      for (const fieldRef of Object.keys(whereObject)) {
        const { table, column } = parseQualifiedFieldRef(fieldRef, tableName);
        if (!DYNAMIC_SCRIPT_ALLOWED_TABLES.includes(table)) {
          throw new Error(`WHERE: tabla no permitida (${table})`);
        }
        ensureDynamicColumnAllowed(table, [column]);
      }
    }

    if (type === 'select' && Array.isArray(script.columnas)) {
      const parsedColumns = script.columnas
        .map((entry) => parseQualifiedFieldRef(String(entry || '').trim(), tableName));

      parsedColumns.forEach(({ table, column }) => {
        if (!DYNAMIC_SCRIPT_ALLOWED_TABLES.includes(table)) {
          throw new Error(`SELECT: tabla no permitida (${table})`);
        }
        ensureDynamicColumnAllowed(table, [column]);
      });
    }
  }

  return {
    type,
    tableName,
    setObject,
    dataObject,
    whereObject,
  };
}

function parseQualifiedFieldRef(fieldRef, defaultTable = '') {
  const raw = String(fieldRef || '').trim();
  if (!raw) {
    throw new Error('Referencia de campo inválida');
  }

  const parts = raw.split('.').map((part) => part.trim()).filter(Boolean);
  if (parts.length === 1) {
    if (!defaultTable) {
      throw new Error(`Campo ${raw} requiere tabla explícita`);
    }
    return { table: defaultTable, column: parts[0] };
  }

  if (parts.length !== 2) {
    throw new Error(`Campo calificado inválido: ${raw}`);
  }

  return { table: parts[0].toLowerCase(), column: parts[1] };
}

function validateRelationalScriptDefinition(script) {
  if (!script || typeof script !== 'object' || Array.isArray(script)) {
    throw new Error('Script relacional inválido');
  }

  const type = normalizeDynamicScriptType(script.tipo);
  if (type !== 'select') {
    throw new Error('Script relacional solo soporta tipo select');
  }

  const baseTable = String(script.tabla || '').trim().toLowerCase();
  if (!DYNAMIC_SCRIPT_ALLOWED_TABLES.includes(baseTable)) {
    throw new Error(`Tabla base no permitida: ${baseTable || 'vacía'}`);
  }

  const joins = Array.isArray(script.join) ? script.join : [];
  if (joins.length === 0) {
    throw new Error('Script relacional requiere al menos un join');
  }

  joins.forEach((joinItem, index) => {
    if (!isPlainObject(joinItem)) {
      throw new Error(`Join ${index + 1} inválido`);
    }

    const joinTable = String(joinItem.tabla || '').trim().toLowerCase();
    if (!DYNAMIC_SCRIPT_ALLOWED_TABLES.includes(joinTable)) {
      throw new Error(`Join ${index + 1}: tabla no permitida (${joinTable || 'vacía'})`);
    }

    const onObject = isPlainObject(joinItem.on) ? joinItem.on : null;
    if (!onObject || Object.keys(onObject).length === 0) {
      throw new Error(`Join ${index + 1}: requiere objeto on`);
    }

    Object.entries(onObject).forEach(([leftRef, rightRef]) => {
      const left = parseQualifiedFieldRef(leftRef, baseTable);
      const right = parseQualifiedFieldRef(rightRef);

      if (!DYNAMIC_SCRIPT_ALLOWED_TABLES.includes(left.table)) {
        throw new Error(`Join ${index + 1}: tabla no permitida en ON (${left.table})`);
      }
      if (!DYNAMIC_SCRIPT_ALLOWED_TABLES.includes(right.table)) {
        throw new Error(`Join ${index + 1}: tabla no permitida en ON (${right.table})`);
      }

      ensureDynamicColumnAllowed(left.table, [left.column]);
      ensureDynamicColumnAllowed(right.table, [right.column]);
      ensureDynamicJoinRelationAllowed(left, right, `Join ${index + 1}`);
    });

    if (Array.isArray(joinItem.columnas)) {
      joinItem.columnas.forEach((columnName) => {
        const col = String(columnName || '').trim();
        if (col) ensureDynamicColumnAllowed(joinTable, [col]);
      });
    }
  });

  if (Array.isArray(script.columnas)) {
    script.columnas.forEach((columnName) => {
      const col = String(columnName || '').trim();
      if (col) ensureDynamicColumnAllowed(baseTable, [col]);
    });
  }

  const whereObject = isPlainObject(script.where) ? script.where : null;
  if (whereObject) {
    Object.entries(whereObject).forEach(([fieldRef, rawValue]) => {
      const { table, column } = parseQualifiedFieldRef(fieldRef, baseTable);
      if (!DYNAMIC_SCRIPT_ALLOWED_TABLES.includes(table)) {
        throw new Error(`WHERE: tabla no permitida (${table})`);
      }
      ensureDynamicColumnAllowed(table, [column]);

      const { fieldRef: rightFieldRef } = resolveWhereEntry(rawValue);
      if (rightFieldRef) {
        const right = parseQualifiedFieldRef(rightFieldRef, baseTable);
        if (!DYNAMIC_SCRIPT_ALLOWED_TABLES.includes(right.table)) {
          throw new Error(`WHERE: tabla no permitida (${right.table})`);
        }
        ensureDynamicColumnAllowed(right.table, [right.column]);
      }
    });
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

function validateRelationalMutationScriptDefinition(script, mutationType) {
  if (!script || typeof script !== 'object' || Array.isArray(script)) {
    throw new Error('Script relacional inválido');
  }

  const type = normalizeDynamicScriptType(script.tipo);
  if (type !== mutationType || !['update', 'delete'].includes(type)) {
    throw new Error(`Script relacional solo soporta tipo ${mutationType}`);
  }

  const baseTable = String(script.tabla || '').trim().toLowerCase();
  if (!DYNAMIC_SCRIPT_ALLOWED_TABLES.includes(baseTable)) {
    throw new Error(`Tabla base no permitida: ${baseTable || 'vacía'}`);
  }

  const joins = Array.isArray(script.join) ? script.join : [];
  if (joins.length === 0) {
    throw new Error('Script relacional requiere al menos un join');
  }

  joins.forEach((joinItem, index) => {
    if (!isPlainObject(joinItem)) {
      throw new Error(`Join ${index + 1} inválido`);
    }

    const joinTable = String(joinItem.tabla || '').trim().toLowerCase();
    if (!DYNAMIC_SCRIPT_ALLOWED_TABLES.includes(joinTable)) {
      throw new Error(`Join ${index + 1}: tabla no permitida (${joinTable || 'vacía'})`);
    }

    const onObject = isPlainObject(joinItem.on) ? joinItem.on : null;
    if (!onObject || Object.keys(onObject).length === 0) {
      throw new Error(`Join ${index + 1}: requiere objeto on`);
    }

    Object.entries(onObject).forEach(([leftRef, rightRef]) => {
      const left = parseQualifiedFieldRef(leftRef, baseTable);
      const right = parseQualifiedFieldRef(rightRef, baseTable);

      if (!DYNAMIC_SCRIPT_ALLOWED_TABLES.includes(left.table)) {
        throw new Error(`Join ${index + 1}: tabla no permitida en ON (${left.table})`);
      }
      if (!DYNAMIC_SCRIPT_ALLOWED_TABLES.includes(right.table)) {
        throw new Error(`Join ${index + 1}: tabla no permitida en ON (${right.table})`);
      }

      ensureDynamicColumnAllowed(left.table, [left.column]);
      ensureDynamicColumnAllowed(right.table, [right.column]);
      ensureDynamicJoinRelationAllowed(left, right, `Join ${index + 1}`);
    });
  });

  const whereObject = isPlainObject(script.where) ? script.where : null;
  if (!whereObject || Object.keys(whereObject).length === 0) {
    throw new Error(`${mutationType.toUpperCase()} relacional requiere un objeto where válido`);
  }

  Object.entries(whereObject).forEach(([fieldRef, rawValue]) => {
    const { table, column } = parseQualifiedFieldRef(fieldRef, baseTable);
    if (!DYNAMIC_SCRIPT_ALLOWED_TABLES.includes(table)) {
      throw new Error(`WHERE: tabla no permitida (${table})`);
    }
    ensureDynamicColumnAllowed(table, [column]);

    const { fieldRef: rightFieldRef } = resolveWhereEntry(rawValue);
    if (rightFieldRef) {
      const right = parseQualifiedFieldRef(rightFieldRef, baseTable);
      if (!DYNAMIC_SCRIPT_ALLOWED_TABLES.includes(right.table)) {
        throw new Error(`WHERE: tabla no permitida (${right.table})`);
      }
      ensureDynamicColumnAllowed(right.table, [right.column]);
    }
  });

  if (type === 'update') {
    const setObject = isPlainObject(script.set) ? script.set : null;
    if (!setObject || Object.keys(setObject).length === 0) {
      throw new Error('UPDATE relacional requiere un objeto set válido');
    }
    ensureDynamicColumnAllowed(baseTable, Object.keys(setObject));
  }
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

function buildAnalyticSelectQuery(script, normalizedInputParams, workflowContext = {}) {
    const baseTable = String(script.tabla || '').trim().toLowerCase();
    const joins = Array.isArray(script.join) ? script.join : [];
    const groupByCols = Array.isArray(script.group_by) ? script.group_by.filter(Boolean) : [];
    const aggregates = Array.isArray(script.aggregates) ? script.aggregates : [];
    const orderByItems = Array.isArray(script.order_by) ? script.order_by : [];
    const havingItems = Array.isArray(script.having) ? script.having : [];

    // -- JOIN clauses ---------------------------------------------------------
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

    // -- WHERE ----------------------------------------------------------------
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
        return `"${table}"."${column}" ${op} "${right.table}"."${right.column}"`;
      }
      whereValues.push(value);
      return `"${table}"."${column}" ${op} $${whereValues.length}`;
    });
    const whereClause = whereConditions.length ? ` WHERE ${whereConditions.join(' AND ')}` : '';

    // -- SELECT list: group-by cols + aggregates ------------------------------
    const groupByColsSql = groupByCols.map((col) => {
      const ref = parseQualifiedFieldRef(col, baseTable);
      return `"${ref.table}"."${ref.column}"`;
    });

    const ALLOWED_AGG_FUNCS = new Set(['COUNT', 'COUNT DISTINCT', 'SUM', 'AVG', 'MAX', 'MIN']);
    const aggregateExpressions = aggregates.map((agg) => {
      const func = String(agg.func || 'COUNT').toUpperCase().trim();
      if (!ALLOWED_AGG_FUNCS.has(func)) throw new Error(`Función de agregación no permitida: ${func}`);
      const colRaw = String(agg.column || '*').trim();
      const alias = String(agg.alias || '').trim();
      let colSql;
      if (colRaw === '*') {
        colSql = '*';
      } else {
        const ref = parseQualifiedFieldRef(colRaw, baseTable);
        colSql = `"${ref.table}"."${ref.column}"`;
      }
      const expr = func === 'COUNT DISTINCT'
        ? `COUNT(DISTINCT ${colSql})`
        : `${func}(${colSql})`;
      return alias ? `${expr} AS "${alias}"` : expr;
    });

    const selectParts = [...groupByColsSql, ...aggregateExpressions];
    const selectSql = selectParts.length > 0 ? selectParts.join(', ') : '*';

    // -- GROUP BY -------------------------------------------------------------
    const groupBySql = groupByColsSql.length > 0
      ? ` GROUP BY ${groupByColsSql.join(', ')}`
      : '';

    // -- HAVING ---------------------------------------------------------------
    // Build HAVING by finding the aggregate expression for each alias
    const aggByAlias = {};
    aggregates.forEach((agg, i) => {
      if (agg.alias) aggByAlias[agg.alias] = aggregateExpressions[i].split(' AS ')[0]; // raw expr without alias
    });
    const havingConditions = havingItems
      .filter((h) => h.alias && aggByAlias[h.alias] && h.op && h.value !== '' && h.value !== undefined)
      .map((h) => {
        const op = ALLOWED_WHERE_OPERATORS.has(h.op) ? h.op : '=';
        const val = Number.isFinite(Number(h.value)) ? Number(h.value) : `'${String(h.value).replace(/'/g, "''")}'`;
        return `${aggByAlias[h.alias]} ${op} ${val}`;
      });
    const havingClause = havingConditions.length > 0 ? ` HAVING ${havingConditions.join(' AND ')}` : '';

    // -- ORDER BY -------------------------------------------------------------
    const orderBySql = orderByItems
      .filter((ob) => ob.column)
      .map((ob) => {
        const dir = String(ob.direction || 'ASC').toUpperCase() === 'DESC' ? 'DESC' : 'ASC';
        // If ordering by an alias (aggregate), use it directly; otherwise qualify
        if (aggByAlias[ob.column] !== undefined) return `"${ob.column}" ${dir}`;
        const ref = parseQualifiedFieldRef(ob.column, baseTable);
        return `"${ref.table}"."${ref.column}" ${dir}`;
      });
    const orderByClause = orderBySql.length > 0 ? ` ORDER BY ${orderBySql.join(', ')}` : '';

    const limitValue = normalizeLimitValue(script.limit, 0);
    const limitClause = limitValue > 0 ? ` LIMIT ${limitValue}` : '';

    return {
      baseTable,
      whereValues,
      query: `SELECT ${selectSql} FROM "${baseTable}" ${joinClauses.join(' ')}${whereClause}${groupBySql}${havingClause}${orderByClause}${limitClause}`.trim(),
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
      if (!DYNAMIC_SCRIPT_ALLOWED_TABLES.includes(right.table)) {
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

function resolveDynamicScriptNode(node, params, context = {}) {
  if (typeof node === 'string') {
    return resolveValue(node, context, params);
  }

  if (Array.isArray(node)) {
    return node.map((item) => resolveDynamicScriptNode(item, params, context));
  }

  if (node && typeof node === 'object') {
    return Object.fromEntries(
      Object.entries(node).map(([key, value]) => [key, resolveDynamicScriptNode(value, params, context)])
    );
  }

  return node;
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

function normalizeLimitValue(limitValue, defaultValue = 0) {
  if (limitValue === undefined || limitValue === null || limitValue === '') {
    return defaultValue;
  }

  const parsed = Number(limitValue);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error('Límite inválido');
  }

  return Math.min(Math.floor(parsed), 100);
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
        if (!DYNAMIC_SCRIPT_ALLOWED_TABLES.includes(right.table)) {
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

  console.log('[SCRIPT RECIBIDO]: script estructurado validado');

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
    console.log('[WORKFLOW ACTIVADO]');
    try {
      return await executeWorkflowScript(script, validatedRuntimeParams, emit);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Error en workflow';
      emit(`❌ ${message}`, 'error');
      return { success: false, message };
    }
  }

  if (Array.isArray(script.join)) {
    console.log('[MOTOR RELACIONAL ACTIVADO]');
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
    console.log('[MOTOR SQL MANUAL ACTIVADO]');
    try {
      return await executeManualSqlScript(script, validatedRuntimeParams, emit);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Error en SQL manual';
      emit(`❌ ${message}`, 'error');
      return { success: false, message };
    }
  }

  if (String(script.modo || '').trim().toLowerCase() === 'script') {
    console.log('[MODO EMPRESARIAL ACTIVADO]');
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
    console.log('[MOTOR DINÁMICO ACTIVADO]');
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
    console.log('[MODO ANTIGUO]');
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
    if (!DYNAMIC_SCRIPT_ALLOWED_TABLES.includes(normalizedTableName)) {
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
  console.log(`[EXECUTE] 🚀 Ejecutando acción "${actionKey}" en artículo ${articleId}`);

  const result = await action.execute(safeParams, pool, emit || (() => {}));
  console.log(`[EXECUTE] ✅ Acción completada: ${result.message}`);
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
  allowedHeaders: ['Content-Type', 'Authorization', 'x-user-role'],
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
app.use('/api', apiLimiter);
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

app.get('/api/distributed/databases', (req, res) => {
  try {
    res.status(200).json({
      success: true,
      databases: multiDbRegistry.getDatabaseSummaries(),
      note: 'Configurar MULTI_DB_CONFIG_FILE (por defecto ./config/multidb.databases.json) y marcar una entrada con primary=true para definir la base principal del backend',
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error?.message || 'No se pudo leer el registro distribuido',
    });
  }
});

app.post('/api/query/distributed', async (req, res) => {
  try {
    await ensureMultiDbReady({ reason: 'api-query-distributed' });
    const { text, parameterValue = null, limit = 50, databases = [] } = req.body || {};
    const inputText = String(text || '').trim();
    if (!inputText) {
      return res.status(400).json({
        success: false,
        error: 'El campo text es requerido',
      });
    }

    const errorAnalysisResponse = await resolveErrorAnalysisResponse(inputText, pool);
    if (errorAnalysisResponse) {
      return res.status(200).json(errorAnalysisResponse);
    }

    const safeLimit = Math.max(1, Math.min(Number(limit) || 50, 100));
    const result = await multiDatabaseEngine.execute(inputText, {
      parameterValue,
      limit: safeLimit,
      databases,
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
    return res.status(400).json({
      success: false,
      error: error?.message || 'No se pudo ejecutar la consulta distribuida',
      responseMode: 'distributed-multi-db',
    });
  }
});

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

// GET /api/actions - Catálogo de acciones permitidas para scripts empresariales
app.get('/api/actions', (req, res) => {
  try {
    const user = getUserFromRequest(req);
    const role = req.headers['x-user-role'] || user?.role;
    if (!role || role !== 'admin') {
      return res.status(403).json({
        success: false,
        error: 'Acceso denegado: se requiere rol admin',
      });
    }

    const normalizeCategory = (value) => String(value || '')
      .trim()
      .toLowerCase()
      .normalize('NFD')
      .replace(/[^\w\s-]/g, '');

    const requestedCategory = normalizeCategory(req.query.categoria || req.query.category || '');
    const filtered = requestedCategory
      ? BUSINESS_ACTION_CATALOG.filter((action) => normalizeCategory(action.categoria || '') === requestedCategory)
      : BUSINESS_ACTION_CATALOG;

    return res.status(200).json(filtered);
  } catch (error) {
    console.error('[ACTIONS] ❌ Error obteniendo catálogo de acciones:', error);
    return res.status(500).json({
      success: false,
      error: 'Error interno del servidor',
    });
  }
});

// Auth Endpoints
app.post('/api/auth/login', async (req, res) => {
  try {
    const { username, password } = req.body;

    if (!username || !password) {
      return res.status(400).json({
        error: 'Usuario y contraseña requeridos',
      });
    }

    // Buscar usuario real en DB
    const userQuery = 'SELECT id, username, password, role FROM users WHERE username = $1';
    const result = await pool.query(userQuery, [username]);

    if (result.rowCount === 0) {
      return res.status(401).json({
        success: false,
        error: 'Usuario o contraseña incorrecta',
      });
    }

    const user = result.rows[0];
    const passwordValid = await bcrypt.compare(password, user.password);

    if (!passwordValid) {
      return res.status(401).json({
        success: false,
        error: 'Usuario o contraseña incorrecta',
      });
    }

    // Generar token sencillo (mock) - puedes cambiar por JWT real
    const token = 'mock-jwt-token-' + Date.now();
    const sessionUser = {
      id: user.id,
      username: user.username,
      name: user.username,
      role: user.role,
    };

    sessions.set(token, sessionUser);

    return res.status(200).json({
      success: true,
      token,
      user: sessionUser,
    });
  } catch (error) {
    console.error('[AUTH] ❌ Error login:', error);
    res.status(500).json({ error: 'Error en login' });
  }
});

app.post('/api/auth/register', (req, res) => {
  try {
    const { username, email, password } = req.body;

    if (!username || !email || !password) {
      return res.status(400).json({
        error: 'Usuario, email y contraseña requeridos',
      });
    }

    const token = 'mock-jwt-token-' + Date.now();
    
    return res.status(201).json({
      success: true,
      token,
      user: {
        id: Date.now(),
        username,
        email,
        name: username,
        role: 'user',
      },
    });
  } catch (error) {
    res.status(500).json({ error: 'Error en registro' });
  }
});

const sessions = new Map();

function getUserFromRequest(req) {
  const authHeader = req.headers.authorization || '';
  const token = authHeader.replace('Bearer ', '').trim();
  return sessions.get(token) || null;
}

app.get('/api/auth/me', (req, res) => {
  try {
    const authHeader = req.headers.authorization;
    
    if (!authHeader) {
      return res.status(401).json({
        error: 'No autorizado',
      });
    }

    const token = authHeader.replace('Bearer ', '').trim();
    const user = sessions.get(token);

    if (!user) {
      return res.status(401).json({
        error: 'Sesión no válida o expirada',
      });
    }

    return res.status(200).json({
      success: true,
      user,
    });
  } catch (error) {
    res.status(500).json({ error: 'Error obteniendo usuario' });
  }
});

// Articles Endpoint - Consulta BD real
app.get('/api/articles', async (req, res) => {
  try {
    const query = `
      SELECT
        id,
        titulo,
        COALESCE(descripcion, '') AS descripcion,
        tags,
        categoria,
        subcategoria,
        contenido,
        pasos,
        campos_formulario,
        script,
        script_json,
        contenido_md,
        tipo_solucion,
        fecha as creado_en
      FROM knowledge_base
      ORDER BY titulo
    `;

    const result = await pool.query(query);

    // Transformar datos para mantener compatibilidad
    const articles = result.rows.map(article => ({
      id: article.id,
      titulo: article.titulo,
      categoria: article.categoria,
      subcategoria: article.subcategoria,
      tags: article.tags || [],
      descripcion: article.descripcion,
      contenido: article.contenido,
      pasos: article.pasos,
      camposFormulario: article.campos_formulario,
      accionScript: article.script,
      script_json: article.script_json,
      contenido_md: article.contenido_md,
      tipo_solucion: article.tipo_solucion || 'lectura',
      creado_en: article.creado_en
    }));

    console.log(`[ARTICLES] ✅ ${articles.length} artículos obtenidos de BD`);

    res.status(200).json({
      success: true,
      data: articles
    });

  } catch (error) {
    console.error('[ARTICLES] ❌ Error obteniendo artículos:', error);
    res.status(500).json({
      success: false,
      error: 'Error interno del servidor',
      details: error.message
    });
  }
});

// Comments Endpoints
app.get('/api/comments', async (req, res) => {
  try {
    const articleId = req.query.article_id;
    if (!articleId) {
      return res.status(400).json({
        success: false,
        error: 'article_id es requerido',
      });
    }

    const result = await pool.query(
      `SELECT id, article_id, parent_id, author_username, author_role, content, created_at
       FROM comments
       WHERE article_id = $1
       ORDER BY created_at ASC`,
      [String(articleId)]
    );

    return res.status(200).json({
      success: true,
      data: result.rows,
    });
  } catch (error) {
    console.error('[COMMENTS] ❌ Error obteniendo comentarios:', error);
    return res.status(500).json({
      success: false,
      error: 'Error interno del servidor',
      details: error.message,
    });
  }
});

app.post('/api/comments', async (req, res) => {
  try {
    const { article_id, parent_id, author_username, author_role, content } = req.body;

    if (!article_id || !author_username || !content?.trim()) {
      return res.status(400).json({
        success: false,
        error: 'Campos requeridos: article_id, author_username, content',
      });
    }

    const result = await pool.query(
      `INSERT INTO comments (article_id, parent_id, author_username, author_role, content)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING id, article_id, parent_id, author_username, author_role, content, created_at`,
      [
        String(article_id),
        parent_id ? Number(parent_id) : null,
        String(author_username),
        String(author_role || 'user'),
        String(content).trim(),
      ]
    );

    return res.status(201).json({
      success: true,
      data: result.rows[0],
    });
  } catch (error) {
    console.error('[COMMENTS] ❌ Error creando comentario:', error);
    return res.status(500).json({
      success: false,
      error: 'Error interno del servidor',
      details: error.message,
    });
  }
});

app.delete('/api/comments/:id', async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (Number.isNaN(id)) {
      return res.status(400).json({
        success: false,
        error: 'ID inválido',
      });
    }

    await pool.query('DELETE FROM comments WHERE id = $1', [id]);
    return res.status(200).json({ success: true });
  } catch (error) {
    console.error('[COMMENTS] ❌ Error eliminando comentario:', error);
    return res.status(500).json({
      success: false,
      error: 'Error interno del servidor',
      details: error.message,
    });
  }
});

// POST /api/articles - Crear nuevo artículo con MD
app.post('/api/articles', async (req, res) => {
  try {
    const user = getUserFromRequest(req);
    if (!user) {
      return res.status(401).json({
        success: false,
        error: 'Usuario no autenticado'
      });
    }
    const role = req.headers['x-user-role'] || user?.role;
    if (!role || role !== 'admin') {
      return res.status(403).json({
        success: false,
        error: 'Acceso denegado: se requiere rol admin para crear artículos.'
      });
    }

    const {
      titulo,
      categoria,
      subcategoria,
      tags,
      descripcion,
      contenido_md,
      tipo_solucion,
      accion_script,
      campos_formulario,
      script_json
    } = req.body;

    // Validaciones básicas
    if (!titulo || !contenido_md) {
      return res.status(400).json({
        success: false,
        error: 'Título y contenido_md son obligatorios'
      });
    }

    if (!['lectura', 'ejecutable', 'database', 'script'].includes(tipo_solucion)) {
      return res.status(400).json({
        success: false,
        error: 'tipo_solucion debe ser "lectura", "ejecutable", "database" o "script"'
      });
    }

    if (tipo_solucion === 'script') {
      await ensureDynamicSchemaReady(false);
      const parsedBusinessScript = parseStructuredScript(script_json);
      try {
        validateBusinessScriptDefinition(parsedBusinessScript);
      } catch (validationError) {
        return res.status(400).json({
          success: false,
          error: validationError instanceof Error ? validationError.message : 'script_json empresarial inválido',
        });
      }
    }

    // Preparar datos para inserción
    const insertQuery = `
      INSERT INTO knowledge_base (
        titulo,
        descripcion,
        tags,
        contenido,
        categoria,
        subcategoria,
        contenido_md,
        tipo_solucion,
        campos_formulario,
        script,
        script_json,
        creado_por,
        fecha
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
      RETURNING id, titulo, categoria, subcategoria, contenido_md, tipo_solucion, campos_formulario, script, script_json, fecha as creado_en
    `;

    // Determinar userId (UUID) para creado_por
    let userId = user?.id;
    if (!userId && user?.username) {
      const userQuery = 'SELECT id FROM users WHERE username = $1';
      const userResult = await pool.query(userQuery, [user.username]);
      if (userResult.rowCount > 0) {
        userId = userResult.rows[0].id;
      }
    }

    if (!userId) {
      return res.status(400).json({
        success: false,
        error: 'No se pudo determinar el ID de usuario para creado_por'
      });
    }

    const values = [
      titulo,
      descripcion || '',
      tags ? (Array.isArray(tags) ? tags : tags.split(',').map(t => t.trim())) : [],
      contenido_md,  // Set contenido to the same as contenido_md
      categoria || '',
      subcategoria || '',
      contenido_md,
      tipo_solucion,
      campos_formulario ? (typeof campos_formulario === 'string' ? campos_formulario : JSON.stringify(campos_formulario)) : null,
      accion_script || null,
      script_json ? (typeof script_json === 'string' ? script_json : JSON.stringify(script_json)) : null,
      userId
    ];

    const result = await pool.query(insertQuery, values);
    const row = result.rows[0];
    const newArticle = {
      ...row,
      camposFormulario: row.campos_formulario,
      accionScript: row.script,
      script_json: row.script_json,
    };

    console.log(`[ARTICLES] ✅ Nuevo artículo creado: ${newArticle.titulo} (ID: ${newArticle.id})`);

    res.status(201).json({
      success: true,
      data: newArticle,
      message: 'Artículo creado exitosamente'
    });

  } catch (error) {
    console.error('[ARTICLES] ❌ Error creando artículo:', error);
    res.status(500).json({
      success: false,
      error: 'Error interno del servidor',
      details: error.message
    });
  }
});

// DELETE /api/articles/:id - Eliminar artículo
app.delete('/api/articles/:id', async (req, res) => {
  try {
    const user = getUserFromRequest(req);
    const role = req.headers['x-user-role'] || user?.role;
    if (!role || role !== 'admin') {
      return res.status(403).json({
        success: false,
        error: 'Acceso denegado: se requiere rol admin para eliminar artículos.'
      });
    }

    const { id } = req.params;

    if (!id) {
      return res.status(400).json({
        success: false,
        error: 'ID de artículo inválido'
      });
    }

    const deleteQuery = 'DELETE FROM knowledge_base WHERE id = $1 RETURNING id, titulo';
    const result = await pool.query(deleteQuery, [id]);

    if (result.rows.length === 0) {
      return res.status(404).json({
        success: false,
        error: 'Artículo no encontrado'
      });
    }

    console.log(`[ARTICLES] ✅ Artículo eliminado: ${result.rows[0].titulo} (ID: ${result.rows[0].id})`);

    res.status(200).json({
      success: true,
      message: 'Artículo eliminado exitosamente'
    });

  } catch (error) {
    console.error('[ARTICLES] ❌ Error eliminando artículo:', error);
    res.status(500).json({
      success: false,
      error: 'Error interno del servidor',
      details: error.message
    });
  }
});

// PUT /api/articles/:id - Actualizar un artículo existente (admin)
app.put('/api/articles/:id', async (req, res) => {
  try {
    const user = getUserFromRequest(req);
    const role = req.headers['x-user-role'] || user?.role;
    if (!role || role !== 'admin') {
      return res.status(403).json({
        success: false,
        error: 'Acceso denegado: se requiere rol admin para editar artículos.'
      });
    }

    const { id } = req.params;
    if (!id) {
      return res.status(400).json({
        success: false,
        error: 'ID de artículo requerido'
      });
    }

    const {
      titulo,
      categoria,
      subcategoria,
      tags,
      descripcion,
      contenido_md,
      tipo_solucion,
      accion_script,
      campos_formulario,
      script_json
    } = req.body;

    if (!titulo || !contenido_md || !tipo_solucion) {
      return res.status(400).json({
        success: false,
        error: 'Los campos titulo, contenido_md y tipo_solucion son requeridos'
      });
    }

    if (!['lectura', 'ejecutable', 'database', 'script'].includes(tipo_solucion)) {
      return res.status(400).json({
        success: false,
        error: 'tipo_solucion debe ser "lectura", "ejecutable", "database" o "script"'
      });
    }

    if (tipo_solucion === 'script') {
      await ensureDynamicSchemaReady(false);
      const parsedBusinessScript = parseStructuredScript(script_json);
      try {
        validateBusinessScriptDefinition(parsedBusinessScript);
      } catch (validationError) {
        return res.status(400).json({
          success: false,
          error: validationError instanceof Error ? validationError.message : 'script_json empresarial inválido',
        });
      }
    }

    const updateQuery = `
      UPDATE knowledge_base SET
        titulo = $1,
        descripcion = $2,
        tags = $3,
        contenido = $4,
        categoria = $5,
        subcategoria = $6,
        contenido_md = $7,
        tipo_solucion = $8,
        campos_formulario = $9,
        script = $10,
        script_json = $11,
        actualizado = NOW()
      WHERE id = $12
      RETURNING id, titulo, categoria, subcategoria, descripcion, tags, contenido_md, tipo_solucion, campos_formulario, script, script_json, fecha as creado_en
    `;

    const values = [
      titulo,
      descripcion || '',
      tags ? (Array.isArray(tags) ? tags : tags.split(',').map((t) => t.trim())) : [],
      contenido_md,  // Set contenido to contenido_md
      categoria || '',
      subcategoria || '',
      contenido_md,
      tipo_solucion,
      campos_formulario ? (typeof campos_formulario === 'string' ? campos_formulario : JSON.stringify(campos_formulario)) : null,
      accion_script || null,
      script_json ? (typeof script_json === 'string' ? script_json : JSON.stringify(script_json)) : null,
      id
    ];

    const result = await pool.query(updateQuery, values);

    if (result.rows.length === 0) {
      return res.status(404).json({
        success: false,
        error: 'Artículo no encontrado'
      });
    }

    const updatedRow = result.rows[0];
    res.status(200).json({
      success: true,
      data: {
        ...updatedRow,
        camposFormulario: updatedRow.campos_formulario,
        accionScript: updatedRow.script,
        script_json: updatedRow.script_json,
      },
      message: 'Artículo actualizado exitosamente'
    });

  } catch (error) {
    console.error('[ARTICLES] ❌ Error editando artículo:', error);
    res.status(500).json({
      success: false,
      error: 'Error interno del servidor',
      details: error.message
    });
  }
});

// GET /api/execute/stream/:articleId - SSE streaming endpoint for executing solution actions
app.get('/api/execute/stream/:articleId', async (req, res) => {
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
    } catch (e) {
      // Response may already be closed
    }
  }
});

// POST /api/execute - JSON endpoint for executing configured article actions
app.post('/api/execute', async (req, res) => {
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

// POST /api/repair - compatibility endpoint using same execution engine
app.post('/api/repair', async (req, res) => {
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

// GET /api/query/suggestions - Deterministic suggestions based on schema and relations
app.get('/api/query/suggestions', async (req, res) => {
  try {
    await ensureDynamicSchemaReady(false);
    const q = String(req.query.q || '');
    return res.status(200).json({
      success: true,
      suggestions: buildNaturalLanguageSuggestions(q),
    });
  } catch (error) {
    console.error('[QUERY SUGGESTIONS] ❌ Error:', error);
    return res.status(500).json({ success: false, error: 'Error interno del servidor' });
  }
});

// POST /api/sql/manual - Raw SQL endpoint using the same canonical parser as article manual SQL
app.post('/api/sql/manual', async (req, res) => {
  const user = getUserFromRequest(req);
  const role = getRoleFromRequest(req, user);
  if (!hasRequiredRole(role, 'admin')) {
    return res.status(403).json({
      success: false,
      error: 'Acceso denegado: se requiere ADMIN o SUPERADMIN para ejecutar SQL manual.',
    });
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
        : {
            ...rawBodyParams,
            __write_confirmed: req.body.writeConfirmed,
          }
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

    const result = await executeManualSqlScript({
      sql,
      databaseId: String(req.body?.databaseId || '').trim(),
    }, rawParams, (msg) => {
      const clean = String(msg || '').trim();
      if (clean) progressEvents.push(clean);
    });
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

// GET /api/query-history - historial de consultas automaticas
app.get('/api/query-history', async (req, res) => {
  const user = getUserFromRequest(req);
  const role = getRoleFromRequest(req, user);
  if (!hasRequiredRole(role, 'admin')) {
    return res.status(403).json({
      success: false,
      error: 'Acceso denegado: se requiere ADMIN o SUPERADMIN.',
    });
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
    return res.status(200).json({
      success: true,
      data: result.rows,
      count: result.rowCount || 0,
    });
  } catch (error) {
    console.error('[QUERY_HISTORY] ❌ Error:', error);
    return res.status(500).json({ success: false, error: 'Error interno del servidor' });
  }
});

// DELETE /api/query-history - limpiar historial y cache asociado (admin)
app.delete('/api/query-history', async (req, res) => {
  const user = getUserFromRequest(req);
  const role = getRoleFromRequest(req, user);
  if (!hasRequiredRole(role, 'admin')) {
    return res.status(403).json({
      success: false,
      error: 'Acceso denegado: se requiere ADMIN o SUPERADMIN.',
    });
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

// GET /api/process/:processName - Execute workflow by process slug (auto endpoint)
app.get('/api/process/:processName', async (req, res) => {
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
      // Fallback when unaccent extension is not installed
      return pool.query(
        `SELECT id, titulo
         FROM knowledge_base
         LIMIT 200`
      );
    });

    let article = articleResult.rows?.[0] || null;
    if (!article || !article.id) {
      const fallbackResult = await pool.query(
        'SELECT id, titulo FROM knowledge_base ORDER BY id DESC LIMIT 200'
      );
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
      dv: QUERY_CACHE_DATA_VERSION,
    });

    const cached = getQueryCacheEntry(cacheKey);
    if (cached) {
      return res.status(200).json({
        success: true,
        process: normalizedProcess,
        data: cached.data,
        count: cached.count,
        cached: true,
      });
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

    const payload = {
      data: rows,
      count: Number(execution.rowCount || rows.length || 0),
    };

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

    return res.status(200).json({
      success: true,
      process: normalizedProcess,
      ...payload,
      cached: false,
    });
  } catch (error) {
    console.error('[PROCESS] ❌ Error ejecutando proceso dinámico:', error);
    return res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : 'Error interno del servidor',
    });
  }
});

// GET /api/admin/tables - List available tables (admin only)
app.get('/api/admin/tables', async (req, res) => {
  try {
    const user = getUserFromRequest(req);
    const role = req.headers['x-user-role'] || user?.role;
    if (!role || role !== 'admin') {
      return res.status(403).json({
        success: false,
        error: 'Acceso denegado: se requiere rol admin',
      });
    }

    const schemaSnapshot = await ensureDynamicSchemaReady(false);
    const tables = Object.keys(schemaSnapshot.tablas || {}).sort((a, b) => a.localeCompare(b));

    res.status(200).json({
      success: true,
      data: tables
    });
  } catch (error) {
    console.error('[ADMIN] ❌ Error listando tablas:', error);
    res.status(500).json({
      success: false,
      error: 'Error interno del servidor',
      details: error.message
    });
  }
});

// GET /api/admin/tables/:tableName/columns - Get column structure of a table (admin only)
app.get('/api/admin/tables/:tableName/columns', async (req, res) => {
  try {
    const user = getUserFromRequest(req);
    const role = req.headers['x-user-role'] || user?.role;
    if (!role || role !== 'admin') {
      return res.status(403).json({
        success: false,
        error: 'Acceso denegado: se requiere rol admin',
      });
    }

    const { tableName } = req.params;

    const schemaSnapshot = await ensureDynamicSchemaReady(false);
    const normalizedTable = normalizeSchemaIdentifier(tableName);
    const tableColumns = schemaSnapshot.tablas?.[normalizedTable] || null;
    if (!normalizedTable || !tableColumns) {
      return res.status(400).json({
        success: false,
        error: 'Tabla no encontrada en schema dinámico'
      });
    }

    const result = await pool.query(`
      SELECT
        column_name,
        data_type,
        is_nullable,
        column_default
      FROM information_schema.columns
      WHERE table_schema = 'public'
      AND table_name = $1
      ORDER BY ordinal_position
    `, [normalizedTable]);

    const columns = result.rows.map(col => ({
      name: col.column_name,
      type: col.data_type,
      nullable: col.is_nullable === 'YES',
      default: col.column_default
    }));

    res.status(200).json({
      success: true,
      data: {
        tableName: normalizedTable,
        columns: columns
      }
    });
  } catch (error) {
    console.error('[ADMIN] ❌ Error obteniendo columnas:', error);
    res.status(500).json({
      success: false,
      error: 'Error interno del servidor',
      details: error.message
    });
  }
});

// GET /api/db/schema - Get database schema (tables and columns) for Workflow Builder
app.get('/api/db/schema', async (req, res) => {
  try {
    const user = getUserFromRequest(req);
    const role = req.headers['x-user-role'] || user?.role;
    if (!role || role !== 'admin') {
      return res.status(403).json({
        success: false,
        error: 'Acceso denegado: se requiere rol admin'
      });
    }

    const forceRefresh = String(req.query.refresh || '').trim().toLowerCase() === 'true';
    const schemaSnapshot = await ensureDynamicSchemaReady(forceRefresh);

    const schemaPayload = {
      tablas: schemaSnapshot.tablas,
      relaciones: schemaSnapshot.relaciones,
      loadedAt: schemaSnapshot.loadedAt,
    };

    res.status(200).json({
      success: true,
      source: 'database',
      schema: schemaPayload,
      data: schemaPayload
    });
  } catch (error) {
    console.error('[DB SCHEMA] ❌ Error obteniendo schema:', error);
    res.status(200).json({
      success: false,
      source: 'fallback',
      schema: null,
      data: null,
      error: 'No se pudo detectar el schema desde la base de datos',
      details: error.message
    });
  }
});

// GET /api/db/schema-full - Full schema with PK, FK, column types (MEJORADO CON NUEVA HERRAMIENTA)
app.get('/api/db/schema-full', async (req, res) => {
  try {
    const forceRefresh = String(req.query.refresh || '').trim().toLowerCase() === 'true';

    if (forceRefresh) {
      schemaCache.invalidate();
    }

    const connectionFingerprint = schemaDetector.getConnectionFingerprint();
    const schemaCacheKey = `full-schema:${connectionFingerprint}`;

    // Cache determinístico por huella de conexión: al cambiar .env cambia la key.
    const fullSchema = await schemaCache.getOrFetch(schemaCacheKey, async () => {
      console.log('🔍 Detectando schema con SchemaDetector...');
      return schemaDetector.getFullSchema();
    });

    res.status(200).json({
      success: true,
      source: 'database-auto-detected',
      schema: fullSchema,
      tables: fullSchema.tables || fullSchema.tablas || [],
      columns: fullSchema.columns || {},
      foreignKeys: fullSchema.foreignKeys || [],
      cacheStats: schemaCache.getStats()
    });
  } catch (error) {
    console.error('[DB SCHEMA-FULL] ❌ Error:', error.message);
    res.status(500).json({
      success: false,
      schema: null,
      error: 'Error detectando schema automáticamente',
      details: error.message
    });
  }
});

// GET /api/query/analyze - Analiza palabras clave y detecta tablas
app.get('/api/query/analyze', async (req, res) => {
  try {
    const { text } = req.query;
    
    if (!text || text.trim().length === 0) {
      return res.status(400).json({
        success: false,
        error: 'Parámetro "text" requerido'
      });
    }

    const analisis = await queryBuilder.analyzeKeywords(text);
    
    res.status(200).json({
      success: true,
      analisis,
      mensaje: 'Análisis de palabras clave completado'
    });
  } catch (error) {
    console.error('[QUERY ANALYZE] ❌ Error:', error.message);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// POST /api/query/generate - Genera SQL automáticamente basado en entrada
// BODY: { "text": "usuarios con logs", "limit": 10, "offset": 0 }
app.post('/api/query/generate', async (req, res) => {
  try {
    const { text, limit = 50, offset = 0 } = req.body;
    await refreshSemanticLearningCache(false);
    
    if (!text || text.trim().length === 0) {
      return res.status(400).json({
        success: false,
        error: 'Campo "text" es requerido en el body'
      });
    }

    // MODO ANÁLISIS DE ERRORES
    const errorAnalysisResponse = await resolveErrorAnalysisResponse(text, pool);
    if (errorAnalysisResponse) {
      return res.status(200).json(errorAnalysisResponse);
    }

    const normalizedLimit = Math.min(parseInt(limit) || 50, 1000);
    const normalizedOffset = Math.max(parseInt(offset) || 0, 0);

    // 0) Ollama SQL generation — handles ANY natural-language DB question
    // Runs before the semantic engine so mis-parses ("entidad 'de'", ILIKE on engine names) never happen.
    console.log(`[AI-SQL-GEN] PASO 0 iniciado para: "${text}"`);
    try {
      await ensureMultiDbReady({ reason: 'query-generate-ai' });
      const _ollamaRegistry = multiDbRegistry;
      const _ollamaGenerated = await (async () => {
        const allDbs = typeof _ollamaRegistry?.getDatabases === 'function' ? _ollamaRegistry.getDatabases() : [];
        const dbList = allDbs.filter((d) => d.enabled).map((d) => `${d.id} (${d.type})`).join(', ');

        // Build schema from all DBs
        const schemaParts = [];
        for (const db of allDbs.filter((d) => d.enabled)) {
          try {
            let metaSql;
            if (db.type === 'postgres') metaSql = "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name, ordinal_position";
            else if (db.type === 'oracle') metaSql = 'SELECT table_name, column_name FROM USER_TAB_COLUMNS ORDER BY table_name, column_id';
            else continue;
            const rows = await _ollamaRegistry.executeCompiledQuery({ databaseId: db.id, sql: metaSql, params: [] });
            const grouped = new Map();
            for (const row of rows || []) {
              const t = String(row.TABLE_NAME || row.table_name || '').trim();
              const c = String(row.COLUMN_NAME || row.column_name || '').trim();
              if (t && c) { const curr = grouped.get(t) || []; if (curr.length < 6) curr.push(c); grouped.set(t, curr); }
            }
            if (grouped.size > 0) {
              schemaParts.push(`--- ${db.type.toUpperCase()} (${db.id}) ---`);
              for (const [t, cols] of [...grouped.entries()].slice(0, 12)) schemaParts.push(`${t}(${cols.join(', ')})`);
            }
          } catch { /* skip */ }
        }
        const schema = schemaParts.length > 0 ? schemaParts.join('\n') : 'Sin esquema disponible';

        const prompt = [
          'You are a SQL generator. Read the user question and return ONLY a compact JSON on one line.',
          '',
          'STRICT RULES:',
          '1. Output ONLY a single-line JSON object. No markdown, no code blocks, no extra text.',
          '2. Format: {"sql":"...","database_id":"...","explanation":"..."}',
          '3. explanation must be SHORT (max 10 words in Spanish). Keep the whole JSON under 250 chars.',
          '4. sql: one SELECT only. No bind variables (:1 :name). No placeholder values like \'your-value\'.',
          '5. NEVER add WHERE filters unless the user explicitly asks to filter by a value.',
          '6. When the user asks to "show table X" or "dame la tabla X" → use SELECT * with a row limit. No WHERE.',
          '7. Never INSERT/UPDATE/DELETE/DROP/ALTER/TRUNCATE.',
          '8. database_id: exact id from Available databases list.',
          '9. Never use backtick quotes. Use plain identifiers.',
          '10. PostgreSQL row limit: LIMIT 50. Oracle row limit: WHERE ROWNUM <= 50. Never mix them.',
          '',
          'EXAMPLES (copy this exact syntax):',
          '  User asks to list Oracle tables  → {"sql":"SELECT table_name FROM USER_TABLES ORDER BY table_name","database_id":"oracle_test","explanation":"Lista tablas Oracle"}',
          '  User asks to show Oracle table T → {"sql":"SELECT * FROM T WHERE ROWNUM <= 50","database_id":"oracle_test","explanation":"Datos de T"}',
          '  User asks to describe Oracle T   → {"sql":"SELECT column_name,data_type,nullable FROM USER_TAB_COLUMNS WHERE table_name=\'T\' ORDER BY column_id","database_id":"oracle_test","explanation":"Estructura de T"}',
          '  User asks to list Postgres tables → {"sql":"SELECT table_name FROM information_schema.tables WHERE table_schema=\'public\' ORDER BY table_name","database_id":"pg_main","explanation":"Lista tablas Postgres"}',
          '  User asks to show Postgres table T → {"sql":"SELECT * FROM T LIMIT 50","database_id":"pg_main","explanation":"Datos de T"}',
          '  User asks to describe Postgres T  → {"sql":"SELECT column_name,data_type,is_nullable FROM information_schema.columns WHERE table_name=\'T\' ORDER BY ordinal_position","database_id":"pg_main","explanation":"Estructura de T"}',
          '',
          `Available databases: ${dbList}`,
          '',
          'Schema (use real table names from here):',
          schema,
          '',
          `User question: ${text}`,
          '',
          'JSON (one line, no extra text):',
        ].join('\n');

        const { askOllama: _ask } = await import('./infrastructure/ai/OllamaClient.js');
        console.log(`[AI-SQL-GEN] llamando Ollama model=${process.env.OLLAMA_MODEL || 'deepseek-coder'} timeout=30000ms`);
        const response = await _ask(prompt, { timeoutMs: 30000, model: process.env.OLLAMA_MODEL || undefined });
        console.log(`[AI-SQL-GEN] Ollama ok=${response?.ok} json=${response?.json ? 'presente' : 'null'} error=${response?.error || 'ninguno'}`);
        if (!response?.ok || !response?.json) return null;

        const sql = String(response.json?.sql || '').trim();
        let databaseId = String(response.json?.database_id || '').trim();
        const explanation = String(response.json?.explanation || '').trim();

        console.log(`[AI-SQL-GEN] JSON recibido: sql="${sql.slice(0,80)}" db="${databaseId}"`);

        if (!sql) { console.warn('[AI-SQL-GEN] rechazado: sql vacío'); return null; }
        // Block non-SELECT and bind variables
        const norm = sql.replace(/\s+/g, ' ').toLowerCase();
        if (!norm.startsWith('select') && !norm.startsWith('with')) { console.warn(`[AI-SQL-GEN] rechazado: no es SELECT/WITH`); return null; }
        if (/\b(insert|update|delete|drop|truncate|alter)\b/.test(norm)) { console.warn('[AI-SQL-GEN] rechazado: contiene DML/DDL'); return null; }
        if (/:[a-zA-Z0-9_]+/.test(sql)) { console.warn('[AI-SQL-GEN] rechazado: bind placeholder detectado'); return null; }

        // Validate/fallback databaseId
        const targetDb = allDbs.find((d) => d.id === databaseId && d.enabled);
        if (!targetDb) {
          const oracleMentioned = /oracle/i.test(text);
          const postgresMentioned = /postgres/i.test(text);
          const fb = oracleMentioned ? allDbs.find((d) => d.type === 'oracle' && d.enabled)
            : postgresMentioned ? allDbs.find((d) => d.type === 'postgres' && d.enabled)
            : allDbs.find((d) => d.enabled);
          if (!fb) return null;
          databaseId = fb.id;
        }

        // ── Sanitize SQL dialect errors from Ollama ──────────────────────────
        const resolvedDbType = allDbs.find((d) => d.id === databaseId)?.type || 'postgres';
        let cleanSql = sql
          // Remove trailing semicolon (Oracle rejects it in driver calls)
          .trimEnd().replace(/;+$/, '')
          // Remove MySQL-style backtick quoting → plain identifier
          .replace(/`([^`]+)`/g, '$1')
          // Remove hallucinated placeholder WHERE clauses like WHERE col='your-value' / WHERE col=?
          .replace(/\bWHERE\s+\w+\s*=\s*'your-[^']*'/gi, '')
          .replace(/\bWHERE\s+\w+\s*=\s*\?/gi, '')
          .replace(/\bAND\s+\w+\s*=\s*'your-[^']*'/gi, '')
          .replace(/\bAND\s+\w+\s*=\s*\?/gi, '')
          .trim();

        if (resolvedDbType === 'postgres') {
          // Oracle ROWNUM → Postgres LIMIT
          cleanSql = cleanSql
            .replace(/\bWHERE\s+ROWNUM\s*<=\s*(\d+)/gi, 'LIMIT $1')
            .replace(/\bWHERE\s+ROWNUM\s*<\s*(\d+)/gi, (_, n) => `LIMIT ${Math.max(1, parseInt(n) - 1)}`)
            .replace(/\bAND\s+ROWNUM\s*<=\s*(\d+)/gi, 'LIMIT $1')
            .replace(/\bAND\s+ROWNUM\s*<\s*(\d+)/gi, (_, n) => `LIMIT ${Math.max(1, parseInt(n) - 1)}`);
          // Add LIMIT if there's none and no WHERE clause
          if (!/\bLIMIT\b/i.test(cleanSql) && !/\bWHERE\b/i.test(cleanSql)) {
            cleanSql = cleanSql + ' LIMIT 50';
          }
        } else if (resolvedDbType === 'oracle') {
          // Postgres LIMIT → Oracle ROWNUM
          cleanSql = cleanSql.replace(/\bLIMIT\s+(\d+)/gi, 'WHERE ROWNUM <= $1');
          // Fix double WHERE: "WHERE x=1 WHERE ROWNUM" → "WHERE x=1 AND ROWNUM"
          cleanSql = cleanSql.replace(/WHERE\s+((?!ROWNUM).+?)\s+WHERE\s+ROWNUM/i, 'WHERE $1 AND ROWNUM');
        }

        console.log(`[AI-SQL-GEN] Parsed: sql="${cleanSql.slice(0, 120)}" db="${databaseId}"`);
        return { sql: cleanSql, databaseId, explanation };
      })();

      if (_ollamaGenerated?.sql && _ollamaGenerated?.databaseId) {
        console.log(`[AI-SQL-GEN] generate "${text}" → ${_ollamaGenerated.databaseId}: ${_ollamaGenerated.sql.slice(0, 80)}`);
        return res.status(200).json({
          success: true,
          exito: true,
          query: { sql: _ollamaGenerated.sql, databaseId: _ollamaGenerated.databaseId },
          sql: _ollamaGenerated.sql,
          tablaBase: null,
          debug: { motor: 'ollama-sql-gen', databaseId: _ollamaGenerated.databaseId },
          timestamp: Date.now(),
        });
      }
    } catch (ollamaGenErr) {
      console.warn('[AI-SQL-GEN] generate fallback:', ollamaGenErr instanceof Error ? ollamaGenErr.message : ollamaGenErr);
    }

    // 1) Preferred path: distributed intelligence engine (same core used by /api/query)
    // This gives better entity detection from schema + semantic learning.
    let distributedError = null;
    try {
      await ensureMultiDbReady({ reason: 'query-generate' });
      const requestedDatabases = resolveRequestedDatabasesForQuery(text, req.body?.databases || []);
      const distributedResult = await multiDatabaseEngine.execute(text, {
        limit: normalizedLimit,
        offset: normalizedOffset,
        databases: requestedDatabases,
      });

      if (distributedResult?.success && distributedResult.executionType !== 'unresolved') {
        const primarySql = distributedResult?.scripts?.[0]?.sql || null;
        const selectedTable = distributedResult?.resolution?.entities?.[0]?.primary?.tableName
          || distributedResult?.resolution?.entities?.[0]?.table
          || null;
        const confidence = Number(distributedResult?.confidence || distributedResult?.resolution?.confidence || 0);

        if (primarySql) {
          const syntheticResult = {
            exito: true,
            query: { sql: primarySql },
            analisis: { tablaBase: selectedTable },
            debug: {
              tablaSeleccionada: selectedTable,
              confianza: confidence,
              motor: 'multi-database-engine',
              executionType: distributedResult.executionType,
              warnings: distributedResult.warnings || [],
              sources: distributedResult.source || [],
            },
          };

          await learnSemanticMappingsFromSuccessfulQuery(text, syntheticResult);

          return res.status(200).json({
            success: true,
            sql: primarySql,
            tablaBase: selectedTable,
            debug: syntheticResult.debug,
            timestamp: Date.now(),
            ...syntheticResult,
          });
        }
      } else {
        distributedError = distributedResult?.message || distributedResult?.error || null;
      }
    } catch (error) {
      distributedError = error instanceof Error ? error.message : String(error || '');
    }

    // 2) Compatibility fallback: legacy queryBuilder.generateQuery
    const resultado = await queryBuilder.generateQuery(text, {
      limit: normalizedLimit,
      offset: normalizedOffset,
    });

    if (!resultado.exito) {
      return res.status(400).json({
        success: false,
        error: distributedError || resultado?.error || 'No se pudo interpretar la consulta con suficiente confianza.',
        sql: null,
        tablaBase: null,
        debug: {
          ...(resultado?.debug || {}),
          distributedError,
        },
        timestamp: Date.now(),
        ...resultado,
      });
    }

    await learnSemanticMappingsFromSuccessfulQuery(text, resultado);

    res.status(200).json({
      success: true,
      sql: resultado?.query?.sql || null,
      tablaBase: resultado?.analisis?.tablaBase || resultado?.debug?.tablaSeleccionada || null,
      debug: resultado?.debug || null,
      timestamp: Date.now(),
      ...resultado,
    });
  } catch (error) {
    console.error('[QUERY GENERATE] ❌ Error:', error.message);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// POST /api/query/execute-generated - Ejecuta una consulta generada automáticamente
// BODY: { "sql": "SELECT ...", "mode": "simple" }
app.post('/api/query/execute-generated', async (req, res) => {
  try {
    const user = getUserFromRequest(req);
    const role = normalizeRole(req.headers['x-user-role'] || user?.role || 'user');
    const { sql } = req.body;

    if (!sql || sql.trim().length === 0) {
      return res.status(400).json({
        success: false,
        error: 'Campo "sql" es requerido'
      });
    }

    const proceduralMode = shouldForceProceduralMode(sql) || isComplexSqlScript(sql);
    if (proceduralMode) {
      if (!hasRequiredRole(role, 'admin')) {
        return res.status(403).json({
          success: false,
          error: 'Scripts procedurales requieren rol ADMIN o SUPERADMIN.',
        });
      }

      const progress = [
        '🔍 Paso 1: Analizando script',
        '📊 Paso 2: Procesando lógica',
        '📈 Paso 3: Generando resultado',
      ];

      const proceduralResult = await executeManualSqlScript({ sql }, {}, (msg) => {
        const clean = String(msg || '').trim();
        if (clean) progress.push(clean);
      });

      const executionMs = Number(proceduralResult?.executionMs || 0);
      const rows = Array.isArray(proceduralResult?.data) ? proceduralResult.data : [];

      await logQueryHistory({
        username: user?.username || 'anonymous',
        role,
        queryText: null,
        generatedSql: sql,
        executionMs,
        rowCount: rows.length,
        wasCached: false,
        status: 'ok'
      });

      return res.status(200).json(
        buildProductionSqlResponse({
          success: true,
          rows,
          notices: (proceduralResult?.notices || []).concat(progress),
          semanticCandidate: proceduralResult?.resultadoSemantico || (rows[0] || {}),
          explicacion: proceduralResult?.resumenHumano || proceduralResult?.explicacion || proceduralResult?.message || '',
          message: proceduralResult?.message || 'Script procedural ejecutado correctamente',
        }),
      );
    }

    const explicitDatabaseId = String(req.body?.databaseId || '').trim();
    const explicitDatabaseHint = String(req.body?.databaseHint || '').trim();

    // ── Directive + syntax-based routing ─────────────────────────────────────
    const _directiveId = extractDatabaseDirective(sql);
    const _detectedEngine = (!explicitDatabaseId && !explicitDatabaseHint && !_directiveId)
      ? detectSqlSyntaxEngine(sql) : null;
    const _engineDb = _detectedEngine ? multiDbRegistry.findDatabaseByType(_detectedEngine) : null;

    // Native Oracle PL/SQL execution (BEGIN...END with Oracle packages)
    const plsqlDirectiveDb = _directiveId ? multiDbRegistry.getDatabaseById(_directiveId) : null;
    const oraclePlSqlTarget = plsqlDirectiveDb?.type === 'oracle'
      ? plsqlDirectiveDb
      : (isNativePlSql(sql) ? multiDbRegistry.findDatabaseByType('oracle') : null);

    if (oraclePlSqlTarget) {
      const plsqlRows = await multiDbRegistry.executeOraclePlSqlBlock(oraclePlSqlTarget.id, sql);
      await logQueryHistory({
        username: user?.username || 'anonymous',
        role,
        queryText: null,
        generatedSql: sql,
        executionMs: 0,
        rowCount: plsqlRows.length,
        wasCached: false,
        status: 'ok',
      });
      return res.status(200).json(
        buildProductionSqlResponse({
          success: true,
          rows: plsqlRows,
          notices: [buildEngineLog({ engine: 'oracle', queryType: 'plsql', databaseId: oraclePlSqlTarget.id, reason: 'native PL/SQL execution', sql })],
          explicacion: 'Script PL/SQL ejecutado en Oracle.',
        }),
      );
    }

    // Propagate directive/engine routing to executeSafeSelectQuery
    if (_directiveId && !explicitDatabaseId) {
      req.body = { ...req.body, databaseId: _directiveId };
    } else if (_engineDb && !explicitDatabaseId && !explicitDatabaseHint) {
      req.body = { ...req.body, databaseHint: _detectedEngine };
    }

    // Validar SQL solo cuando no hay routing explícito multi-DB.
    // En modo multi-DB, el control final lo realiza executeSafeSelectQuery.
    if (!explicitDatabaseId && !explicitDatabaseHint && !_directiveId && !_engineDb) {
      const validacion = queryBuilder.validateSQL(sql, role);
      if (!validacion.valido) {
        return res.status(403).json({
          success: false,
          error: `SQL rechazado: ${validacion.razon}`
        });
      }
    }

    // Ejecutar con seguridad (incluye auto-reparación para compatibilidad de tipos)
    const startTime = Date.now();
    let sqlToRun = String(sql || '');
    let autoRepaired = false;
    let result;

    const executeGeneratedSelectOptions = buildCrossDbSelectOptions({
      databaseId: req.body?.databaseId,
      requestedDatabases: Array.isArray(req.body?.databases) ? req.body.databases : [],
      queryHintText: [req.body?.query, req.body?.text, req.body?.prompt].filter(Boolean).join(' '),
    });

    try {
      result = await executeSafeSelectQuery(sqlToRun, [], 5000, executeGeneratedSelectOptions);
    } catch (firstError) {
      const message = String(firstError?.message || '');
      const looksLikeIlikeTypeError = /operator does not exist: .*~~\*/i.test(message);

      if (!looksLikeIlikeTypeError) throw firstError;

      const repairedSql = normalizeGeneratedSelectSql(sqlToRun);
      if (!repairedSql || repairedSql === sqlToRun) throw firstError;

      const repairedValidation = queryBuilder.validateSQL(repairedSql, role);
      if (!repairedValidation.valido) throw firstError;

      sqlToRun = repairedSql;
      autoRepaired = true;
      result = await executeSafeSelectQuery(sqlToRun, [], 5000, executeGeneratedSelectOptions);
    }

    const executionMs = Date.now() - startTime;

    // Registrar en historial
    await logQueryHistory({
      username: user?.username || 'anonymous',
      role,
      queryText: null,
      generatedSql: sqlToRun,
      executionMs,
      rowCount: result.rowCount || 0,
      wasCached: false,
      status: 'ok'
    });

    const emptyResult = queryBuilder.handleEmptyResults({ rows: result.rows, rowCount: result.rowCount || 0 }, { sql: sqlToRun });

    res.status(200).json(
      buildProductionSqlResponse({
        success: true,
        rows: emptyResult.data,
        notices: [],
        semanticCandidate: emptyResult.data?.[0] || {},
        explicacion: buildHumanSqlExplanation(emptyResult.data || []),
        message: emptyResult.message || '',
      }),
    );
  } catch (error) {
    console.error('[QUERY EXECUTE] ❌ Error:', error.message);

    // Registrar error en historial
    const user = getUserFromRequest(req);
    await logQueryHistory({
      username: user?.username || 'anonymous',
      role: normalizeRole(req.headers['x-user-role'] || 'user'),
      queryText: null,
      generatedSql: req.body.sql || '',
      executionMs: 0,
      rowCount: 0,
      wasCached: false,
      status: 'error'
    });

    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// POST /api/query/analyze - Analiza consulta y detecta si requiere input
// BODY: { "text": "usuario admin" }
// RESPONSE: { inputRequired: boolean, prompt: string, analysis: {...} }
app.post('/api/query/analyze', async (req, res) => {
  try {
    const { text } = req.body;

    if (!text || text.trim().length === 0) {
      return res.status(400).json({
        success: false,
        error: 'Campo "text" es requerido'
      });
    }

    const analysis = queryAnalyzer.analyzeQuery(text);

    res.status(200).json({
      success: true,
      analysis,
      inputRequired: analysis.inputRequerido.requiereInput,
      prompt: analysis.inputPrompt,
      canExecute: analysis.puedeEjecutarse,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('[QUERY ANALYZE] ❌ Error:', error.message);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// POST /api/query/execute-with-explanation - Ejecuta query Y genera explicación
// BODY: { "text": "usuarios con logs", "inputValue": null, "parameterValue": null }
// RESPONSE: { explanation: string, data: [...], analysisDetails: {...} }
// 
// SOPORTA FLUJO PARAMETRIZADO:
// Si text es incompleto (ej: "usuario") y no hay parameterValue:
//   → Retorna prompt pidiendo parámetro
// Si hay parameterValue, lo inyecta y ejecuta normalmente
app.post('/api/query/execute-with-explanation', async (req, res) => {
  try {
    const user = getUserFromRequest(req);
    const role = normalizeRole(req.headers['x-user-role'] || user?.role || 'user');
    const { text, inputValue, parameterValue, limit = 50, offset = 0, databases = [] } = req.body;
    await refreshSemanticLearningCache(false);

    if (!text || text.trim().length === 0) {
      return res.status(400).json({
        success: false,
        error: 'Campo "text" es requerido'
      });
    }

    // ============================================================
    // MODO ANÁLISIS DE ERRORES
    // Detectar si el usuario pegó un mensaje de error o stack trace
    // ============================================================
    const errorAnalysisResponse = await resolveErrorAnalysisResponse(text, pool);
    if (errorAnalysisResponse) {
      return res.status(200).json(errorAnalysisResponse);
    }

    // ============================================================
    // NUEVA CAPA: DETECCIÓN DE CONSULTAS PARAMETRIZADAS
    // ============================================================
    // Verificar si es una consulta que requiere parámetro
    const parameterDetection = queryParameterizer.processParameterizedQuery(text, parameterValue);

    // Si requiere parámetro y no lo tiene
    if (parameterDetection.esParametrizado && parameterDetection.requiereInput && !parameterValue) {
      return res.status(202).json({
        success: false,
        requiresParameter: true,
        prompt: parameterDetection.prompt,
        message: `Esta consulta requiere un parámetro: ${parameterDetection.prompt.message}`,
        expectedField: parameterDetection.prompt.field,
        examples: parameterDetection.prompt.examples,
        queryOriginal: parameterDetection.inputOriginal,
        httpCode: 202
      });
    }

    // Si hay parámetro, inyectarlo en la query
    let finalText = text;
    if (parameterDetection.esParametrizado && parameterValue && parameterDetection.queryEnhanced) {
      finalText = parameterDetection.queryEnhanced;
    }

    // Si hay inputValue (compatibilidad con flujo anterior), usarlo también
    const effectiveInputValue = parameterValue || inputValue;

    // Decisión de ejecución: normal (1 base) vs distribuida (múltiples bases)
    const effectiveDatabases = resolveRequestedDatabasesForQuery(finalText, databases);
    const executionDecision = decideQueryExecutionMode(finalText, effectiveDatabases);
    if (executionDecision.mode !== 'normal') {
      const distributedExecution = await executeDistributedWithExplanation({
        text: finalText,
        parameterValue: effectiveInputValue,
        limit,
        databases: effectiveDatabases,
        user,
        role,
        sourceRoute: '/api/query/execute-with-explanation',
      });

      return res.status(distributedExecution.statusCode).json(distributedExecution.payload);
    }

    // ============================================================
    // FLUJO NORMAL: ANÁLISIS Y EJECUCIÓN
    // ============================================================

    // FASE 1-4: Analizar y validar
    const analysis = queryAnalyzer.analyzeQuery(finalText, effectiveInputValue);

    // Si requiere input y no lo tiene, retornar prompt
    if (analysis.inputRequerido.requiereInput) {
      return res.status(400).json({
        success: false,
        requiresInput: true,
        prompt: analysis.inputPrompt,
        analysis,
        message: analysis.inputRequerido.sugerencia
      });
    }

    // Si hay error de validación de input
    if (analysis.inputValidacion && !analysis.inputValidacion.valido) {
      return res.status(400).json({
        success: false,
        error: `Validación fallida: ${analysis.inputValidacion.razon}`,
        analysis
      });
    }

    // Generar SQL con el input validado o sin él
    const modifiedText = effectiveInputValue ? 
      `${finalText} "${effectiveInputValue}"` : 
      finalText;

    const queryResult = await queryBuilder.generateQuery(modifiedText, {
      limit: Math.min(parseInt(limit) || 50, 1000),
      offset: Math.max(parseInt(offset) || 0, 0)
    });

    if (!queryResult.exito) {
      return res.status(400).json({
        success: false,
        error: queryResult.error || 'No se pudo generar la consulta SQL',
        analysis,
        sugerencias: queryResult.sugerencias
      });
    }

    // Validar SQL generado
    const validacion = queryBuilder.validateSQL(queryResult.query.sql, role);
    if (!validacion.valido) {
      return res.status(403).json({
        success: false,
        error: `SQL rechazado: ${validacion.razon}`,
        analysis
      });
    }

    // FASE 5-6: Ejecutar SQL y generar explicación
    const startTime = Date.now();
    let result;

    const explanationSelectOptions = buildCrossDbSelectOptions({
      requestedDatabases: effectiveDatabases,
      queryHintText: finalText,
    });

    try {
      result = await executeSafeSelectQuery(queryResult.query.sql, [], 5000, explanationSelectOptions);
    } catch (firstError) {
      const message = String(firstError?.message || '');
      const looksLikeIlikeTypeError = /operator does not exist: .*~~\*/i.test(message);

      if (!looksLikeIlikeTypeError) throw firstError;

      const repairedSql = normalizeGeneratedSelectSql(queryResult.query.sql);
      if (!repairedSql || repairedSql === queryResult.query.sql) throw firstError;

      const repairedValidation = queryBuilder.validateSQL(repairedSql, role);
      if (!repairedValidation.valido) throw firstError;

      result = await executeSafeSelectQuery(repairedSql, [], 5000, explanationSelectOptions);
    }

    const executionMs = Date.now() - startTime;

    // Generar explicación ANTES de retornar datos
    const explanationResult = resultExplainer.explain(result.rows, queryResult.query.sql);
    const explanation = explanationResult.explicacionCompleta;

    // Registrar en historial
    await logQueryHistory({
      username: user?.username || 'anonymous',
      role,
      queryText: text,
      generatedSql: queryResult.query.sql,
      executionMs,
      rowCount: result.rowCount || 0,
      wasCached: false,
      status: 'ok'
    });

    await learnSemanticMappingsFromSuccessfulQuery(text, queryResult);

    const emptyResult = queryBuilder.handleEmptyResults({ rows: result.rows, rowCount: result.rowCount || 0 }, { input: text, queryResult });

    // RESULTADO FINAL: Explicación + JSON
    res.status(200).json(
      buildProductionSqlResponse({
        success: true,
        rows: emptyResult.data,
        notices: [],
        semanticCandidate: emptyResult.data?.[0] || {},
        explicacion: explanation,
        message: emptyResult.message || '',
      }),
    );
  } catch (error) {
    console.error('[QUERY EXECUTE WITH EXPLANATION] ❌ Error:', error.message);

    const user = getUserFromRequest(req);
    await logQueryHistory({
      username: user?.username || 'anonymous',
      role: normalizeRole(req.headers['x-user-role'] || 'user'),
      queryText: req.body.text || '',
      generatedSql: '',
      executionMs: 0,
      rowCount: 0,
      wasCached: false,
      status: 'error'
    });

    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// POST /api/query/parameterized - Flujo parametrizado para consultas incompletas
// PASO 1: Usuario envía texto incompleto
//   Body: { "text": "usuario" }
//   Response: { requiresParameter: true, prompt: {...} }
//
// PASO 2: Usuario envía parámetro
//   Body: { "text": "usuario", "parameterValue": "admin" }
//   Response: { explanation: "...", data: [...] }
app.post('/api/query/parameterized', async (req, res) => {
  try {
    const user = getUserFromRequest(req);
    const role = normalizeRole(req.headers['x-user-role'] || user?.role || 'user');
    const { text, parameterValue, limit = 50, offset = 0, databases = [] } = req.body;
    await refreshSemanticLearningCache(false);

    if (!text || text.trim().length === 0) {
      return res.status(400).json({
        success: false,
        error: 'Campo "text" es requerido'
      });
    }

    const errorAnalysisResponse = await resolveErrorAnalysisResponse(text, pool);
    if (errorAnalysisResponse) {
      return res.status(200).json(errorAnalysisResponse);
    }

    // FASE 1: Detectar si requiere parámetro
    const parameterDetection = queryParameterizer.processParameterizedQuery(text, parameterValue);

    // FASE 2: Si requiere parámetro, retornar prompt
    if (parameterDetection.esParametrizado && parameterDetection.requiereInput && !parameterValue) {
      return res.status(202).json({
        success: false,
        requiresParameter: true,
        field: parameterDetection.prompt.field,
        message: parameterDetection.prompt.message,
        examples: parameterDetection.prompt.examples,
        prompt: parameterDetection.prompt,
        queryText: parameterDetection.inputOriginal,
        httpStatus: 202,
        note: 'Envíe nuevamente con parameterValue para completar la consulta'
      });
    }

    // Si hay error de validación
    if (parameterDetection.error) {
      return res.status(400).json({
        success: false,
        error: parameterDetection.error,
        queryText: parameterDetection.inputOriginal
      });
    }

    // FASE 3: Inyectar parámetro si es necesario
    let finalText = parameterDetection.queryEnhanced || text;

    // Decisión de ejecución: normal (1 base) vs distribuida (múltiples bases)
    const effectiveDatabases = resolveRequestedDatabasesForQuery(finalText, databases);
    const executionDecision = decideQueryExecutionMode(finalText, effectiveDatabases);
    if (executionDecision.mode !== 'normal') {
      const distributedExecution = await executeDistributedWithExplanation({
        text: finalText,
        parameterValue,
        limit,
        databases: effectiveDatabases,
        user,
        role,
        sourceRoute: '/api/query/parameterized',
      });

      return res.status(distributedExecution.statusCode).json(distributedExecution.payload);
    }

    // FASE 4: Ejecutar con QueryBuilder
    const queryResult = await queryBuilder.generateQuery(finalText, {
      limit: Math.min(parseInt(limit) || 50, 1000),
      offset: Math.max(parseInt(offset) || 0, 0)
    });

    if (!queryResult.exito) {
      return res.status(400).json({
        success: false,
        error: queryResult.error || 'No se pudo generar la consulta SQL',
        sugerencias: queryResult.sugerencias
      });
    }

    // Validar SQL
    const validacion = queryBuilder.validateSQL(queryResult.query.sql, role);
    if (!validacion.valido) {
      return res.status(403).json({
        success: false,
        error: `SQL rechazado: ${validacion.razon}`
      });
    }

    // FASE 5: Ejecutar SQL
    const startTime = Date.now();
    let result;

    const parameterizedSelectOptions = buildCrossDbSelectOptions({
      requestedDatabases: effectiveDatabases,
      queryHintText: finalText,
    });

    try {
      result = await executeSafeSelectQuery(queryResult.query.sql, [], 5000, parameterizedSelectOptions);
    } catch (firstError) {
      const message = String(firstError?.message || '');
      const looksLikeIlikeTypeError = /operator does not exist: .*~~\*/i.test(message);

      if (!looksLikeIlikeTypeError) throw firstError;

      const repairedSql = normalizeGeneratedSelectSql(queryResult.query.sql);
      if (!repairedSql || repairedSql === queryResult.query.sql) throw firstError;

      const repairedValidation = queryBuilder.validateSQL(repairedSql, role);
      if (!repairedValidation.valido) throw firstError;

      result = await executeSafeSelectQuery(repairedSql, [], 5000, parameterizedSelectOptions);
    }

    const executionMs = Date.now() - startTime;

    // FASE 6: Generar explicación
    const explanationResult = resultExplainer.explain(result.rows, queryResult.query.sql);
    const explanation = explanationResult.explicacionCompleta;

    // Registrar en historial
    await logQueryHistory({
      username: user?.username || 'anonymous',
      role,
      queryText: text,
      generatedSql: queryResult.query.sql,
      executionMs,
      rowCount: result.rowCount || 0,
      wasCached: false,
      status: 'ok'
    });

    await learnSemanticMappingsFromSuccessfulQuery(text, queryResult);

    const emptyResult = queryBuilder.handleEmptyResults({ rows: result.rows, rowCount: result.rowCount || 0 }, { input: text, queryResult });

    // FASE 7: Respuesta final con explicación + datos
    res.status(200).json(
      buildProductionSqlResponse({
        success: true,
        rows: emptyResult.data,
        notices: [],
        semanticCandidate: emptyResult.data?.[0] || {},
        explicacion: explanation,
        message: emptyResult.message || '',
      }),
    );
  } catch (error) {
    console.error('[QUERY PARAMETERIZED] ❌ Error:', error.message);

    const user = getUserFromRequest(req);
    await logQueryHistory({
      username: user?.username || 'anonymous',
      role: normalizeRole(req.headers['x-user-role'] || 'user'),
      queryText: req.body.text || '',
      generatedSql: '',
      executionMs: 0,
      rowCount: 0,
      wasCached: false,
      status: 'error'
    });

    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// GET /api/query/suggestions - Deterministic suggestions based on schema and relations
app.get('/api/query/suggestions', async (req, res) => {
  try {
    await ensureDynamicSchemaReady(false);
    await refreshSemanticLearningCache(false);
    const q = String(req.query.q || '');
    return res.status(200).json({
      success: true,
      suggestions: buildNaturalLanguageSuggestions(q),
    });
  } catch (error) {
    console.error('[QUERY SUGGESTIONS] ❌ Error:', error);
    return res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : 'Error interno del servidor',
    });
  }
});

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
