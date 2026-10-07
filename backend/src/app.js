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
import {
  BUSINESS_ACTION_CATALOG,
  createBusinessActionsRegistry,
} from './actionsRegistry.js';
import SchemaDetector from './infrastructure/database/SchemaDetector.js';
import SchemaCache from './infrastructure/cache/SchemaCache.js';
import QueryBuilder from './infrastructure/query/QueryBuilder.js';
import QueryAnalyzer from './dominio/consultas/QueryAnalyzer.js';
import ResultExplainer from './dominio/presentacion/ResultExplainer.js';
import QueryParameterizer from './dominio/consultas/QueryParameterizer.js';
import MultiDbRegistry from './infrastructure/distributed/MultiDbRegistry.js';
import MultiDatabaseEngine from './infrastructure/distributed/MultiDatabaseEngine.js';
import { isComplexSqlScript, shouldForceProceduralMode } from './dominio/sql/plsql.js';
import {
  detectSqlSyntaxEngine,
  extractDatabaseDirective,
  isNativePlSql,
  buildEngineLog,
} from './dominio/sql/SqlEngineDetector.js';
import { bootstrapGlobalSystems } from './bootstrap.js';
import { createQueryRouter } from './adaptadores/http/QueryRoute.js';
import { initDynamicRoutes } from './adaptadores/http/DynamicRoutes.js';
import { createResponseSecurityMiddleware } from './adaptadores/http/responseSecurity.js';
import { getUserFromRequest, getUserFromRequestAsync, initSessionStore, adjuntarSesion } from './adaptadores/http/sesion.js';
import { createAuthRouter } from './adaptadores/http/authRoute.js';
import { createContentRouter } from './adaptadores/http/contentRoute.js';
import { createExecuteRouter, createRepairRouter } from './adaptadores/http/executeRoute.js';
import { createHistoryRouter } from './adaptadores/http/historyRoute.js';
import { createProcessRouter } from './adaptadores/http/processRoute.js';
import { createAdminRouter, createDbSchemaRouter } from './adaptadores/http/adminRoute.js';
import { createActionsRouter } from './adaptadores/http/actionsRoute.js';
import { createSqlRouter } from './adaptadores/http/sqlRoute.js';
import { createDistributedRouter } from './adaptadores/http/distributedRoute.js';
// Módulo de soporte (tickets, diagnósticos, vigilancia) absorbido desde ticket v2.0.
// Está en TypeScript: Node >= 22.18 lo ejecuta sin compilar (ver SOPORTE.md).
import { crearSoporteClienteRouter, crearSoporteRouter, iniciarSoporte } from './soporte/index.ts';
import logger from './infrastructure/logger/logger.js';
import { correlationIdMiddleware } from './adaptadores/http/correlationId.js';
import { normalizeSchemaIdentifier } from './dominio/sql/guardas.js';
import { getRoleFromRequest, hasRequiredRole, normalizeRole } from './dominio/acceso/roles.js';
import { CORS_ALLOWED_ORIGINS, extractHostFromRequestOptions, isOriginAllowed, isOutboundBlocked } from './dominio/red/salida.js';
import { extractDatabaseHintsFromText as deducirBases, resolveRequestedDatabasesForQuery as resolverBasesPedidas } from './dominio/bases/resolucion.js';
import { normalizeGeneratedSelectSql, slugifyProcessName } from './dominio/texto/normalizar.js';
import { crearRutasConsultas } from './adaptadores/http/consultas.js';
import { validateBusinessScriptDefinition } from './dominio/scripts/validacion.js';
import { buildHumanSqlExplanation, buildProductionSqlResponse } from './dominio/presentacion/explicacion.js';
import { configurarPool as configurarPoolAuditoria, logArticleChange, logArticleExecution, logError, logLoginEvent, logQueryHistory, logRequest, logSystemEvent } from './adaptadores/postgres/auditoria.js';
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
  ensureSessionsSchema,
  ensureAuditTimelineView,
} from './adaptadores/postgres/esquemas.js';
import {
  clavesDeRelacion, columnasPermitidas, configurarEsquemaDinamico, ensureDynamicSchemaReady,
  refreshDynamicSchemaCache,
  relacionesDelEsquema, resetDynamicSchemaCache, tablasPermitidas, tiposDeColumna,
} from './adaptadores/postgres/esquema-dinamico.js';
import {
  asegurarTipoDeColumna,
} from './dominio/sql/relacional.js';
import { crearGuardianesDeSoporte } from './adaptadores/http/sesion-soporte.js';
import { crearConexionPostgres } from './adaptadores/postgres/conexion.js';
import { ALLOWED_ACTIONS } from './aplicacion/acciones.js';
import { crearAprendizajeSemantico } from './aplicacion/aprendizaje.js';
import { crearEjecucionDeScripts } from './aplicacion/ejecucion.js';
import { QUERY_CACHE_TTL_MS, bumpQueryCacheDataVersion, getQueryCacheDataVersion, getQueryCacheEntry, setQueryCacheEntry } from './adaptadores/memoria/cache-consultas.js';
import { ALIAS_DE_ACCION, parseStructuredScript } from './dominio/scripts/estructurado.js';

import { buildCrossDbSelectOptions, enforceStrictProductionSqlPayload, isStrictSqlResponsePath } from './dominio/consultas/decidir.js';

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



// ── La conexión a PostgreSQL ──────────────────────────────────────────────────
//
// `pool` se declara antes de la fábrica y vacío a propósito: la fábrica recibe un
// getter que lo lee, y el getter solo corre cuando alguien pide el pool — nunca
// durante esta línea. Así el orden no importa y no hay que repetir la lógica de
// construir el pool en los dos sitios que lo hacen.
let pool = null;

const {
  createPgPool,
  probeConfiguredDatabasesConnectivity,
  testDatabaseConnection,
} = crearConexionPostgres({
  obtenerPool: () => pool,
  multiDbRegistry,
  // Por función y no por objeto: schemaDetector se construye más abajo, porque
  // necesita el pool que esta misma fábrica crea.
  refrescarHuella: () => schemaDetector.refreshConnectionFingerprint(),
});

pool = createPgPool();
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




async function reloadActiveDatabaseConnection(reason = 'reload') {
  const previousPool = pool;
  const previousRegistry = BUSINESS_ACTIONS_REGISTRY;
  const previousMultiDbConfig = multiDbRegistry.getConfigSnapshot();
  // Lo aprendido se guarda antes de tocar nada, para poder devolverlo si la base
  // nueva no responde. Antes esto metia la mano en el cache; ahora lo pide.
  const aprendidoAnterior = guardarEstado();

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
      ensureSessionsSchema(),
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
    restaurarEstado(aprendidoAnterior);
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












// ============================================================
// ALLOWED ACTIONS - Whitelist of executable repair actions
// No shell access. All DB queries use parameterized statements.
// ============================================================












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
  decideQueryExecutionMode,
  resolveErrorAnalysisResponse,
  executeArticleById,
  executeDistributedWithExplanation,
  executeErrorAnalysis,
  executeManualSqlScript,
  executeSafeSelectQuery,
} = crearEjecucionDeScripts({
  logQueryHistory,
  ensureDynamicSchemaReady,
  refreshDynamicSchemaCache,
  tablasPermitidas,
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

// El aprendizaje semántico, con su propio estado dentro. El caché de lo aprendido no
// sale: lo que sale son las funciones, y para la recarga de configuración, dos que
// guardan y devuelven el estado.
const {
  applyLearnedSemanticSnapshot,
  buildNaturalLanguageSuggestions,
  guardarEstado,
  learnSemanticMappingsFromSuccessfulQuery,
  refreshSemanticLearningCache,
  resetLearnedSemanticCache,
  restaurarEstado,
} = crearAprendizajeSemantico({
  bumpQueryCacheDataVersion,
  relacionesDelEsquema,
  tablasPermitidas,
  obtenerPool: () => pool,
  schemaCache,
  schemaDetector,
  queryBuilder,
  queryParameterizer,
});



























// ============================================================
// FIN MODO ANÁLISIS DE ERRORES
// ============================================================
















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
  getQueryCacheDataVersion,
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


// Soporte: /api/soporte/{estado,resumen,tickets,consultas,playbooks,diagnosticos,monitores,alertas}

const { sesionSoporte, soloAdminSoporte } = crearGuardianesDeSoporte({
  obtenerUsuario: getUserFromRequestAsync,
  normalizarRol: normalizeRole,
});

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
    await ensureSessionsSchema();
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
