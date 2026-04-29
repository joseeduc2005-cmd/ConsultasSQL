/**
 * DynamicRoutes – Generación automática de endpoints REST CRUD
 *
 * En lugar de registrar una ruta por tabla (que requeriría conocer el schema
 * en tiempo de arranque y ademas registrarse ANTES del 404 handler), este
 * modulo registra un ÚNICO middleware genérico bajo /api/:table que:
 *   1. Carga el schema de la base de datos la primera vez que recibe una petición.
 *   2. Carga el schema de la base de datos (con TTL de caché) en cada petición.
 *   3. Si el :table del path existe en el schema real, atiende la petición CRUD.
 *   4. Si no existe, pasa al siguiente middleware (404 handler normal).
 *
 * Esto permite que el middleware se registre de forma síncrona antes del 404
 * handler, sin necesidad de conexión a la DB en el momento del arranque.
 *
 * Rutas soportadas:
 *   GET    /api/{table}        → listar con paginación y filtros por columna
 *   GET    /api/{table}/:id    → obtener registro por PK
 *   POST   /api/{table}        → insertar nuevo registro
 *   PUT    /api/{table}/:id    → actualizar registro por PK
 *   DELETE /api/{table}/:id    → eliminar registro por PK
 *
 * Seguridad:
 *   - Identificadores de tabla/columna siempre entre comillas dobles (no concatenación de valores)
 *   - Valores siempre via parámetros posicionales ($1, $2, …)
 *   - Columnas en POST/PUT validadas contra el schema real
 *   - PK detectada automáticamente
 *   - Tablas internas del motor excluidas de la exposición pública
 */

/**
 * Tablas internas del motor que no deben exponerse públicamente.
 */
const INTERNAL_TABLES = new Set([
  'query_history',
  'semantic_learning',
  'knowledge_base',
  'business_logs',
  'schema_cache',
]);

/**
 * Rutas base ya manejadas por handlers explícitos en app.js.
 * El middleware dinámico las ignora para no interferir.
 */
const RESERVED_PATHS = new Set([
  'auth',
  'articles',
  'comments',
  'actions',
  'db',
  'admin',
  'process',
  'repair',
  'execute',
  'sql',
  'distributed',
]);

const DEFAULT_PAGE_SIZE = 50;
const MAX_PAGE_SIZE = 500;
const SCHEMA_CACHE_TTL_MS = 60_000;

// ── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Envuelve un identificador PostgreSQL entre comillas dobles.
 * Solo permite caracteres de identificador seguros (letras, números, _).
 */
function quoteIdentifier(name) {
  const safe = String(name || '').replace(/[^a-zA-Z0-9_]/g, '');
  if (!safe) throw new Error(`Identificador inválido: "${name}"`);
  return `"${safe}"`;
}

function extractPrimaryKey(tableSchema) {
  if (tableSchema?.pkPrincipal) return String(tableSchema.pkPrincipal).trim();
  if (Array.isArray(tableSchema?.clavesPrimarias) && tableSchema.clavesPrimarias.length > 0) {
    return String(tableSchema.clavesPrimarias[0]).trim();
  }
  return null;
}

function getWritableColumns(tableSchema, pk) {
  return (tableSchema?.columnas || [])
    .map((col) => String(col?.nombre || '').trim())
    .filter((name) => name && name !== pk);
}

function filterBodyToSchema(body, writableColumns) {
  const allowed = new Set(writableColumns);
  const valid = {};
  const unknown = [];
  for (const [key, value] of Object.entries(body || {})) {
    if (allowed.has(key)) {
      valid[key] = value;
    } else {
      unknown.push(key);
    }
  }
  return { valid, unknown };
}

function getAllowedFilterColumns(tableSchema) {
  return new Set(
    (tableSchema?.columnas || []).map((col) => String(col?.nombre || '').trim()).filter(Boolean)
  );
}

function buildSelectQuery(tableName, tableSchema, queryParams, limit, offset) {
  const quotedTable = quoteIdentifier(tableName);
  const allowedColumns = getAllowedFilterColumns(tableSchema);

  const params = [];
  const conditions = [];

  for (const [key, value] of Object.entries(queryParams || {})) {
    if (key === 'limit' || key === 'offset' || key === 'page') continue;
    if (!allowedColumns.has(key) || value === undefined || value === null) continue;
    params.push(value);
    conditions.push(`${quoteIdentifier(key)} = $${params.length}`);
  }

  let text = `SELECT * FROM ${quotedTable}`;
  if (conditions.length > 0) {
    text += ` WHERE ${conditions.join(' AND ')}`;
  }
  params.push(limit, offset);
  text += ` LIMIT $${params.length - 1} OFFSET $${params.length}`;

  return { text, values: params };
}

// ── Schema cache per instance ─────────────────────────────────────────────────

function createSchemaCache() {
  return {
    data: null,
    loadedAt: 0,
    loading: null,
  };
}

async function getSchemaWithCache(cache, schemaDetector) {
  if (cache.data && Date.now() - cache.loadedAt < SCHEMA_CACHE_TTL_MS) {
    return cache.data;
  }

  if (cache.loading) return cache.loading;

  cache.loading = schemaDetector
    .getFullSchema()
    .then((schema) => {
      cache.data = schema;
      cache.loadedAt = Date.now();
      cache.loading = null;
      return schema;
    })
    .catch((error) => {
      cache.loading = null;
      throw error;
    });

  return cache.loading;
}

// ── CRUD handlers ─────────────────────────────────────────────────────────────

async function handleList(req, res, pool, tableName, tableSchema) {
  const rawLimit = Number(req.query?.limit);
  const rawPage = Number(req.query?.page ?? 1);
  const limit = Number.isFinite(rawLimit) && rawLimit > 0 ? Math.min(rawLimit, MAX_PAGE_SIZE) : DEFAULT_PAGE_SIZE;
  const page = Number.isFinite(rawPage) && rawPage > 0 ? rawPage : 1;
  const offset = (page - 1) * limit;

  const { text, values } = buildSelectQuery(tableName, tableSchema, req.query, limit, offset);
  const result = await pool.query(text, values);

  return res.status(200).json({
    success: true,
    table: tableName,
    data: result.rows,
    rowCount: result.rowCount,
    page,
    limit,
  });
}

async function handleGetById(req, res, pool, tableName, tableSchema, pk) {
  const idValue = req.params.id;
  const { rows } = await pool.query(
    `SELECT * FROM ${quoteIdentifier(tableName)} WHERE ${quoteIdentifier(pk)} = $1 LIMIT 1`,
    [idValue]
  );
  if (rows.length === 0) {
    return res.status(404).json({ success: false, error: `Registro no encontrado en ${tableName}.` });
  }
  return res.status(200).json({ success: true, table: tableName, data: rows[0] });
}

async function handleInsert(req, res, pool, tableName, tableSchema, pk) {
  if (!req.body || typeof req.body !== 'object' || Array.isArray(req.body)) {
    return res.status(400).json({ success: false, error: 'Body debe ser un objeto JSON.' });
  }

  const writableColumns = getWritableColumns(tableSchema, pk);
  const { valid, unknown } = filterBodyToSchema(req.body, writableColumns);

  if (Object.keys(valid).length === 0) {
    return res.status(400).json({
      success: false,
      error: 'No se enviaron columnas válidas.',
      unknownColumns: unknown,
      allowedColumns: writableColumns,
    });
  }

  const keys = Object.keys(valid);
  const values = Object.values(valid);
  const columnList = keys.map(quoteIdentifier).join(', ');
  const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');
  const returnClause = pk ? ` RETURNING ${quoteIdentifier(pk)}` : '';

  const { rows } = await pool.query(
    `INSERT INTO ${quoteIdentifier(tableName)} (${columnList}) VALUES (${placeholders})${returnClause}`,
    values
  );

  const response = { success: true, table: tableName, data: rows[0] ?? {} };
  if (unknown.length > 0) response.ignoredColumns = unknown;
  return res.status(201).json(response);
}

async function handleUpdate(req, res, pool, tableName, tableSchema, pk) {
  if (!req.body || typeof req.body !== 'object' || Array.isArray(req.body)) {
    return res.status(400).json({ success: false, error: 'Body debe ser un objeto JSON.' });
  }

  const writableColumns = getWritableColumns(tableSchema, pk);
  const { valid, unknown } = filterBodyToSchema(req.body, writableColumns);

  if (Object.keys(valid).length === 0) {
    return res.status(400).json({
      success: false,
      error: 'No se enviaron columnas válidas para actualizar.',
      unknownColumns: unknown,
      allowedColumns: writableColumns,
    });
  }

  const idValue = req.params.id;
  const keys = Object.keys(valid);
  const values = Object.values(valid);
  const setClauses = keys.map((key, i) => `${quoteIdentifier(key)} = $${i + 1}`).join(', ');
  values.push(idValue);
  const idParamIndex = values.length;

  const { rows, rowCount } = await pool.query(
    `UPDATE ${quoteIdentifier(tableName)} SET ${setClauses} WHERE ${quoteIdentifier(pk)} = $${idParamIndex} RETURNING *`,
    values
  );

  if (rowCount === 0) {
    return res.status(404).json({ success: false, error: `Registro no encontrado en ${tableName}.` });
  }

  const response = { success: true, table: tableName, data: rows[0] };
  if (unknown.length > 0) response.ignoredColumns = unknown;
  return res.status(200).json(response);
}

async function handleDelete(req, res, pool, tableName, pk) {
  const idValue = req.params.id;
  const { rowCount } = await pool.query(
    `DELETE FROM ${quoteIdentifier(tableName)} WHERE ${quoteIdentifier(pk)} = $1`,
    [idValue]
  );
  if (rowCount === 0) {
    return res.status(404).json({ success: false, error: `Registro no encontrado en ${tableName}.` });
  }
  return res.status(200).json({ success: true, table: tableName, deleted: true, id: idValue });
}

// ── Main export ───────────────────────────────────────────────────────────────

/**
 * Crea el middleware de CRUD dinámico.
 * Debe registrarse ANTES del 404 handler en app.js.
 *
 * Rutas que atiende (detectadas a partir de req.path):
 *   GET  /api/:table
 *   GET  /api/:table/:id
 *   POST /api/:table
 *   PUT  /api/:table/:id
 *   DELETE /api/:table/:id
 *
 * Rutas ya manejadas por código explícito (RESERVED_PATHS) son ignoradas.
 *
 * @param {{ schemaDetector: object, pool: import('pg').Pool }} deps
 * @returns {import('express').RequestHandler}
 */
export function createDynamicCrudMiddleware({ schemaDetector, pool }) {
  const schemaCache = createSchemaCache();

  return async function dynamicCrudHandler(req, res, next) {
    // Only handle paths that look like /api/:table or /api/:table/:id
    const pathMatch = req.path.match(/^\/([^/]+?)(?:\/([^/]+))?(?:\/)?$/);
    if (!pathMatch) return next();

    const [, tableName, id] = pathMatch;

    // Skip reserved/internal paths
    if (!tableName || RESERVED_PATHS.has(tableName) || INTERNAL_TABLES.has(tableName)) {
      return next();
    }

    // Load schema (cached)
    let schema;
    try {
      schema = await getSchemaWithCache(schemaCache, schemaDetector);
    } catch {
      return next(); // schema unavailable – fall through to 404
    }

    const tableSchema = schema?.schema?.[tableName];
    if (!tableSchema) return next(); // table doesn't exist – fall through to 404

    const pk = extractPrimaryKey(tableSchema);
    const method = req.method.toUpperCase();

    try {
      // ── Collection routes (no :id) ──────────────────────────────────────────
      if (!id) {
        if (method === 'GET') return await handleList(req, res, pool, tableName, tableSchema);
        if (method === 'POST') return await handleInsert(req, res, pool, tableName, tableSchema, pk);
        return next();
      }

      // ── Resource routes (with :id) ──────────────────────────────────────────
      if (!pk) {
        return res.status(422).json({
          success: false,
          error: `No se detectó clave primaria en "${tableName}". Operaciones por ID no disponibles.`,
        });
      }

      if (method === 'GET') return await handleGetById(req, res, pool, tableName, tableSchema, pk);
      if (method === 'PUT') return await handleUpdate(req, res, pool, tableName, tableSchema, pk);
      if (method === 'DELETE') return await handleDelete(req, res, pool, tableName, pk);

      return next();
    } catch (error) {
      console.error(`[DynamicRoutes ${method} /api/${tableName}${id ? '/' + id : ''}]`, error?.message || error);
      return res.status(500).json({
        success: false,
        error: `Error interno al procesar ${method} /api/${tableName}.`,
      });
    }
  };
}

/**
 * Registra el middleware de CRUD dinámico en la aplicación Express.
 * Debe llamarse de forma SÍNCRONA antes de que se registre el 404 handler.
 *
 * El schema real se cargará en la primera petición (lazy loading).
 *
 * @param {import('express').Application} app
 * @param {{ schemaDetector: object, pool: import('pg').Pool }} deps
 */
export function initDynamicRoutes(app, { schemaDetector, pool }) {
  app.use('/api', createDynamicCrudMiddleware({ schemaDetector, pool }));
  console.log('✓ DynamicRoutes: middleware CRUD dinámico registrado (schema cargado al primer request)');
}

export default initDynamicRoutes;
