/**
 * El cache de como esta hecha la base ahora mismo.
 *
 * Que tablas hay, que columnas tiene cada una, de que tipo son, cual es su clave
 * primaria y que claves foraneas las unen. El sistema lo necesita para dos cosas:
 * validar que un script solo toque lo que existe, y saber por donde se puede unir
 * una tabla con otra.
 *
 * A diferencia de los demas adaptadores, este tiene estado, y por eso vive entero
 * aqui en vez de repartido: las seis variables y las funciones que las mueven son
 * una sola pieza. Fuera solo salen accesores de lectura, asi que nadie de fuera
 * puede reasignarlas — antes cualquiera de las siete mil lineas de app.js podia.
 *
 * El cache se invalida por dos motivos: porque pasaron sesenta segundos, o porque
 * cambio la huella de la conexion. Lo segundo importa mas que lo primero: al apuntar
 * a otra base, seguir creyendo el esquema de la anterior dejaria pasar scripts
 * contra tablas que en la nueva no existen.
 *
 * Nota de lo que se encontro al moverlo: `getDynamicSchemaFull` esta definida y no
 * la llama nadie en todo el backend. Se movio tal cual, sin decidir por nadie si
 * se borra, pero conviene saberlo.
 */
import { buildRelationKey } from '../../dominio/scripts/validacion.js';
import { normalizeSchemaIdentifier } from '../../dominio/sql/guardas.js';

let obtenerPool = () => {
  throw new Error('El esquema dinamico no tiene pool: falta llamar a configurarEsquemaDinamico al arrancar.');
};

let obtenerHuellaConexion = () => '';
let publicarContextoDeEsquema = () => {};

export function configurarEsquemaDinamico({ pool, huellaConexion, publicarContexto }) {
  if (pool) obtenerPool = pool;
  if (huellaConexion) obtenerHuellaConexion = huellaConexion;
  if (publicarContexto) publicarContextoDeEsquema = publicarContexto;
}

// ── Lectura desde fuera ────────────────────────────────────────────────────────
// Funciones y no las variables: exportar un `let` dejaria que quien lo importe se
// quede con una referencia vieja justo despues de un refresco.
export const tablasPermitidas = () => DYNAMIC_SCRIPT_ALLOWED_TABLES;
export const columnasPermitidas = () => DYNAMIC_SCRIPT_ALLOWED_COLUMNS;
export const tiposDeColumna = () => DYNAMIC_SCRIPT_COLUMN_TYPES;
export const clavesPrimarias = () => DYNAMIC_SCRIPT_TABLE_PKS;
export const relacionesDelEsquema = () => DYNAMIC_SCHEMA_RELATIONSHIPS;
export const clavesDeRelacion = () => DYNAMIC_SCHEMA_RELATION_KEYS;

export function resetDynamicSchemaCache() {
  DYNAMIC_SCRIPT_ALLOWED_TABLES = [];
  DYNAMIC_SCRIPT_ALLOWED_COLUMNS = {};
  DYNAMIC_SCRIPT_COLUMN_TYPES = {};
  DYNAMIC_SCRIPT_TABLE_PKS = {};
  DYNAMIC_SCHEMA_RELATIONSHIPS = [];
  DYNAMIC_SCHEMA_RELATION_KEYS = new Set();
  DYNAMIC_SCHEMA_CACHE.loadedAt = 0;
  DYNAMIC_SCHEMA_CACHE.fingerprint = '';
  DYNAMIC_SCHEMA_CACHE.refreshPromise = null;
  publicarContextoDeEsquema({ tables: [] });
}

let DYNAMIC_SCRIPT_ALLOWED_TABLES = [];
// Safe whitelist of WHERE operators — prevents any form of SQL injection via operator field
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

export function inferRuntimeTypeFromPgType(dataType, udtName) {
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

export function getDynamicSchemaSnapshot() {
  return {
    tablas: Object.fromEntries(
      Object.entries(DYNAMIC_SCRIPT_ALLOWED_COLUMNS).map(([tableName, columns]) => [tableName, [...columns]])
    ),
    relaciones: [...DYNAMIC_SCHEMA_RELATIONSHIPS],
    fingerprint: DYNAMIC_SCHEMA_CACHE.fingerprint,
    loadedAt: DYNAMIC_SCHEMA_CACHE.loadedAt,
  };
}

export function getDynamicSchemaFull() {
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

export async function refreshDynamicSchemaCache(force = false) {
  const currentFingerprint = obtenerHuellaConexion();
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
    const tablesResult = await obtenerPool().query(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE'
      ORDER BY table_name
    `);

    const columnsResult = await obtenerPool().query(`
      SELECT table_name, column_name, data_type, udt_name
      FROM information_schema.columns
      WHERE table_schema = 'public'
      ORDER BY table_name, ordinal_position
    `);

    const fkResult = await obtenerPool().query(`
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

    const pkResult = await obtenerPool().query(`
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
    publicarContextoDeEsquema({ tables: nextTables });

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

export async function ensureDynamicSchemaReady(force = false) {
  const snapshot = await refreshDynamicSchemaCache(force);
  if (!snapshot || !snapshot.tablas || Object.keys(snapshot.tablas).length === 0) {
    throw new Error('No se pudo detectar el schema dinámico de la base de datos');
  }
  return snapshot;
}
