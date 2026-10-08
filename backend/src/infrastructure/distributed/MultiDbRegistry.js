import fs from 'fs';
import { ObjectId } from 'mongodb';
import { asegurarConsultaDeMongo } from '../../dominio/mongo/consulta.js';
import path from 'path';
import { Pool } from 'pg';
import {
  connectDatabase,
  getConnection,
  getDatabaseContext,
  getSchema,
  closeConnection,
} from '../universal-db/index.js';

function normalizeText(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9_\s-]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function dedupe(values = []) {
  return [...new Set((values || []).map((item) => String(item || '').trim()).filter(Boolean))];
}

function normalizeIdentifier(value) {
  return normalizeText(value).replace(/\s+/g, '_');
}

function hasPrimaryRole(entry = {}) {
  if (entry?.primary === true || entry?.isPrimary === true) {
    return true;
  }

  const role = normalizeIdentifier(entry?.role || entry?.purpose || '');
  if (role === 'primary' || role === 'system') {
    return true;
  }

  const roles = Array.isArray(entry?.roles) ? entry.roles : [];
  return roles
    .map((item) => normalizeIdentifier(item))
    .some((item) => item === 'primary' || item === 'system');
}

function buildFingerprint(entry = {}) {
  const type = normalizeIdentifier(entry.type || 'postgres');
  const id = normalizeIdentifier(entry.id || 'database');
  const host = normalizeIdentifier(entry.host || entry.hostname || 'local');
  const database = normalizeIdentifier(entry.database || entry.service || entry.sid || id);
  return `${type}:${id}:${host}:${database}`;
}

function normalizeSchemaForeignKey(foreignKey = {}) {
  const column = normalizeIdentifier(foreignKey.column || foreignKey.columna);
  const referencedTable = normalizeIdentifier(foreignKey.referencesTable || foreignKey.tablaReferenciada || foreignKey.referencedTable);
  const referencedColumn = normalizeIdentifier(foreignKey.referencesColumn || foreignKey.columnaReferenciada || foreignKey.referencedColumn);

  if (!column || !referencedTable || !referencedColumn) {
    return null;
  }

  return {
    column,
    referencedTable,
    referencedColumn,
    constraintName: String(foreignKey.constraintName || foreignKey.nombreConstraint || '').trim(),
  };
}

function extractSchemaTables(entry = {}) {
  if (Array.isArray(entry?.schema?.tables)) {
    return entry.schema.tables;
  }

  if (entry?.schema?.tables && typeof entry.schema.tables === 'object') {
    return Object.entries(entry.schema.tables).map(([tableName, tableSchema]) => ({
      ...(tableSchema || {}),
      name: tableName,
    }));
  }

  if (Array.isArray(entry?.tables)) {
    return entry.tables;
  }

  return [];
}

function normalizeSchemaTable(table = {}) {
  const name = normalizeIdentifier(table.name || table.table || table.id);
  const columns = (table.columns || table.columnas || []).map((column) => {
    if (typeof column === 'string') {
      return { name: normalizeIdentifier(column), type: 'text' };
    }

    return {
      name: normalizeIdentifier(column?.name || column?.nombre),
      type: normalizeIdentifier(column?.type || column?.tipo || 'text') || 'text',
      key: Boolean(column?.key || column?.isKey || column?.primaryKey),
    };
  }).filter((column) => column.name);

  const keyColumns = dedupe([
    ...(table.keyColumns || []),
    ...(table.primaryKeys || []),
    ...(Array.isArray(table.primaryKey) ? table.primaryKey : [table.primaryKey]),
    ...columns.filter((column) => column.key).map((column) => column.name),
  ].map((column) => normalizeIdentifier(column)));

  const foreignKeys = (table.foreignKeys || table.clavesForaneas || [])
    .map((foreignKey) => normalizeSchemaForeignKey(foreignKey))
    .filter(Boolean);

  return {
    name,
    // Preserva el schema de origen para routing inteligente (bpc_ordenes, bpc_comercios, etc.)
    schema: String(table.schema || 'public').toLowerCase(),
    columns,
    keyColumns,
    foreignKeys,
  };
}

function normalizeLearning(learning = {}) {
  const tableAliases = {};
  for (const [term, mappedTables] of Object.entries(learning?.tableAliases || {})) {
    const normalizedTerm = normalizeIdentifier(term);
    if (!normalizedTerm) continue;
    tableAliases[normalizedTerm] = dedupe((mappedTables || []).map((item) => normalizeIdentifier(item)));
  }

  const columnKeywords = {};
  for (const [term, mappedColumns] of Object.entries(learning?.columnKeywords || {})) {
    const normalizedTerm = normalizeIdentifier(term);
    if (!normalizedTerm) continue;
    columnKeywords[normalizedTerm] = dedupe((mappedColumns || []).map((item) => normalizeIdentifier(item)));
  }

  return {
    tableAliases,
    columnKeywords,
  };
}


/**
 * De los marcadores canónicos a los tipos del driver.
 *
 * El lector del dominio devuelve `{ $oid }`, `{ $date }` y `{ $regularExpression }` en
 * vez de objetos del driver, que es lo que permite probarlo sin levantar un Mongo. La
 * conversión a ObjectId, Date y RegExp es cosa de esta capa, que es la que habla con el
 * motor.
 */
function conTiposDeMongo(valor) {
  if (Array.isArray(valor)) return valor.map(conTiposDeMongo);
  if (!valor || typeof valor !== 'object') return valor;

  const claves = Object.keys(valor);
  if (claves.length === 1) {
    if (claves[0] === '$oid' && typeof valor.$oid === 'string') {
      // Un identificador mal formado no se convierte a la fuerza: vale más que el motor
      // no encuentre nada a que esto reviente con un error del driver.
      return ObjectId.isValid(valor.$oid) ? new ObjectId(valor.$oid) : valor.$oid;
    }
    if (claves[0] === '$date') {
      const d = new Date(valor.$date);
      return Number.isNaN(d.getTime()) ? valor.$date : d;
    }
    if (claves[0] === '$regularExpression') {
      const { pattern, options } = valor.$regularExpression ?? {};
      try { return new RegExp(String(pattern ?? ''), String(options ?? '')); } catch { return valor; }
    }
    if (claves[0] === '$numberDecimal') return Number(valor.$numberDecimal);
  }

  const salida = {};
  for (const [k, v] of Object.entries(valor)) salida[k] = conTiposDeMongo(v);
  return salida;
}

export class MultiDbRegistry {
  constructor(config = null) {
    this.databases = new Map();
    this.clients = new Map();
    this.runtimeLearning = new Map();
    this.connectionStatus = new Map(); // id → 'ok' | 'fail' | 'unknown'
    // Generous default (2 min) so long / complex SQL on external DBs is never
    // cut short — matches DBeaver-style behavior. Override with SQL_QUERY_TIMEOUT_MS.
    this.queryTimeoutMs = Math.max(1000, Number(process.env.SQL_QUERY_TIMEOUT_MS) || 120000);
    this.loadConfig(config || this.readConfigFromEnv());
    this._watchConfigFile(); // hot-reload when multidb.databases.json changes
  }

  /** Watch the config file and auto-reload + introspect new DBs when it changes */
  _watchConfigFile() {
    const filePath = this.resolveConfigFilePath();
    if (!filePath || !fs.existsSync(filePath)) return;

    let debounceTimer = null;
    fs.watch(filePath, { persistent: false }, (eventType) => {
      if (eventType !== 'change') return;
      clearTimeout(debounceTimer);
      debounceTimer = setTimeout(async () => {
        console.log('[MultiDbRegistry] 🔄 multidb.databases.json cambió — recargando configuración...');
        try {
          const newConfig = this.readConfigFromEnv();
          const existingIds = new Set(this.databases.keys());
          this.loadConfig(newConfig);

          // Introspect only newly-added databases
          const newIds = [...this.databases.keys()].filter((id) => !existingIds.has(id));
          if (newIds.length > 0) {
            console.log(`[MultiDbRegistry] 🆕 Nuevas bases detectadas: ${newIds.join(', ')} — iniciando introspección...`);
            await this.introspectEmptySchemas();
          }
          console.log('[MultiDbRegistry] ✔ Recarga completada.');
        } catch (err) {
          console.error('[MultiDbRegistry] ✘ Error en hot-reload:', err?.message || err);
        }
      }, 800); // debounce 800ms
    });
    console.log(`[MultiDbRegistry] 👁 Watching config: ${filePath}`);
  }

  /** Marca el resultado del último probe de conectividad para una BD */
  setConnectionStatus(id, status /* 'ok' | 'fail' */) {
    if (id) this.connectionStatus.set(String(id).trim(), status);
  }

  /** Devuelve true si la BD tiene status 'ok' confirmado */
  isConnected(id) {
    return this.connectionStatus.get(String(id || '').trim()) === 'ok';
  }

  resolveConfigFilePath() {
    const configFile = String(
      process.env.MULTI_DB_CONFIG_FILE
      || process.env.DATABASES_CONFIG_FILE
      || './config/multidb.databases.json'
      || ''
    ).trim();

    if (!configFile) {
      return '';
    }

    return path.isAbsolute(configFile)
      ? configFile
      : path.resolve(process.cwd(), configFile);
  }

  readConfigFromEnv() {
    const resolvedPath = this.resolveConfigFilePath();

    console.log(`[MultiDbRegistry] readConfig → buscando config en: ${resolvedPath || '(no path)'}`);

    if (resolvedPath) {
      const exists = fs.existsSync(resolvedPath);
      console.log(`[MultiDbRegistry] readConfig → archivo ${exists ? '✔ encontrado' : '✘ NO existe'}: ${resolvedPath}`);
      if (exists) {
        try {
          const fileContents = fs.readFileSync(resolvedPath, 'utf8').trim();
          console.log(`[MultiDbRegistry] readConfig → archivo leído (${fileContents.length} bytes)`);
          if (fileContents) {
            const parsedFile = JSON.parse(fileContents);
            if (parsedFile && typeof parsedFile === 'object') {
              const total = (parsedFile?.databases || []).length;
              const enabled = (parsedFile?.databases || []).filter((d) => d.enabled !== false).length;
              console.log(`[MultiDbRegistry] readConfig → JSON válido: ${total} bases totales, ${enabled} habilitadas`);
              return parsedFile;
            }
          }
        } catch (error) {
          console.error('[MultiDbRegistry] readConfig → ✘ Config file inválido:', error?.message || error);
        }
      }
    }

    const raw = String(process.env.MULTI_DB_CONFIG || process.env.DATABASES_CONFIG || '').trim();
    if (!raw) {
      console.warn('[MultiDbRegistry] readConfig → ⚠ No se encontró archivo ni variable de entorno. Usando config vacía.');
      return { databases: [] };
    }

    console.log('[MultiDbRegistry] readConfig → leyendo desde variable de entorno MULTI_DB_CONFIG');
    try {
      const parsed = JSON.parse(raw);
      return parsed && typeof parsed === 'object' ? parsed : { databases: [] };
    } catch (error) {
      console.error('[MultiDbRegistry] readConfig → ✘ Config JSON inválido:', error?.message || error);
      return { databases: [] };
    }
  }

  loadConfig(config = {}) {
    this.databases.clear();
    this.clients.clear();
    this.runtimeLearning.clear();

    const entries = config?.databases || [];
    console.log(`[MultiDbRegistry] loadConfig → procesando ${entries.length} entradas`);

    for (const entry of entries) {
      const normalized = this.normalizeDatabase(entry);
      if (!normalized) {
        console.warn(`[MultiDbRegistry] loadConfig → ✘ entrada inválida (sin id/type):`, JSON.stringify(entry).slice(0, 80));
        continue;
      }
      const status = normalized.enabled === false ? 'DISABLED' : 'ENABLED';
      console.log(`[MultiDbRegistry] loadConfig → [${status}] id="${normalized.id}" type=${normalized.type} host=${normalized.host}:${normalized.port}/${normalized.database}`);
      this.databases.set(normalized.id, normalized);
      this.runtimeLearning.set(normalized.id, normalizeLearning(normalized.semanticLearning));
    }

    const enabledCount = [...this.databases.values()].filter((d) => d.enabled !== false).length;
    console.log(`[MultiDbRegistry] loadConfig → ✔ ${this.databases.size} registradas, ${enabledCount} habilitadas`);
  }

  normalizeDatabase(entry = {}) {
    const id = normalizeIdentifier(entry.id || entry.name);
    const type = normalizeIdentifier(entry.type || 'postgres');
    if (!id || !type) return null;

    const schemaTables = extractSchemaTables(entry).map(normalizeSchemaTable).filter((table) => table.name);
    const semanticLearning = normalizeLearning(entry.semanticLearning || entry.learnedMappings || {});

    return {
      id,
      label: String(entry.label || entry.name || id).trim(),
      type,
      primary: hasPrimaryRole(entry),
      enabled: entry.enabled !== false,
      fingerprint: String(entry.fingerprint || buildFingerprint({ ...entry, id, type })).trim(),
      connectionString: String(entry.connectionString || entry.url || '').trim(),
      host: String(entry.host || '').trim(),
      port: Number(entry.port) || null,
      database: String(entry.database || entry.service || entry.sid || '').trim(),
      user: String(entry.user || entry.username || '').trim(),
      password: String(entry.password || '').trim(),
      connection: entry.connection || entry.pool || null,
      schema: {
        tables: schemaTables,
      },
      semanticLearning,
    };
  }

  getConfigSnapshot() {
    return {
      databases: [...this.databases.values()].map((database) => ({
        id: database.id,
        label: database.label,
        type: database.type,
        primary: database.primary === true,
        enabled: database.enabled !== false,
        fingerprint: database.fingerprint,
        connectionString: database.connectionString,
        host: database.host,
        port: database.port,
        database: database.database,
        user: database.user,
        password: database.password,
        schema: database.schema,
        semanticLearning: database.semanticLearning,
      })),
    };
  }

  reloadFromEnv() {
    this.loadConfig(this.readConfigFromEnv());
    return this.getDatabases();
  }

  async registerDatabase(entry = {}) {
    const hasDirectConnection = Boolean(entry?.connection || entry?.pool);
    const hasStructuredSchema = Array.isArray(entry?.schema?.tables)
      || (entry?.schema?.tables && typeof entry.schema.tables === 'object')
      || Array.isArray(entry?.tables);

    let normalized = this.normalizeDatabase(entry);
    let poolClient = entry?.connection || entry?.pool || null;

    if (!hasDirectConnection && !hasStructuredSchema) {
      const connectionResult = await connectDatabase(entry);
      const dbId = String(entry.id || connectionResult.dbId || '').trim() || connectionResult.dbId;
      const schema = await getSchema(connectionResult.dbId);
      const context = getDatabaseContext(connectionResult.dbId);
      poolClient = getConnection(connectionResult.dbId);

      normalized = this.normalizeDatabase({
        ...entry,
        id: dbId,
        fingerprint: context?.fingerprint || connectionResult.fingerprint,
        schema,
        connection: poolClient,
      });
    }

    if (!normalized) {
      throw new Error('No se pudo registrar la base de datos');
    }

    this.databases.set(normalized.id, normalized);
    this.runtimeLearning.set(normalized.id, normalizeLearning(normalized.semanticLearning));

    if (poolClient) {
      this.clients.set(normalized.id, {
        type: normalized.type,
        pool: poolClient,
      });
    }

    return normalized;
  }

  getDatabases() {
    return [...this.databases.values()].filter((database) => database.enabled !== false);
  }

  getPrimaryDatabase(options = {}) {
    const includeDisabled = options?.includeDisabled === true;
    const databases = includeDisabled
      ? [...this.databases.values()]
      : this.getDatabases();

    if (databases.length === 0) {
      return null;
    }

    return databases.find((database) => database.primary === true)
      || databases[0]
      || null;
  }

  buildConnectionString(database = {}) {
    const direct = String(database?.connectionString || database?.url || '').trim();
    if (direct) {
      return direct;
    }

    const type = normalizeIdentifier(database?.type || 'postgres');
    const host = String(database?.host || '').trim();
    const databaseName = String(database?.database || database?.service || database?.sid || '').trim();
    const user = String(database?.user || database?.username || '').trim();
    const password = String(database?.password || '').trim();

    if (!host || !databaseName || !user) {
      return '';
    }

    const encodedUser = encodeURIComponent(user);
    const encodedPassword = encodeURIComponent(password);
    const auth = password ? `${encodedUser}:${encodedPassword}` : encodedUser;

    if (type === 'postgres') {
      const port = Number(database?.port) || 5432;
      return `postgresql://${auth}@${host}:${port}/${databaseName}`;
    }

    if (type === 'mysql') {
      const port = Number(database?.port) || 3306;
      return `mysql://${auth}@${host}:${port}/${databaseName}`;
    }

    if (type === 'oracle') {
      const port = Number(database?.port) || 1521;
      return `oracle://${auth}@${host}:${port}/${databaseName}`;
    }

    if (type === 'mongodb') {
      const port = Number(database?.port) || 27017;
      const dbPart = databaseName ? `/${databaseName}` : '';
      return `mongodb://${auth}@${host}:${port}${dbPart}`;
    }

    return '';
  }

  resolvePrimaryConnection(options = {}) {
    const expectedType = normalizeIdentifier(options?.expectedType || '') || '';
    const allowEnvFallback = options?.allowEnvFallback !== false;
    const databases = options?.includeDisabled
      ? [...this.databases.values()]
      : this.getDatabases();

    let resolved = null;

    if (expectedType) {
      // Busca primero una DB del tipo esperado que tenga primary:true, luego cualquiera de ese tipo
      resolved = databases.find((db) => db.primary === true && normalizeIdentifier(db.type) === expectedType)
        || databases.find((db) => normalizeIdentifier(db.type) === expectedType)
        || null;
    } else {
      // Sin preferencia de tipo: la marcada primary primero, luego la primera disponible
      resolved = databases.find((db) => db.primary === true) || databases[0] || null;
    }

    if (resolved) {
      const connectionString = this.buildConnectionString(resolved);
      if (!connectionString) {
        throw new Error(`La base ${resolved.id} no tiene credenciales suficientes para construir la conexión`);
      }

      return {
        source: 'registry',
        database: resolved,
        connectionString,
      };
    }

    const fallbackConnectionString = String(process.env.DATABASE_URL || '').trim();
    if (allowEnvFallback && fallbackConnectionString) {
      return {
        source: 'env',
        database: null,
        connectionString: fallbackConnectionString,
      };
    }

    throw new Error('No se encontró ninguna base configurada en MULTI_DB_CONFIG_FILE ni DATABASE_URL como fallback');
  }

  getDatabaseById(databaseId) {
    return this.databases.get(normalizeIdentifier(databaseId));
  }

  getDatabaseSummaries() {
    return this.getDatabases().map((database) => ({
      id: database.id,
      label: database.label,
      type: database.type,
      primary: database.primary === true,
      fingerprint: database.fingerprint,
      connected: this.isConnected(database.id),
      tables: database.schema.tables.map((table) => table.name),
    }));
  }

  getSchemaTables(databaseId) {
    return this.getDatabaseById(databaseId)?.schema?.tables || [];
  }

  getLearningSnapshot(databaseId) {
    const database = this.getDatabaseById(databaseId);
    const baseLearning = normalizeLearning(database?.semanticLearning || {});
    const runtimeLearning = normalizeLearning(this.runtimeLearning.get(normalizeIdentifier(databaseId)) || {});

    const mergedTableAliases = { ...baseLearning.tableAliases };
    for (const [term, tables] of Object.entries(runtimeLearning.tableAliases || {})) {
      mergedTableAliases[term] = dedupe([...(mergedTableAliases[term] || []), ...(tables || [])]);
    }

    const mergedColumnKeywords = { ...baseLearning.columnKeywords };
    for (const [term, columns] of Object.entries(runtimeLearning.columnKeywords || {})) {
      mergedColumnKeywords[term] = dedupe([...(mergedColumnKeywords[term] || []), ...(columns || [])]);
    }

    return {
      tableAliases: mergedTableAliases,
      columnKeywords: mergedColumnKeywords,
    };
  }

  learn(databaseId, keyword, mappedTable, mappedColumn = '') {
    const normalizedDbId = normalizeIdentifier(databaseId);
    if (!this.databases.has(normalizedDbId)) return;

    const normalizedKeyword = normalizeIdentifier(keyword);
    const normalizedTable = normalizeIdentifier(mappedTable);
    const normalizedColumn = normalizeIdentifier(mappedColumn);
    if (!normalizedKeyword || !normalizedTable) return;

    const current = normalizeLearning(this.runtimeLearning.get(normalizedDbId) || {});
    current.tableAliases[normalizedKeyword] = dedupe([...(current.tableAliases[normalizedKeyword] || []), normalizedTable]);
    if (normalizedColumn) {
      current.columnKeywords[normalizedKeyword] = dedupe([...(current.columnKeywords[normalizedKeyword] || []), normalizedColumn]);
    }
    this.runtimeLearning.set(normalizedDbId, current);
  }

  async ensureClient(database) {
    if (this.clients.has(database.id)) {
      return this.clients.get(database.id);
    }

    let client;
    if (database.type === 'postgres') {
      client = {
        type: 'postgres',
        pool: new Pool({
          connectionString: database.connectionString || undefined,
          host: database.connectionString ? undefined : database.host || undefined,
          port: database.connectionString ? undefined : database.port || undefined,
          database: database.connectionString ? undefined : database.database || undefined,
          user: database.connectionString ? undefined : database.user || undefined,
          password: database.connectionString ? undefined : database.password || undefined,
          max: 5,
          idleTimeoutMillis: 30000,
          connectionTimeoutMillis: 5000,
        }),
      };
    } else if (database.type === 'mysql') {
      const mysql = await import('mysql2/promise');
      client = {
        type: 'mysql',
        pool: mysql.createPool({
          uri: database.connectionString || undefined,
          host: database.connectionString ? undefined : database.host || undefined,
          port: database.connectionString ? undefined : database.port || undefined,
          database: database.connectionString ? undefined : database.database || undefined,
          user: database.connectionString ? undefined : database.user || undefined,
          password: database.connectionString ? undefined : database.password || undefined,
          connectionLimit: 5,
        }),
      };
    } else if (database.type === 'oracle') {
      const oracleModule = await import('oracledb');
      // ESM/CJS interop: some bundlers expose the module on .default
      const oracledb = oracleModule?.default ?? oracleModule;
      const connectStr = database.connectionString || `${database.host}:${database.port || 1521}/${database.database}`;
      client = {
        type: 'oracle',
        driver: oracledb,
        pool: await oracledb.createPool({
          user: database.user || undefined,
          password: database.password || undefined,
          connectString: connectStr,
          poolMin: 0,
          poolMax: 4,
        }),
      };
    } else if (database.type === 'mssql') {
      // SQL Server via mssql package (must be installed: npm install mssql)
      let mssql;
      try {
        mssql = (await import('mssql')).default ?? (await import('mssql'));
      } catch {
        throw new Error(
          `Motor SQL Server (mssql) no está instalado. ` +
          `Ejecuta: npm install mssql  y reinicia el servidor.`,
        );
      }
      const config = {
        user: database.user,
        password: database.password,
        server: database.host || 'localhost',
        port: Number(database.port) || 1433,
        database: database.database,
        options: {
          encrypt: Boolean(database.encrypt ?? false),
          trustServerCertificate: Boolean(database.trustServerCertificate ?? true),
        },
        connectionTimeout: Number(database.connectionTimeoutMillis) || 5000,
        pool: { max: 5, min: 0, idleTimeoutMillis: 30000 },
      };
      client = {
        type: 'mssql',
        driver: mssql,
        pool: await mssql.connect(config),
      };
    } else if (database.type === 'mongodb') {
      let mongodb;
      try {
        mongodb = await import('mongodb');
      } catch {
        throw new Error(
          `Driver MongoDB no instalado. Ejecuta: npm install mongodb  y reinicia el servidor.`,
        );
      }
      const MongoClient = mongodb.MongoClient ?? mongodb.default?.MongoClient;
      const uri = database.connectionString || (() => {
        const user = database.user ? encodeURIComponent(database.user) : '';
        const pass = database.password ? encodeURIComponent(database.password) : '';
        const auth = user ? (pass ? `${user}:${pass}@` : `${user}@`) : '';
        return `mongodb://${auth}${database.host || 'localhost'}:${database.port || 27017}`;
      })();
      const mongoClient = new MongoClient(uri, { serverSelectionTimeoutMS: 5000 });
      await mongoClient.connect();
      client = {
        type: 'mongodb',
        driver: mongodb,
        client: mongoClient,
        dbName: database.database || 'test',
      };
    } else {
      throw new Error(
        `Motor '${database.type}' no soportado. ` +
        `Motores válidos: postgres, oracle, mysql, mssql, mongodb.`,
      );
    }

    this.clients.set(database.id, client);
    return client;
  }

  async executeCompiledQuery(compiledQuery) {
    const database = this.getDatabaseById(compiledQuery?.databaseId);
    if (!database) {
      throw new Error(`Base no registrada: ${compiledQuery?.databaseId || 'desconocida'}`);
    }

    // Strip trailing semicolons — drivers don't need them and some reject them.
    const sql = String(compiledQuery.sql || '').trimEnd().replace(/;\s*$/, '');

    const client = await this.ensureClient(database);

    // Returns the row array. For write operations (UPDATE/DELETE/INSERT) the
    // driver's affected-row count is attached as a non-enumerable `.rowCount`
    // on the array so callers can report it (SELECTs leave it as rows.length).
    const withRowCount = (rows, count) => {
      const arr = Array.isArray(rows) ? rows : [];
      if (typeof count === 'number' && Number.isFinite(count)) {
        Object.defineProperty(arr, 'rowCount', { value: count, enumerable: false });
      }
      return arr;
    };

    if (client.type === 'postgres') {
      const result = await client.pool.query({
        text: sql,
        values: compiledQuery.params || [],
        query_timeout: this.queryTimeoutMs,
      });
      // pg exposes rowCount for both reads and writes
      return withRowCount(result.rows || [], result.rowCount);
    }

    if (client.type === 'mysql') {
      const [rowsOrHeader] = await client.pool.execute({
        sql,
        values: compiledQuery.params || [],
        timeout: this.queryTimeoutMs,
      });
      // SELECT → array of rows; write → ResultSetHeader with affectedRows
      if (Array.isArray(rowsOrHeader)) return withRowCount(rowsOrHeader, rowsOrHeader.length);
      return withRowCount([], Number(rowsOrHeader?.affectedRows ?? 0));
    }

    if (client.type === 'oracle') {
      const connection = await client.pool.getConnection();
      try {
        connection.callTimeout = this.queryTimeoutMs;
        const result = await connection.execute(sql, compiledQuery.params || {}, {
          outFormat: client.driver?.OUT_FORMAT_OBJECT,
          autoCommit: true,
        });
        // SELECT → result.rows; write → result.rowsAffected (no rows)
        if (Array.isArray(result.rows)) return withRowCount(result.rows, result.rows.length);
        return withRowCount([], Number(result.rowsAffected ?? 0));
      } finally {
        await connection.close();
      }
    }

    if (client.type === 'mssql') {
      const request = client.pool.request();
      // Bind named parameters @p1, @p2... — evita SQL injection por concatenación
      (compiledQuery.params || []).forEach((val, i) => {
        request.input(`p${i + 1}`, val ?? null);
      });
      const result = await request.query(sql);
      if (Array.isArray(result.recordset) && result.recordset.length > 0) {
        return withRowCount(result.recordset, result.recordset.length);
      }
      const affected = Array.isArray(result.rowsAffected)
        ? result.rowsAffected.reduce((a, b) => a + Number(b || 0), 0)
        : Number(result.rowsAffected || 0);
      return withRowCount(result.recordset || [], affected);
    }

    if (client.type === 'mongodb') {
      // sql is a JSON string (or object) describing the MongoDB operation
      // La consulta, tal como la escribe la gente.
      //
      // Esto hacía `JSON.parse`, y lo que se copia de Compass o de mongosh no es JSON:
      // las claves van sin comillas, las cadenas con comilla simple y los valores
      // envueltos —ObjectId('…'), ISODate('…'), NumberInt('4')—. Reventaba con
      // «Unexpected token 'O'», o sea que para usar Mongo había que traducir la consulta
      // a mano antes de pegarla, que es justo el trabajo que ahorra la herramienta.
      //
      // `asegurarConsultaDeMongo` acepta las dos formas y se niega si lo que llega
      // escribe, que es la misma regla que para SQL en otra sintaxis.
      let mongoOp;
      try {
        mongoOp = conTiposDeMongo(asegurarConsultaDeMongo(compiledQuery.sql));
      } catch (error) {
        throw new Error(
          `${error.message}\n`
          + 'Ejemplos:\n'
          + "  { collection: 'orders', find: { status: 'active' }, limit: 100 }\n"
          + "  { collection: 'orders', find: { _id: ObjectId('6824b0c404c5ed34e4070f02') } }\n"
          + "  { collection: 'orders', aggregate: [ { $unwind: '$details' }, { $group: { _id: '$details.estado', n: { $sum: 1 } } } ] }"
        );
      }

      const { collection: colName, ...ops } = mongoOp || {};
      if (!colName) throw new Error('MongoDB: debe especificar el campo "collection" en el JSON.');

      const db = client.client.db(client.dbName);
      const col = db.collection(colName);

      // ── READ ──────────────────────────────────────────────────────────────
      if (ops.find !== undefined) {
        const cursor = col.find(ops.find || {}, { projection: ops.projection || {} });
        if (ops.sort) cursor.sort(ops.sort);
        if (ops.skip) cursor.skip(Number(ops.skip));
        const limit = Number(ops.limit || 1000);
        cursor.limit(limit);
        return await cursor.toArray();
      }
      if (ops.findOne !== undefined) {
        const doc = await col.findOne(ops.findOne || {}, { projection: ops.projection || {} });
        return doc ? [doc] : [];
      }
      if (ops.aggregate) {
        return await col.aggregate(ops.aggregate).toArray();
      }
      if (ops.countDocuments !== undefined) {
        const count = await col.countDocuments(ops.countDocuments || {});
        return [{ count }];
      }
      if (ops.distinct !== undefined) {
        const values = await col.distinct(ops.distinct, ops.filter || {});
        return values.map((v) => ({ value: v }));
      }

      // Aquí vivían insertOne, insertMany, updateOne, updateMany, replaceOne, deleteOne
      // y deleteMany. Ya no se puede llegar: `asegurarConsultaDeMongo` rechaza cualquiera
      // de ellas antes de ejecutar nada, igual que el guardián de SQL rechaza un UPDATE.
      // Dejarlas escritas sería dejar la puerta cerrada con la llave puesta al lado.

      throw new Error(
        'MongoDB: operación no reconocida. El sistema solo consulta: '
        + 'find, findOne, aggregate, countDocuments, distinct.'
      );
    }

    throw new Error(
      `Motor '${client.type}' no tiene implementación de executeCompiledQuery. ` +
      `Motores válidos: postgres, oracle, mysql, mssql, mongodb.`,
    );
  }

  /**
   * Returns the first enabled database of the given engine type, or null.
   * Used for syntax-based auto-routing (e.g. ROWNUM → oracle).
   */
  findDatabaseByType(type) {
    const normalized = String(type || '').trim().toLowerCase();
    for (const db of this.databases.values()) {
      if (db.enabled !== false && String(db.type || '').toLowerCase() === normalized) {
        return db;
      }
    }
    return null;
  }

  /**
   * Executes an Oracle PL/SQL anonymous block natively on an Oracle connection.
   * Captures DBMS_OUTPUT lines and returns them as row objects { output: string }.
   * If the block produces no output, returns [{ resultado: 'Script PL/SQL ejecutado correctamente.' }].
   *
   * @param {string} databaseId
   * @param {string} sql  - Full PL/SQL block (DECLARE...BEGIN...END or BEGIN...END)
   * @returns {Promise<Array<{output?:string, resultado?:string}>>}
   */
  async executeOraclePlSqlBlock(databaseId, sql) {
    const database = this.getDatabaseById(databaseId);
    if (!database) {
      throw new Error(`Base no registrada: ${databaseId}`);
    }

    const client = await this.ensureClient(database);
    if (client.type !== 'oracle') {
      throw new Error(`executeOraclePlSqlBlock requiere Oracle, pero '${databaseId}' es ${client.type}`);
    }

    const oracledb = client.driver;
    const connection = await client.pool.getConnection();

    try {
      connection.callTimeout = this.queryTimeoutMs;

      const hasDbmsOutput = /\bDBMS_OUTPUT\b/i.test(sql);

      if (hasDbmsOutput) {
        await connection.execute('BEGIN DBMS_OUTPUT.ENABLE(NULL); END;');
      }

      // Strip SQL*Plus block terminator — oracledb rejects the trailing "/"
      const cleanSql = String(sql || '').trimEnd().replace(/\s*\n\s*\/\s*$/, '').trimEnd();
      await connection.execute(cleanSql);

      if (hasDbmsOutput) {
        const outResult = await connection.execute(
          'BEGIN DBMS_OUTPUT.GET_LINES(:lines, :numlines); END;',
          {
            lines: {
              dir: oracledb.BIND_OUT,
              type: oracledb.STRING,
              maxArraySize: 500,
              maxSize: 32767,
            },
            numlines: {
              dir: oracledb.BIND_INOUT,
              type: oracledb.NUMBER,
              val: 500,
            },
          },
        );

        const lineCount = Number(outResult.outBinds?.numlines ?? 0);
        const rawLines = Array.isArray(outResult.outBinds?.lines) ? outResult.outBinds.lines : [];
        const lines = rawLines.slice(0, lineCount).filter((l) => l !== null && l !== undefined);

        if (lines.length > 0) {
          return lines.map((line) => ({ output: String(line ?? '') }));
        }
      }

      return [{ resultado: 'Script PL/SQL ejecutado correctamente.' }];
    } finally {
      await connection.close();
    }
  }

  /**
   * Auto-introspects schema for all enabled databases that currently have no tables registered.
   * This is called at startup so the distributed engine can route queries to non-primary DBs.
   * @returns {Promise<Array<{id:string, status:'ok'|'fail'|'skipped-has-schema', tableCount?:number, error?:string}>>}
   */
  async introspectEmptySchemas() {
    const results = [];
    for (const [id, db] of this.databases.entries()) {
      if (db.enabled === false) {
        console.log(`[MultiDbRegistry] introspectEmptySchemas → [${id}] SKIP (disabled)`);
        continue;
      }
      const tables = db.schema?.tables;
      const hasSchema = Array.isArray(tables) && tables.length > 0;
      if (hasSchema) {
        console.log(`[MultiDbRegistry] introspectEmptySchemas → [${id}] SKIP (ya tiene ${tables.length} tablas en config)`);
        results.push({ id, status: 'skipped-has-schema', tableCount: tables.length });
        continue;
      }
      console.log(`[MultiDbRegistry] introspectEmptySchemas → [${id}] iniciando introspección (${db.type} ${db.host}:${db.port}/${db.database})`);

      // ── MongoDB: native collection listing (UniversalDatabaseConnector no soporta MongoDB) ──
      if (db.type === 'mongodb') {
        try {
          const mongodb = await import('mongodb');
          const MongoClient = mongodb.MongoClient ?? mongodb.default?.MongoClient;
          const auth = db.user && db.password ? `${encodeURIComponent(db.user)}:${encodeURIComponent(db.password)}@` : '';
          const dbPart = db.database ? `/${db.database}` : '';
          const uri = db.connectionString || `mongodb://${auth}${db.host || 'localhost'}:${db.port || 27017}${dbPart}?authSource=${db.authSource || 'admin'}`;
          const client = new MongoClient(uri, { serverSelectionTimeoutMS: 5000 });
          await client.connect();
          const mdb = client.db(db.database);
          const collections = await mdb.listCollections().toArray();
          await client.close();
          const tables = collections.map((c) => ({ name: c.name, columns: [] }));
          const schema = { tables };
          const updated = this.normalizeDatabase({ ...db, schema });
          this.databases.set(id, updated);
          console.log(`[MultiDbRegistry] introspectEmptySchemas → [${id}] ✔ MongoDB: ${tables.length} colección(es): ${tables.map((t) => t.name).join(', ')}`);
          results.push({ id, status: 'ok', tableCount: tables.length });
        } catch (err) {
          const errMsg = String(err?.message || err);
          console.error(`[MultiDbRegistry] introspectEmptySchemas → [${id}] ✘ MongoDB FAIL: ${errMsg}`);
          results.push({ id, status: 'fail', error: errMsg });
        }
        continue;
      }

      // ── SQL engines supported by UniversalDatabaseConnector ─────────────────
      const UNIVERSAL_CONNECTOR_TYPES = new Set(['postgres', 'mysql', 'oracle']);

      if (!UNIVERSAL_CONNECTOR_TYPES.has(db.type)) {
        // Unknown/future engine — try mssql native, otherwise mark as schema-unknown
        if (db.type === 'mssql' || db.type === 'sqlserver') {
          try {
            const mssql = (await import('mssql')).default ?? (await import('mssql'));
            const cfg = {
              server: db.host, port: Number(db.port) || 1433,
              database: db.database, user: db.user, password: db.password,
              options: { encrypt: false, trustServerCertificate: true },
              connectionTimeout: 5000,
            };
            const pool = await mssql.connect(cfg);
            const r = await pool.request().query(
              `SELECT TABLE_NAME as name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'`
            );
            await pool.close();
            const tables = (r.recordset || []).map((row) => ({ name: row.name, columns: [] }));
            const schema = { tables };
            this.databases.set(id, this.normalizeDatabase({ ...db, schema }));
            console.log(`[MultiDbRegistry] introspectEmptySchemas → [${id}] ✔ MSSQL: ${tables.length} tabla(s)`);
            results.push({ id, status: 'ok', tableCount: tables.length });
          } catch (err) {
            console.error(`[MultiDbRegistry] introspectEmptySchemas → [${id}] ✘ MSSQL FAIL: ${err?.message}`);
            results.push({ id, status: 'fail', error: String(err?.message || err) });
          }
        } else {
          // Completely unknown engine — register with empty schema, execution will be attempted at runtime
          console.warn(`[MultiDbRegistry] introspectEmptySchemas → [${id}] ⚠ Motor "${db.type}" sin introspección implementada — registrado con schema vacío. Se intentará ejecución en runtime.`);
          results.push({ id, status: 'skipped-unknown-engine', tableCount: 0 });
        }
        continue;
      }

      let connectionResult = null;
      try {
        connectionResult = await connectDatabase(db);

        // Si la conexión fue reutilizada de una probe anterior, el schema en caché
        // puede tener 0 tablas porque fue tomado antes de que el init SQL terminara.
        // Forzar reconexión limpia para garantizar schema fresco.
        if (connectionResult.reused) {
          await closeConnection(connectionResult.dbId).catch(() => {});
          connectionResult = await connectDatabase(db);
        }

        console.log(`[MultiDbRegistry] introspectEmptySchemas → [${id}] conexión OK, obteniendo schema...`);
        const schema = await getSchema(connectionResult.dbId);
        const updated = this.normalizeDatabase({ ...db, schema });
        this.databases.set(id, updated);
        const tableCount = updated.schema.tables.length;
        console.log(`[MultiDbRegistry] introspectEmptySchemas → [${id}] ✔ ${tableCount} tablas cargadas`);
        if (tableCount === 0) {
          console.warn(`[MultiDbRegistry] introspectEmptySchemas → [${id}] ⚠ 0 tablas — usuario "${db.user}" puede no tener permisos sobre los esquemas de esta base`);
        }
        results.push({ id, status: 'ok', tableCount });
      } catch (err) {
        const errMsg = String(err?.message || err);
        console.error(`[MultiDbRegistry] introspectEmptySchemas → [${id}] ✘ FAIL: ${errMsg}`);
        results.push({ id, status: 'fail', error: errMsg });
      } finally {
        if (connectionResult?.dbId) {
          await closeConnection(connectionResult.dbId).catch(() => {});
        }
      }
    }
    return results;
  }
}

export default MultiDbRegistry;