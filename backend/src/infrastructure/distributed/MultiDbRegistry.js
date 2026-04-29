import fs from 'fs';
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

export class MultiDbRegistry {
  constructor(config = null) {
    this.databases = new Map();
    this.clients = new Map();
    this.runtimeLearning = new Map();
    this.queryTimeoutMs = Math.max(1000, Number(process.env.SQL_QUERY_TIMEOUT_MS) || 8000);
    this.loadConfig(config || this.readConfigFromEnv());
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

    if (resolvedPath) {
      try {
        if (fs.existsSync(resolvedPath)) {
          const fileContents = fs.readFileSync(resolvedPath, 'utf8').trim();
          if (fileContents) {
            const parsedFile = JSON.parse(fileContents);
            if (parsedFile && typeof parsedFile === 'object') {
              return parsedFile;
            }
          }
        }
      } catch (error) {
        console.error('[MULTI_DB] Config file inválido:', error?.message || error);
      }
    }

    const raw = String(process.env.MULTI_DB_CONFIG || process.env.DATABASES_CONFIG || '').trim();
    if (!raw) {
      return { databases: [] };
    }

    try {
      const parsed = JSON.parse(raw);
      return parsed && typeof parsed === 'object' ? parsed : { databases: [] };
    } catch (error) {
      console.error('[MULTI_DB] Config JSON inválido:', error?.message || error);
      return { databases: [] };
    }
  }

  loadConfig(config = {}) {
    this.databases.clear();
    this.clients.clear();
    this.runtimeLearning.clear();

    for (const entry of config?.databases || []) {
      const normalized = this.normalizeDatabase(entry);
      if (!normalized) continue;
      this.databases.set(normalized.id, normalized);
      this.runtimeLearning.set(normalized.id, normalizeLearning(normalized.semanticLearning));
    }
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
      fingerprint: database.fingerprint,
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
    } else {
      throw new Error(
        `Motor '${database.type}' no soportado. ` +
        `Motores válidos: postgres, oracle, mysql, mssql.`,
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

    if (client.type === 'postgres') {
      const result = await client.pool.query({
        text: sql,
        values: compiledQuery.params || [],
        query_timeout: this.queryTimeoutMs,
      });
      return result.rows || [];
    }

    if (client.type === 'mysql') {
      const [rows] = await client.pool.execute({
        sql,
        values: compiledQuery.params || [],
        timeout: this.queryTimeoutMs,
      });
      return Array.isArray(rows) ? rows : [];
    }

    if (client.type === 'oracle') {
      const connection = await client.pool.getConnection();
      try {
        connection.callTimeout = this.queryTimeoutMs;
        const result = await connection.execute(sql, compiledQuery.params || {}, {
          outFormat: client.driver?.OUT_FORMAT_OBJECT,
        });
        return result.rows || [];
      } finally {
        await connection.close();
      }
    }

    if (client.type === 'mssql') {
      const request = client.pool.request();
      const result = await request.query(sql);
      return result.recordset || [];
    }

    throw new Error(
      `Motor '${client.type}' no tiene implementación de executeCompiledQuery. ` +
      `Motores válidos: postgres, oracle, mysql, mssql.`,
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
      if (db.enabled === false) continue;
      const tables = db.schema?.tables;
      const hasSchema = Array.isArray(tables) && tables.length > 0;
      if (hasSchema) {
        results.push({ id, status: 'skipped-has-schema', tableCount: tables.length });
        continue;
      }
      let connectionResult = null;
      try {
        connectionResult = await connectDatabase(db);
        const schema = await getSchema(connectionResult.dbId);
        const updated = this.normalizeDatabase({ ...db, schema });
        this.databases.set(id, updated);
        const tableCount = updated.schema.tables.length;
        results.push({ id, status: 'ok', tableCount });
      } catch (err) {
        const errMsg = String(err?.message || err);
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