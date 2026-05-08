import SchemaCache from './SchemaCache.js';
import { buildSafeError, UniversalDbError } from './errors.js';
import { buildDatabaseFingerprint, createDatabaseId } from './fingerprint.js';
import PostgresAdapter from '../adapters/PostgresAdapter.js';
import MySqlAdapter from '../adapters/MySqlAdapter.js';
import OracleAdapter from '../adapters/OracleAdapter.js';

function validateConfig(config = {}) {
  const required = ['type', 'host', 'port', 'database', 'user', 'password'];
  const missing = required.filter((key) => config[key] === undefined || config[key] === null || String(config[key]).trim() === '');

  if (missing.length > 0) {
    throw new UniversalDbError(
      'INVALID_DATABASE_CONFIG',
      `Missing required database config fields: ${missing.join(', ')}`,
      { missingFields: missing }
    );
  }

  const normalizedType = String(config.type || '').trim().toLowerCase();
  if (!['postgres', 'mysql', 'oracle'].includes(normalizedType)) {
    throw new UniversalDbError('UNSUPPORTED_DATABASE_ENGINE', `Unsupported database engine: ${config.type}`);
  }

  const parsedPort = Number(config.port);
  if (!Number.isFinite(parsedPort) || parsedPort <= 0) {
    throw new UniversalDbError('INVALID_DATABASE_CONFIG', 'Field "port" must be a valid positive number');
  }

  return {
    ...config,
    type: normalizedType,
    host: String(config.host).trim(),
    port: parsedPort,
    database: String(config.database).trim(),
    user: String(config.user).trim(),
    password: String(config.password),
  };
}

function pickAdapter(config) {
  if (config.type === 'postgres') return new PostgresAdapter(config);
  if (config.type === 'mysql') return new MySqlAdapter(config);
  if (config.type === 'oracle') return new OracleAdapter(config);
  throw new UniversalDbError('UNSUPPORTED_DATABASE_ENGINE', `Unsupported database engine: ${config.type}`);
}

export default class UniversalDatabaseConnector {
  constructor(options = {}) {
    this.schemaTtlMs = Math.max(1000, Number(options.schemaTtlMs) || 60000);
    this.dbContexts = new Map();
  }

  _buildContextPublicShape(context) {
    return {
      dbId: context.dbId,
      type: context.type,
      fingerprint: context.fingerprint,
      connectedAt: context.connectedAt,
      schema: context.schema,
      schemaCache: context.schemaCache.getMeta(),
      semanticLearning: context.semanticLearning,
    };
  }

  _getContextOrThrow(dbId) {
    const key = String(dbId || '').trim();
    if (!key || !this.dbContexts.has(key)) {
      throw new UniversalDbError('DATABASE_NOT_FOUND', `Database context not found for dbId: ${key || 'undefined'}`);
    }

    return this.dbContexts.get(key);
  }

  async connectDatabase(config = {}) {
    const safeConfig = validateConfig(config);
    const dbId = createDatabaseId(safeConfig);
    const dbLabel = `[${safeConfig.type}] ${safeConfig.host}:${safeConfig.port}/${safeConfig.database} (id=${dbId})`;

    if (this.dbContexts.has(dbId)) {
      console.log(`[UniversalDatabaseConnector] connectDatabase → reutilizando contexto existente para ${dbLabel}`);
      return {
        dbId,
        reused: true,
        fingerprint: this.dbContexts.get(dbId).fingerprint,
      };
    }

    const secretValues = [safeConfig.password];
    console.log(`[UniversalDatabaseConnector] connectDatabase → iniciando conexión a ${dbLabel}`);

    try {
      const adapter = pickAdapter(safeConfig);
      console.log(`[UniversalDatabaseConnector] createPool → ${dbLabel}`);
      await adapter.createPool();

      console.log(`[UniversalDatabaseConnector] ping → ${dbLabel}`);
      await adapter.ping();
      console.log(`[UniversalDatabaseConnector] ping OK → ${dbLabel}`);

      console.log(`[UniversalDatabaseConnector] introspectSchema → ${dbLabel}`);
      const schema = await adapter.introspectSchema();
      const tableNames = Object.keys(schema?.tables || {});
      console.log(`[UniversalDatabaseConnector] introspectSchema OK → ${dbLabel}: ${tableNames.length} tablas encontradas`);
      if (tableNames.length === 0) {
        console.warn(`[UniversalDatabaseConnector] ⚠ ADVERTENCIA: 0 tablas encontradas para ${dbLabel}. Verificar permisos del usuario o esquemas disponibles.`);
      } else {
        console.log(`[UniversalDatabaseConnector] tablas: ${tableNames.slice(0, 10).join(', ')}${tableNames.length > 10 ? ` ... (+${tableNames.length - 10} más)` : ''}`);
      }

      const fingerprint = buildDatabaseFingerprint({
        type: safeConfig.type,
        database: safeConfig.database,
        tables: tableNames,
      });

      const schemaCache = new SchemaCache(this.schemaTtlMs);
      schemaCache.set(schema);

      this.dbContexts.set(dbId, {
        dbId,
        type: safeConfig.type,
        config: {
          type: safeConfig.type,
          host: safeConfig.host,
          port: safeConfig.port,
          database: safeConfig.database,
          user: safeConfig.user,
        },
        adapter,
        schemaCache,
        schema,
        fingerprint,
        semanticLearning: {
          tableAliases: {},
          columnKeywords: {},
          updatedAt: Date.now(),
        },
        connectedAt: new Date().toISOString(),
      });

      console.log(`[UniversalDatabaseConnector] ✔ conexión establecida → ${dbLabel}`);
      return {
        dbId,
        reused: false,
        fingerprint,
      };
    } catch (error) {
      const safeErr = buildSafeError(error, {
        defaultCode: 'DATABASE_CONNECTION_FAILED',
        defaultMessage: `Failed to connect to ${safeConfig.type} database`,
        secretValues,
      });
      console.error(`[UniversalDatabaseConnector] ✘ conexión FALLIDA → ${dbLabel}: [${safeErr.code}] ${safeErr.message}`);
      throw safeErr;
    }
  }

  async getSchema(dbId) {
    const context = this._getContextOrThrow(dbId);

    try {
      const cached = context.schemaCache.get();
      if (cached) {
        return cached;
      }

      const schema = await context.adapter.introspectSchema();
      context.schema = schema;
      context.schemaCache.set(schema);
      context.fingerprint = buildDatabaseFingerprint({
        type: context.type,
        database: context.config.database,
        tables: Object.keys(schema?.tables || {}),
      });

      return schema;
    } catch (error) {
      throw buildSafeError(error, {
        defaultCode: 'SCHEMA_DISCOVERY_FAILED',
        defaultMessage: `Failed to discover schema for dbId: ${dbId}`,
      });
    }
  }

  getConnection(dbId) {
    const context = this._getContextOrThrow(dbId);
    return context.adapter.getPool();
  }

  async refreshSchema(dbId) {
    const context = this._getContextOrThrow(dbId);

    try {
      const schema = await context.adapter.introspectSchema();
      context.schema = schema;
      context.schemaCache.clear();
      context.schemaCache.set(schema);
      context.fingerprint = buildDatabaseFingerprint({
        type: context.type,
        database: context.config.database,
        tables: Object.keys(schema?.tables || {}),
      });

      return {
        dbId: context.dbId,
        fingerprint: context.fingerprint,
        schema,
      };
    } catch (error) {
      throw buildSafeError(error, {
        defaultCode: 'SCHEMA_REFRESH_FAILED',
        defaultMessage: `Failed to refresh schema for dbId: ${dbId}`,
      });
    }
  }

  async closeConnection(dbId) {
    const context = this._getContextOrThrow(dbId);

    try {
      await context.adapter.close();
      this.dbContexts.delete(context.dbId);
      return {
        dbId: context.dbId,
        closed: true,
      };
    } catch (error) {
      throw buildSafeError(error, {
        defaultCode: 'DATABASE_CLOSE_FAILED',
        defaultMessage: `Failed to close database connection for dbId: ${dbId}`,
      });
    }
  }

  async closeAllConnections() {
    const dbIds = [...this.dbContexts.keys()];
    const results = [];

    for (const dbId of dbIds) {
      results.push(await this.closeConnection(dbId));
    }

    return results;
  }

  getContext(dbId) {
    const context = this._getContextOrThrow(dbId);
    return this._buildContextPublicShape(context);
  }

  listContexts() {
    return [...this.dbContexts.values()].map((context) => this._buildContextPublicShape(context));
  }
}
