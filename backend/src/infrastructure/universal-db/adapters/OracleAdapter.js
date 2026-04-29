import BaseAdapter from './BaseAdapter.js';
import { ORACLE_SCHEMA_QUERIES } from '../schema/introspectionQueries.js';

function toBooleanNullable(value) {
  return String(value || '').toUpperCase() === 'Y';
}

export default class OracleAdapter extends BaseAdapter {
  constructor(config) {
    super(config);
    this.engine = 'oracle';
    this.oracledb = null;
  }

  buildConnectString() {
    if (this.config.connectString) return this.config.connectString;
    return `${this.config.host}:${this.config.port}/${this.config.database}`;
  }

  async createPool() {
    if (this.pool) return this.pool;

    const oracleModule = await import('oracledb');
    // ESM/CJS interop: some environments expose createPool on default export.
    this.oracledb = oracleModule?.default || oracleModule;

    this.pool = await this.oracledb.createPool({
      user: this.config.user,
      password: this.config.password,
      connectString: this.buildConnectString(),
      poolMin: 0,
      poolMax: Number(this.config.maxPoolSize) || 8,
      poolIncrement: 1,
      queueTimeout: Number(this.config.connectionTimeoutMillis) || 5000,
    });

    return this.pool;
  }

  async withConnection(callback) {
    const pool = await this.createPool();
    const connection = await pool.getConnection();

    try {
      return await callback(connection);
    } finally {
      await connection.close();
    }
  }

  async ping() {
    await this.withConnection(async (connection) => {
      await connection.execute('SELECT 1 FROM dual');
    });
  }

  async introspectSchema() {
    return this.withConnection(async (connection) => {
      const [tablesRes, columnsRes, pkRes, fkRes] = await Promise.all([
        connection.execute(ORACLE_SCHEMA_QUERIES.tables, [], { outFormat: this.oracledb.OUT_FORMAT_OBJECT }),
        connection.execute(ORACLE_SCHEMA_QUERIES.columns, [], { outFormat: this.oracledb.OUT_FORMAT_OBJECT }),
        connection.execute(ORACLE_SCHEMA_QUERIES.primaryKeys, [], { outFormat: this.oracledb.OUT_FORMAT_OBJECT }),
        connection.execute(ORACLE_SCHEMA_QUERIES.foreignKeys, [], { outFormat: this.oracledb.OUT_FORMAT_OBJECT }),
      ]);

      return this.normalizeSchema({
        tablesRows: tablesRes.rows || [],
        columnsRows: columnsRes.rows || [],
        primaryKeyRows: pkRes.rows || [],
        foreignKeyRows: fkRes.rows || [],
      });
    });
  }

  normalizeSchema(raw = {}) {
    const tables = {};

    for (const row of raw.tablesRows || []) {
      const tableName = String(row.TABLE_NAME || '').toLowerCase();
      if (!tableName) continue;

      tables[tableName] = {
        columns: [],
        primaryKey: null,
        primaryKeys: [],
        foreignKeys: [],
      };
    }

    for (const row of raw.columnsRows || []) {
      const tableName = String(row.TABLE_NAME || '').toLowerCase();
      if (!tables[tableName]) continue;

      tables[tableName].columns.push({
        name: String(row.COLUMN_NAME || '').toLowerCase(),
        type: String(row.DATA_TYPE || '').toLowerCase(),
        nullable: toBooleanNullable(row.NULLABLE),
        default: row.DATA_DEFAULT ?? null,
      });
    }

    for (const row of raw.primaryKeyRows || []) {
      const tableName = String(row.TABLE_NAME || '').toLowerCase();
      if (!tables[tableName]) continue;

      const pkColumn = String(row.COLUMN_NAME || '').toLowerCase();
      if (!pkColumn) continue;

      tables[tableName].primaryKeys.push(pkColumn);
      if (!tables[tableName].primaryKey) {
        tables[tableName].primaryKey = pkColumn;
      }
    }

    for (const row of raw.foreignKeyRows || []) {
      const tableName = String(row.TABLE_NAME || '').toLowerCase();
      if (!tables[tableName]) continue;

      tables[tableName].foreignKeys.push({
        column: String(row.COLUMN_NAME || '').toLowerCase(),
        referencesTable: String(row.REFERENCED_TABLE || '').toLowerCase(),
        referencesColumn: String(row.REFERENCED_COLUMN || '').toLowerCase(),
        constraintName: String(row.CONSTRAINT_NAME || ''),
      });
    }

    return { tables };
  }
}
