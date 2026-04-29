import BaseAdapter from './BaseAdapter.js';
import { MYSQL_SCHEMA_QUERIES } from '../schema/introspectionQueries.js';

function toBooleanNullable(value) {
  return String(value || '').toUpperCase() === 'YES';
}

export default class MySqlAdapter extends BaseAdapter {
  constructor(config) {
    super(config);
    this.engine = 'mysql';
  }

  async createPool() {
    if (this.pool) return this.pool;

    const mysql = await import('mysql2/promise');

    this.pool = mysql.createPool({
      host: this.config.host,
      port: this.config.port,
      database: this.config.database,
      user: this.config.user,
      password: this.config.password,
      connectionLimit: Number(this.config.maxPoolSize) || 10,
      connectTimeout: Number(this.config.connectionTimeoutMillis) || 5000,
      waitForConnections: true,
      queueLimit: 0,
    });

    return this.pool;
  }

  async ping() {
    const pool = await this.createPool();
    await pool.query('SELECT 1');
  }

  async introspectSchema() {
    const pool = await this.createPool();
    const schemaName = this.config.database;

    const [tablesRes, columnsRes, pkRes, fkRes] = await Promise.all([
      pool.execute(MYSQL_SCHEMA_QUERIES.tables, [schemaName]),
      pool.execute(MYSQL_SCHEMA_QUERIES.columns, [schemaName]),
      pool.execute(MYSQL_SCHEMA_QUERIES.primaryKeys, [schemaName]),
      pool.execute(MYSQL_SCHEMA_QUERIES.foreignKeys, [schemaName]),
    ]);

    return this.normalizeSchema({
      tablesRows: tablesRes[0] || [],
      columnsRows: columnsRes[0] || [],
      primaryKeyRows: pkRes[0] || [],
      foreignKeyRows: fkRes[0] || [],
    });
  }

  normalizeSchema(raw = {}) {
    const tables = {};

    for (const row of raw.tablesRows || []) {
      const tableName = String(row.table_name || '').toLowerCase();
      if (!tableName) continue;

      tables[tableName] = {
        columns: [],
        primaryKey: null,
        primaryKeys: [],
        foreignKeys: [],
      };
    }

    for (const row of raw.columnsRows || []) {
      const tableName = String(row.table_name || '').toLowerCase();
      if (!tables[tableName]) continue;

      tables[tableName].columns.push({
        name: String(row.column_name || '').toLowerCase(),
        type: String(row.data_type || '').toLowerCase(),
        nullable: toBooleanNullable(row.is_nullable),
        default: row.column_default ?? null,
      });
    }

    for (const row of raw.primaryKeyRows || []) {
      const tableName = String(row.table_name || '').toLowerCase();
      if (!tables[tableName]) continue;

      const pkColumn = String(row.column_name || '').toLowerCase();
      if (!pkColumn) continue;

      tables[tableName].primaryKeys.push(pkColumn);
      if (!tables[tableName].primaryKey) {
        tables[tableName].primaryKey = pkColumn;
      }
    }

    for (const row of raw.foreignKeyRows || []) {
      const tableName = String(row.table_name || '').toLowerCase();
      if (!tables[tableName]) continue;

      tables[tableName].foreignKeys.push({
        column: String(row.column_name || '').toLowerCase(),
        referencesTable: String(row.referenced_table || '').toLowerCase(),
        referencesColumn: String(row.referenced_column || '').toLowerCase(),
        constraintName: String(row.constraint_name || ''),
      });
    }

    return { tables };
  }
}
