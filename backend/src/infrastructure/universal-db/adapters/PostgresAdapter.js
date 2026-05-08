import { Pool } from 'pg';
import BaseAdapter from './BaseAdapter.js';
import { POSTGRES_SCHEMA_QUERIES } from '../schema/introspectionQueries.js';

function toBooleanNullable(value) {
  return String(value || '').toUpperCase() === 'YES';
}

export default class PostgresAdapter extends BaseAdapter {
  constructor(config) {
    super(config);
    this.engine = 'postgres';
  }

  async createPool() {
    if (this.pool) return this.pool;

    this.pool = new Pool({
      host: this.config.host,
      port: this.config.port,
      database: this.config.database,
      user: this.config.user,
      password: this.config.password,
      max: Number(this.config.maxPoolSize) || 10,
      idleTimeoutMillis: Number(this.config.idleTimeoutMillis) || 30000,
      connectionTimeoutMillis: Number(this.config.connectionTimeoutMillis) || 5000,
    });

    return this.pool;
  }

  async ping() {
    const pool = await this.createPool();
    await pool.query('SELECT 1');
  }

  async introspectSchema() {
    const pool = await this.createPool();
    const dbLabel = `${this.config.host}:${this.config.port}/${this.config.database}`;
    console.log(`[PostgresAdapter] introspectSchema → iniciando para ${dbLabel}`);

    const [tablesRes, columnsRes, pkRes, fkRes] = await Promise.all([
      pool.query(POSTGRES_SCHEMA_QUERIES.tables),
      pool.query(POSTGRES_SCHEMA_QUERIES.columns),
      pool.query(POSTGRES_SCHEMA_QUERIES.primaryKeys),
      pool.query(POSTGRES_SCHEMA_QUERIES.foreignKeys),
    ]);

    console.log(`[PostgresAdapter] introspectSchema → ${dbLabel}: tablas=${tablesRes.rows.length} columnas=${columnsRes.rows.length} PKs=${pkRes.rows.length} FKs=${fkRes.rows.length}`);

    // Log esquemas encontrados agrupados
    const schemaGroups = {};
    for (const row of tablesRes.rows) {
      const s = row.table_schema;
      schemaGroups[s] = (schemaGroups[s] || 0) + 1;
    }
    console.log(`[PostgresAdapter] introspectSchema → ${dbLabel}: esquemas detectados:`, schemaGroups);

    const result = this.normalizeSchema({
      tablesRows: tablesRes.rows,
      columnsRows: columnsRes.rows,
      primaryKeyRows: pkRes.rows,
      foreignKeyRows: fkRes.rows,
    });

    console.log(`[PostgresAdapter] introspectSchema → ${dbLabel}: tablas normalizadas=${Object.keys(result.tables).length}`);
    return result;
  }

  // Convierte "schema"+"table_name" en una clave: si el schema es 'public' usa solo
  // el nombre de tabla (compatibilidad hacia atrás); para otros schemas usa schema.tabla
  _tableKey(schema, tableName) {
    const s = String(schema || '').toLowerCase();
    const t = String(tableName || '').toLowerCase();
    return s === 'public' ? t : `${s}.${t}`;
  }

  normalizeSchema(raw = {}) {
    const tables = {};

    for (const row of raw.tablesRows || []) {
      const key = this._tableKey(row.table_schema, row.table_name);
      if (!key) continue;

      tables[key] = {
        columns: [],
        primaryKey: null,
        primaryKeys: [],
        foreignKeys: [],
        schema: String(row.table_schema || 'public').toLowerCase(),
      };
    }

    for (const row of raw.columnsRows || []) {
      const key = this._tableKey(row.table_schema, row.table_name);
      if (!tables[key]) continue;

      tables[key].columns.push({
        name: String(row.column_name || '').toLowerCase(),
        type: String(row.data_type || '').toLowerCase(),
        nullable: toBooleanNullable(row.is_nullable),
        default: row.column_default ?? null,
      });
    }

    for (const row of raw.primaryKeyRows || []) {
      const key = this._tableKey(row.table_schema, row.table_name);
      if (!tables[key]) continue;

      const pkColumn = String(row.column_name || '').toLowerCase();
      if (!pkColumn) continue;

      tables[key].primaryKeys.push(pkColumn);
      if (!tables[key].primaryKey) {
        tables[key].primaryKey = pkColumn;
      }
    }

    for (const row of raw.foreignKeyRows || []) {
      const key = this._tableKey(row.table_schema, row.table_name);
      if (!tables[key]) continue;

      tables[key].foreignKeys.push({
        column: String(row.column_name || '').toLowerCase(),
        referencesTable: String(row.referenced_table || '').toLowerCase(),
        referencesColumn: String(row.referenced_column || '').toLowerCase(),
        constraintName: String(row.constraint_name || ''),
      });
    }

    return { tables };
  }
}
