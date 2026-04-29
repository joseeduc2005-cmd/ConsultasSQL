function normalizeIdentifier(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9_\s-]/g, ' ')
    .replace(/\s+/g, '_')
    .trim();
}

function parseConnectionMetadata(connectionString = '') {
  const fallback = {
    type: 'postgres',
    host: 'localhost',
    port: 5432,
    database: 'default',
    user: 'app',
  };

  if (!connectionString) {
    return {
      ...fallback,
      connectionString: '',
    };
  }

  try {
    const parsed = new URL(connectionString);
    const protocol = String(parsed.protocol || '').replace(':', '').toLowerCase();
    const inferredType = protocol.includes('mysql') ? 'mysql' : protocol.includes('oracle') ? 'oracle' : 'postgres';

    return {
      type: inferredType,
      host: parsed.hostname || fallback.host,
      port: Number(parsed.port) || fallback.port,
      database: String(parsed.pathname || '').replace(/^\//, '') || fallback.database,
      user: decodeURIComponent(parsed.username || fallback.user),
      connectionString,
    };
  } catch {
    return {
      ...fallback,
      connectionString,
    };
  }
}

function resolvePrimaryDatabaseMetadata(multiDbRegistry) {
  const primaryDatabase = typeof multiDbRegistry?.getPrimaryDatabase === 'function'
    ? multiDbRegistry.getPrimaryDatabase({ includeDisabled: true })
    : null;

  if (primaryDatabase) {
    const connectionString = typeof multiDbRegistry?.buildConnectionString === 'function'
      ? multiDbRegistry.buildConnectionString(primaryDatabase)
      : String(primaryDatabase.connectionString || '').trim();

    return {
      id: primaryDatabase.id,
      label: primaryDatabase.label || 'Primary Active Database',
      type: primaryDatabase.type || 'postgres',
      host: primaryDatabase.host,
      port: primaryDatabase.port,
      database: primaryDatabase.database,
      user: primaryDatabase.user,
      primary: true,
      connectionString,
    };
  }

  const fallback = parseConnectionMetadata(String(process.env.DATABASE_URL || '').trim());
  return {
    id: buildDefaultDatabaseId(fallback),
    label: 'Default Active Database',
    ...fallback,
    primary: true,
  };
}

function buildRegistrySchema(fullSchema = {}) {
  const schemaMap = fullSchema?.schema || {};
  const tableNames = Array.isArray(fullSchema?.tables)
    ? fullSchema.tables
    : Object.keys(schemaMap);

  const tables = tableNames
    .map((tableName) => {
      const tableSchema = schemaMap?.[tableName] || {};
      const primaryKeys = Array.isArray(tableSchema?.clavesPrimarias)
        ? tableSchema.clavesPrimarias
        : tableSchema?.pkPrincipal
          ? [tableSchema.pkPrincipal]
          : [];

      const columns = (tableSchema?.columnas || [])
        .map((column) => {
          const name = String(column?.nombre || '').trim();
          if (!name) return null;
          return {
            name,
            type: String(column?.tipo || 'text').trim() || 'text',
            key: primaryKeys.includes(name) || tableSchema?.pkPrincipal === name,
          };
        })
        .filter(Boolean);

      const foreignKeys = (tableSchema?.clavesForaneas || [])
        .map((fk) => {
          const column = String(fk?.columna || '').trim();
          const referencedTable = String(fk?.tablaReferenciada || '').trim();
          const referencedColumn = String(fk?.columnaReferenciada || '').trim();
          if (!column || !referencedTable || !referencedColumn) return null;
          return {
            column,
            referencedTable,
            referencedColumn,
            constraintName: String(fk?.nombreConstraint || '').trim(),
          };
        })
        .filter(Boolean);

      if (!tableName) return null;
      return {
        name: String(tableName).trim(),
        columns,
        keyColumns: primaryKeys,
        foreignKeys,
      };
    })
    .filter((table) => table?.name);

  return { tables };
}

function buildDefaultDatabaseId(metadata = {}) {
  const databaseName = normalizeIdentifier(metadata.database || 'primary');
  return `default_${databaseName || 'primary'}`;
}

/**
 * Bootstrap conservador del subsistema multi-db.
 *
 * Garantiza que exista al menos una base registrada para evitar fallos
 * de /api/query cuando no hay MULTI_DB_CONFIG definido.
 */
export async function bootstrapGlobalSystems({
  multiDbRegistry,
  schemaDetector,
  queryBuilder,
  pool,
  logger = console,
}) {
  const existing = multiDbRegistry.getDatabases();
  const metadata = resolvePrimaryDatabaseMetadata(multiDbRegistry);

  const fullSchema = typeof queryBuilder?.getFullSchema === 'function'
    ? await queryBuilder.getFullSchema()
    : await schemaDetector.getFullSchema();

  const registrySchema = buildRegistrySchema(fullSchema);
  const tableCount = registrySchema.tables.length;
  if (tableCount === 0) {
    throw new Error('No se detectaron tablas para registrar la base por defecto');
  }

  await multiDbRegistry.registerDatabase({
    id: metadata.id || buildDefaultDatabaseId(metadata),
    label: metadata.label || 'Primary Active Database',
    type: metadata.type || 'postgres',
    host: metadata.host,
    port: metadata.port,
    database: metadata.database,
    user: metadata.user,
    connectionString: metadata.connectionString,
    primary: true,
    connection: pool,
    schema: registrySchema,
    enabled: true,
  });

  const registered = multiDbRegistry.getDatabases().map((database) => database.id);
  logger.log(`✓ MultiDB bootstrap listo: ${registered.length} base(s) registrada(s)`);

  if (!queryBuilder?.queryIntelligenceEngine) {
    logger.warn('⚠ QueryBuilder no expone queryIntelligenceEngine; revisar integración semántica');
  }

  return {
    bootstrapped: true,
    reason: existing.length === 0 ? 'primary-database-registered' : 'primary-database-refreshed',
    registeredDatabases: registered,
  };
}

export default bootstrapGlobalSystems;
