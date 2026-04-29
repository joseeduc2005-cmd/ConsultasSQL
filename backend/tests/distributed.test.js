import MultiDatabaseEngine from '../src/infrastructure/distributed/MultiDatabaseEngine.js';
import QueryIntelligenceEngine from '../src/infrastructure/query/QueryIntelligenceEngine.js';

function createQueryBuilderStub() {
  const queryBuilder = {
    learnedSemanticDictionary: { tableAliases: {}, columnKeywords: {} },
    semanticAliases: {
      usuarios: ['users'],
      usuario: ['users'],
      logs: ['logs'],
      log: ['logs'],
    },
    similarityScore(a, b) {
      const left = String(a || '');
      const right = String(b || '');
      if (left === right) return 1;
      if (!left || !right) return 0;
      if (left.includes(right) || right.includes(left)) return 0.88;
      return 0.2;
    },
    getMergedSemanticAliases() {
      return this.semanticAliases;
    },
    extractQuotedStrings(input) {
      return Array.from(String(input || '').matchAll(/"([^"]+)"|'([^']+)'/g))
        .map((match) => match[1] || match[2])
        .filter(Boolean);
    },
    buildJoinPlan(schema, tables) {
      const edges = [];
      if (tables.includes('users') && tables.includes('logs')) {
        edges.push({ leftTable: 'users', leftColumn: 'id', rightTable: 'logs', rightColumn: 'user_id' });
      }
      return {
        baseTable: tables[0],
        joinEdges: edges,
        joinedTables: new Set(tables),
        missingTables: edges.length ? [] : tables.slice(1),
      };
    },
    hasUsableJoinPlan(joinPlan, tables) {
      if ((tables || []).length <= 1) return false;
      return (joinPlan?.missingTables || []).length === 0 && (joinPlan?.joinEdges || []).length > 0;
    },
    pickPrimaryKey(tableSchema) {
      return tableSchema?.pkPrincipal || tableSchema?.clavesPrimarias?.[0] || null;
    },
    pickBestIdentityColumn(tableSchema) {
      return tableSchema?.columnas?.find((column) => ['username', 'name', 'nombre', 'email'].includes(column.nombre))?.nombre || null;
    },
    findBestAttributeColumn(attribute, tableSchema) {
      return tableSchema?.columnas?.find((column) => String(column.nombre || '').includes(attribute))?.nombre || null;
    },
  };
  queryBuilder.queryIntelligenceEngine = new QueryIntelligenceEngine(queryBuilder);
  return queryBuilder;
}

function createRegistryStub() {
  const databases = [
    {
      id: 'db1',
      type: 'postgres',
      fingerprint: 'db1-fp',
      schema: {
        tables: [
          {
            name: 'users',
            keyColumns: ['id'],
            columns: [
              { name: 'id', type: 'integer', key: true },
              { name: 'username', type: 'varchar' },
              { name: 'email', type: 'varchar' },
            ],
          },
        ],
      },
    },
    {
      id: 'db2',
      type: 'postgres',
      fingerprint: 'db2-fp',
      schema: {
        tables: [
          {
            name: 'logs',
            keyColumns: ['id'],
            columns: [
              { name: 'id', type: 'integer', key: true },
              { name: 'user_id', type: 'integer' },
              { name: 'message', type: 'varchar' },
            ],
            foreignKeys: [
              { column: 'user_id', referencedTable: 'users', referencedColumn: 'id' },
            ],
          },
        ],
      },
    },
  ];

  return {
    getDatabases() {
      return databases;
    },
    getPrimaryDatabase() {
      return databases[0];
    },
    buildConnectionString() {
      return 'postgresql://postgres:test@localhost:5432/db1';
    },
    getDatabaseById(databaseId) {
      return databases.find((database) => database.id === databaseId) || null;
    },
    getLearningSnapshot() {
      return { tableAliases: {}, columnKeywords: {} };
    },
    learn() {
      return undefined;
    },
    async registerDatabase(config) {
      return config;
    },
    async executeCompiledQuery(compiledQuery) {
      if (compiledQuery.role === 'base') {
        return [
          { users_id: 1, users_username: 'juan', users_email: 'juan@example.com' },
        ];
      }

      if (compiledQuery.role === 'related') {
        return [
          { merge_key: 1, logs_id: 10, logs_user_id: 1, logs_message: 'login' },
          { merge_key: 1, logs_id: 11, logs_user_id: 1, logs_message: 'update profile' },
        ];
      }

      return [];
    },
  };
}

describe('Distributed engine behavior', () => {
  test('single simple query usuarios should not be unresolved and should keep high confidence', async () => {
    const singleDbRegistry = {
      getDatabases() {
        return [
          {
            id: 'db1',
            type: 'postgres',
            fingerprint: 'db1-fp',
            schema: {
              tables: [
                {
                  name: 'users',
                  keyColumns: ['id'],
                  columns: [
                    { name: 'id', type: 'integer', key: true },
                    { name: 'username', type: 'varchar' },
                  ],
                },
              ],
            },
          },
        ];
      },
      getPrimaryDatabase() {
        return this.getDatabases()[0];
      },
      buildConnectionString() {
        return 'postgresql://postgres:test@localhost:5432/db1';
      },
      getDatabaseById(databaseId) {
        return this.getDatabases().find((database) => database.id === databaseId) || null;
      },
      getLearningSnapshot() {
        return { tableAliases: {}, columnKeywords: {} };
      },
      learn() {
        return undefined;
      },
      async executeCompiledQuery() {
        return [
          { users_id: 1, users_username: 'juan' },
        ];
      },
    };

    const engine = new MultiDatabaseEngine(singleDbRegistry, createQueryBuilderStub());
    const result = await engine.execute('usuarios', { limit: 10 });

    expect(result.executionType).toBe('single-db');
    expect(result.success).toBe(true);
    expect(result.confidence).toBeGreaterThan(0.8);
    expect(Array.isArray(result.data)).toBe(true);
    expect(result.data.length).toBeGreaterThan(0);
  });

  test('users in db1 and logs in db2 should execute distributed and merge results', async () => {
    const engine = new MultiDatabaseEngine(createRegistryStub(), createQueryBuilderStub());
    const result = await engine.execute('usuarios con logs', { limit: 20 });

    expect(result.success).toBe(true);
    expect(result.executionType).toBe('distributed');
    expect(result.source).toEqual(expect.arrayContaining(['db1', 'db2']));
    expect(Array.isArray(result.data)).toBe(true);
    expect(result.data.length).toBeGreaterThan(0);
    expect(result.data[0]).toEqual(
      expect.objectContaining({
        users_id: 1,
        logs_count: 2,
        logs: expect.any(Array),
      })
    );
  });
});
