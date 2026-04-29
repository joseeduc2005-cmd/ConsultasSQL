import QueryIntelligenceEngine from '../src/infrastructure/query/QueryIntelligenceEngine.js';

function createQueryBuilderStub() {
  return {
    learnedSemanticDictionary: { tableAliases: {}, columnKeywords: {} },
    similarityScore(a, b) {
      const left = String(a || '');
      const right = String(b || '');
      if (left === right) return 1;
      if (!left || !right) return 0;
      if (left.includes(right) || right.includes(left)) return 0.88;
      return 0.2;
    },
    extractQuotedStrings(input) {
      return Array.from(String(input || '').matchAll(/"([^"]+)"|'([^']+)'/g))
        .map((match) => match[1] || match[2])
        .filter(Boolean);
    },
    buildJoinPlan(schema, tables) {
      return {
        baseTable: tables[0],
        joinEdges: [],
        joinedTables: new Set(tables),
        missingTables: tables.slice(1),
      };
    },
    hasUsableJoinPlan() {
      return false;
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
}

function createSchema() {
  return {
    tables: ['users'],
    schema: {
      users: {
        columnas: [
          { nombre: 'id', tipo: 'uuid' },
          { nombre: 'username', tipo: 'varchar' },
        ],
        clavesPrimarias: ['id'],
        pkPrincipal: 'id',
        clavesForaneas: [],
      },
    },
    semanticIndex: {
      users: { tokens: ['users', 'user', 'usuarios', 'usuario'], columnas: {} },
    },
  };
}

describe('Schema validation and empty response behavior', () => {
  test('clientes premium should return clear entity-not-found error when schema has no customers table', () => {
    const intelligence = new QueryIntelligenceEngine(createQueryBuilderStub());
    const result = intelligence.buildSmartQuery('clientes premium', createSchema(), { tablaBase: 'users', topScore: 0.2, confianza: 0.2 });

    expect(result.error).toBeTruthy();
    expect(String(result.error).toLowerCase()).toContain('entidad');
    expect(String(result.error).toLowerCase()).toContain('no existe');
  });

  test('empty result should return no-results message and empty data array', () => {
    const intelligence = new QueryIntelligenceEngine(createQueryBuilderStub());
    const handled = intelligence.handleEmptyResults({ rows: [], rowCount: 0 }, { input: 'usuarios llamados xxxxxx' });

    expect(handled.data).toEqual([]);
    expect(handled.rowCount).toBe(0);
    expect(String(handled.message || '').toLowerCase()).toContain('no se encontraron');
  });
});
