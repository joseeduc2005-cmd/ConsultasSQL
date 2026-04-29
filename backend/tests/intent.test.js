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
      const joinEdges = [];
      if (tables.includes('users') && tables.includes('logs')) {
        joinEdges.push({ leftTable: 'users', leftColumn: 'id', rightTable: 'logs', rightColumn: 'user_id' });
      }
      return {
        baseTable: tables[0],
        joinEdges,
        joinedTables: new Set(tables),
        missingTables: joinEdges.length > 0 ? [] : tables.slice(1),
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
}

function createSchema() {
  return {
    tables: ['users', 'logs'],
    schema: {
      users: {
        columnas: [
          { nombre: 'id', tipo: 'uuid' },
          { nombre: 'username', tipo: 'varchar' },
          { nombre: 'active', tipo: 'boolean' },
        ],
        clavesPrimarias: ['id'],
        pkPrincipal: 'id',
        clavesForaneas: [],
      },
      logs: {
        columnas: [
          { nombre: 'id', tipo: 'uuid' },
          { nombre: 'user_id', tipo: 'uuid' },
        ],
        clavesPrimarias: ['id'],
        pkPrincipal: 'id',
        clavesForaneas: [
          { columna: 'user_id', tablaReferenciada: 'users', columnaReferenciada: 'id' },
        ],
      },
    },
    semanticIndex: {
      users: { tokens: ['users', 'user', 'usuarios', 'usuario'], columnas: {} },
      logs: { tokens: ['logs', 'log'], columnas: {} },
    },
  };
}

describe('Intent interpretation behavior', () => {
  test('usuarios con 0 logs should map to IS NULL semantics', () => {
    const intelligence = new QueryIntelligenceEngine(createQueryBuilderStub());
    const result = intelligence.buildSmartQuery('usuarios con 0 logs', createSchema(), { tablaBase: 'users', topScore: 0.93, confianza: 0.93 });

    expect(result.error).toBeUndefined();
    expect(result.plan.whereConditions).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ table: 'logs', operator: 'IS NULL' }),
      ])
    );
  });

  test('usuarios con más de 5 logs should map to COUNT + HAVING semantics', () => {
    const intelligence = new QueryIntelligenceEngine(createQueryBuilderStub());
    const result = intelligence.buildSmartQuery('usuarios con más de 5 logs', createSchema(), { tablaBase: 'users', topScore: 0.95, confianza: 0.95 });

    expect(result.error).toBeUndefined();
    expect(result.plan.requiresAggregation).toBe(true);
    expect(result.plan.aggregationFn).toBe('COUNT');
    expect(result.plan.havingCondition).toEqual(
      expect.objectContaining({ aggregation: 'COUNT', operator: '>', value: 5 })
    );
  });

  test('usuarios llamados juan should map to identity-name filtering', () => {
    const intelligence = new QueryIntelligenceEngine(createQueryBuilderStub());
    const result = intelligence.buildSmartQuery('usuarios llamados juan', createSchema(), { tablaBase: 'users', topScore: 0.92, confianza: 0.92 });

    expect(result.error).toBeUndefined();
    expect(result.plan.whereConditions).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ table: 'users', operator: 'ILIKE', value: '%juan%' }),
      ])
    );
  });

  test('usuario admin should infer implicit identity filtering', () => {
    const intelligence = new QueryIntelligenceEngine(createQueryBuilderStub());
    const result = intelligence.buildSmartQuery('usuario admin', createSchema(), { tablaBase: 'users', topScore: 0.9, confianza: 0.9 });

    expect(result.error).toBeUndefined();
    expect(result.plan.whereConditions).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ table: 'users', operator: 'ILIKE', value: '%admin%' }),
      ])
    );
  });

  test('usuarios con mas logs should not treat "mas" as entity', () => {
    const intelligence = new QueryIntelligenceEngine(createQueryBuilderStub());
    const result = intelligence.buildSmartQuery('usuarios con mas logs', createSchema(), { tablaBase: 'users', topScore: 0.91, confianza: 0.91 });

    expect(result.error).toBeUndefined();
    expect(result.plan.whereConditions).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ table: 'logs', operator: 'IS NOT NULL' }),
      ])
    );
  });
});
