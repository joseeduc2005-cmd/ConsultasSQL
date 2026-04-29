import IntentScoringEngine from '../src/infrastructure/query/IntentScoringEngine.js';
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
          { nombre: 'created_at', tipo: 'timestamp' },
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
      logs: { tokens: ['logs', 'log', 'actividad', 'activity'], columnas: {} },
    },
  };
}

describe('Intent scoring behavior', () => {
  test('high score should execute directly', () => {
    const scoring = new IntentScoringEngine(createQueryBuilderStub());
    const selection = scoring.selectBestInterpretation([
      { interpretation: 'high-confidence', score: 0.91 },
      { interpretation: 'alternative', score: 0.66 },
    ]);

    expect(selection.bestScore).toBeGreaterThan(0.8);
    expect(selection.requiresClarification).toBe(false);
    expect(selection.executeWithWarning).toBe(false);
  });

  test('medium score should execute with warning', () => {
    const scoring = new IntentScoringEngine(createQueryBuilderStub());
    const selection = scoring.selectBestInterpretation([
      { interpretation: 'medium-confidence', score: 0.72 },
      { interpretation: 'alternative', score: 0.42 },
    ]);

    expect(selection.bestScore).toBeGreaterThanOrEqual(0.5);
    expect(selection.bestScore).toBeLessThanOrEqual(0.8);
    expect(selection.requiresClarification).toBe(false);
    expect(selection.executeWithWarning).toBe(true);
  });

  test('low score should request clarification', () => {
    const scoring = new IntentScoringEngine(createQueryBuilderStub());
    const selection = scoring.selectBestInterpretation([
      { interpretation: 'low-confidence', score: 0.44 },
      { interpretation: 'alternative', score: 0.3 },
    ]);

    expect(selection.bestScore).toBeLessThan(0.5);
    expect(selection.requiresClarification).toBe(true);
  });

  test('usuarios activos should return ambiguity warning and suggestions', () => {
    const intelligence = new QueryIntelligenceEngine(createQueryBuilderStub());
    const result = intelligence.buildSmartQuery('usuarios activos', createSchema(), { tablaBase: 'users', topScore: 0.83, confianza: 0.83 });

    expect(result.warning).toBeTruthy();
    expect(String(result.warning).toLowerCase()).toContain('ambigua');
    expect(Array.isArray(result.ambiguity?.suggestions || result.ambiguity?.options)).toBe(true);
    expect((result.ambiguity?.suggestions || result.ambiguity?.options || []).length).toBeGreaterThan(0);
  });
});
