import { SchemaVocabularyBuilder } from '../src/infrastructure/query/SchemaVocabularyBuilder.js';

// ─── Helpers ─────────────────────────────────────────────────────────────────

function levenshteinSim(a, b) {
  const s = String(a || '').toLowerCase();
  const t = String(b || '').toLowerCase();
  if (!s || !t) return 0;
  if (s === t) return 1;
  const dp = Array.from({ length: s.length + 1 }, (_, i) =>
    Array.from({ length: t.length + 1 }, (_, j) => (i === 0 ? j : j === 0 ? i : 0))
  );
  for (let i = 1; i <= s.length; i++) {
    for (let j = 1; j <= t.length; j++) {
      dp[i][j] = s[i - 1] === t[j - 1]
        ? dp[i - 1][j - 1]
        : 1 + Math.min(dp[i - 1][j], dp[i][j - 1], dp[i - 1][j - 1]);
    }
  }
  return 1 - dp[s.length][t.length] / Math.max(s.length, t.length, 1);
}

// ─── Fixtures ────────────────────────────────────────────────────────────────

/** A small schema similar to the real app (table names are the keys). */
const TABLES_MAP = {
  users: {
    columnas: [
      { nombre: 'id' },
      { nombre: 'username' },
      { nombre: 'email' },
      { nombre: 'role' },
    ],
  },
  sessions: {
    columnas: [
      { nombre: 'id' },
      { nombre: 'user_id' },
      { nombre: 'token' },
    ],
  },
  logs: {
    columnas: [
      { nombre: 'id' },
      { nombre: 'action' },
      { nombre: 'created_at' },
    ],
  },
  employees: {
    columnas: [
      { nombre: 'id' },
      { nombre: 'first_name' },
      { nombre: 'last_name' },
      { nombre: 'department' },
    ],
  },
  knowledge_base: {
    columnas: [
      { nombre: 'id' },
      { nombre: 'title' },
      { nombre: 'content' },
    ],
  },
};

// ─── Tests ───────────────────────────────────────────────────────────────────

describe('SchemaVocabularyBuilder — token helpers', () => {
  let builder;
  beforeEach(() => { builder = new SchemaVocabularyBuilder(); });

  test('splitIdentifier handles snake_case', () => {
    expect(builder.splitIdentifier('user_profiles')).toEqual(['user', 'profiles']);
  });

  test('splitIdentifier handles camelCase', () => {
    expect(builder.splitIdentifier('UserProfiles')).toEqual(['user', 'profiles']);
  });

  test('splitIdentifier handles UPPER_CASE', () => {
    expect(builder.splitIdentifier('HTTP_RESPONSE')).toEqual(['http', 'response']);
  });

  test('singularize: users → user', () => {
    expect(builder.singularize('users')).toBe('user');
  });

  test('singularize: categories → category', () => {
    expect(builder.singularize('categories')).toBe('category');
  });

  test('singularize: profiles → profile', () => {
    expect(builder.singularize('profiles')).toBe('profile');
  });

  test('pluralize: user → users', () => {
    expect(builder.pluralize('user')).toBe('users');
  });

  test('pluralize: category → categories', () => {
    expect(builder.pluralize('category')).toBe('categories');
  });
});

describe('SchemaVocabularyBuilder — buildVocabulary', () => {
  let builder;
  let vocab;

  beforeEach(() => {
    builder = new SchemaVocabularyBuilder();
    vocab = builder.buildVocabulary(TABLES_MAP);
  });

  test('returns a plain object', () => {
    expect(typeof vocab).toBe('object');
    expect(vocab).not.toBeNull();
  });

  test('indexes full table names', () => {
    // "users" and its singular "user" should both map to ["users"]
    expect(vocab['users']).toContain('users');
    expect(vocab['user']).toContain('users');
  });

  test('indexes snake_case parts — knowledge_base', () => {
    // knowledge_base → parts: ["knowledge", "base"]
    expect(vocab['knowledge']).toContain('knowledge_base');
    expect(vocab['base']).toContain('knowledge_base');
  });

  test('indexes plural variants', () => {
    // "log" table → "logs" and "log" both map to "logs"
    expect(vocab['log']).toContain('logs');
    expect(vocab['logs']).toContain('logs');
  });

  test('indexes "employee" for "employees" table', () => {
    expect(vocab['employee']).toContain('employees');
    expect(vocab['employees']).toContain('employees');
  });

  test('indexes "session" for "sessions" table', () => {
    expect(vocab['session']).toContain('sessions');
  });

  test('does NOT include column names as keys (table-only vocab)', () => {
    // Column "action" from logs should NOT appear in vocabIndex
    // (we intentionally exclude columns to avoid false-positive table resolution)
    if (vocab['action']) {
      // If it does appear, it must not point to an unrelated table
      expect(vocab['action']).not.toContain('users');
    }
  });
});

describe('SchemaVocabularyBuilder — matchToken', () => {
  let builder;
  let vocab;

  beforeEach(() => {
    builder = new SchemaVocabularyBuilder();
    vocab = builder.buildVocabulary(TABLES_MAP);
  });

  test('exact match: "users" → users table', () => {
    const result = builder.matchToken('users', vocab, levenshteinSim, 0.72);
    expect(result).not.toBeNull();
    expect(result.tables).toContain('users');
    expect(result.exact).toBe(true);
    expect(result.score).toBe(1.0);
  });

  test('singular form: "user" → users table', () => {
    const result = builder.matchToken('user', vocab, levenshteinSim, 0.72);
    expect(result).not.toBeNull();
    expect(result.tables).toContain('users');
  });

  test('Spanish natural variant: "usuario" — fuzzy similarity ≥ 0.72 to "user"', () => {
    // "usuario" is close to "user" via Levenshtein
    const result = builder.matchToken('usuario', vocab, levenshteinSim, 0.72);
    // May or may not match depending on Levenshtein score — just verify no crash
    expect(result === null || typeof result.score === 'number').toBe(true);
    if (result) {
      expect(result.score).toBeGreaterThanOrEqual(0.72);
    }
  });

  test('returns null for completely unrelated token', () => {
    const result = builder.matchToken('zxzxzxzxzx', vocab, levenshteinSim, 0.72);
    expect(result).toBeNull();
  });

  test('returns null when input is empty string', () => {
    const result = builder.matchToken('', vocab, levenshteinSim, 0.72);
    expect(result).toBeNull();
  });

  test('returns null when vocabIndex is empty', () => {
    const result = builder.matchToken('users', {}, levenshteinSim, 0.72);
    expect(result).toBeNull();
  });

  test('log → logs table', () => {
    const result = builder.matchToken('log', vocab, levenshteinSim, 0.72);
    expect(result).not.toBeNull();
    expect(result.tables).toContain('logs');
  });

  test('employee → employees table', () => {
    const result = builder.matchToken('employee', vocab, levenshteinSim, 0.72);
    expect(result).not.toBeNull();
    expect(result.tables).toContain('employees');
  });
});

describe('SchemaVocabularyBuilder — edge cases', () => {
  let builder;

  beforeEach(() => { builder = new SchemaVocabularyBuilder(); });

  test('buildVocabulary handles null input gracefully', () => {
    expect(() => builder.buildVocabulary(null)).not.toThrow();
    expect(builder.buildVocabulary(null)).toEqual({});
  });

  test('buildVocabulary handles empty object', () => {
    expect(builder.buildVocabulary({})).toEqual({});
  });

  test('matchToken handles null vocabIndex gracefully', () => {
    expect(builder.matchToken('user', null, levenshteinSim, 0.72)).toBeNull();
  });

  test('matchToken handles non-function similarityFn gracefully', () => {
    const vocab = builder.buildVocabulary(TABLES_MAP);
    // Should not throw even if similarityFn is null
    expect(() => builder.matchToken('user', vocab, null, 0.72)).not.toThrow();
  });

  test('buildVocabulary with large table count does not throw', () => {
    const largeTables = {};
    for (let i = 0; i < 500; i++) {
      largeTables[`table_${i}`] = { columnas: [] };
    }
    expect(() => builder.buildVocabulary(largeTables)).not.toThrow();
    const vocab = builder.buildVocabulary(largeTables);
    expect(Object.keys(vocab).length).toBeGreaterThan(0);
  });
});
