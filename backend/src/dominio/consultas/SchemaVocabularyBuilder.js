/**
 * SchemaVocabularyBuilder
 *
 * Constructs a dynamic vocabulary index directly from the live database schema.
 * No hardcoded dictionaries — every token is derived from real table/column names.
 *
 * vocabIndex shape: { normalizedToken: string[] (tableNames) }
 *
 * Token generation per table:
 *   - Full normalized table name
 *   - snake_case / camelCase parts
 *   - Singular and plural variants of each part
 *
 * Matching uses the injected similarityFn (Levenshtein-based from QueryBuilder).
 */
export class SchemaVocabularyBuilder {
  // ─── Token helpers ──────────────────────────────────────────────────────────

  /**
   * Normalize a single token: lowercase, strip accents, keep [a-z0-9].
   */
  normalizeToken(token) {
    return String(token || '')
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[^a-z0-9]/g, '')
      .trim();
  }

  /**
   * Split an identifier (table / column name) into word parts.
   * Handles: snake_case, camelCase, PascalCase, kebab-case, UPPER_CASE.
   *
   * Examples:
   *   user_profiles   → ['user', 'profiles']
   *   UserProfiles    → ['user', 'profiles']
   *   HTTP_RESPONSE   → ['http', 'response']
   */
  splitIdentifier(name) {
    return String(name || '')
      // ABCDef → ABC Def
      .replace(/([A-Z]+)([A-Z][a-z])/g, '$1 $2')
      // camelCase → camel Case
      .replace(/([a-z\d])([A-Z])/g, '$1 $2')
      .replace(/[_\-\s]+/g, ' ')
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .split(' ')
      .map((p) => p.replace(/[^a-z0-9]/g, ''))
      .filter(Boolean);
  }

  /**
   * Generate singular form (basic English/Spanish rules).
   * Examples: users→user, profiles→profile, empleados→empleado
   */
  singularize(word) {
    const w = String(word || '').trim();
    if (w.length < 4) return w;
    // -ies → -y  (categories → category)
    if (w.length > 5 && w.endsWith('ies')) return w.slice(0, -3) + 'y';
    // -es → context-aware strip:
    //   buses→bus (stem ends in s), boxes→box (x), churches→church (ch) → strip -es
    //   profiles→profile, tables→table, employees→employee           → strip only -s
    if (w.length > 4 && w.endsWith('es')) {
      const stem = w.slice(0, -2);
      if (/[sxz]$/.test(stem) || stem.endsWith('ch') || stem.endsWith('sh')) {
        return stem;
      }
      // Strip only -s to preserve the trailing -e
      return w.slice(0, -1);
    }
    // -s → strip   (users → user)
    if (w.length > 3 && w.endsWith('s')) return w.slice(0, -1);
    return w;
  }

  /**
   * Generate plural form.
   * Examples: user→users, category→categories
   */
  pluralize(word) {
    const w = String(word || '').trim();
    if (!w || w.length < 2) return w;
    if (w.endsWith('s') || w.endsWith('x') || w.endsWith('z')) return w + 'es';
    if (w.length > 2 && w.endsWith('y')) return w.slice(0, -1) + 'ies';
    return w + 's';
  }

  /**
   * Expand a single token into all its variants: original, singular, plural.
   * Returns a Set of non-empty strings.
   */
  expandToken(token) {
    const base = this.normalizeToken(token);
    if (!base || base.length < 2) return new Set();
    const singular = this.singularize(base);
    const plural = this.pluralize(base);
    const singularOfPlural = this.singularize(plural);
    return new Set([base, singular, plural, singularOfPlural].filter((t) => t && t.length >= 2));
  }

  // ─── Core API ────────────────────────────────────────────────────────────────

  /**
   * Build a vocabulary index from a schema object.
   *
   * @param {Record<string, object>} tablesMap  Plain object: { tableName: tableDefinition }
   * @returns {Record<string, string[]>}         { token: [tableName, ...] }
   *
   * Only TABLE name tokens are indexed (not column names) to avoid false-positive
   * table resolution from column keywords.
   */
  buildVocabulary(tablesMap) {
    if (!tablesMap || typeof tablesMap !== 'object') return {};

    // intermediate: token → Set<tableName>
    const vocabMap = new Map();

    const addEntry = (token, tableName) => {
      const t = this.normalizeToken(token);
      if (!t || t.length < 2) return;
      if (!vocabMap.has(t)) vocabMap.set(t, new Set());
      vocabMap.get(t).add(tableName);
    };

    for (const tableName of Object.keys(tablesMap)) {
      if (!tableName) continue;

      // ── Full table name variants ───────────────────────────────────────────
      const normalizedFull = this.normalizeToken(tableName);
      for (const variant of this.expandToken(normalizedFull)) {
        addEntry(variant, tableName);
      }

      // ── Per-part variants (snake_case / camelCase splitting) ──────────────
      const parts = this.splitIdentifier(tableName);
      for (const part of parts) {
        for (const variant of this.expandToken(part)) {
          addEntry(variant, tableName);
        }
      }
    }

    // Serialize to plain object (Sets → sorted arrays for determinism)
    const result = {};
    for (const [token, tableSet] of vocabMap.entries()) {
      result[token] = [...tableSet].sort();
    }
    return result;
  }

  /**
   * Match a normalized input token against the vocabulary index.
   *
   * Priority:
   *   1. Exact key lookup  (score = 1.0)
   *   2. Fuzzy match via similarityFn over all tokens (score ≥ threshold)
   *
   * @param {string}   normalizedInput  Already-normalized input token
   * @param {Record<string, string[]>} vocabIndex
   * @param {(a: string, b: string) => number} similarityFn
   * @param {number}  [threshold=0.72]  Minimum similarity to consider
   * @returns {{ tables: string[], score: number, exact: boolean, matchedToken: string } | null}
   */
  matchToken(normalizedInput, vocabIndex, similarityFn, threshold = 0.72) {
    if (!normalizedInput || !vocabIndex) return null;

    // 1. Exact lookup (O(1))
    const directHit = vocabIndex[normalizedInput];
    if (directHit && directHit.length > 0) {
      return {
        tables: directHit,
        score: 1.0,
        exact: true,
        matchedToken: normalizedInput,
      };
    }

    // 2. Fuzzy scan (O(|vocab|))
    let best = null;
    for (const [vocabToken, tables] of Object.entries(vocabIndex)) {
      const sim = typeof similarityFn === 'function' ? similarityFn(normalizedInput, vocabToken) : 0;
      if (sim >= threshold && (!best || sim > best.score)) {
        best = { tables, score: sim, exact: false, matchedToken: vocabToken };
      }
    }

    return best;
  }
}
