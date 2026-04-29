function normalizeText(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9_\s-]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeIdentifier(value) {
  return normalizeText(value).replace(/\s+/g, '_');
}

function dedupe(values = []) {
  return [...new Set((values || []).map((item) => String(item || '').trim()).filter(Boolean))];
}

const STOP_WORDS = new Set([
  'con', 'de', 'del', 'la', 'el', 'los', 'las', 'y', 'para', 'por', 'en', 'al', 'un', 'una',
  'todos', 'todas', 'quiero', 'dame', 'mostrar', 'muestra', 'lista', 'ver', 'consultar', 'consulta',
  'the', 'all', 'with', 'and', 'show', 'list', 'find', 'get', 'me',
]);

const PREFERRED_MERGE_KEYS = ['user_id', 'usuario_id', 'customer_id', 'cliente_id', 'employee_id', 'id', 'uuid', 'username', 'usuario'];

export class DistributedQueryOrchestrator {
  constructor(registry) {
    this.registry = registry;
  }

  tokenize(text) {
    return normalizeText(text)
      .split(' ')
      .map((token) => this.normalizeEntityToken(token))
      .filter((token) => token && !STOP_WORDS.has(token));
  }

  normalizeEntityToken(value) {
    const normalized = normalizeIdentifier(value);
    if (!normalized) return '';
    if (normalized.length > 4 && normalized.endsWith('es')) return normalized.slice(0, -2);
    if (normalized.length > 3 && normalized.endsWith('s')) return normalized.slice(0, -1);
    return normalized;
  }

  levenshteinDistance(a, b) {
    const s = String(a || '');
    const t = String(b || '');
    if (s === t) return 0;
    if (!s.length) return t.length;
    if (!t.length) return s.length;

    const dp = Array.from({ length: s.length + 1 }, () => new Array(t.length + 1).fill(0));
    for (let i = 0; i <= s.length; i += 1) dp[i][0] = i;
    for (let j = 0; j <= t.length; j += 1) dp[0][j] = j;

    for (let i = 1; i <= s.length; i += 1) {
      for (let j = 1; j <= t.length; j += 1) {
        const cost = s[i - 1] === t[j - 1] ? 0 : 1;
        dp[i][j] = Math.min(
          dp[i - 1][j] + 1,
          dp[i][j - 1] + 1,
          dp[i - 1][j - 1] + cost
        );
      }
    }

    return dp[s.length][t.length];
  }

  similarityScore(a, b) {
    const left = this.normalizeEntityToken(a);
    const right = this.normalizeEntityToken(b);
    if (!left || !right) return 0;
    if (left === right) return 1;
    if (left.includes(right) || right.includes(left)) return 0.9;
    const distance = this.levenshteinDistance(left, right);
    return 1 - (distance / Math.max(left.length, right.length, 1));
  }

  extractParameterValue(text, explicitValue = null) {
    if (explicitValue !== null && explicitValue !== undefined && String(explicitValue).trim()) {
      return String(explicitValue).trim();
    }

    const rawText = String(text || '');
    const quoted = rawText.match(/"([^"]+)"|'([^']+)'/);
    if (quoted) return quoted[1] || quoted[2] || null;

    const uuid = rawText.match(/\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/i);
    if (uuid) return uuid[0];

    const tokens = this.tokenize(rawText);
    return tokens.length >= 3 ? tokens[tokens.length - 1] : null;
  }

  resolveDatabaseMatches(text, requestedDatabases = []) {
    const normalizedRequested = new Set((requestedDatabases || []).map((item) => normalizeIdentifier(item)).filter(Boolean));
    const tokens = this.tokenize(text);
    const matches = [];

    for (const database of this.registry.getDatabases()) {
      if (normalizedRequested.size > 0 && !normalizedRequested.has(database.id)) {
        continue;
      }

      const learning = this.registry.getLearningSnapshot(database.id);
      for (const table of database.schema.tables || []) {
        const tableName = this.normalizeEntityToken(table.name);
        const aliasTerms = Object.entries(learning.tableAliases || {})
          .filter(([, mappedTables]) => (mappedTables || []).includes(tableName))
          .map(([term]) => this.normalizeEntityToken(term));

        let score = 0;
        const matchedTerms = [];
        for (const token of tokens) {
          const tableScore = this.similarityScore(token, tableName);
          const aliasScore = Math.max(0, ...aliasTerms.map((term) => this.similarityScore(token, term)));
          const best = Math.max(tableScore, aliasScore);
          if (best >= 0.58) {
            score += best;
            matchedTerms.push(token);
          }
        }

        if (score > 0) {
          matches.push({
            databaseId: database.id,
            databaseType: database.type,
            databaseFingerprint: database.fingerprint,
            tableName: table.name,
            table,
            score,
            matchedTerms: dedupe(matchedTerms),
          });
        }
      }
    }

    const sorted = matches.sort((left, right) => right.score - left.score);
    if (sorted.length > 0) {
      return sorted.filter((match, index) => index === 0 || match.score >= 0.7);
    }

    const fallback = [];
    for (const database of this.registry.getDatabases()) {
      const firstTable = database.schema.tables?.[0];
      if (!firstTable) continue;
      fallback.push({
        databaseId: database.id,
        databaseType: database.type,
        databaseFingerprint: database.fingerprint,
        tableName: firstTable.name,
        table: firstTable,
        score: 0.1,
        matchedTerms: [],
      });
    }
    return fallback.slice(0, 1);
  }

  pickCandidateColumns(table = {}) {
    const columns = table.columns || [];
    const preferred = columns.filter((column) => /(^|_)(id|uuid|name|nombre|user|usuario|email|mail|cliente|customer|empleado|employee|code|codigo)($|_)/i.test(column.name));
    return (preferred.length ? preferred : columns).slice(0, 4);
  }

  buildCompiledQuery(match, parameterValue, limit = 50) {
    const columns = this.pickCandidateColumns(match.table);
    const safeLimit = Math.max(1, Math.min(Number(limit) || 50, 200));
    const quotedTable = this.quoteIdentifier(match.tableName);
    const baseSql = `SELECT * FROM ${quotedTable}`;

    if (!parameterValue || columns.length === 0) {
      return {
        databaseId: match.databaseId,
        databaseType: match.databaseType,
        sql: `${baseSql} ${this.limitClause(match.databaseType, safeLimit)}`.trim(),
        params: this.limitParams(match.databaseType, safeLimit),
        tableName: match.tableName,
        matchedTerms: match.matchedTerms,
      };
    }

    const compiled = this.buildWhereClause(match.databaseType, columns, parameterValue, safeLimit);
    return {
      databaseId: match.databaseId,
      databaseType: match.databaseType,
      sql: `${baseSql} WHERE ${compiled.whereClause} ${compiled.limitClause}`.trim(),
      params: compiled.params,
      tableName: match.tableName,
      matchedTerms: match.matchedTerms,
    };
  }

  buildWhereClause(databaseType, columns, parameterValue, limit) {
    const searchValue = String(parameterValue || '').trim();
    const likeValue = `%${searchValue}%`;

    if (databaseType === 'postgres') {
      const predicates = columns.map((column, index) => `${this.quoteIdentifier(column.name)}::text ILIKE $${index + 1}::text`);
      return {
        whereClause: `(${predicates.join(' OR ')})`,
        params: columns.map(() => likeValue),
        limitClause: `LIMIT ${Math.max(1, Math.min(Number(limit) || 50, 200))}`,
      };
    }

    if (databaseType === 'mysql') {
      const predicates = columns.map((column) => `CAST(${this.quoteIdentifier(column.name)} AS CHAR) LIKE ?`);
      return {
        whereClause: `(${predicates.join(' OR ')})`,
        params: [...columns.map(() => likeValue), Math.max(1, Math.min(Number(limit) || 50, 200))],
        limitClause: 'LIMIT ?',
      };
    }

    if (databaseType === 'oracle') {
      const predicates = columns.map((column, index) => `LOWER(TO_CHAR(${this.quoteIdentifier(column.name)})) LIKE LOWER(:search_${index})`);
      const binds = Object.fromEntries(columns.map((column, index) => [`search_${index}`, likeValue]));
      binds.limit_value = Math.max(1, Math.min(Number(limit) || 50, 200));
      return {
        whereClause: `(${predicates.join(' OR ')})`,
        params: binds,
        limitClause: 'FETCH FIRST :limit_value ROWS ONLY',
      };
    }

    const predicates = columns.map((column) => `${this.quoteIdentifier(column.name)} LIKE ?`);
    return {
      whereClause: `(${predicates.join(' OR ')})`,
      params: [...columns.map(() => likeValue), Math.max(1, Math.min(Number(limit) || 50, 200))],
      limitClause: 'LIMIT ?',
    };
  }

  limitClause(databaseType, limit) {
    if (databaseType === 'oracle') return `FETCH FIRST ${limit} ROWS ONLY`;
    return `LIMIT ${limit}`;
  }

  limitParams(databaseType, limit) {
    if (databaseType === 'oracle') return {};
    if (databaseType === 'mysql') return [limit];
    return [];
  }

  quoteIdentifier(identifier) {
    const safe = String(identifier || '').replace(/"/g, '');
    return `"${safe}"`;
  }

  mergeDistributedRows(executions = []) {
    const successful = executions.filter((item) => item.success && Array.isArray(item.rows) && item.rows.length > 0);
    if (successful.length === 0) {
      return [];
    }

    if (successful.length === 1) {
      return successful[0].rows.map((row) => ({ ...row, __sourceDatabase: successful[0].databaseId }));
    }

    const keyIntersection = successful.reduce((acc, item, index) => {
      const rowKeys = new Set(Object.keys(item.rows[0] || {}).map((key) => normalizeIdentifier(key)));
      if (index === 0) return rowKeys;
      return new Set([...acc].filter((key) => rowKeys.has(key)));
    }, new Set());

    const mergeKey = PREFERRED_MERGE_KEYS.find((key) => keyIntersection.has(key)) || [...keyIntersection][0];
    if (!mergeKey) {
      return successful.flatMap((item) => item.rows.map((row) => ({ ...row, __sourceDatabase: item.databaseId })));
    }

    const merged = new Map();
    for (const item of successful) {
      for (const row of item.rows) {
        const keyValue = row[mergeKey];
        if (keyValue === undefined || keyValue === null || keyValue === '') continue;
        const current = merged.get(String(keyValue)) || { [mergeKey]: keyValue };
        merged.set(String(keyValue), {
          ...current,
          ...row,
          __databases: dedupe([...(current.__databases || []), item.databaseId]),
        });
      }
    }

    return [...merged.values()];
  }

  buildProgressSteps() {
    return [
      'Paso 1: Detectando datos...',
      'Paso 2: Consultando bases...',
      'Paso 3: Unificando resultados...',
      'Paso 4: Generando resultado...',
    ];
  }

  async execute(text, options = {}) {
    const inputText = String(text || '').trim();
    if (!inputText) {
      throw new Error('Texto de consulta requerido');
    }

    const configuredDatabases = this.registry.getDatabases();
    if (configuredDatabases.length === 0) {
      throw new Error('No hay bases distribuidas configuradas. Defina MULTI_DB_CONFIG en el backend.');
    }

    const selectedMatches = this.resolveDatabaseMatches(inputText, options.databases || []);
    const parameterValue = this.extractParameterValue(inputText, options.parameterValue);
    const compiledQueries = selectedMatches.map((match) => this.buildCompiledQuery(match, parameterValue, options.limit || 50));

    const executions = await Promise.all(compiledQueries.map(async (compiledQuery) => {
      try {
        const rows = await this.registry.executeCompiledQuery(compiledQuery);
        const learningKeyword = compiledQuery.matchedTerms?.[0] || this.normalizeEntityToken(compiledQuery.tableName);
        this.registry.learn(compiledQuery.databaseId, learningKeyword, compiledQuery.tableName);

        return {
          success: true,
          databaseId: compiledQuery.databaseId,
          databaseType: compiledQuery.databaseType,
          tableName: compiledQuery.tableName,
          sql: compiledQuery.sql,
          rows,
          rowCount: rows.length,
        };
      } catch (error) {
        return {
          success: false,
          databaseId: compiledQuery.databaseId,
          databaseType: compiledQuery.databaseType,
          tableName: compiledQuery.tableName,
          sql: compiledQuery.sql,
          rows: [],
          rowCount: 0,
          error: error?.message || String(error),
        };
      }
    }));

    const mergedRows = this.mergeDistributedRows(executions);

    return {
      success: executions.some((item) => item.success),
      text: inputText,
      steps: this.buildProgressSteps(),
      databases: selectedMatches.map((match) => ({
        id: match.databaseId,
        type: match.databaseType,
        fingerprint: match.databaseFingerprint,
        table: match.tableName,
        matchedTerms: match.matchedTerms,
      })),
      scripts: executions.map((item) => ({
        databaseId: item.databaseId,
        databaseType: item.databaseType,
        sql: item.sql,
      })),
      partialResults: executions,
      mergedResults: mergedRows,
      errors: executions.filter((item) => !item.success).map((item) => ({
        databaseId: item.databaseId,
        error: item.error,
      })),
    };
  }
}

export default DistributedQueryOrchestrator;