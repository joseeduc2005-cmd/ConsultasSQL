import IntentScoringEngine from './IntentScoringEngine.js';
import { SchemaVocabularyBuilder } from './SchemaVocabularyBuilder.js';

export class QueryIntelligenceEngine {
  constructor(queryBuilder) {
    this.queryBuilder = queryBuilder;
    this.intentScoringEngine = new IntentScoringEngine(queryBuilder);
    this.debugMode = String(process.env.DEBUG_QUERY_ENGINE || '').trim().toLowerCase() === 'true';
    this.vocabBuilder = new SchemaVocabularyBuilder();
    // Vocab cache: rebuilt lazily when schema changes (keyed by sorted table list)
    this._vocabCache = { fingerprint: null, index: null };
    this.stopWords = new Set([
      'con', 'de', 'del', 'la', 'el', 'los', 'las', 'y', 'por', 'para', 'un', 'una', 'unos', 'unas', 'en', 'al',
      'ver', 'dame', 'muestra', 'mostrar', 'lista', 'busca', 'buscar', 'trae', 'obtener', 'obten',
      'todos', 'todas', 'que', 'es', 'son', 'sus', 'su', 'este', 'esta', 'estos', 'estas',
      'mas', 'más', 'menos', 'exactamente', 'sin', 'ninguno', 'ninguna', 'ningun', 'ningunoa',
      'top', 'cuantos', 'cuantas', 'cuanto', 'cantidad', 'total', 'por', 'agrupado', 'segun',
      // Generic DB words that should never be treated as table/entity names.
      'tabla', 'tablas', 'base', 'dato', 'datos', 'database', 'bd',
      'postgres', 'postgre', 'postgresql', 'oracle', 'mysql'
    ]);
    // Learned table mappings: injected from semantic_learning DB at runtime
    // Format: { normalizedTerm: { tableName: string, confidence: number } }
    this.learnedTableMappings = {};
  }

  /**
   * Inject learned table mappings from semantic_learning.
   * Only entries with confidence >= 0.7 are stored here.
   * @param {{ [normalizedTerm: string]: { tableName: string, confidence: number }[] }} mappings
   */
  setLearnedTableMappings(mappings = {}) {
    this.learnedTableMappings = {};
    for (const [term, entries] of Object.entries(mappings || {})) {
      const normalizedTerm = this.normalizeToken(term);
      if (!normalizedTerm) continue;
      // Keep only the highest-confidence entry per term
      const best = (entries || [])
        .filter((entry) => Number(entry?.confidence || 0) >= 0.7)
        .sort((a, b) => Number(b.confidence || 0) - Number(a.confidence || 0))[0];
      if (best) this.learnedTableMappings[normalizedTerm] = best;
    }
  }

  debugLog(event, payload = {}) {
    if (!this.debugMode) return;
    try {
      console.log(`[QUERY_INTELLIGENCE_DEBUG] ${event}: ${JSON.stringify(payload)}`);
    } catch {
      console.log(`[QUERY_INTELLIGENCE_DEBUG] ${event}`);
    }
  }

  normalizeText(value) {
    return String(value || '')
      .trim()
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '');
  }

  normalizeToken(value) {
    const normalized = this.normalizeText(value)
      .replace(/[^a-z0-9_\s]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();

    if (!normalized) return '';
    if (normalized.length > 4 && normalized.endsWith('es')) return normalized.slice(0, -2);
    if (normalized.length > 3 && normalized.endsWith('s')) return normalized.slice(0, -1);
    return normalized;
  }

  tokenize(value) {
    return this.normalizeText(value)
      .split(/[^a-z0-9_]+/)
      .map((token) => this.normalizeToken(token))
      .filter(Boolean);
  }

  isStopWord(token) {
    return this.stopWords.has(this.normalizeToken(token));
  }

  /**
   * Return (or build + cache) the vocabulary index for the given schema.
   * The index is rebuilt only when the set of tables changes.
   */
  getVocabIndex(schema) {
    const tablesObj = schema?.schema || {};
    const fp = Object.keys(tablesObj).sort().join('|');
    if (this._vocabCache.fingerprint === fp && this._vocabCache.index) {
      return this._vocabCache.index;
    }
    const index = this.vocabBuilder.buildVocabulary(tablesObj);
    this._vocabCache = { fingerprint: fp, index };
    this.debugLog('vocab_index_rebuilt', { tableCount: Object.keys(tablesObj).length, tokenCount: Object.keys(index).length });
    return index;
  }

  scoreTermAgainstTable(term, tableName, entry = {}) {
    const normalizedTerm = this.normalizeToken(term);
    const normalizedTable = this.normalizeToken(tableName);
    const tokens = [normalizedTable, ...(entry?.tokens || []).map((token) => this.normalizeToken(token))]
      .filter(Boolean);

    let bestScore = 0;
    let exact = false;

    for (const token of tokens) {
      if (!token) continue;
      if (normalizedTerm === token) {
        return { score: 1, exact: true, matchedToken: token };
      }

      if (token.includes(normalizedTerm) || normalizedTerm.includes(token)) {
        bestScore = Math.max(bestScore, 0.84);
      }

      const similarity = this.queryBuilder.similarityScore(normalizedTerm, token);
      if (similarity > bestScore) {
        bestScore = similarity;
      }
    }

    return { score: bestScore, exact, matchedToken: null };
  }

  extractEntityTerms(input, conditions = {}) {
    const tokens = this.tokenize(input).filter((token) => !this.isStopWord(token));
    const entityTerms = [];

    const rawInput = String(input || '');
    // Prefer explicit "tabla <nombre>" or "tablas <nombre>" mentions.
    const explicitTableMatch = rawInput.match(/\btablas?\s+([a-zA-Z_][a-zA-Z0-9_]*)\b/i);
    if (explicitTableMatch?.[1]) {
      entityTerms.push(this.normalizeToken(explicitTableMatch[1]));
    }

    if (tokens[0]) {
      entityTerms.push(tokens[0]);
    }

    if (conditions?.relation?.entityTerm) {
      entityTerms.push(this.normalizeToken(conditions.relation.entityTerm));
    }

    return [...new Set(entityTerms.filter(Boolean))];
  }

  detectConditions(input) {
    const text = this.normalizeText(input);
    const quotedValues = this.queryBuilder.extractQuotedStrings(input);
    const normalizedTokens = this.tokenize(input).filter((token) => !this.isStopWord(token));

    let relation = null;
    const filters = [];

    const aggregateMatch = text.match(/\b(?:con\s+)?(mas de|menos de|exactamente)\s+(\d+)\s+([a-z0-9_]+)/i);
    if (aggregateMatch) {
      const operatorMap = {
        'mas de': '>',
        'menos de': '<',
        'exactamente': '=',
      };

      relation = {
        mode: 'count-comparison',
        operator: operatorMap[aggregateMatch[1]] || '=',
        value: Number(aggregateMatch[2]),
        entityTerm: this.normalizeToken(aggregateMatch[3]),
      };
    }

    if (!relation) {
      const zeroWithCon = text.match(/\bcon\s+(?:0|cero)\s+([a-z0-9_]+)/i);
      const zeroWithSin = text.match(/\bsin\s+([a-z0-9_]+)/i);
      const zeroWithNone = text.match(/\bningun(?:o|a)?\s+([a-z0-9_]+)/i);
      const comparativePresence = text.match(/\bcon\s+(?:mas|más|menos)\s+([a-z0-9_]+)/i);
      const presence = text.match(/\bcon\s+([a-z0-9_]+)/i);

      if (zeroWithCon) {
        relation = { mode: 'absence', entityTerm: this.normalizeToken(zeroWithCon[1]) };
      } else if (zeroWithSin) {
        relation = { mode: 'absence', entityTerm: this.normalizeToken(zeroWithSin[1]) };
      } else if (zeroWithNone) {
        relation = { mode: 'absence', entityTerm: this.normalizeToken(zeroWithNone[1]) };
      } else if (comparativePresence) {
        // "usuarios con más logs" / "usuarios con menos logs"
        // should resolve relation entity as logs (not "mas"/"menos").
        const relatedEntity = this.normalizeToken(comparativePresence[1]);
        if (relatedEntity) {
          relation = { mode: 'presence', entityTerm: relatedEntity };
        }
      } else if (presence) {
        const relatedEntity = this.normalizeToken(presence[1]);
        if (relatedEntity && !['activo', 'activa', 'activos', 'activas', 'inactivo', 'inactivos', 'mas', 'más', 'menos'].includes(relatedEntity)) {
          relation = { mode: 'presence', entityTerm: relatedEntity };
        }
      }
    }

    const namedMatch = text.match(/\b(?:llamado|llamada|llamados|llamadas|named|called)\s+(.+)$/i);
    const namedValue = quotedValues[0] || (namedMatch ? String(namedMatch[1] || '').trim() : '');
    if (namedValue) {
      filters.push({
        type: 'identity-match',
        value: namedValue.replace(/^["']|["']$/g, '').trim(),
      });
    } else if (!relation && normalizedTokens.length >= 2) {
      // Implicit identity heuristic for short natural phrases:
      // "usuario admin" => entity=user, filter="admin"
      // Avoid status adjectives and quantifiers that are not identity values.
      const trailingValue = normalizedTokens.slice(1).join(' ').trim();
      const blockedImplicitValues = new Set([
        'activo', 'activa', 'activos', 'activas',
        'inactivo', 'inactiva', 'inactivos', 'inactivas',
        'mas', 'más', 'menos', 'exactamente',
      ]);

      if (trailingValue && !blockedImplicitValues.has(trailingValue)) {
        filters.push({
          type: 'identity-match',
          value: trailingValue,
        });
      }
    }

    return {
      relation,
      filters,
    };
  }

  parseIntent(input) {
    const conditions = this.detectConditions(input);
    return {
      input,
      normalizedInput: this.normalizeText(input),
      entityTerms: this.extractEntityTerms(input, conditions),
      conditions,
    };
  }

  resolveSingleEntity(term, schema, analysis = null, excludeTables = []) {
    const semanticIndex = schema?.semanticIndex || {};
    const normalizedTerm = this.normalizeToken(term);
    if (!normalizedTerm) return null;

    this.debugLog('resolve_single_entity_schema', {
      term: normalizedTerm,
      tables: schema?.tables || Object.keys(semanticIndex || {}),
    });

    // ── Step 1: Learned mappings from semantic_learning (highest priority) ──
    // Only entries with confidence ≥ 0.7 are present in learnedTableMappings.
    const learnedEntry = this.learnedTableMappings[normalizedTerm];
    if (learnedEntry && Number(learnedEntry.confidence || 0) >= 0.7) {
      const learnedTable = this.normalizeToken(learnedEntry.tableName);
      const tableExists = learnedTable
        && !excludeTables.includes(learnedTable)
        && (schema?.schema?.[learnedTable] || Object.keys(semanticIndex).includes(learnedTable));
      if (tableExists) {
        this.debugLog('resolve_learned_match', { term: normalizedTerm, table: learnedTable, confidence: learnedEntry.confidence });
        return {
          sourceTerm: term,
          table: learnedTable,
          exact: true,
          score: Math.min(1, Number(learnedEntry.confidence) + 0.05),
          learnedMatch: true,
        };
      }
    }

    // ── Step 1.5: Dynamic vocabulary index (schema-derived tokens) ───────────
    // Builds tokens from real table names (snake_case/camelCase split, singular/plural).
    // No hardcoded dictionaries — works with any database.
    const vocabIndex = this.getVocabIndex(schema);
    const vocabMatch = this.vocabBuilder.matchToken(
      normalizedTerm,
      vocabIndex,
      (a, b) => this.queryBuilder.similarityScore(a, b),
      0.72
    );

    // ── Step 2: Semantic scoring over all tables (existing logic) ─────────────
    const availableTables = Object.keys(semanticIndex);
    if (availableTables.length === 0) {
      this.debugLog('resolve_no_schema', { term: normalizedTerm });
      return null;
    }

    let best = null;
    for (const tableName of availableTables) {
      if (excludeTables.includes(tableName)) continue;
      const entry = semanticIndex[tableName] || {};
      const scored = this.scoreTermAgainstTable(normalizedTerm, tableName, entry);
      if (!best || scored.score > best.score) {
        best = { table: tableName, ...scored };
      }
    }

    // Merge vocab candidate: elevate score when vocab provides a better match.
    // Vocab match wins only if its score exceeds the semantic scoring result.
    if (vocabMatch && vocabMatch.tables.length > 0) {
      for (const candidateTable of vocabMatch.tables) {
        if (excludeTables.includes(candidateTable)) continue;
        if (!best || vocabMatch.score > best.score) {
          best = {
            table: candidateTable,
            score: vocabMatch.score,
            exact: vocabMatch.exact,
            matchedToken: vocabMatch.matchedToken,
            vocabMatch: true,
          };
        }
      }
      this.debugLog('resolve_vocab_candidate', {
        term: normalizedTerm,
        vocabToken: vocabMatch.matchedToken,
        score: vocabMatch.score,
        tables: vocabMatch.tables,
      });
    }

    // ── Step 3: Threshold-based resolution ───────────────────────────────────
    // ≥ 0.85 → high confidence (execute immediately)
    if (best?.exact || (best && best.score >= 0.85)) {
      return {
        sourceTerm: term,
        table: best.table,
        exact: best.exact || best.score >= 0.99,
        score: best.score,
      };
    }

    // 0.72–0.85 → medium confidence (execute with warning)
    if (best && best.score >= 0.72) {
      return {
        sourceTerm: term,
        table: best.table,
        exact: false,
        score: best.score,
        warning: `Confianza media (${(best.score * 100).toFixed(0)}%) en '${best.table}' - verifica el resultado`,
      };
    }

    // < 0.72 → reject to prevent false positives ("cosas raras" → random table)
    this.debugLog('resolve_rejected_low_confidence', { term: normalizedTerm, bestScore: best?.score ?? 0 });
    return null;
  }

  resolveEntities(inputOrIntent, schema, analysis = null) {
    const intent = typeof inputOrIntent === 'string' ? this.parseIntent(inputOrIntent) : inputOrIntent;
    const warnings = [];
    const entities = [];

    for (const term of intent.entityTerms || []) {
      const resolved = this.resolveSingleEntity(term, schema, analysis, entities.map((entry) => entry.table));
      if (!resolved) {
        return {
          intent,
          entities,
          warnings,
          error: `La entidad '${term}' no existe en esta base de datos`,
          suggestions: (schema?.tables || []).slice(0, 5),
        };
      }

      if (resolved.warning) warnings.push(resolved.warning);
      entities.push(resolved);
    }

    return { intent, entities, warnings };
  }

  validateEntities(entities, schema) {
    for (const entity of entities || []) {
      if (!schema?.schema?.[entity.table]) {
        return {
          valid: false,
          error: `La entidad '${entity.sourceTerm || entity.table}' no existe en esta base de datos`,
        };
      }
    }

    return { valid: true };
  }

  detectRelations(schema, entities) {
    const tables = (entities || []).map((entity) => entity.table).filter(Boolean);
    if (tables.length <= 1) {
      return {
        valid: true,
        tables,
        joinPlan: {
          baseTable: tables[0] || null,
          joinedTables: new Set(tables),
          joinEdges: [],
          missingTables: [],
        },
      };
    }

    const joinPlan = this.queryBuilder.buildJoinPlan(schema, tables);
    if (!this.queryBuilder.hasUsableJoinPlan(joinPlan, tables)) {
      return {
        valid: false,
        error: `No existe una relación detectable entre '${tables[0]}' y '${tables[1]}' en el schema actual`,
      };
    }

    return {
      valid: true,
      tables: [...joinPlan.joinedTables],
      joinPlan,
    };
  }

  findStatusActiveColumn(tableSchema = {}) {
    const columns = Array.isArray(tableSchema?.columnas) ? tableSchema.columnas : [];
    const preferred = ['active', 'activo', 'is_active', 'enabled', 'habilitado', 'status', 'estado'];

    for (const candidate of preferred) {
      const found = columns.find((column) => this.normalizeToken(column?.nombre) === this.normalizeToken(candidate));
      if (found) return found.nombre;
    }

    return null;
  }

  findActivityRelatedTable(schema, baseTable) {
    const base = schema?.schema?.[baseTable];
    if (!base) return null;

    const baseFks = Array.isArray(base?.clavesForaneas) ? base.clavesForaneas : [];
    for (const foreignKey of baseFks) {
      const referenced = String(foreignKey?.tablaReferenciada || '').trim();
      if (/(log|audit|activity|session|bitacora)/i.test(referenced)) {
        return referenced;
      }
    }

    for (const [tableName, tableSchema] of Object.entries(schema?.schema || {})) {
      if (tableName === baseTable) continue;
      if (!/(log|audit|activity|session|bitacora)/i.test(tableName)) continue;
      const fks = Array.isArray(tableSchema?.clavesForaneas) ? tableSchema.clavesForaneas : [];
      const linksBase = fks.some((foreignKey) => this.normalizeToken(foreignKey?.tablaReferenciada) === this.normalizeToken(baseTable));
      if (linksBase) return tableName;
    }

    return null;
  }

  applyInterpretationPlanPatch(selectedInterpretation, plan, schema, baseEntity, relatedEntity) {
    const patch = selectedInterpretation?.planPatch || {};
    if (!baseEntity || !plan) return;

    if (patch.statusActive) {
      const baseSchema = schema?.schema?.[baseEntity.table];
      const statusColumn = this.findStatusActiveColumn(baseSchema);
      if (statusColumn) {
        const alreadyExists = (plan.whereConditions || []).some((condition) => this.normalizeToken(condition?.table) === this.normalizeToken(baseEntity.table)
          && this.normalizeToken(condition?.column) === this.normalizeToken(statusColumn));
        if (!alreadyExists) {
          plan.whereConditions.push({
            table: baseEntity.table,
            column: statusColumn,
            operator: '=',
            value: 'true',
            castText: false,
          });
        }
      }
    }

    if (patch.activityRecent && baseEntity && !relatedEntity) {
      const activityTable = this.findActivityRelatedTable(schema, baseEntity.table);
      if (activityTable) {
        const activitySchema = schema?.schema?.[activityTable];
        const activityKey = this.queryBuilder.pickPrimaryKey(activitySchema)
          || (Array.isArray(activitySchema?.columnas) ? activitySchema.columnas[0]?.nombre : null);

        if (activityKey) {
          plan.requiresJoin = true;
          plan.joinTables = [...new Set([baseEntity.table, activityTable])];
          plan.whereConditions.push({
            table: activityTable,
            column: activityKey,
            operator: 'IS NOT NULL',
          });
        }
      }
    }
  }

  semanticFallback(term, schema, analysis = null, excludeTables = []) {
    const semanticIndex = schema?.semanticIndex || {};
    const normalizedTerm = this.normalizeToken(term);

    if (analysis?.tablaBase && !excludeTables.includes(analysis.tablaBase) && schema?.schema?.[analysis.tablaBase]) {
      const scored = this.scoreTermAgainstTable(normalizedTerm, analysis.tablaBase, semanticIndex[analysis.tablaBase] || {});
      if (scored.score >= 0.45 || (analysis?.topScore || 0) >= 0.45) {
        return { table: analysis.tablaBase, score: Math.max(scored.score, analysis?.confianza || 0) };
      }
    }

    let best = null;
    for (const [tableName, entry] of Object.entries(semanticIndex)) {
      if (excludeTables.includes(tableName)) continue;
      const scored = this.scoreTermAgainstTable(normalizedTerm, tableName, entry);
      if (!best || scored.score > best.score) {
        best = { table: tableName, score: scored.score };
      }
    }

    if (best && best.score >= 0.56) {
      return best;
    }

    return null;
  }

  buildSmartQuery(inputOrIntent, schema, analysis = null) {
    const intent = typeof inputOrIntent === 'string' ? this.parseIntent(inputOrIntent) : inputOrIntent;
    const resolved = this.resolveEntities(intent, schema, analysis);
    if (resolved.error) {
      return {
        intent,
        warnings: resolved.warnings || [],
        error: resolved.error,
        suggestions: resolved.suggestions || [],
      };
    }

    const validation = this.validateEntities(resolved.entities, schema);
    if (!validation.valid) {
      return {
        intent,
        warnings: resolved.warnings || [],
        error: validation.error,
        suggestions: (schema?.tables || []).slice(0, 5),
      };
    }

    const baseEntity = resolved.entities[0] || null;
    const relatedEntity = resolved.entities[1] || null;
    const plan = {
      baseTable: baseEntity?.table || null,
      selectTables: baseEntity?.table ? [baseEntity.table] : [],
      joinTables: baseEntity?.table ? [baseEntity.table] : [],
      requiresJoin: false,
      requiresAggregation: false,
      aggregationFn: null,
      metricTable: null,
      metricColumn: null,
      whereConditions: [],
      havingCondition: null,
    };

    if (baseEntity && Array.isArray(intent?.conditions?.filters)) {
      const baseSchema = schema?.schema?.[baseEntity.table];
      for (const filter of intent.conditions.filters) {
        if (filter.type === 'identity-match') {
          const identityColumn = this.queryBuilder.pickBestIdentityColumn(baseSchema)
            || this.queryBuilder.findBestAttributeColumn('name', baseSchema)
            || this.queryBuilder.findBestAttributeColumn('username', baseSchema)
            || this.queryBuilder.findBestAttributeColumn('email', baseSchema);

          if (!identityColumn) {
            return {
              intent,
              warnings: resolved.warnings || [],
              error: `No existe una columna compatible para buscar por nombre en '${baseEntity.table}'`,
              suggestions: [baseEntity.table],
            };
          }

          plan.whereConditions.push({
            table: baseEntity.table,
            column: identityColumn,
            operator: 'ILIKE',
            value: `%${filter.value}%`,
            castText: true,
          });
        }
      }
    }

    if (baseEntity && relatedEntity && intent?.conditions?.relation) {
      const relationInfo = this.detectRelations(schema, [baseEntity, relatedEntity]);
      if (!relationInfo.valid) {
        return {
          intent,
          warnings: resolved.warnings || [],
          error: relationInfo.error,
          suggestions: [baseEntity.table, relatedEntity.table].filter(Boolean),
        };
      }

      const relatedSchema = schema?.schema?.[relatedEntity.table];
      const metricColumn = this.queryBuilder.pickPrimaryKey(relatedSchema)
        || (Array.isArray(relatedSchema?.columnas) ? relatedSchema.columnas[0]?.nombre : null);

      if (!metricColumn) {
        return {
          intent,
          warnings: resolved.warnings || [],
          error: `No existe una columna clave interpretable en '${relatedEntity.table}'`,
          suggestions: [relatedEntity.table],
        };
      }

      plan.requiresJoin = true;
      plan.joinTables = relationInfo.tables;

      if (intent.conditions.relation.mode === 'absence') {
        plan.whereConditions.push({
          table: relatedEntity.table,
          column: metricColumn,
          operator: 'IS NULL',
        });
      } else if (intent.conditions.relation.mode === 'presence') {
        plan.whereConditions.push({
          table: relatedEntity.table,
          column: metricColumn,
          operator: 'IS NOT NULL',
        });
      } else if (intent.conditions.relation.mode === 'count-comparison') {
        if (intent.conditions.relation.value === 0 && intent.conditions.relation.operator === '=') {
          plan.whereConditions.push({
            table: relatedEntity.table,
            column: metricColumn,
            operator: 'IS NULL',
          });
        } else {
          plan.requiresAggregation = true;
          plan.aggregationFn = 'COUNT';
          plan.metricTable = relatedEntity.table;
          plan.metricColumn = metricColumn;
          plan.havingCondition = {
            aggregation: 'COUNT',
            table: relatedEntity.table,
            column: metricColumn,
            operator: intent.conditions.relation.operator,
            value: intent.conditions.relation.value,
          };
        }
      }
    }

    const scoredInterpretations = this.intentScoringEngine.generateInterpretations(intent, {
      intent,
      schema,
      analysis,
      resolvedEntities: resolved.entities,
      relationDetected: Boolean((plan?.joinTables || []).length > 1 || plan?.requiresJoin),
      learning: this.queryBuilder?.learnedSemanticDictionary || {},
    });
    const interpretationSelection = this.intentScoringEngine.selectBestInterpretation(scoredInterpretations);
    const ambiguity = this.intentScoringEngine.handleAmbiguity(interpretationSelection);

    if (ambiguity?.requiresClarification) {
      return {
        intent,
        warnings: [...new Set([...(resolved.warnings || []), ambiguity.warning].filter(Boolean))],
        warning: ambiguity.warning,
        error: ambiguity.message,
        suggestions: ambiguity.suggestions || [],
        ambiguity,
        confidence: interpretationSelection?.bestScore || 0,
        interpretations: scoredInterpretations,
      };
    }

    this.applyInterpretationPlanPatch(interpretationSelection?.best, plan, schema, baseEntity, relatedEntity);

    const mergedWarnings = [...new Set([
      ...(resolved.warnings || []),
      ...(ambiguity?.warning ? [ambiguity.warning] : []),
    ])];

    return {
      intent,
      warnings: mergedWarnings,
      warning: mergedWarnings[0] || null,
      confidence: interpretationSelection?.bestScore || 0,
      confidenceBand: interpretationSelection?.confidenceBand || 'low',
      interpretations: scoredInterpretations,
      interpretationSelection,
      ambiguity,
      plan,
      resolvedEntities: resolved.entities,
    };
  }

  handleEmptyResults(result, context = {}) {
    const rows = Array.isArray(result?.rows)
      ? result.rows
      : Array.isArray(result?.data)
        ? result.data
        : [];

    if (rows.length > 0) {
      return {
        ...result,
        data: rows,
        rowCount: result?.rowCount ?? rows.length,
        isEmpty: false,
      };
    }

    const input = String(context?.input || '').trim();
    const parsed = input ? this.parseIntent(input) : null;
    const identityFilter = parsed?.conditions?.filters?.find((filter) => filter.type === 'identity-match');
    const baseTable = context?.queryResult?.debug?.tablaSeleccionada || context?.queryResult?.analisis?.tablaBase || 'registros';

    let message = 'No se encontraron resultados para este criterio';
    if (identityFilter?.value) {
      message = `No se encontraron ${baseTable} llamados '${identityFilter.value}'`;
    } else if (input) {
      message = `No se encontraron resultados para este criterio`;
    }

    return {
      ...result,
      data: rows,
      rowCount: result?.rowCount ?? 0,
      isEmpty: true,
      message,
    };
  }
}

export default QueryIntelligenceEngine;