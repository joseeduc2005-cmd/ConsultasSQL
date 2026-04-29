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

const PREFERRED_MERGE_KEYS = ['user_id', 'usuario_id', 'customer_id', 'cliente_id', 'employee_id', 'empleado_id', 'id', 'uuid', 'username', 'usuario'];

export class MultiDatabaseEngine {
  constructor(registry, queryBuilder) {
    this.registry = registry;
    this.queryBuilder = queryBuilder;
    this.intelligenceEngine = queryBuilder?.queryIntelligenceEngine;
    this.debugMode = String(process.env.DEBUG_QUERY_ENGINE || '').trim().toLowerCase() === 'true';
  }

  async registerDatabase(config) {
    return this.registry.registerDatabase(config);
  }

  singularize(value) {
    const normalized = normalizeIdentifier(value);
    if (!normalized) return '';
    if (normalized.length > 4 && normalized.endsWith('es')) return normalized.slice(0, -2);
    if (normalized.length > 3 && normalized.endsWith('s')) return normalized.slice(0, -1);
    return normalized;
  }

  pluralize(value) {
    const normalized = normalizeIdentifier(value);
    if (!normalized) return '';
    if (normalized.endsWith('s')) return normalized;
    return `${normalized}s`;
  }

  quoteIdentifier(databaseType, identifier) {
    const safe = String(identifier || '').replace(/["`]/g, '');
    return databaseType === 'mysql' ? `\`${safe}\`` : `"${safe}"`;
  }

  limitClause(databaseType, limit) {
    const safeLimit = Math.max(1, Math.min(Number(limit) || 50, 5000));
    if (databaseType === 'oracle') return `FETCH FIRST ${safeLimit} ROWS ONLY`;
    return `LIMIT ${safeLimit}`;
  }

  escapeLiteral(value) {
    return String(value || '').replace(/'/g, "''");
  }

  toTextExpression(databaseType, expression) {
    if (databaseType === 'oracle') return `LOWER(TO_CHAR(${expression}))`;
    if (databaseType === 'mysql') return `LOWER(CAST(${expression} AS CHAR))`;
    return `LOWER(CAST(${expression} AS TEXT))`;
  }

  createParamBinder(databaseType) {
    const values = [];
    const named = {};

    return {
      add: (value) => {
        if (databaseType === 'oracle') {
          const name = `p${Object.keys(named).length + 1}`;
          named[name] = value;
          return `:${name}`;
        }

        values.push(value);
        if (databaseType === 'mysql') {
          return '?';
        }

        return `$${values.length}`;
      },
      getParams: () => (databaseType === 'oracle' ? named : values),
    };
  }

  getDatabaseSchema(database) {
    const learning = this.registry.getLearningSnapshot(database.id);
    const globalSemanticAliases = typeof this.queryBuilder?.getMergedSemanticAliases === 'function'
      ? this.queryBuilder.getMergedSemanticAliases()
      : (this.queryBuilder?.semanticAliases || {});
    const schema = {};
    const semanticIndex = {};
    const tables = [];

    for (const table of database?.schema?.tables || []) {
      const tableName = normalizeIdentifier(table.name);
      if (!tableName) continue;

      const columns = (table.columns || []).map((column) => ({
        nombre: normalizeIdentifier(column.name),
        tipo: normalizeIdentifier(column.type || 'text') || 'text',
      })).filter((column) => column.nombre);

      const primaryKeys = dedupe((table.keyColumns || []).map((column) => normalizeIdentifier(column)));
      const foreignKeys = (table.foreignKeys || []).map((foreignKey) => ({
        columna: normalizeIdentifier(foreignKey.column),
        tablaReferenciada: normalizeIdentifier(foreignKey.referencedTable),
        columnaReferenciada: normalizeIdentifier(foreignKey.referencedColumn),
        nombreConstraint: String(foreignKey.constraintName || '').trim(),
      })).filter((foreignKey) => foreignKey.columna && foreignKey.tablaReferenciada && foreignKey.columnaReferenciada);

      schema[tableName] = {
        columnas: columns,
        clavesPrimarias: primaryKeys,
        pkPrincipal: primaryKeys[0] || null,
        clavesForaneas: foreignKeys,
      };

      const aliasTerms = Object.entries(learning?.tableAliases || {})
        .filter(([, mappedTables]) => (mappedTables || []).includes(tableName))
        .map(([term]) => normalizeIdentifier(term));

      const builtInAliases = Object.entries(globalSemanticAliases || {})
        .filter(([, mappedTables]) => (mappedTables || []).map((mappedTable) => normalizeIdentifier(mappedTable)).includes(tableName))
        .map(([term]) => normalizeIdentifier(term));

      const tokens = dedupe([
        tableName,
        this.singularize(tableName),
        this.pluralize(tableName),
        ...aliasTerms,
        ...builtInAliases,
      ]);

      const columnTokenIndex = {};
      for (const column of columns) {
        const normalizedColumn = normalizeIdentifier(column.nombre);
        columnTokenIndex[normalizedColumn] = dedupe([
          normalizedColumn,
          this.singularize(normalizedColumn),
        ]);
      }

      semanticIndex[tableName] = {
        tokens,
        columnas: columnTokenIndex,
      };

      tables.push(tableName);
    }

    return {
      tables,
      tablas: tables,
      schema,
      semanticIndex,
    };
  }

  getFilteredDatabases(requestedDatabases = []) {
    const requested = new Set((requestedDatabases || []).map((databaseId) => normalizeIdentifier(databaseId)).filter(Boolean));
    return this.registry
      .getDatabases()
      .filter((database) => requested.size === 0 || requested.has(database.id))
      .filter((database) => {
        const schemaTables = database?.schema?.tables;
        return Array.isArray(schemaTables) && schemaTables.length > 0;
      });
  }

  debugLog(event, payload = {}) {
    if (!this.debugMode) return;
    try {
      const serialized = JSON.stringify(payload);
      console.log(`[MULTI_DB_DEBUG] ${event}: ${serialized}`);
    } catch {
      console.log(`[MULTI_DB_DEBUG] ${event}`);
    }
  }

  buildEntityCandidates(term, database) {
    const schema = this.getDatabaseSchema(database);
    const results = [];

    // ── Step 1: Check semantic_learning-backed learned mappings first ──────────
    const learnedMappings = this.intelligenceEngine?.learnedTableMappings || {};
    const normalizedTerm = String(term || '')
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[^a-z0-9_\s]/g, '')
      .trim();
    const learnedEntry = learnedMappings[normalizedTerm];
    if (learnedEntry && Number(learnedEntry.confidence || 0) >= 0.7) {
      const learnedTable = String(learnedEntry.tableName || '').trim().toLowerCase();
      if (learnedTable && (schema.tables || []).includes(learnedTable)) {
        results.push({
          databaseId: database.id,
          databaseType: database.type,
          databaseFingerprint: database.fingerprint,
          tableName: learnedTable,
          score: Math.min(1, Number(learnedEntry.confidence) + 0.05),
          exact: true,
          schema,
          learnedMatch: true,
        });
        return results; // exact learned match — skip similarity
      }
    }

    // ── Step 2: Dynamic similarity against all schema tables (threshold 0.72) ──
    for (const tableName of schema.tables || []) {
      const scoreResult = this.intelligenceEngine.scoreTermAgainstTable(term, tableName, schema.semanticIndex?.[tableName] || {});
      if (scoreResult.score >= 0.72) {
        results.push({
          databaseId: database.id,
          databaseType: database.type,
          databaseFingerprint: database.fingerprint,
          tableName,
          score: scoreResult.score,
          exact: Boolean(scoreResult.exact || scoreResult.score >= 0.99),
          schema,
        });
      }
    }

    return results.sort((left, right) => right.score - left.score);
  }

  resolveEntitiesAcrossDatabases(input, requestedDatabases = []) {
    const intent = this.intelligenceEngine.parseIntent(input);
    const databases = this.getFilteredDatabases(requestedDatabases);
    const resolutions = [];
    const warnings = [];

    this.debugLog('schema_received', {
      databases: databases.map((database) => ({
        id: database.id,
        tableCount: Array.isArray(database?.schema?.tables) ? database.schema.tables.length : 0,
        tables: (database?.schema?.tables || []).map((table) => normalizeIdentifier(table?.name)).filter(Boolean),
      })),
    });

    for (const entityTerm of intent.entityTerms || []) {
      const matches = databases.flatMap((database) => this.buildEntityCandidates(entityTerm, database));

      if (matches.length === 0) {
        return {
          success: false,
          intent,
          entities: resolutions,
          warnings,
          error: `La entidad '${entityTerm}' no existe en las bases de datos registradas`,
        };
      }

      const exactMatches = matches.filter((match) => match.exact);
      const selectedMatches = exactMatches.length > 0 ? exactMatches : matches.filter((match, index) => index === 0 || match.score >= exactMatches[0]?.score || match.score >= 0.84);
      const primaryMatch = selectedMatches[0];

      if (!primaryMatch.exact) {
        warnings.push(`Interpretado como '${primaryMatch.tableName}' en '${primaryMatch.databaseId}'`);
      }

      resolutions.push({
        entity: entityTerm,
        matches: selectedMatches.map((match) => ({
          databaseId: match.databaseId,
          databaseType: match.databaseType,
          databaseFingerprint: match.databaseFingerprint,
          tableName: match.tableName,
          score: match.score,
          exact: match.exact,
        })),
        primary: {
          databaseId: primaryMatch.databaseId,
          databaseType: primaryMatch.databaseType,
          databaseFingerprint: primaryMatch.databaseFingerprint,
          tableName: primaryMatch.tableName,
          score: primaryMatch.score,
          exact: primaryMatch.exact,
        },
      });

      this.debugLog('entity_interpreted', {
        term: entityTerm,
        primary: resolutions[resolutions.length - 1]?.primary || null,
      });
    }

    const matchedDatabases = dedupe(resolutions.flatMap((entry) => entry.matches.map((match) => match.databaseId)));

    const executionInterpretations = this.buildExecutionInterpretations(intent, resolutions);
    const interpretationSelection = this.intelligenceEngine.intentScoringEngine.selectBestInterpretation(executionInterpretations);
    const ambiguity = this.intelligenceEngine.intentScoringEngine.handleAmbiguity(interpretationSelection);

    return {
      success: true,
      intent,
      entities: resolutions,
      warnings: dedupe(warnings),
      matchedDatabases,
      interpretations: executionInterpretations,
      interpretationSelection,
      ambiguity,
      confidence: interpretationSelection?.bestScore || 0,
      confidenceBand: interpretationSelection?.confidenceBand || 'low',
    };
  }

  buildExecutionInterpretations(intent, entities = []) {
    const interpretationCandidates = [];
    const entityCount = (entities || []).length;
    const coverageMap = new Map();

    for (const entity of entities || []) {
      for (const match of entity.matches || []) {
        const current = coverageMap.get(match.databaseId) || new Set();
        current.add(entity.entity);
        coverageMap.set(match.databaseId, current);
      }
    }

    for (const [databaseId, coveredEntities] of coverageMap.entries()) {
      if (coveredEntities.size !== entityCount || entityCount === 0) continue;
      const targetDatabase = this.registry.getDatabaseById(databaseId);
      const targetSchema = targetDatabase ? this.getDatabaseSchema(targetDatabase) : {};

      const candidate = this.intelligenceEngine.intentScoringEngine.scoreInterpretation({
        interpretation: `Ejecutar en ${databaseId}`,
        type: 'single-db-execution',
        resolvedEntities: entities.map((entity) => ({
          sourceTerm: entity.entity,
          table: entity.primary?.tableName,
          score: (entity.matches || []).find((match) => match.databaseId === databaseId)?.score || 0,
        })),
        relation: intent?.conditions?.relation || null,
        optionsLabel: `usar solo ${databaseId}`,
      }, {
        intent,
        relationDetected: true,
        schema: targetSchema,
        learning: this.registry.getLearningSnapshot(databaseId),
      });

      candidate.executionTarget = { mode: 'single-db', source: [databaseId], executionType: 'SINGLE_DB' };
      interpretationCandidates.push(candidate);
    }

    const distributedSource = dedupe((entities || []).flatMap((entity) => (entity.matches || []).map((match) => match.databaseId)));
    if (distributedSource.length > 1) {
      const distributedCandidate = this.intelligenceEngine.intentScoringEngine.scoreInterpretation({
        interpretation: `Ejecución distribuida en ${distributedSource.join(', ')}`,
        type: 'distributed-execution',
        resolvedEntities: entities.map((entity) => ({
          sourceTerm: entity.entity,
          table: entity.primary?.tableName,
          score: entity.primary?.score || 0,
        })),
        relation: intent?.conditions?.relation || null,
        optionsLabel: `ejecución distribuida (${distributedSource.join(', ')})`,
      }, {
        intent,
        relationDetected: true,
        schema: {},
        learning: {},
      });

      const distributionPenalty = Math.max(0, (distributedSource.length - 1) * 0.08);
      distributedCandidate.score = Math.max(0, Math.min(1, distributedCandidate.score - distributionPenalty));
      distributedCandidate.executionTarget = { mode: 'distributed', source: distributedSource, executionType: 'DISTRIBUTED' };
      interpretationCandidates.push(distributedCandidate);
    }

    return interpretationCandidates.sort((left, right) => right.score - left.score);
  }

  decideExecution(resolution) {
    if (!resolution?.success) {
      return {
        executionType: 'UNRESOLVED',
        mode: 'unresolved',
        source: [],
      };
    }

    // STRICT CONFIDENCE THRESHOLD
    // < 0.6: reject (low confidence)
    // 0.6 - 0.8: warn (medium confidence)
    // > 0.8: execute (high confidence)
    const confidence = resolution?.confidence ?? 0;
    if (confidence < 0.6) {
      return {
        executionType: 'UNRESOLVED',
        mode: 'unresolved',
        source: [],
        confidence,
        confidenceBand: 'low',
        message: 'No se pudo interpretar la consulta con suficiente confianza. Por favor, sé más específico.',
        suggestions: resolution?.suggestions || [],
      };
    }

    const entityCount = (resolution.entities || []).length;
    if (entityCount === 0) {
      return {
        executionType: 'NORMAL',
        mode: 'normal',
        source: [],
      };
    }

    const coverage = new Map();
    for (const entity of resolution.entities || []) {
      for (const match of entity.matches || []) {
        const current = coverage.get(match.databaseId) || new Set();
        current.add(entity.entity);
        coverage.set(match.databaseId, current);
      }
    }

    const fullCoverageDatabases = [...coverage.entries()]
      .filter(([, entitySet]) => entitySet.size === entityCount)
      .map(([databaseId]) => databaseId);

    const interpretationSelection = resolution?.interpretationSelection || null;
    const ambiguity = resolution?.ambiguity || null;
    if (ambiguity?.requiresClarification || interpretationSelection?.requiresClarification) {
      return {
        executionType: 'UNRESOLVED',
        mode: 'unresolved',
        source: [],
        confidence: interpretationSelection?.bestScore || 0,
        confidenceBand: interpretationSelection?.confidenceBand || 'low',
        message: ambiguity?.message || 'Tu consulta es ambigua. Necesito más contexto.',
        suggestions: ambiguity?.suggestions || [],
      };
    }

    if (interpretationSelection?.best?.executionTarget?.mode) {
      const selectionConfidence = interpretationSelection.bestScore || 0;
      return {
        executionType: interpretationSelection.best.executionTarget.executionType || 'DISTRIBUTED',
        mode: interpretationSelection.best.executionTarget.mode,
        source: interpretationSelection.best.executionTarget.source || [],
        confidence: selectionConfidence,
        confidenceBand: interpretationSelection.confidenceBand || 'low',
        // WARN if medium confidence (0.6 - 0.8)
        warning: (selectionConfidence >= 0.6 && selectionConfidence < 0.8)
          ? `Confianza media (${(selectionConfidence * 100).toFixed(0)}%) - verifica el resultado`
          : (interpretationSelection.executeWithWarning ? (ambiguity?.warning || 'Confianza media en la interpretación') : null),
      };
    }

    if (fullCoverageDatabases.length === 1) {
      return {
        executionType: 'SINGLE_DB',
        mode: 'single-db',
        source: fullCoverageDatabases,
        confidence: resolution?.confidence || 0,
        confidenceBand: resolution?.confidenceBand || 'low',
        // WARN if medium confidence (0.6 - 0.8)
        warning: (confidence >= 0.6 && confidence < 0.8)
          ? `Confianza media (${(confidence * 100).toFixed(0)}%) - verifica el resultado`
          : null,
      };
    }

    const uniqueDatabases = dedupe(resolution.entities.flatMap((entity) => (entity.matches || []).map((match) => match.databaseId)));
    if (uniqueDatabases.length > 0) {
      return {
        executionType: 'DISTRIBUTED',
        mode: 'distributed',
        source: fullCoverageDatabases.length > 1 ? fullCoverageDatabases : uniqueDatabases,
        confidence: resolution?.confidence || 0,
        confidenceBand: resolution?.confidenceBand || 'low',
      };
    }

    return {
      executionType: 'NORMAL',
      mode: 'normal',
      source: [],
      confidence: resolution?.confidence || 0,
      confidenceBand: resolution?.confidenceBand || 'low',
    };
  }

  learnFromResolution(resolution, sourceDatabases = []) {
    const sourceSet = new Set((sourceDatabases || []).map((databaseId) => normalizeIdentifier(databaseId)).filter(Boolean));
    for (const entity of resolution?.entities || []) {
      for (const match of entity?.matches || []) {
        if (sourceSet.size > 0 && !sourceSet.has(normalizeIdentifier(match.databaseId))) continue;
        this.registry.learn(match.databaseId, entity.entity, match.tableName);
      }
    }
  }

  pickSelectableColumns(tableSchema = {}) {
    const columns = Array.isArray(tableSchema?.columnas) ? tableSchema.columnas : [];
    return columns.map((col) => col.nombre).filter(Boolean);
  }

  findRelationEdge(schema, baseTable, relatedTable) {
    const baseSchema = schema?.schema?.[baseTable];
    const relatedSchema = schema?.schema?.[relatedTable];
    const relatedFks = Array.isArray(relatedSchema?.clavesForaneas) ? relatedSchema.clavesForaneas : [];
    const baseFks = Array.isArray(baseSchema?.clavesForaneas) ? baseSchema.clavesForaneas : [];

    const direct = relatedFks.find((foreignKey) => normalizeIdentifier(foreignKey.tablaReferenciada) === normalizeIdentifier(baseTable));
    if (direct) {
      return {
        baseTable,
        baseColumn: normalizeIdentifier(direct.columnaReferenciada),
        relatedTable,
        relatedColumn: normalizeIdentifier(direct.columna),
      };
    }

    const inverse = baseFks.find((foreignKey) => normalizeIdentifier(foreignKey.tablaReferenciada) === normalizeIdentifier(relatedTable));
    if (inverse) {
      return {
        baseTable,
        baseColumn: normalizeIdentifier(inverse.columna),
        relatedTable,
        relatedColumn: normalizeIdentifier(inverse.columnaReferenciada),
      };
    }

    const basePk = baseSchema?.pkPrincipal || baseSchema?.clavesPrimarias?.[0] || 'id';
    const baseSingular = this.singularize(baseTable);
    const candidates = [
      `${baseSingular}_id`,
      `${normalizeIdentifier(baseTable)}_id`,
      basePk,
      'id',
      'uuid',
    ];

    const relatedColumns = new Set((relatedSchema?.columnas || []).map((column) => normalizeIdentifier(column.nombre)));
    const relatedColumn = candidates.find((candidate) => relatedColumns.has(candidate));
    if (relatedColumn) {
      return {
        baseTable,
        baseColumn: normalizeIdentifier(basePk),
        relatedTable,
        relatedColumn,
      };
    }

    return null;
  }

  detectMergeKeys(baseTable, relatedTable, baseSchema, relatedSchema) {
    const basePk = normalizeIdentifier(baseSchema?.pkPrincipal || baseSchema?.clavesPrimarias?.[0] || 'id');
    const baseColumns = new Set((baseSchema?.columnas || []).map((column) => normalizeIdentifier(column.nombre)));
    const relatedColumns = new Set((relatedSchema?.columnas || []).map((column) => normalizeIdentifier(column.nombre)));

    // Prefer explicit declared FK relations before heuristic key matching.
    const directForeignKey = (relatedSchema?.clavesForaneas || []).find(
      (fk) => normalizeIdentifier(fk.tablaReferenciada) === normalizeIdentifier(baseTable),
    );
    if (directForeignKey) {
      return {
        baseKey: normalizeIdentifier(directForeignKey.columnaReferenciada),
        relatedKey: normalizeIdentifier(directForeignKey.columna),
      };
    }

    const inverseForeignKey = (baseSchema?.clavesForaneas || []).find(
      (fk) => normalizeIdentifier(fk.tablaReferenciada) === normalizeIdentifier(relatedTable),
    );
    if (inverseForeignKey) {
      return {
        baseKey: normalizeIdentifier(inverseForeignKey.columna),
        relatedKey: normalizeIdentifier(inverseForeignKey.columnaReferenciada),
      };
    }

    const baseSingular = this.singularize(baseTable);
    const candidates = dedupe([
      `${baseSingular}_id`,
      `${normalizeIdentifier(baseTable)}_id`,
      basePk,
      'id',
      'uuid',
      'username',
      'usuario',
    ]);

    for (const candidate of candidates) {
      if (baseColumns.has(candidate) && relatedColumns.has(candidate)) {
        return { baseKey: candidate, relatedKey: candidate };
      }
    }

    return {
      baseKey: basePk,
      relatedKey: candidates.find((candidate) => relatedColumns.has(candidate)) || basePk,
    };
  }

  compileBaseFilterClause(databaseType, alias, conditions = [], tableName, binder = null) {
    const tableConditions = (conditions || []).filter((condition) => normalizeIdentifier(condition.table) === normalizeIdentifier(tableName));
    if (tableConditions.length === 0) return { sql: '', params: binder ? binder.getParams() : (databaseType === 'oracle' ? {} : []) };

    const localBinder = binder || this.createParamBinder(databaseType);

    const clauses = tableConditions.map((condition) => {
      const qualified = `${this.quoteIdentifier(databaseType, alias)}.${this.quoteIdentifier(databaseType, condition.column)}`;
      if (condition.operator === 'IS NULL' || condition.operator === 'IS NOT NULL') {
        return `${qualified} ${condition.operator}`;
      }

      const safeOperator = ['=', '!=', '<>', '>', '<', '>=', '<=', 'LIKE', 'ILIKE'].includes(String(condition.operator || '').toUpperCase())
        ? String(condition.operator).toUpperCase()
        : '=';

      const paramPlaceholder = localBinder.add(condition.value);

      if (condition.operator === 'ILIKE') {
        const expression = condition.castText ? this.toTextExpression(databaseType, qualified) : `LOWER(${qualified})`;
        return `${expression} LIKE LOWER(${paramPlaceholder})`;
      }

      return `${qualified} ${safeOperator} ${paramPlaceholder}`;
    });

    return {
      sql: clauses.join(' AND '),
      params: localBinder.getParams(),
    };
  }

  compileSingleDatabaseQuery(database, plan, intent, limit = 50) {
    const schema = this.getDatabaseSchema(database);
    const baseTable = normalizeIdentifier(plan?.baseTable);
    const relatedTable = normalizeIdentifier(plan?.joinTables?.find((table) => normalizeIdentifier(table) !== baseTable));
    const baseSchema = schema?.schema?.[baseTable];
    if (!baseTable || !baseSchema) {
      throw new Error('No se pudo determinar la tabla base para la ejecución single-db');
    }

    const baseAlias = 't1';
    const binder = this.createParamBinder(database.type);
    const baseFilterResult = this.compileBaseFilterClause(database.type, baseAlias, plan?.whereConditions || [], baseTable, binder);
    const baseFilterClause = baseFilterResult.sql;

    if (!plan?.requiresJoin || !relatedTable) {
      let sql = `SELECT ${this.quoteIdentifier(database.type, baseAlias)}.* FROM ${this.quoteIdentifier(database.type, baseTable)} ${this.quoteIdentifier(database.type, baseAlias)}`;
      if (baseFilterClause) {
        sql += ` WHERE ${baseFilterClause}`;
      }
      sql += ` ${this.limitClause(database.type, limit)}`;

      return {
        databaseId: database.id,
        databaseType: database.type,
        sql,
        params: binder.getParams(),
        tableName: baseTable,
        role: 'base',
      };
    }

    const relation = this.findRelationEdge(schema, baseTable, relatedTable);
    if (!relation) {
      throw new Error(`No existe relación detectable entre '${baseTable}' y '${relatedTable}' en '${database.id}'`);
    }

    const relatedAlias = 't2';
    const relatedSchema = schema.schema?.[relatedTable];
    const metricColumn = normalizeIdentifier(plan?.metricColumn || relatedSchema?.pkPrincipal || relatedSchema?.clavesPrimarias?.[0] || 'id');
    const joinClause = `LEFT JOIN ${this.quoteIdentifier(database.type, relatedTable)} ${this.quoteIdentifier(database.type, relatedAlias)} ON ${this.quoteIdentifier(database.type, baseAlias)}.${this.quoteIdentifier(database.type, relation.baseColumn)} = ${this.quoteIdentifier(database.type, relatedAlias)}.${this.quoteIdentifier(database.type, relation.relatedColumn)}`;

    if (plan?.requiresAggregation) {
      const groupKey = normalizeIdentifier(baseSchema?.pkPrincipal || baseSchema?.clavesPrimarias?.[0] || 'id');
      const labelColumn = this.queryBuilder.pickBestIdentityColumn(baseSchema) || groupKey;
      let sql = `SELECT ${this.quoteIdentifier(database.type, baseAlias)}.${this.quoteIdentifier(database.type, groupKey)} AS ${this.quoteIdentifier(database.type, `${baseTable}_${groupKey}`)}, ${this.quoteIdentifier(database.type, baseAlias)}.${this.quoteIdentifier(database.type, labelColumn)} AS ${this.quoteIdentifier(database.type, `${baseTable}_${labelColumn}`)}, COUNT(${this.quoteIdentifier(database.type, relatedAlias)}.${this.quoteIdentifier(database.type, metricColumn)}) AS ${this.quoteIdentifier(database.type, `total_${relatedTable}`)} FROM ${this.quoteIdentifier(database.type, baseTable)} ${this.quoteIdentifier(database.type, baseAlias)} ${joinClause}`;
      if (baseFilterClause) {
        sql += ` WHERE ${baseFilterClause}`;
      }
      sql += ` GROUP BY ${this.quoteIdentifier(database.type, baseAlias)}.${this.quoteIdentifier(database.type, groupKey)}, ${this.quoteIdentifier(database.type, baseAlias)}.${this.quoteIdentifier(database.type, labelColumn)}`;
      if (plan?.havingCondition) {
        const havingOperator = ['=', '!=', '<>', '>', '<', '>=', '<='].includes(String(plan.havingCondition.operator || '').toUpperCase())
          ? String(plan.havingCondition.operator).toUpperCase()
          : '=';
        const havingValue = Number(plan.havingCondition.value);
        const safeHavingValue = Number.isFinite(havingValue) ? havingValue : 0;
        const havingPlaceholder = binder.add(safeHavingValue);
        sql += ` HAVING COUNT(${this.quoteIdentifier(database.type, relatedAlias)}.${this.quoteIdentifier(database.type, metricColumn)}) ${havingOperator} ${havingPlaceholder}`;
      }
      sql += ` ${this.limitClause(database.type, limit)}`;

      return {
        databaseId: database.id,
        databaseType: database.type,
        sql,
        params: binder.getParams(),
        tableName: baseTable,
        role: 'base',
      };
    }

    const relatedNullCondition = (plan?.whereConditions || []).find((condition) => normalizeIdentifier(condition.table) === relatedTable && (condition.operator === 'IS NULL' || condition.operator === 'IS NOT NULL'));
    let whereClause = baseFilterClause;
    if (relatedNullCondition) {
      const relatedExpr = `${this.quoteIdentifier(database.type, relatedAlias)}.${this.quoteIdentifier(database.type, relatedNullCondition.column)}`;
      whereClause = [whereClause, `${relatedExpr} ${relatedNullCondition.operator}`].filter(Boolean).join(' AND ');
    }

    let sql = `SELECT ${this.quoteIdentifier(database.type, baseAlias)}.* FROM ${this.quoteIdentifier(database.type, baseTable)} ${this.quoteIdentifier(database.type, baseAlias)} ${joinClause}`;
    if (whereClause) {
      sql += ` WHERE ${whereClause}`;
    }
    sql += ` ${this.limitClause(database.type, limit)}`;

    return {
      databaseId: database.id,
      databaseType: database.type,
      sql,
      params: binder.getParams(),
      tableName: baseTable,
      role: 'base',
    };
  }

  compileDistributedBaseQuery(database, baseTable, plan, limit = 50) {
    const schema = this.getDatabaseSchema(database);
    const tableSchema = schema?.schema?.[baseTable];
    if (!tableSchema) {
      throw new Error(`Tabla base no encontrada en '${database.id}': ${baseTable}`);
    }

    const alias = 't1';
    const columns = this.pickSelectableColumns(tableSchema);
    const basePk = normalizeIdentifier(tableSchema?.pkPrincipal || tableSchema?.clavesPrimarias?.[0] || columns[0] || 'id');
    const binder = this.createParamBinder(database.type);
    const selectClause = columns
      .map((column) => `${this.quoteIdentifier(database.type, alias)}.${this.quoteIdentifier(database.type, column)} AS ${this.quoteIdentifier(database.type, `${baseTable}_${column}`)}`)
      .join(', ');
    const whereFilterResult = this.compileBaseFilterClause(database.type, alias, plan?.whereConditions || [], baseTable, binder);
    const whereClause = whereFilterResult.sql;

    let sql = `SELECT ${selectClause} FROM ${this.quoteIdentifier(database.type, baseTable)} ${this.quoteIdentifier(database.type, alias)}`;
    if (whereClause) {
      sql += ` WHERE ${whereClause}`;
    }
    sql += ` ${this.limitClause(database.type, limit)}`;

    return {
      databaseId: database.id,
      databaseType: database.type,
      sql,
      params: binder.getParams(),
      tableName: baseTable,
      role: 'base',
      baseKey: `${baseTable}_${basePk}`,
    };
  }

  buildCrossDatabasePlan(resolution, database) {
    const intent = resolution?.intent || { conditions: { relation: null, filters: [] } };
    const baseEntity = resolution?.entities?.[0]?.primary || null;
    const relatedEntity = resolution?.entities?.[1]?.primary || null;
    const schema = this.getDatabaseSchema(database);
    const baseTable = normalizeIdentifier(baseEntity?.tableName);
    const baseSchema = schema?.schema?.[baseTable];

    const plan = {
      baseTable,
      selectTables: baseTable ? [baseTable] : [],
      joinTables: dedupe([baseTable, relatedEntity?.tableName].filter(Boolean)),
      requiresJoin: Boolean(intent?.conditions?.relation),
      requiresAggregation: intent?.conditions?.relation?.mode === 'count-comparison' && Number(intent?.conditions?.relation?.value) !== 0,
      aggregationFn: intent?.conditions?.relation?.mode === 'count-comparison' && Number(intent?.conditions?.relation?.value) !== 0 ? 'COUNT' : null,
      metricTable: normalizeIdentifier(relatedEntity?.tableName || ''),
      metricColumn: 'id',
      whereConditions: [],
      havingCondition: null,
    };

    for (const filter of intent?.conditions?.filters || []) {
      if (filter.type !== 'identity-match') continue;
      const identityColumn = this.queryBuilder.pickBestIdentityColumn(baseSchema)
        || this.intelligenceEngine.queryBuilder.findBestAttributeColumn('name', baseSchema)
        || this.intelligenceEngine.queryBuilder.findBestAttributeColumn('username', baseSchema)
        || this.intelligenceEngine.queryBuilder.findBestAttributeColumn('email', baseSchema);

      if (!identityColumn) continue;
      plan.whereConditions.push({
        table: baseTable,
        column: identityColumn,
        operator: 'ILIKE',
        value: `%${filter.value}%`,
        castText: true,
      });
    }

    if (plan.requiresAggregation) {
      plan.havingCondition = {
        aggregation: 'COUNT',
        table: plan.metricTable,
        column: 'id',
        operator: intent.conditions.relation.operator,
        value: intent.conditions.relation.value,
      };
    }

    return plan;
  }

  compileDistributedRelatedQuery(database, baseTable, relatedTable, relationMode, limit = 50) {
    const schema = this.getDatabaseSchema(database);
    const baseSchema = schema?.schema?.[baseTable];
    const relatedSchema = schema?.schema?.[relatedTable];
    if (!relatedSchema) {
      throw new Error(`Tabla relacionada no encontrada en '${database.id}': ${relatedTable}`);
    }

    const mergeKeys = this.detectMergeKeys(baseTable, relatedTable, baseSchema || {}, relatedSchema || {});
    const alias = 't1';
    const mergeExpression = `${this.quoteIdentifier(database.type, alias)}.${this.quoteIdentifier(database.type, mergeKeys.relatedKey)}`;

    if (relationMode === 'count-comparison') {
      const sql = `SELECT ${mergeExpression} AS ${this.quoteIdentifier(database.type, 'merge_key')}, COUNT(*) AS ${this.quoteIdentifier(database.type, 'related_count')} FROM ${this.quoteIdentifier(database.type, relatedTable)} ${this.quoteIdentifier(database.type, alias)} GROUP BY ${mergeExpression}`;
      return {
        databaseId: database.id,
        databaseType: database.type,
        sql,
        params: database.type === 'oracle' ? {} : [],
        tableName: relatedTable,
        role: 'related',
        relatedKey: mergeKeys.relatedKey,
        baseKey: mergeKeys.baseKey,
        aggregateOnly: true,
      };
    }

    const columns = this.pickSelectableColumns(relatedSchema);
    const selectClause = dedupe(['merge_key', ...columns.map((column) => `${relatedTable}_${column}`)]).map((column) => {
      if (column === 'merge_key') {
        return `${mergeExpression} AS ${this.quoteIdentifier(database.type, 'merge_key')}`;
      }
      const originalColumn = column.replace(`${relatedTable}_`, '');
      return `${this.quoteIdentifier(database.type, alias)}.${this.quoteIdentifier(database.type, originalColumn)} AS ${this.quoteIdentifier(database.type, column)}`;
    }).join(', ');

    let sql = `SELECT ${selectClause} FROM ${this.quoteIdentifier(database.type, relatedTable)} ${this.quoteIdentifier(database.type, alias)}`;
    sql += ` ${this.limitClause(database.type, Math.max(Number(limit) || 50, 500))}`;

    return {
      databaseId: database.id,
      databaseType: database.type,
      sql,
      params: database.type === 'oracle' ? {} : [],
      tableName: relatedTable,
      role: 'related',
      relatedKey: mergeKeys.relatedKey,
      baseKey: mergeKeys.baseKey,
      attachField: this.pluralize(relatedTable),
      aggregateOnly: false,
    };
  }

  getRowValue(row, keyName) {
    const normalizedKey = normalizeIdentifier(keyName);
    for (const [rowKey, rowValue] of Object.entries(row || {})) {
      if (normalizeIdentifier(rowKey) === normalizedKey) {
        return rowValue;
      }
    }
    return undefined;
  }

  applyDistributedFilter(rows, relation, attachField) {
    if (!relation?.mode) return rows;
    if (relation.mode === 'absence') {
      return rows.filter((row) => Array.isArray(row[attachField]) ? row[attachField].length === 0 : Number(row[`${attachField}_count`] || 0) === 0);
    }

    if (relation.mode === 'presence') {
      return rows.filter((row) => Array.isArray(row[attachField]) ? row[attachField].length > 0 : Number(row[`${attachField}_count`] || 0) > 0);
    }

    if (relation.mode === 'count-comparison') {
      return rows.filter((row) => {
        const countValue = Number(row[`${attachField}_count`] || 0);
        if (relation.operator === '>') return countValue > Number(relation.value);
        if (relation.operator === '<') return countValue < Number(relation.value);
        return countValue === Number(relation.value);
      });
    }

    return rows;
  }

  mergeResults(results = [], context = {}) {
    const baseExecutions = results.filter((result) => result.success && result.role === 'base');
    const relatedExecutions = results.filter((result) => result.success && result.role === 'related');

    if (baseExecutions.length === 0) {
      return results
        .filter((result) => result.success)
        .flatMap((result) => (result.rows || []).map((row) => ({
          ...row,
          __sourceDatabase: result.databaseId,
        })));
    }

    const baseMap = new Map();
    for (const execution of baseExecutions) {
      const baseKey = execution.baseKey || context.baseKey || PREFERRED_MERGE_KEYS[0];
      for (const row of execution.rows || []) {
        const keyValue = this.getRowValue(row, baseKey);
        const mapKey = keyValue === undefined || keyValue === null || keyValue === ''
          ? `${execution.databaseId}:${baseMap.size}`
          : String(keyValue);
        const current = baseMap.get(mapKey) || {};
        baseMap.set(mapKey, {
          ...current,
          ...row,
          __databases: dedupe([...(current.__databases || []), execution.databaseId]),
        });
      }
    }

    for (const execution of relatedExecutions) {
      const attachField = execution.attachField || this.pluralize(execution.tableName || 'related');
      const aggregateOnly = Boolean(execution.aggregateOnly);

      if (aggregateOnly) {
        for (const row of execution.rows || []) {
          const mergeKey = this.getRowValue(row, 'merge_key');
          if (mergeKey === undefined || mergeKey === null || mergeKey === '') continue;
          const current = baseMap.get(String(mergeKey));
          if (!current) continue;
          const nextCount = Number(this.getRowValue(row, 'related_count') || 0);
          baseMap.set(String(mergeKey), {
            ...current,
            [`${attachField}_count`]: nextCount,
            __databases: dedupe([...(current.__databases || []), execution.databaseId]),
          });
        }
        continue;
      }

      const relatedMap = new Map();
      for (const row of execution.rows || []) {
        const mergeKey = this.getRowValue(row, 'merge_key');
        if (mergeKey === undefined || mergeKey === null || mergeKey === '') continue;
        const current = relatedMap.get(String(mergeKey)) || [];
        relatedMap.set(String(mergeKey), [...current, row]);
      }

      for (const [mergeKey, relatedRows] of relatedMap.entries()) {
        const current = baseMap.get(String(mergeKey));
        if (!current) continue;
        baseMap.set(String(mergeKey), {
          ...current,
          [attachField]: relatedRows,
          [`${attachField}_count`]: relatedRows.length,
          __databases: dedupe([...(current.__databases || []), execution.databaseId]),
        });
      }
    }

    const mergedRows = [...baseMap.values()];
    const attachField = relatedExecutions[0]?.attachField;
    const filteredRows = attachField
      ? this.applyDistributedFilter(mergedRows, context?.relation, attachField)
      : mergedRows;

    return filteredRows;
  }

  async executeSingle(text, resolution, decision, options = {}) {
    const targetDatabaseId = decision?.source?.[0];
    const database = this.registry.getDatabaseById(targetDatabaseId);
    if (!database) {
      throw new Error(`Base de datos no encontrada: ${targetDatabaseId}`);
    }

    const schema = this.getDatabaseSchema(database);
    const smartQuery = this.intelligenceEngine.buildSmartQuery(text, schema, {
      tablaBase: resolution?.entities?.[0]?.primary?.tableName || null,
      confianza: resolution?.entities?.[0]?.primary?.score || 0,
      topScore: resolution?.entities?.[0]?.primary?.score || 0,
    });

    if (smartQuery?.error) {
      return {
        success: false,
        data: [],
        source: [database.id],
        executionType: 'single-db',
        errors: [{ databaseId: database.id, error: smartQuery.error }],
        scripts: [],
        partialResults: [],
        mergedResults: [],
        warnings: smartQuery.warnings || [],
      };
    }

    const compiledQuery = this.compileSingleDatabaseQuery(database, smartQuery.plan, smartQuery.intent, options.limit || 50);
    const rows = await this.registry.executeCompiledQuery(compiledQuery);
    const mergedResults = rows.map((row) => ({ ...row, __sourceDatabase: database.id }));
    this.learnFromResolution(resolution, [database.id]);

    return {
      success: true,
      data: mergedResults,
      source: [database.id],
      executionType: 'single-db',
      confidence: decision?.confidence || resolution?.confidence || 0,
      confidenceBand: decision?.confidenceBand || resolution?.confidenceBand || 'low',
      warnings: smartQuery.warnings || [],
      scripts: [{ databaseId: database.id, databaseType: database.type, sql: compiledQuery.sql }],
      partialResults: [{
        success: true,
        role: 'base',
        databaseId: database.id,
        databaseType: database.type,
        tableName: compiledQuery.tableName,
        sql: compiledQuery.sql,
        rows,
        rowCount: rows.length,
      }],
      mergedResults,
      errors: [],
      resolution,
      decision,
      message: mergedResults.length === 0 ? 'No se encontraron resultados para este criterio' : undefined,
    };
  }

  async executeDistributed(text, resolution, decision, options = {}) {
    const intent = resolution.intent;
    const entityMap = new Map((resolution.entities || []).map((entity) => [entity.entity, entity]));
    const baseEntity = resolution.entities?.[0];
    const relatedEntity = resolution.entities?.[1] || null;
    const executions = [];

    const fullCoverageDatabases = (decision?.source || []).filter((databaseId) => {
      return (resolution.entities || []).every((entity) => (entity.matches || []).some((match) => match.databaseId === databaseId));
    });

    if (fullCoverageDatabases.length > 0) {
      const compiledPerDatabase = fullCoverageDatabases.map((databaseId) => {
        const database = this.registry.getDatabaseById(databaseId);
        const schema = this.getDatabaseSchema(database);
        const smartQuery = this.intelligenceEngine.buildSmartQuery(text, schema, {
          tablaBase: baseEntity?.matches?.find((match) => match.databaseId === databaseId)?.tableName || baseEntity?.primary?.tableName || null,
          confianza: baseEntity?.primary?.score || 0,
          topScore: baseEntity?.primary?.score || 0,
        });
        if (smartQuery?.error) {
          return {
            database,
            error: smartQuery.error,
            warning: smartQuery.warning || null,
          };
        }

        return {
          database,
          compiledQuery: this.compileSingleDatabaseQuery(database, smartQuery.plan, smartQuery.intent, options.limit || 50),
          smartQuery,
        };
      });

      const rowsPerDb = await Promise.all(compiledPerDatabase.map(async (item) => {
        if (item.error) {
          return {
            success: false,
            role: 'base',
            databaseId: item.database.id,
            databaseType: item.database.type,
            tableName: baseEntity?.primary?.tableName || '',
            sql: '',
            rows: [],
            rowCount: 0,
            error: item.error,
          };
        }

        try {
          const rows = await this.registry.executeCompiledQuery(item.compiledQuery);
          return {
            success: true,
            role: 'base',
            databaseId: item.database.id,
            databaseType: item.database.type,
            tableName: item.compiledQuery.tableName,
            sql: item.compiledQuery.sql,
            rows,
            rowCount: rows.length,
          };
        } catch (error) {
          return {
            success: false,
            role: 'base',
            databaseId: item.database.id,
            databaseType: item.database.type,
            tableName: item.compiledQuery?.tableName || '',
            sql: item.compiledQuery?.sql || '',
            rows: [],
            rowCount: 0,
            error: error?.message || String(error),
          };
        }
      }));

      const mergedResults = this.mergeResults(rowsPerDb, {});
      if (rowsPerDb.some((item) => item.success)) {
        this.learnFromResolution(resolution, fullCoverageDatabases);
      }
      return {
        success: rowsPerDb.some((item) => item.success),
        data: mergedResults,
        source: fullCoverageDatabases,
        executionType: 'distributed',
        confidence: decision?.confidence || resolution?.confidence || 0,
        confidenceBand: decision?.confidenceBand || resolution?.confidenceBand || 'low',
        scripts: rowsPerDb.map((item) => ({ databaseId: item.databaseId, databaseType: item.databaseType, sql: item.sql })),
        partialResults: rowsPerDb,
        mergedResults,
        errors: rowsPerDb.filter((item) => !item.success).map((item) => ({ databaseId: item.databaseId, error: item.error })),
        warnings: dedupe(compiledPerDatabase.flatMap((item) => item.smartQuery?.warnings || [])),
        resolution,
        decision,
        message: mergedResults.length === 0 ? 'No se encontraron resultados para este criterio' : undefined,
      };
    }

    const baseMatch = baseEntity?.primary;
    if (!baseMatch) {
      throw new Error('No se pudo determinar la entidad base para la ejecución distribuida');
    }

    const baseDatabase = this.registry.getDatabaseById(baseMatch.databaseId);
    const baseSchema = this.getDatabaseSchema(baseDatabase);
    const crossDatabasePlan = this.buildCrossDatabasePlan(resolution, baseDatabase);
    const baseCompiled = this.compileDistributedBaseQuery(baseDatabase, crossDatabasePlan.baseTable, crossDatabasePlan, options.limit || 50);
    executions.push(baseCompiled);

    if (relatedEntity?.primary) {
      const relatedMatches = relatedEntity.matches || [];
      for (const relatedMatch of relatedMatches) {
        if (relatedMatch.databaseId === baseDatabase.id && fullCoverageDatabases.length === 0) {
          continue;
        }

        const relatedDatabase = this.registry.getDatabaseById(relatedMatch.databaseId);
        const relatedCompiled = this.compileDistributedRelatedQuery(
          relatedDatabase,
          crossDatabasePlan.baseTable,
          relatedMatch.tableName,
          resolution?.intent?.conditions?.relation?.mode || null,
          options.limit || 50
        );
        executions.push(relatedCompiled);
      }
    }

    const partialResults = await Promise.all(executions.map(async (execution) => {
      try {
        const rows = await this.registry.executeCompiledQuery(execution);
        return {
          success: true,
          ...execution,
          rows,
          rowCount: rows.length,
        };
      } catch (error) {
        return {
          success: false,
          ...execution,
          rows: [],
          rowCount: 0,
          error: error?.message || String(error),
        };
      }
    }));

    const relationContext = {
      relation: resolution?.intent?.conditions?.relation || null,
      baseKey: partialResults.find((item) => item.role === 'related')?.baseKey || baseSchema.schema?.[crossDatabasePlan.baseTable]?.pkPrincipal || 'id',
    };
    const mergedResults = this.mergeResults(partialResults, relationContext);
    if (partialResults.some((item) => item.success)) {
      this.learnFromResolution(resolution, dedupe(partialResults.map((item) => item.databaseId)));
    }

    return {
      success: partialResults.some((item) => item.success),
      data: mergedResults,
      source: dedupe(partialResults.map((item) => item.databaseId)),
      executionType: 'distributed',
      confidence: decision?.confidence || resolution?.confidence || 0,
      confidenceBand: decision?.confidenceBand || resolution?.confidenceBand || 'low',
      warnings: resolution.warnings || [],
      scripts: partialResults.map((item) => ({ databaseId: item.databaseId, databaseType: item.databaseType, sql: item.sql })),
      partialResults,
      mergedResults,
      errors: partialResults.filter((item) => !item.success).map((item) => ({ databaseId: item.databaseId, error: item.error })),
      resolution,
      decision,
      message: mergedResults.length === 0 ? 'No se encontraron resultados para este criterio' : undefined,
    };
  }

  async execute(text, options = {}) {
    const inputText = String(text || '').trim();
    if (!inputText) {
      throw new Error('Texto de consulta requerido');
    }

    const databases = this.getFilteredDatabases(options.databases || []);
    if (databases.length === 0) {
      throw new Error('No hay bases registradas para ejecución multi-base');
    }

    const resolution = this.resolveEntitiesAcrossDatabases(inputText, options.databases || []);
    const decision = this.decideExecution(resolution);

    this.debugLog('execution_decision', {
      input: inputText,
      executionType: decision?.executionType,
      mode: decision?.mode,
      confidence: decision?.confidence || resolution?.confidence || 0,
      confidenceBand: decision?.confidenceBand || resolution?.confidenceBand || 'low',
      warnings: resolution?.warnings || [],
    });

    if (decision.mode === 'unresolved') {
      return {
        success: false,
        data: [],
        source: [],
        executionType: 'unresolved',
        scripts: [],
        partialResults: [],
        mergedResults: [],
        errors: [{ error: resolution.error }],
        warnings: [...new Set([...(resolution.warnings || []), decision.warning].filter(Boolean))],
        confidence: decision?.confidence || resolution?.confidence || 0,
        confidenceBand: decision?.confidenceBand || resolution?.confidenceBand || 'low',
        message: decision?.message || resolution?.ambiguity?.message || resolution?.error || 'Tu consulta es ambigua',
        suggestions: decision?.suggestions || resolution?.ambiguity?.suggestions || [],
        resolution,
        decision,
      };
    }

    const relationDetected = Boolean(resolution?.intent?.conditions?.relation);
    const entityCount = Array.isArray(resolution?.entities) ? resolution.entities.length : 0;
    const shouldForceDistributed = Boolean(options?.forceDistributed)
      && (relationDetected || entityCount >= 2);

    if (shouldForceDistributed) {
      return this.executeDistributed(inputText, resolution, {
        ...decision,
        mode: 'distributed',
        executionType: 'distributed',
      }, options);
    }

    if (decision.mode === 'single-db') {
      return this.executeSingle(inputText, resolution, decision, options);
    }

    if (decision.mode === 'distributed') {
      return this.executeDistributed(inputText, resolution, decision, options);
    }

    return {
      success: false,
      data: [],
      source: [],
      executionType: 'normal',
      scripts: [],
      partialResults: [],
      mergedResults: [],
      errors: [{ error: 'No se detectó ejecución multi-base aplicable' }],
      warnings: resolution.warnings || [],
      resolution,
      decision,
    };
  }
}

export default MultiDatabaseEngine;