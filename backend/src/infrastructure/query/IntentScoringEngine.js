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

function clamp(value, min = 0, max = 1) {
  return Math.max(min, Math.min(max, Number(value) || 0));
}

function dedupe(values = []) {
  return [...new Set((values || []).map((item) => String(item || '').trim()).filter(Boolean))];
}

export class IntentScoringEngine {
  constructor(queryBuilder) {
    this.queryBuilder = queryBuilder;
    this.ambiguityDeltaThreshold = 0.2;
  }

  generateInterpretations(inputOrIntent, context = {}) {
    const intent = typeof inputOrIntent === 'string'
      ? { input: inputOrIntent, normalizedInput: normalizeText(inputOrIntent), conditions: { relation: null, filters: [] } }
      : (inputOrIntent || { input: '', normalizedInput: '', conditions: { relation: null, filters: [] } });

    const resolvedEntities = context?.resolvedEntities || [];
    const baseEntity = resolvedEntities[0] || null;
    const relatedEntity = resolvedEntities[1] || null;
    const relation = intent?.conditions?.relation || null;
    const normalizedInput = normalizeText(intent?.normalizedInput || intent?.input || '');

    const interpretations = [];

    interpretations.push({
      interpretation: relatedEntity
        ? `${baseEntity?.table || 'entidad'} con ${relatedEntity?.table || 'relacion'}`
        : `${baseEntity?.table || 'entidad'} general`,
      type: 'base-intent',
      resolvedEntities,
      relation,
      planPatch: {},
      optionsLabel: relatedEntity
        ? `${baseEntity?.table || 'entidad'} relacionados con ${relatedEntity?.table || 'otra entidad'}`
        : `listar ${baseEntity?.table || 'entidad'}`,
    });

    if (/\bactiv[oa]s?\b/.test(normalizedInput) && baseEntity) {
      interpretations.push({
        interpretation: `${baseEntity.table} con estado activo`,
        type: 'status-active',
        resolvedEntities,
        relation,
        planPatch: { statusActive: true },
        optionsLabel: `${baseEntity.table} con active = true`,
      });

      interpretations.push({
        interpretation: `${baseEntity.table} con actividad reciente`,
        type: 'activity-recent',
        resolvedEntities,
        relation,
        planPatch: { activityRecent: true },
        optionsLabel: `${baseEntity.table} con actividad reciente`,
      });
    }

    if (relation?.mode === 'count-comparison') {
      interpretations.push({
        interpretation: `comparacion por conteo ${relation.operator} ${relation.value}`,
        type: 'count-comparison',
        resolvedEntities,
        relation,
        planPatch: { enforceHaving: true },
        optionsLabel: `conteo ${relation.operator} ${relation.value}`,
      });
    }

    const scored = interpretations.map((interpretation, index) => this.scoreInterpretation({
      id: `interpretation_${index + 1}`,
      ...interpretation,
    }, context));

    return scored.sort((left, right) => right.score - left.score);
  }

  scoreInterpretation(interpretation = {}, context = {}) {
    const resolvedEntities = interpretation?.resolvedEntities || context?.resolvedEntities || [];
    const schema = context?.schema || {};
    const learning = context?.learning || {};
    // Use both explicit tables list AND dynamic Object.keys(schema.schema) so the
    // engine works with any database without hardcoded table names.
    const staticTables = (schema?.tables || schema?.tablas || []).map((table) => normalizeText(table));
    const dynamicTables = Object.keys(schema?.schema || {}).map((table) => normalizeText(table));
    const schemaTables = new Set([...staticTables, ...dynamicTables]);

    const semanticMatches = resolvedEntities
      .map((entity) => Number(entity?.score || entity?.primary?.score || 0))
      .filter((score) => Number.isFinite(score));
    const semanticScore = semanticMatches.length > 0
      ? semanticMatches.reduce((sum, value) => sum + value, 0) / semanticMatches.length
      : 0.2;

    let columnScore = 0.35;
    if (interpretation?.type === 'status-active') {
      const baseEntity = resolvedEntities[0] || null;
      const baseTable = String(baseEntity?.table || baseEntity?.primary?.tableName || '').trim();
      const columns = schema?.schema?.[baseTable]?.columnas || [];
      const hasStatusColumn = columns.some((column) => /(^|_)(active|activo|status|estado|enabled|habilitado)($|_)/i.test(String(column?.nombre || '')));
      columnScore = hasStatusColumn ? 0.95 : 0.35;
    }

    if (interpretation?.type === 'activity-recent') {
      columnScore = 0.55;
      const relatedEntity = resolvedEntities[1] || null;
      const relatedTable = String(relatedEntity?.table || relatedEntity?.primary?.tableName || '').trim();
      if (/(log|audit|session|activity|bitacora)/i.test(relatedTable)) {
        columnScore = 0.8;
      }
    }

    const relationScore = interpretation?.relation
      ? (context?.relationDetected === false ? 0.2 : 0.9)
      : 0.6;

    const normalizedTerms = dedupe([
      ...(context?.intent?.entityTerms || []),
      ...(resolvedEntities || []).map((entity) => entity?.sourceTerm || entity?.entity || ''),
    ].map((term) => normalizeText(term)));

    let historyHits = 0;
    let historyChecks = 0;
    for (const term of normalizedTerms) {
      historyChecks += 1;
      if (learning?.tableAliases?.[term] || learning?.columnKeywords?.[term]) {
        historyHits += 1;
      }
    }
    let historyScore = historyChecks > 0 ? historyHits / historyChecks : 0.25;

    // Extra boost when entities were matched via DB-confirmed semantic_learning entries
    const learnedMatchCount = (resolvedEntities || []).filter((e) => e?.learnedMatch || e?.primary?.learnedMatch).length;
    if (learnedMatchCount > 0) {
      historyScore = Math.min(1, historyScore + (learnedMatchCount * 0.2));
    }

    let score = (semanticScore * 0.45) + (columnScore * 0.2) + (relationScore * 0.2) + (historyScore * 0.15);

    const resolvedTables = dedupe((resolvedEntities || [])
      .map((entity) => entity?.table || entity?.primary?.tableName || '')
      .map((table) => normalizeText(table))
      .filter(Boolean));

    let schemaAlignmentScore = 0;
    for (const table of resolvedTables) {
      if (schemaTables.has(table)) schemaAlignmentScore += 0.2;
    }

    const hasRelationInSchema = Boolean(
      interpretation?.relation
      && resolvedTables.length >= 2
      && schema?.schema?.[resolvedTables[0]]
      && schema?.schema?.[resolvedTables[1]]
    );
    if (hasRelationInSchema) {
      schemaAlignmentScore += 0.15;
    }

    score += Math.min(schemaAlignmentScore, 0.4);

    if (interpretation?.type === 'base-intent') {
      score += 0.03;
      if (interpretation?.relation) score -= 0.08;
    }
    if (interpretation?.type === 'count-comparison') score += 0.18;
    score = clamp(score, 0, 1);

    return {
      ...interpretation,
      score,
      scoringBreakdown: {
        semanticScore: Number(semanticScore.toFixed(4)),
        columnScore: Number(columnScore.toFixed(4)),
        relationScore: Number(relationScore.toFixed(4)),
        historyScore: Number(historyScore.toFixed(4)),
      },
    };
  }

  selectBestInterpretation(interpretations = []) {
    const sorted = [...(interpretations || [])].sort((left, right) => Number(right?.score || 0) - Number(left?.score || 0));
    const best = sorted[0] || null;
    const second = sorted[1] || null;
    const bestScore = Number(best?.score || 0);
    const delta = best && second ? Number((bestScore - Number(second?.score || 0)).toFixed(4)) : 1;
    const isAmbiguous = Boolean(best && second && delta < this.ambiguityDeltaThreshold);

    let confidenceBand = 'low';
    if (bestScore > 0.8) confidenceBand = 'high';
    else if (bestScore >= 0.5) confidenceBand = 'medium';

    const countDriven = String(best?.type || '').includes('count');
    const requiresClarification = bestScore < 0.5 || (isAmbiguous && bestScore < 0.8 && !countDriven);
    const executeWithWarning = !requiresClarification && (confidenceBand === 'medium' || isAmbiguous);

    return {
      interpretations: sorted,
      best,
      second,
      bestScore,
      delta,
      isAmbiguous,
      confidenceBand,
      requiresClarification,
      executeWithWarning,
    };
  }

  handleAmbiguity(selection = {}) {
    const options = (selection?.interpretations || [])
      .slice(0, 3)
      .map((interpretation) => interpretation?.optionsLabel || interpretation?.interpretation)
      .filter(Boolean);

    if (selection?.requiresClarification) {
      return {
        isAmbiguous: true,
        warning: 'Consulta ambigua',
        message: 'Tu consulta es ambigua. Necesito que aclares el criterio principal antes de ejecutar.',
        suggestions: options,
        options,
        requiresClarification: true,
      };
    }

    if (selection?.executeWithWarning) {
      return {
        isAmbiguous: Boolean(selection?.isAmbiguous),
        warning: selection?.isAmbiguous ? 'Consulta ambigua' : 'Confianza media en la interpretación',
        message: selection?.isAmbiguous
          ? 'Se ejecutará la interpretación más probable. Revisa las alternativas sugeridas.'
          : 'Se ejecutará la interpretación detectada con confianza media.',
        suggestions: options,
        options,
        requiresClarification: false,
      };
    }

    return {
      isAmbiguous: false,
      warning: null,
      message: null,
      suggestions: [],
      options: [],
      requiresClarification: false,
    };
  }
}

export default IntentScoringEngine;
