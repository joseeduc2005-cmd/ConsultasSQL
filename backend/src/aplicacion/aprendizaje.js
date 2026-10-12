/**
 * El aprendizaje semántico: qué palabra del negocio apunta a qué tabla.
 *
 * Cuando una consulta sale bien, el sistema se queda con la pareja: «socio» llevó a
 * la tabla de personas, «cartera» a la de créditos. La próxima vez que alguien
 * escriba esa palabra, el motor ya sabe por dónde empezar. Es lo que hace que el
 * buscador mejore con el uso en vez de quedarse como el primer día, y es también lo
 * que permite que una base nueva se vaya entendiendo sola.
 *
 * Tiene estado: un caché en memoria con lo aprendido, respaldado en la tabla
 * semantic_learning. El caché no sale de aquí — fuera solo salen las funciones, y eso
 * cambia algo concreto. Antes `reloadActiveDatabaseConnection` tocaba el caché
 * directamente para poder volver atrás si la base nueva no respondía; ahora eso son
 * dos funciones, guardarEstado y restaurarEstado, y el motivo está escrito.
 *
 * El pool llega por getter, como en todos los demás, porque se reasigna al recargar
 * la configuración multi-base.
 */
import { normalizeSchemaIdentifier } from '../dominio/sql/guardas.js';
import { dedupeSemanticValues, normalizeSemanticLearningTerm } from '../dominio/texto/normalizar.js';

export function crearAprendizajeSemantico({
  // Eran imports directos a los adaptadores; entran por el puerto para que este
  // modulo no sepa de donde salen el esquema ni el cache.
  bumpQueryCacheDataVersion,
  relacionesDelEsquema,
  tablasPermitidas,
  obtenerPool,
  schemaCache,
  schemaDetector,
  queryBuilder,
  queryParameterizer,
}) {
  const LEARNED_SEMANTIC_CACHE = {
    fingerprint: '',
    loadedAt: 0,
    ttlMs: 30 * 1000,
    refreshPromise: null,
    snapshot: {
      tableAliases: {},
      columnKeywords: {},
      suggestionTerms: [],
    },
  };

  function buildLearnedSemanticSnapshot(rows = []) {
    const tableAliases = {};
    const columnKeywords = {};

    for (const row of rows || []) {
      const term = normalizeSemanticLearningTerm(row.normalized_term || row.original_term);
      const targetKind = String(row.target_kind || '').trim().toLowerCase();
      const targetName = normalizeSchemaIdentifier(row.target_name || '');
      if (!term || !targetName) continue;

      if (targetKind === 'table') {
        tableAliases[term] = dedupeSemanticValues([...(tableAliases[term] || []), targetName]);
        continue;
      }

      if (targetKind === 'column') {
        columnKeywords[term] = dedupeSemanticValues([...(columnKeywords[term] || []), targetName]);
      }
    }

    return {
      tableAliases,
      columnKeywords,
      suggestionTerms: dedupeSemanticValues([...Object.keys(tableAliases), ...Object.keys(columnKeywords)]),
    };
  }

  function applyLearnedSemanticSnapshot(snapshot = {}) {
    const safeSnapshot = {
      tableAliases: { ...(snapshot?.tableAliases || {}) },
      columnKeywords: { ...(snapshot?.columnKeywords || {}) },
      suggestionTerms: dedupeSemanticValues(snapshot?.suggestionTerms || []),
    };

    LEARNED_SEMANTIC_CACHE.snapshot = safeSnapshot;
    schemaDetector.setLearnedSemanticDictionary(safeSnapshot);
    queryBuilder.setLearnedSemanticDictionary(safeSnapshot);
    queryParameterizer.setLearnedSemanticDictionary(safeSnapshot);

    // Inject table mappings directly into QueryIntelligenceEngine for primary lookup
    // Format required: { normalizedTerm: { tableName, confidence }[] }
    if (queryBuilder?.queryIntelligenceEngine?.setLearnedTableMappings) {
      const tableMappings = {};
      for (const [term, targetTables] of Object.entries(safeSnapshot.tableAliases || {})) {
        const mappingEntries = (targetTables || []).map((tableName) => ({ tableName, confidence: 0.75 }));
        if (mappingEntries.length > 0) tableMappings[term] = mappingEntries;
      }
      queryBuilder.queryIntelligenceEngine.setLearnedTableMappings(tableMappings);
    }
  }

  function resetLearnedSemanticCache() {
    LEARNED_SEMANTIC_CACHE.fingerprint = '';
    LEARNED_SEMANTIC_CACHE.loadedAt = 0;
    LEARNED_SEMANTIC_CACHE.refreshPromise = null;
    applyLearnedSemanticSnapshot({ tableAliases: {}, columnKeywords: {}, suggestionTerms: [] });
  }

  async function refreshSemanticLearningCache(force = false) {
    const pool = obtenerPool();
    if (!pool || typeof pool.query !== 'function') {
      const emptySnapshot = { tableAliases: {}, columnKeywords: {}, suggestionTerms: [] };
      applyLearnedSemanticSnapshot(emptySnapshot);
      LEARNED_SEMANTIC_CACHE.fingerprint = '';
      LEARNED_SEMANTIC_CACHE.loadedAt = Date.now();
      return emptySnapshot;
    }

    const currentFingerprint = schemaDetector.getConnectionFingerprint();
    const fingerprintChanged = LEARNED_SEMANTIC_CACHE.fingerprint !== currentFingerprint;
    const cacheIsFresh = !force && !fingerprintChanged && (Date.now() - LEARNED_SEMANTIC_CACHE.loadedAt) < LEARNED_SEMANTIC_CACHE.ttlMs;
    if (cacheIsFresh) {
      return LEARNED_SEMANTIC_CACHE.snapshot;
    }

    if (LEARNED_SEMANTIC_CACHE.refreshPromise) {
      await LEARNED_SEMANTIC_CACHE.refreshPromise;
      return LEARNED_SEMANTIC_CACHE.snapshot;
    }

    LEARNED_SEMANTIC_CACHE.refreshPromise = (async () => {
      const result = await obtenerPool().query(`
        SELECT normalized_term, original_term, target_kind, target_name, table_name, confidence, hit_count
        FROM semantic_learning
        WHERE db_fingerprint = $1
        ORDER BY hit_count DESC, confidence DESC, last_seen DESC
      `, [currentFingerprint]);

      const snapshot = buildLearnedSemanticSnapshot(result.rows || []);
      applyLearnedSemanticSnapshot(snapshot);
      LEARNED_SEMANTIC_CACHE.fingerprint = currentFingerprint;
      LEARNED_SEMANTIC_CACHE.loadedAt = Date.now();
    })();

    try {
      await LEARNED_SEMANTIC_CACHE.refreshPromise;
    } finally {
      LEARNED_SEMANTIC_CACHE.refreshPromise = null;
    }

    return LEARNED_SEMANTIC_CACHE.snapshot;
  }

  async function upsertSemanticLearningEntry({ term, targetKind, targetName, tableName = '', confidence = 0.65 }) {
    // Gate: only persist entries with sufficient confidence to prevent low-quality pollution
    if (Number(confidence) < 0.7) {
      return false;
    }

    const connectionFingerprint = schemaDetector.getConnectionFingerprint();
    const normalizedTerm = normalizeSemanticLearningTerm(term);
    const normalizedTargetName = normalizeSchemaIdentifier(targetName);
    const normalizedTableName = normalizeSchemaIdentifier(tableName);
    if (!normalizedTerm || !normalizedTargetName || !['table', 'column'].includes(String(targetKind || '').trim().toLowerCase())) {
      return false;
    }

    await obtenerPool().query(
      `INSERT INTO semantic_learning
        (db_fingerprint, normalized_term, original_term, target_kind, target_name, table_name, confidence, hit_count, last_seen)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 1, NOW())
       ON CONFLICT (db_fingerprint, normalized_term, target_kind, target_name, table_name)
       DO UPDATE SET
         original_term = EXCLUDED.original_term,
         confidence = GREATEST(semantic_learning.confidence, EXCLUDED.confidence),
         hit_count = semantic_learning.hit_count + 1,
         last_seen = NOW()`,
      [
        connectionFingerprint,
        normalizedTerm,
        String(term || '').trim(),
        String(targetKind || '').trim().toLowerCase(),
        normalizedTargetName,
        normalizedTableName,
        Number(confidence) || 0.65,
      ]
    );

    return true;
  }

  async function learnSemanticMappingsFromSuccessfulQuery(queryText, queryResult) {
    const input = String(queryText || '').trim();
    if (!input || !queryResult?.exito) return;

    const baseTable = normalizeSchemaIdentifier(queryResult?.debug?.tablaSeleccionada || queryResult?.analisis?.tablaBase || '');
    if (!baseTable) return;

    const schema = await queryBuilder.getFullSchema();
    const baseSchema = schema?.schema?.[baseTable];
    if (!baseSchema) return;

    const knownVocabulary = queryBuilder.getTokenCorrectionVocabulary();
    const tokens = queryBuilder.tokenizeInputRaw(input)
      .map((token) => queryBuilder.normalizeTokenBase(token))
      .filter((token) => token && token.length >= 4)
      .filter((token) => !knownVocabulary.has(token))
      .filter((token) => !queryBuilder.isLikelyUuid(token) && !queryBuilder.isLikelyNumericId(token));

    const learnableTerms = [...new Set(tokens.map((token) => normalizeSemanticLearningTerm(token)).filter(Boolean))].slice(0, 6);
    if (learnableTerms.length === 0) return;

    let learned = false;
    for (const term of learnableTerms) {
      // Only save high-confidence table associations (≥ 0.85) so the learning store
      // stays clean. The upsertSemanticLearningEntry gate (confidence < 0.7) provides
      // a second safety net.
      const resolutionScore = Number(queryResult?.debug?.confianza || queryResult?.analisis?.confianza || 0.85);
      const tableConfidence = resolutionScore >= 0.85 ? 0.85 : 0;
      if (tableConfidence >= 0.7) {
        learned = (await upsertSemanticLearningEntry({ term, targetKind: 'table', targetName: baseTable, confidence: tableConfidence })) || learned;
      }

      const inferredColumn = queryBuilder.findBestAttributeColumn(term, baseSchema);
      if (inferredColumn) {
        learned = (await upsertSemanticLearningEntry({
          term,
          targetKind: 'column',
          targetName: inferredColumn,
          tableName: baseTable,
          confidence: 0.70,
        })) || learned;
      }
    }

    if (learned) {
      await refreshSemanticLearningCache(true);
      schemaCache.invalidate();
      bumpQueryCacheDataVersion();
    }
  }

  function buildNaturalLanguageSuggestions(rawText = '') {
    const input = String(rawText || '').trim().toLowerCase();
    const suggestions = [];

    for (const tableName of tablasPermitidas()) {
      if (!input || tableName.includes(input) || input.includes(tableName)) {
        const relations = relacionesDelEsquema().filter((r) => r.from.tabla === tableName || r.to.tabla === tableName);
        for (const rel of relations) {
          const other = rel.from.tabla === tableName ? rel.to.tabla : rel.from.tabla;
          suggestions.push(`${tableName} con ${other}`);
        }
      }
    }

    for (const term of LEARNED_SEMANTIC_CACHE.snapshot?.suggestionTerms || []) {
      const normalizedTerm = String(term || '').trim().toLowerCase();
      if (!normalizedTerm) continue;
      if (!input || normalizedTerm.includes(input) || input.includes(normalizedTerm)) {
        suggestions.push(normalizedTerm);

        const mappedTables = LEARNED_SEMANTIC_CACHE.snapshot?.tableAliases?.[normalizedTerm] || [];
        for (const tableName of mappedTables) {
          suggestions.push(`${normalizedTerm} recientes`);
          suggestions.push(`${tableName} con ${normalizedTerm}`);
        }
      }
    }

    return Array.from(new Set(suggestions)).slice(0, 10);
  }

  /**
   * Lo aprendido, para poder devolverlo si la recarga falla.
   *
   * reloadActiveDatabaseConnection tiene que poder volver atras: si la base nueva no
   * responde, se queda la anterior y con ella lo que el sistema habia aprendido de
   * ella. Antes eso se hacia tocando el cache desde fuera; ahora son dos funciones.
   */
  function guardarEstado() {
    return {
      snapshot: { ...LEARNED_SEMANTIC_CACHE.snapshot },
      fingerprint: LEARNED_SEMANTIC_CACHE.fingerprint,
    };
  }

  function restaurarEstado(estado) {
    LEARNED_SEMANTIC_CACHE.fingerprint = estado?.fingerprint || '';
    applyLearnedSemanticSnapshot(estado?.snapshot || {});
    LEARNED_SEMANTIC_CACHE.loadedAt = Date.now();
  }

  return {
    refreshSemanticLearningCache,
    learnSemanticMappingsFromSuccessfulQuery,
    buildNaturalLanguageSuggestions,
    applyLearnedSemanticSnapshot,
    resetLearnedSemanticCache,
    guardarEstado,
    restaurarEstado,
  };
}
