/**
 * QueryRoute – POST /api/query
 *
 * Endpoint inteligente que conecta con MultiDatabaseEngine para interpretar
 * consultas en lenguaje natural, ejecutarlas de forma single o distribuida
 * y devolver una respuesta normalizada.
 *
 * Seguridad:
 *   - Solo permite operaciones de lectura (SELECT)
 *   - Sanitiza el input antes de pasarlo al motor
 *   - Limita longitud de la consulta
 *   - Bloquea patrones DML / DDL peligrosos
 */

import express from 'express';
import {
  analyzeErrorInput,
  buildErrorAnalysisResponse,
  detectErrorAnalysisInput,
  enrichErrorAnalysisWithAi,
  searchErrorLogs,
} from '../infrastructure/query/ErrorAnalysisEngine.js';
import { askOllama } from '../infrastructure/ai/OllamaClient.js';
import { DatabaseRelevanceValidator } from '../infrastructure/query/DatabaseRelevanceValidator.js';
import { ResultExplainer } from '../infrastructure/query/ResultExplainer.js';
import {
  detectSqlSyntaxEngine,
  detectQueryType,
  detectQueryComplexity,
  extractDatabaseDirective,
  buildEngineLog,
} from '../infrastructure/query/SqlEngineDetector.js';

const OLLAMA_MODEL_LIGHT = process.env.OLLAMA_MODEL_LIGHT || process.env.OLLAMA_MODEL || '';

function pickOllamaModel(queryText = '') {
  const complexity = detectQueryComplexity(String(queryText || ''));
  const isSimple = complexity === 'simple';
  const model = isSimple
    ? (OLLAMA_MODEL_LIGHT || undefined)
    : (process.env.OLLAMA_MODEL || undefined);
  console.log(`[MODEL USED] ${model || 'default (OLLAMA_MODEL)'} | [COMPLEXITY] ${complexity}`);
  return model;
}

const MAX_QUERY_LENGTH = 500;
const MAX_QUERY_LIMIT = Number(process.env.MAX_QUERY_LIMIT) > 0
  ? Math.min(Number(process.env.MAX_QUERY_LIMIT), 10000)
  : 5000;

const _resultExplainer = new ResultExplainer();
const AI_NORMALIZATION_CONFIDENCE_THRESHOLD = 0.25;
const AI_NORMALIZATION_MIN_TOKENS = 1;
const AI_ADVANCED_INTENT_CONFIDENCE_THRESHOLD = 0.30;
const FORCE_DISTRIBUTED_MULTI_ENTITY = String(process.env.FORCE_DISTRIBUTED_MULTI_ENTITY || 'false').trim().toLowerCase() === 'true';

/**
 * Palabras clave DML/DDL que nunca deben llegar al motor.
 * El motor ya es semántico (lenguaje natural), por lo que
 * no hay motivo legítimo para enviar SQL crudo en este endpoint.
 */
const BLOCKED_SQL_KEYWORDS = /\b(insert|update|delete|drop|truncate|alter|create|grant|revoke|exec|execute|call|merge|replace|load|import|copy)\b/i;

/**
 * Patrones de inyección SQL: quotes, comentarios, semicolons
 * Detecta intentos comunes de inyección SQL
 */
const SQL_INJECTION_PATTERNS = /('|"|--|;|\*\/|\/\*)|(DROP|DELETE|INSERT|UPDATE)\s+(TABLE|DATABASE|SCHEMA)/i;
const SQL_ACTION_KEYWORDS = /\b(select|insert|update|delete|drop|truncate|alter|create|grant|revoke|exec|execute|call|merge|replace|with)\b/i;
const ENTITY_ALIASES = {
  usuario: 'users',
  usuarios: 'users',
  user: 'users',
  users: 'users',
  log: 'logs',
  logs: 'logs',
  sesion: 'sessions',
  sesiones: 'sessions',
  session: 'sessions',
  sessions: 'sessions',
};

function isSqlLikeText(text) {
  return SQL_ACTION_KEYWORDS.test(String(text || '').toLowerCase()) || String(text || '').includes(';');
}

function isSelectOnlySql(text) {
  const normalized = String(text || '').trim().toLowerCase();
  if (!normalized) return false;
  if (!/^(select|with)\b/.test(normalized)) return false;
  return !BLOCKED_SQL_KEYWORDS.test(normalized);
}

function sanitizeQueryText(raw) {
  const text = String(raw || '')
    .trim()
    .normalize('NFC')
    .replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/g, '') // strip control chars
    .slice(0, MAX_QUERY_LENGTH);

  // Validar injection patterns
  if (SQL_INJECTION_PATTERNS.test(text)) {
    throw new Error('Input contiene patrones potencialmente peligrosos (quotes, comments, SQL keywords)');
  }

  return text;
}

function inferEntityName(engineResult, rows = []) {
  const fromSource = Array.isArray(engineResult?.source) && engineResult.source.length > 0
    ? String(engineResult.source[0] || '').trim()
    : '';
  if (fromSource) return fromSource;

  const fromResolution = String(
    engineResult?.resolution?.entities?.[0]?.primary?.tableName
    || engineResult?.resolution?.entities?.[0]?.table
    || engineResult?.debug?.tablaSeleccionada
    || '',
  ).trim();
  if (fromResolution) return fromResolution;

  const firstRow = rows[0] && typeof rows[0] === 'object' ? rows[0] : null;
  const firstKey = firstRow ? Object.keys(firstRow)[0] : '';
  if (firstKey.includes('_')) {
    return firstKey.split('_')[0];
  }

  return 'resultado';
}

function normalizeTrace(trace = {}) {
  const interpretadoPor = String(trace?.interpretadoPor || '').trim() || 'deterministic';
  const intencion = String(trace?.intencion || '').trim() || 'unknown';
  const confianza = Number(trace?.confianza);

  return {
    interpretadoPor,
    intencion,
    confianza: Number.isFinite(confianza) ? confidenceClamp(confianza) : null,
  };
}

function confidenceClamp(value) {
  if (!Number.isFinite(Number(value))) return 0;
  return Math.max(0, Math.min(1, Number(value)));
}

function buildSuccessResponse(engineResult, trace = {}) {
  const rows = Array.isArray(engineResult.data) ? engineResult.data : [];
  const entidad = inferEntityName(engineResult, rows);

  // Generar resumen inteligente con ResultExplainer
  let resumenHumano = String(engineResult.message || '').trim();
  if (!resumenHumano || resumenHumano === `Se obtuvieron ${rows.length} ${rows.length === 1 ? 'registro' : 'registros'}.`) {
    try {
      const explanation = _resultExplainer.explain(rows);
      resumenHumano = String(explanation?.resumen || explanation?.explicacionCompleta || '').trim()
        || `Se obtuvieron ${rows.length} ${rows.length === 1 ? 'registro' : 'registros'} de ${entidad}.`;
    } catch {
      resumenHumano = `Se obtuvieron ${rows.length} ${rows.length === 1 ? 'registro' : 'registros'} de ${entidad}.`;
    }
  }

  return {
    resumenHumano,
    resultado: rows,
    metadata: {
      entidad,
      total: rows.length,
      executionType: String(engineResult.executionType || 'single-db'),
      sources: Array.isArray(engineResult.source) ? engineResult.source : [],
      confidence: confidenceClamp(engineResult.confidence ?? trace.confianza ?? null),
      trazabilidad: normalizeTrace(trace),
    },
  };
}

function buildAmbiguityResponse(engineResult, trace = {}) {
  return {
    resumenHumano: engineResult.message ?? 'La consulta es ambigua, necesita más contexto.',
    resultado: [],
    metadata: {
      entidad: 'resultado',
      total: 0,
      executionType: 'unresolved',
      sources: [],
      confidence: 0,
      suggestions: Array.isArray(engineResult.suggestions) ? engineResult.suggestions : [],
      trazabilidad: normalizeTrace(trace),
    },
  };
}

function buildEmptyResponse(queryText, fallbackMessage = '', trace = {}, engineResult = {}) {
  const summary = String(fallbackMessage || '').trim() || `Consulta ejecutada correctamente, pero no se encontraron resultados para: "${queryText}".`;
  return {
    resumenHumano: summary,
    resultado: [],
    metadata: {
      entidad: 'resultado',
      total: 0,
      executionType: String(engineResult.executionType || 'single-db'),
      sources: Array.isArray(engineResult.source) ? engineResult.source : [],
      confidence: confidenceClamp(engineResult.confidence ?? null),
      trazabilidad: normalizeTrace(trace),
    },
  };
}

function quoteIdentifier(identifier) {
  return `"${String(identifier || '').replace(/"/g, '""')}"`;
}

function normalizeEntityToken(token = '') {
  const clean = String(token || '').trim().toLowerCase();
  return ENTITY_ALIASES[clean] || clean;
}

function detectEntityInText(text = '') {
  const source = String(text || '').toLowerCase();
  for (const [alias, table] of Object.entries(ENTITY_ALIASES)) {
    const pattern = new RegExp(`\\b${alias}\\b`, 'i');
    if (pattern.test(source)) return table;
  }
  return null;
}

function detectRelatedEntityInText(text = '') {
  const source = String(text || '').toLowerCase();
  if (/\b(log|logs|bitacora|auditoria)\b/i.test(source)) return 'logs';
  if (/\b(sesion|sesiones|session|sessions)\b/i.test(source)) return 'sessions';
  return null;
}

function stripConversationalPrefix(text = '') {
  let source = String(text || '').trim();
  if (!source) return source;

  source = source.replace(/^(?:[a-zA-Z]{2,20}\s+)?(?:quiere|necesita|desea)\s+saber\s+/i, '');
  source = source.replace(/^(?:quiero|necesito|dime|me\s+puedes\s+decir|puedes\s+decirme|podrias\s+decirme|muestrame|mostrarme|quiero\s+ver|consulta(?:r)?(?:\s+sobre)?)\s+/i, '');
  source = source.replace(/^(?:dame|trae|ver|mu[eé]strame|muestra|lista(?:r)?|ens[eé][nñ]ame)\s+(?:todos?|todas?)\s+(?:los|las)?\s+/i, '');
  source = source.replace(/^(?:dame|trae|ver|mu[eé]strame|muestra|lista(?:r)?|ens[eé][nñ]ame)\s+/i, '');
  source = source.replace(/^(?:necesito|quiero)\s+saber\s+/i, '');
  source = source.replace(/^(?:por\s+favor\s+)?(?:sobre\s+)?/i, '').trim();
  source = source.replace(/\s+de\s+la\s+base\s+de\s+datos\b/gi, '').trim();
  source = source.replace(/\s+en\s+la\s+base\s+de\s+datos\b/gi, '').trim();
  return source;
}

function isReservedPseudoColumn(token = '') {
  const normalized = String(token || '').trim().toLowerCase();
  return ['limit', 'top', 'ultimos', 'ultimas', 'ultimo', 'ultima', 'recientes', 'reciente'].includes(normalized);
}

function detectDirectSqlIntent(queryText) {
  const source = String(queryText || '').trim();
  const normalized = source.toLowerCase();

  const groupCountMatch = source.match(/\b(?:conteo|cantidad|total)\b.*\b(usuarios|user|users|logs|log|sesion|sesiones|session|sessions)\b\s+por\s+(rol|role|activo|bloqueado|estado|status)\b/i);
  if (groupCountMatch) {
    return {
      matched: true,
      type: 'group-count',
      table: normalizeEntityToken(groupCountMatch[1]),
      groupByHint: String(groupCountMatch[2] || '').trim().toLowerCase(),
      timeWindow: parseTimeWindowFromText(source),
      limit: MAX_QUERY_LIMIT,
    };
  }

  const countRowsMatch = source.match(/\b(cuantos|cuantas|cu[aá]ntos|cu[aá]ntas|cantidad|total|conteo)\b.*\b(usuarios|user|users|logs|log|sesion|sesiones|session|sessions)\b/i);
  if (countRowsMatch && !/\bpor\b\s+(rol|role|activo|bloqueado|estado|status)\b/i.test(source)) {
    return {
      matched: true,
      type: 'count-rows',
      table: normalizeEntityToken(countRowsMatch[2]),
      timeWindow: parseTimeWindowFromText(source),
      limit: 1,
    };
  }

  const recentRowsMatch = source.match(/\b(usuarios|user|users|logs|log|sesion|sesiones|session|sessions)\b.*\b(?:ultim(?:as|os)?|last)\s+\d{1,4}\s*(?:hora|horas|hour|hours|dia|dias|d[ií]a|d[ií]as|day|days)\b/i);
  if (recentRowsMatch) {
    return {
      matched: true,
      type: 'recent-rows',
      table: normalizeEntityToken(recentRowsMatch[1]),
      timeWindow: parseTimeWindowFromText(source),
      limit: MAX_QUERY_LIMIT,
    };
  }

  const textFilterMatch = source.match(/\b(usuarios|user|users)\b[\s\S]*?\b(?:nombre|username|user_name|email|rol|role)\b[\s\S]*?\b(contenga|contengan|contiene|empiece|empiecen|comience|comiencen|termine|terminen)\b\s*(?:con\s+)?([a-zA-Z0-9_.@-]{1,80})\b/i)
    || source.match(/\b(usuarios|user|users)\b[\s\S]*?\b(contenga|contengan|contiene|empiece|empiecen|comience|comiencen|termine|terminen)\b\s*(?:con\s+)?(?:nombre|username|user_name|email|rol|role)?\s*([a-zA-Z0-9_.@-]{1,80})\b/i);
  if (textFilterMatch) {
    const comparatorSource = String(textFilterMatch[2] || '').toLowerCase();
    let matchMode = 'contains';
    if (/\b(empiece|empiecen|comience|comiencen)\b/.test(comparatorSource)) matchMode = 'starts-with';
    if (/\b(termine|terminen)\b/.test(comparatorSource)) matchMode = 'ends-with';

    return {
      matched: true,
      type: 'text-filter',
      table: normalizeEntityToken(textFilterMatch[1]),
      value: String(textFilterMatch[3] || '').trim(),
      columnHint: /\bemail\b/i.test(source)
        ? 'email'
        : (/\b(rol|role)\b/i.test(source) ? 'role' : 'username'),
      matchMode,
      limit: MAX_QUERY_LIMIT,
    };
  }

  const directUserAttributeMatch = source.match(/\b(usuarios|user|users)\b[\s\S]*?\bcon\s+(nombre|username|user_name|email|rol|role)\s+([a-zA-Z0-9_.@-]{2,80})\b/i);
  if (directUserAttributeMatch) {
    const attribute = String(directUserAttributeMatch[2] || '').trim().toLowerCase();
    const columnHint = /^(email)$/.test(attribute)
      ? 'email'
      : (/^(rol|role)$/.test(attribute) ? 'role' : 'username');

    return {
      matched: true,
      type: 'text-filter',
      table: normalizeEntityToken(directUserAttributeMatch[1]),
      value: String(directUserAttributeMatch[3] || '').trim(),
      columnHint,
      matchMode: 'contains',
      limit: MAX_QUERY_LIMIT,
    };
  }

  const listTablesMatch = source.match(/\b(tabla|tablas)\b.*\b(existen|existentes|hay|base\s+de\s+datos|bd|disponibles|encuentran|encuentra)\b/i)
    || source.match(/\b(dame|muestra|listar|mostrar|ver)\b[\s\S]*?\b(tabla|tablas)\b/i)
    || source.match(/^\s*(listar|mostrar|ver)\s+tablas\s*$/i)
    || source.match(/^\s*todas?\s+las\s+tablas\s*$/i)
    || source.match(/\btablas\b.*\bbase\s+de\s+datos\b/i)
    || source.match(/\b(?:dame|trae|mostrame)\s+.*\b(?:tabla|tablas)\b/i);
  if (listTablesMatch) {
    return {
      matched: true,
      type: 'list-tables',
      table: null,
      limit: MAX_QUERY_LIMIT,
    };
  }

  const listColumnsMatch = source.match(/\b(?:columna|columnas|campos)\b\s+(?:de|del|para)\s+([a-zA-Z_][a-zA-Z0-9_]*)\b/i);
  if (listColumnsMatch) {
    const table = normalizeEntityToken(listColumnsMatch[1]);
    return {
      matched: true,
      type: 'list-columns',
      table,
      limit: MAX_QUERY_LIMIT,
    };
  }

  const listEntityMatch = source.match(/\b(?:todos?|todas?)\b[\s\S]*?\b(usuarios|user|users|logs|log|sesion|sesiones|session|sessions)\b/i)
    || source.match(/\b(?:listar|lista|mostrar|muestra|ver|trae)\b[\s\S]*?\b(usuarios|user|users|logs|log|sesion|sesiones|session|sessions)\b/i);
  if (listEntityMatch) {
    const table = normalizeEntityToken(listEntityMatch[1]);
    return {
      matched: true,
      type: 'simple-limit',
      table,
      limit: MAX_QUERY_LIMIT,
    };
  }

  const startsWithUsersMatch = source.match(/\b(?:usuarios|user|users)\b.*\b(?:comiencen|comience|empiecen|empiece|inicien|inicie|empiezan|inician|que\s+empiecen\s+con|que\s+comiencen\s+con|que\s+inicien\s+con)\b.*\b(?:nombre|username|user_name)?\s*([a-zA-Z0-9_.@-]{1,80})\b/i);
  if (startsWithUsersMatch) {
    const value = String(startsWithUsersMatch[1] || '').trim();
    return {
      matched: true,
      type: 'starts-with-search',
      table: 'users',
      value,
      columnHint: 'username',
      limit: MAX_QUERY_LIMIT,
    };
  }

  const listIdsMatch = source.match(/\b(?:todos?|todas?)\b.*\b(?:id|ids|identificadores|uuid|uuids)\b.*\b(?:existan|existentes|que\s+existan|disponibles)?\b/i);
  if (listIdsMatch) {
    const tableFromContext = detectEntityInText(source) || 'users';
    return {
      matched: true,
      type: 'list-ids',
      table: tableFromContext,
      limit: MAX_QUERY_LIMIT,
    };
  }

  const userWithIdMatch = source.match(/\b(usuario|usuarios|user|users)\s+con\s+(?:el\s+)?(?:users?_)?(?:id|uuid)\s+([0-9a-fA-F-]{8,}|\d{1,20})\b/i);
  if (userWithIdMatch) {
    const table = normalizeEntityToken(userWithIdMatch[1]);
    const value = String(userWithIdMatch[2] || '').trim();
    return {
      matched: true,
      type: 'id-search',
      table,
      value,
      columnHint: `${table}_id`,
      limit: 1,
    };
  }

  const userIdentityByNameMatch = source.match(/(?:dame|mostrar|muestrame|trae|busca)?\s*(?:el\s+)?id\s+(?:del|de)?\s*(usuario|usuarios|user|users)\s+([a-zA-Z0-9_.@-]{2,80})\b/i);
  if (userIdentityByNameMatch) {
    const table = normalizeEntityToken(userIdentityByNameMatch[1]);
    const value = String(userIdentityByNameMatch[2] || '').trim();
    return {
      matched: true,
      type: 'identity-search',
      table,
      value,
      columnHint: 'username',
      limit: 1,
    };
  }

  const idSearchMatch = source.match(/(?:busca(?:r)?\s+(?:el|la)?\s*)?([a-zA-Z_][a-zA-Z0-9_]*)\s+([0-9a-fA-F-]{8,}|\d{1,20})/i);
  if (idSearchMatch) {
    const columnHint = String(idSearchMatch[1] || '').trim();
    if (!isReservedPseudoColumn(columnHint)) {
      const value = String(idSearchMatch[2] || '').trim();
      const token = columnHint.includes('_') ? columnHint.split('_')[0] : columnHint;
      const inferredFromContext = detectEntityInText(source);
      const normalizedToken = normalizeEntityToken(token);
      const table = ['id', 'uuid', 'codigo'].includes(normalizedToken)
        ? inferredFromContext
        : (normalizedToken || inferredFromContext);
      if (!table) return { matched: false };
      return {
        matched: true,
        type: 'id-search',
        table,
        value,
        columnHint,
        limit: 1,
      };
    }
  }

  const looseIdSearchMatch = source.match(/\b(?:id|uuid|user_id|usuario_id)\b\s+([0-9a-fA-F-]{8,}|\d{1,20})\b/i);
  if (looseIdSearchMatch) {
    const value = String(looseIdSearchMatch[1] || '').trim();
    const inferredTable = detectEntityInText(source) || 'users';
    return {
      matched: true,
      type: 'id-search',
      table: inferredTable,
      value,
      columnHint: 'id',
      limit: 1,
    };
  }

  const simpleWithLimit = normalized.match(/^([a-zA-Z_]+)\s+limit\s+(\d{1,4})$/i);
  if (simpleWithLimit) {
    const table = normalizeEntityToken(simpleWithLimit[1]);
    const limit = Math.min(Math.max(Number(simpleWithLimit[2]), 1), MAX_QUERY_LIMIT);
    return { matched: true, type: 'simple-limit', table, limit };
  }

  const simpleTable = normalized.match(/^([a-zA-Z_]+)$/i);
  if (simpleTable) {
    const table = normalizeEntityToken(simpleTable[1]);
    return { matched: true, type: 'simple-table', table, limit: MAX_QUERY_LIMIT };
  }

  return { matched: false };
}

function parsePositiveLimit(value, fallback = MAX_QUERY_LIMIT) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.min(Math.max(Math.floor(parsed), 1), MAX_QUERY_LIMIT);
}

function extractIdentifierLiteral(text = '') {
  const source = String(text || '').trim();
  if (!source) return null;

  const explicitId = source.match(/\b(?:id|uuid|user_id|usuario_id)\b\s+([0-9a-fA-F-]{8,}|\d{1,20})\b/i);
  if (explicitId && explicitId[1]) return String(explicitId[1]).trim();

  const rawUuidOrNumeric = source.match(/^\s*([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|\d{1,20})\s*$/i);
  if (rawUuidOrNumeric && rawUuidOrNumeric[1]) return String(rawUuidOrNumeric[1]).trim();

  return null;
}

function sanitizeIdentifierToken(value = '') {
  const clean = String(value || '').trim().toLowerCase();
  if (!clean) return '';
  const onlySafe = clean.replace(/[^a-z0-9_]/g, '');
  return onlySafe;
}

function shouldAttemptAiNormalization(queryText) {
  const source = String(queryText || '').trim();
  if (!source) return false;
  if (isSqlLikeText(source)) return false;

  const tokens = source.split(/\s+/).filter(Boolean);
  if (tokens.length < AI_NORMALIZATION_MIN_TOKENS) return false;

  return !/^[a-zA-Z_]+\s+limit\s+\d{1,4}$/i.test(source);
}

function parsePositiveHours(value, fallback = 24) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.min(Math.max(Math.floor(parsed), 1), 24 * 30);
}

function sanitizeSortDirection(value = '') {
  const normalized = String(value || '').trim().toLowerCase();
  return normalized === 'asc' ? 'ASC' : 'DESC';
}

function parseHoursFromQueryText(text = '') {
  const source = String(text || '');
  const match = source.match(/\b(?:ultim(?:as|os)?|ultimas?|ultimos?|last)\s+(\d{1,4})\s*(hora|horas|hour|hours)\b/i);
  if (!match) return null;
  return parsePositiveHours(match[1], 24);
}

function parseTimeWindowFromText(text = '') {
  const source = String(text || '').toLowerCase();

  const hourMatch = source.match(/\b(?:ultim(?:as|os)?|last)\s+(\d{1,4})\s*(hora|horas|hour|hours)\b/i);
  if (hourMatch) {
    return {
      value: parsePositiveHours(hourMatch[1], 24),
      unit: 'hour',
    };
  }

  const dayMatch = source.match(/\b(?:ultim(?:os|as)?|last)\s+(\d{1,4})\s*(dia|dias|d[ií]a|d[ií]as|day|days)\b/i);
  if (dayMatch) {
    return {
      value: parsePositiveLimit(dayMatch[1], 7),
      unit: 'day',
    };
  }

  return null;
}

function detectRankingIntentFromText(queryText, defaultLimit = 10) {
  const raw = String(queryText || '').trim();
  if (!raw) return { matched: false };

  const source = raw
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9_\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  const mentionsUsers = /\b(?:usuario|usuarios|user|users)\b/.test(source);
  if (!mentionsUsers) return { matched: false };

  const related = detectRelatedEntityInText(source) || 'logs';
  const mentionsRanking = /\b(?:mas|top|ranking|frecuente|frecuentes|usan|uso|registrados)\b/.test(source);
  const hasRelationalCue = Boolean(related)
    && /\bcon\b/.test(source)
    && (/\b(?:mas|activo|activos|inactivo|inactivos)\b/.test(source));

  if (!mentionsRanking && !hasRelationalCue) return { matched: false };

  const topMatch = source.match(/\btop\s+(\d{1,4})\b/i);
  const limit = parsePositiveLimit(topMatch?.[1], Math.min(defaultLimit, 10));
  const hours = parsePositiveHours(parseHoursFromQueryText(source), 24);

  return {
    matched: true,
    type: 'ranked-activity-users',
    table: 'users',
    relatedTable: related,
    timeWindowHours: hours,
    limit,
    sortDirection: 'DESC',
  };
}

function resolveTableAliasToken(value = '') {
  const normalized = normalizeEntityToken(sanitizeIdentifierToken(value));
  if (!normalized) return '';
  return normalized;
}

function buildDirectIntentFromAi(aiJson, defaultLimit = MAX_QUERY_LIMIT) {
  if (!aiJson || typeof aiJson !== 'object') return null;

  const confidence = Number(aiJson.confianza ?? aiJson.confidence ?? 0);
  if (!Number.isFinite(confidence) || confidence < AI_NORMALIZATION_CONFIDENCE_THRESHOLD) return null;

  const rawTable = sanitizeIdentifierToken(aiJson.table || aiJson.entidad || aiJson.entity || '');
  const table = normalizeEntityToken(rawTable);
  if (!table) return null;

  const intent = String(aiJson.intent || aiJson.action || '').trim().toLowerCase();
  const rawValue = String(aiJson.value ?? aiJson.id ?? '').trim();
  const rawColumnHint = sanitizeIdentifierToken(aiJson.column || aiJson.columna || '');
  const limit = parsePositiveLimit(aiJson.limit, defaultLimit);

  const looksLikeIdSearch = intent.includes('id')
    || intent.includes('lookup')
    || intent.includes('search')
    || /^(\d{1,20}|[0-9a-fA-F-]{8,})$/.test(rawValue);

  if (looksLikeIdSearch && rawValue) {
    return {
      matched: true,
      type: 'id-search',
      table,
      value: rawValue,
      columnHint: rawColumnHint || `${table}_id`,
      limit: 1,
    };
  }

  return {
    matched: true,
    type: 'simple-limit',
    table,
    limit,
  };
}

function buildAdvancedIntentFromAi(aiJson, originalQueryText = '', defaultLimit = MAX_QUERY_LIMIT) {
  if (!aiJson || typeof aiJson !== 'object') return null;

  const confidence = Number(aiJson.confianza ?? aiJson.confidence ?? 0);
  if (!Number.isFinite(confidence) || confidence < AI_ADVANCED_INTENT_CONFIDENCE_THRESHOLD) return null;

  const operation = String(aiJson.operation || aiJson.operacion || aiJson.intent || '').trim().toLowerCase();
  if (!operation) return null;

  if (['list-tables', 'schema-list-tables', 'tables', 'metadata-tables'].includes(operation)) {
    return {
      matched: true,
      type: 'list-tables',
      table: null,
      limit: parsePositiveLimit(aiJson.limit, defaultLimit),
    };
  }

  if (['list-columns', 'schema-list-columns', 'columns', 'metadata-columns'].includes(operation)) {
    const targetTable = resolveTableAliasToken(aiJson.table || aiJson.targetEntity || aiJson.target || aiJson.entity || '');
    if (!targetTable) return null;
    return {
      matched: true,
      type: 'list-columns',
      table: targetTable,
      limit: parsePositiveLimit(aiJson.limit, defaultLimit),
    };
  }

  if (['count-rows', 'count', 'count-entity', 'aggregate-count'].includes(operation)) {
    const targetTable = resolveTableAliasToken(aiJson.table || aiJson.targetEntity || aiJson.target || aiJson.entity || detectEntityInText(originalQueryText) || '');
    if (!targetTable) return null;
    return {
      matched: true,
      type: 'count-rows',
      table: targetTable,
      timeWindow: parseTimeWindowFromText(originalQueryText),
      limit: 1,
    };
  }

  if (['group-count', 'group-by-count', 'aggregate-group'].includes(operation)) {
    const targetTable = resolveTableAliasToken(aiJson.table || aiJson.targetEntity || aiJson.target || aiJson.entity || detectEntityInText(originalQueryText) || '');
    if (!targetTable) return null;
    return {
      matched: true,
      type: 'group-count',
      table: targetTable,
      groupByHint: String(aiJson.groupBy || aiJson.column || aiJson.columna || '').trim().toLowerCase() || 'role',
      timeWindow: parseTimeWindowFromText(originalQueryText),
      limit: parsePositiveLimit(aiJson.limit, defaultLimit),
    };
  }

  if (['recent-rows', 'list-recent', 'recent'].includes(operation)) {
    const targetTable = resolveTableAliasToken(aiJson.table || aiJson.targetEntity || aiJson.target || aiJson.entity || detectEntityInText(originalQueryText) || '');
    if (!targetTable) return null;
    return {
      matched: true,
      type: 'recent-rows',
      table: targetTable,
      timeWindow: parseTimeWindowFromText(originalQueryText),
      limit: parsePositiveLimit(aiJson.limit, defaultLimit),
    };
  }

  if (['text-filter', 'contains', 'starts-with', 'ends-with', 'filter-text'].includes(operation)) {
    const targetTable = resolveTableAliasToken(aiJson.table || aiJson.targetEntity || aiJson.target || aiJson.entity || detectEntityInText(originalQueryText) || '');
    if (!targetTable) return null;
    const matchModeRaw = String(aiJson.matchMode || operation || 'contains').toLowerCase();
    const matchMode = matchModeRaw.includes('start')
      ? 'starts-with'
      : (matchModeRaw.includes('end') ? 'ends-with' : 'contains');

    return {
      matched: true,
      type: 'text-filter',
      table: targetTable,
      columnHint: String(aiJson.column || aiJson.columna || '').trim() || 'username',
      value: String(aiJson.value || '').trim(),
      matchMode,
      limit: parsePositiveLimit(aiJson.limit, defaultLimit),
    };
  }

  const normalizedQuery = String(originalQueryText || '').toLowerCase();
  const mentionsRanking = /\b(mas|más|top|ranking|frecuente|frecuentes|usan|uso)\b/.test(normalizedQuery);

  if (!['ranked-activity-users', 'user-activity-ranking', 'ranking-users', 'ranking', 'aggregate'].includes(operation) && !mentionsRanking) {
    return null;
  }

  const targetEntity = resolveTableAliasToken(aiJson.targetEntity || aiJson.target || aiJson.entity || aiJson.table || 'users');
  if (targetEntity !== 'users') return null;

  const relatedEntity = resolveTableAliasToken(
    aiJson.relatedEntity
    || aiJson.related
    || aiJson.metricSource
    || detectRelatedEntityInText(originalQueryText)
    || 'logs'
  );

  if (!['logs', 'sessions'].includes(relatedEntity)) return null;

  const top = parsePositiveLimit(aiJson.top ?? aiJson.limit, defaultLimit);
  const hours = parsePositiveHours(aiJson.timeWindowHours ?? aiJson.hours ?? parseHoursFromQueryText(originalQueryText), 24);
  const sortDirection = sanitizeSortDirection(aiJson.sortDirection || aiJson.sort || 'desc');

  return {
    matched: true,
    type: 'ranked-activity-users',
    table: 'users',
    relatedTable: relatedEntity,
    timeWindowHours: hours,
    limit: top,
    sortDirection,
  };
}

async function resolveTableColumns(pool, tableName) {
  if (!pool || typeof pool.query !== 'function') return [];

  const result = await pool.query(
    `SELECT column_name
     FROM information_schema.columns
     WHERE table_schema = 'public'
       AND table_name = $1
     ORDER BY ordinal_position`,
    [tableName],
  );

  return (result.rows || []).map((row) => String(row.column_name || '').trim()).filter(Boolean);
}

function pickColumnByPriority(columns = [], priority = []) {
  const lowerMap = new Map(columns.map((column) => [String(column || '').toLowerCase(), column]));
  for (const candidate of priority) {
    const hit = lowerMap.get(String(candidate || '').toLowerCase());
    if (hit) return hit;
  }
  return null;
}

function guessUserForeignKey(columns = [], userIdColumn = '') {
  const lowerColumns = columns.map((column) => String(column || '').toLowerCase());
  const directCandidates = ['user_id', 'users_id', 'usuario_id', 'id_usuario', 'owner_user_id'];
  for (const candidate of directCandidates) {
    const idx = lowerColumns.indexOf(candidate);
    if (idx >= 0) return columns[idx];
  }

  const normalizedUserId = String(userIdColumn || '').toLowerCase();
  if (normalizedUserId) {
    const exactIdx = lowerColumns.indexOf(normalizedUserId);
    if (exactIdx >= 0) return columns[exactIdx];
  }

  const fuzzy = columns.find((column) => /(^user(_id)?$|users?_id$|usuario(_id)?$|id_usuario$)/i.test(column));
  return fuzzy || null;
}

async function tryExecuteAdvancedIntent(pool, directIntent) {
  if (!pool || typeof pool.query !== 'function') return null;
  if (!directIntent?.matched || directIntent?.type !== 'ranked-activity-users') return null;

  const usersTable = 'users';
  const relatedTable = String(directIntent.relatedTable || '').trim().toLowerCase();
  if (!relatedTable) return null;

  const tableExists = async (tableName) => {
    const result = await pool.query(
      `SELECT 1
       FROM information_schema.tables
       WHERE table_schema = 'public'
         AND table_name = $1
       LIMIT 1`,
      [tableName],
    );
    return Array.isArray(result.rows) && result.rows.length > 0;
  };

  const usersExists = await tableExists(usersTable);
  const relatedExists = await tableExists(relatedTable);
  if (!usersExists || !relatedExists) return null;

  const userColumns = await resolveTableColumns(pool, usersTable);
  const relatedColumns = await resolveTableColumns(pool, relatedTable);
  if (!userColumns.length || !relatedColumns.length) return null;

  const userIdColumn = pickColumnByPriority(userColumns, ['id', 'user_id', 'users_id']) || userColumns[0];
  const userLabelColumn = pickColumnByPriority(userColumns, ['username', 'user_name', 'nombre', 'name', 'email', userIdColumn]) || userColumns[0];
  const relatedUserColumn = guessUserForeignKey(relatedColumns, userIdColumn);
  if (!relatedUserColumn) return null;

  const relatedTimeColumn = pickColumnByPriority(relatedColumns, [
    'created_at',
    'timestamp',
    'logged_at',
    'occurred_at',
    'fecha',
    'fecha_hora',
    'createdon',
  ]);

  const safeLimit = parsePositiveLimit(directIntent.limit, 10);
  const timeWindowHours = parsePositiveHours(directIntent.timeWindowHours, 24);
  const sortDirection = sanitizeSortDirection(directIntent.sortDirection || 'DESC');

  const whereClause = relatedTimeColumn
    ? `WHERE r.${quoteIdentifier(relatedTimeColumn)} >= NOW() - ($1::int * INTERVAL '1 hour')`
    : '';

  const queryText = [
    `SELECT u.${quoteIdentifier(userLabelColumn)}::text AS usuario, COUNT(*)::int AS total`,
    `FROM ${quoteIdentifier(relatedTable)} r`,
    `JOIN ${quoteIdentifier(usersTable)} u`,
    `  ON u.${quoteIdentifier(userIdColumn)}::text = r.${quoteIdentifier(relatedUserColumn)}::text`,
    whereClause,
    `GROUP BY u.${quoteIdentifier(userLabelColumn)}`,
    `ORDER BY total ${sortDirection}`,
    `LIMIT ${safeLimit}`,
  ].filter(Boolean).join('\n');

  const params = relatedTimeColumn ? [timeWindowHours] : [];
  let queryResult = await pool.query(queryText, params);
  let rows = queryResult.rows || [];
  let summary = relatedTimeColumn
    ? `Top ${safeLimit} usuarios por actividad en ${relatedTable} durante las ultimas ${timeWindowHours} horas.`
    : `Top ${safeLimit} usuarios por actividad historica en ${relatedTable}.`;

  // Si no hay actividad en la ventana reciente, reintenta en histórico para
  // evitar respuestas vacías en consultas de ranking natural.
  if (relatedTimeColumn && rows.length === 0) {
    const historicalQueryText = [
      `SELECT u.${quoteIdentifier(userLabelColumn)}::text AS usuario, COUNT(*)::int AS total`,
      `FROM ${quoteIdentifier(relatedTable)} r`,
      `JOIN ${quoteIdentifier(usersTable)} u`,
      `  ON u.${quoteIdentifier(userIdColumn)}::text = r.${quoteIdentifier(relatedUserColumn)}::text`,
      `GROUP BY u.${quoteIdentifier(userLabelColumn)}`,
      `ORDER BY total ${sortDirection}`,
      `LIMIT ${safeLimit}`,
    ].join('\n');

    const historicalResult = await pool.query(historicalQueryText);
    if (Array.isArray(historicalResult.rows) && historicalResult.rows.length > 0) {
      queryResult = historicalResult;
      rows = historicalResult.rows;
      summary = `No hubo actividad en las ultimas ${timeWindowHours} horas; mostrando top ${safeLimit} historico en ${relatedTable}.`;
    }
  }

  return {
    success: true,
    executionType: 'single',
    data: rows,
    rowCount: Number(queryResult.rowCount || rows.length || 0),
    message: summary,
    source: [usersTable, relatedTable],
  };
}

async function resolveIdColumn(pool, tableName, columnHint = '') {
  const columnsResult = await pool.query(
    `SELECT column_name
     FROM information_schema.columns
     WHERE table_schema = 'public'
       AND table_name = $1`,
    [tableName],
  );

  const columns = (columnsResult.rows || []).map((row) => String(row.column_name || '').trim()).filter(Boolean);
  if (columns.length === 0) return null;

  const normalizedHint = String(columnHint || '').trim().toLowerCase();
  if (normalizedHint) {
    const direct = columns.find((column) => column.toLowerCase() === normalizedHint);
    if (direct) return direct;
  }

  const preferred = ['id', `${tableName}_id`];
  for (const candidate of preferred) {
    const found = columns.find((column) => column.toLowerCase() === candidate.toLowerCase());
    if (found) return found;
  }

  const suffixId = columns.find((column) => /(^id$|_id$)/i.test(column));
  return suffixId || columns[0];
}

async function resolveIdentityColumn(pool, tableName, columnHint = '') {
  const columnsResult = await pool.query(
    `SELECT column_name
     FROM information_schema.columns
     WHERE table_schema = 'public'
       AND table_name = $1`,
    [tableName],
  );

  const columns = (columnsResult.rows || []).map((row) => String(row.column_name || '').trim()).filter(Boolean);
  if (columns.length === 0) return null;

  const normalizedHint = String(columnHint || '').trim().toLowerCase();
  if (normalizedHint) {
    const direct = columns.find((column) => column.toLowerCase() === normalizedHint);
    if (direct) return direct;
  }

  const preferred = ['username', 'user_name', 'nombre', 'name', 'email', 'id'];
  for (const candidate of preferred) {
    const found = columns.find((column) => column.toLowerCase() === candidate);
    if (found) return found;
  }

  return columns[0];
}

async function resolveBestIdColumn(pool, tableName) {
  const columnsResult = await pool.query(
    `SELECT column_name
     FROM information_schema.columns
     WHERE table_schema = 'public'
       AND table_name = $1`,
    [tableName],
  );

  const columns = (columnsResult.rows || []).map((row) => String(row.column_name || '').trim()).filter(Boolean);
  if (!columns.length) return null;

  const preferred = ['id', `${tableName}_id`, 'user_id', 'users_id', 'uuid'];
  for (const candidate of preferred) {
    const hit = columns.find((column) => column.toLowerCase() === candidate.toLowerCase());
    if (hit) return hit;
  }

  const suffix = columns.find((column) => /(^id$|_id$|uuid$)/i.test(column));
  return suffix || columns[0];
}

async function resolveDateColumn(pool, tableName) {
  const columns = await resolveTableColumns(pool, tableName);
  if (!columns.length) return null;

  const preferred = [
    'created_at',
    'timestamp',
    'logged_at',
    'occurred_at',
    'updated_at',
    'fecha',
    'fecha_hora',
    'createdon',
  ];

  return pickColumnByPriority(columns, preferred);
}

async function resolveGroupByColumn(pool, tableName, groupByHint = '') {
  const columns = await resolveTableColumns(pool, tableName);
  if (!columns.length) return null;

  const hint = String(groupByHint || '').trim().toLowerCase();
  const mappedHint = hint === 'rol' ? 'role' : (hint === 'estado' ? 'status' : hint);

  const direct = columns.find((column) => column.toLowerCase() === mappedHint);
  if (direct) return direct;

  const aliasMap = {
    role: ['role', 'rol', 'tipo', 'user_role'],
    status: ['status', 'estado', 'activo', 'bloqueado'],
    activo: ['activo', 'is_active', 'enabled', 'status'],
    bloqueado: ['bloqueado', 'blocked', 'is_blocked', 'status'],
  };

  const candidates = aliasMap[mappedHint] || [mappedHint];
  for (const candidate of candidates) {
    const hit = columns.find((column) => column.toLowerCase() === candidate.toLowerCase());
    if (hit) return hit;
  }

  return null;
}

async function tryExecuteDeterministicDirectSql(pool, directIntent) {
  if (!pool || typeof pool.query !== 'function') return null;
  if (!directIntent?.matched) return null;

  if (directIntent.type === 'list-tables') {
    const safeLimit = Math.min(Math.max(Number(directIntent.limit || MAX_QUERY_LIMIT), 1), MAX_QUERY_LIMIT);
    const queryResult = await pool.query(
      `SELECT table_name AS tabla
       FROM information_schema.tables
       WHERE table_schema = 'public'
       ORDER BY table_name
       LIMIT ${safeLimit}`,
    );

    return {
      success: true,
      executionType: 'single',
      data: queryResult.rows || [],
      rowCount: Number(queryResult.rowCount || 0),
      message: `Se encontraron ${Number(queryResult.rowCount || 0)} tablas en la base de datos.`,
      source: ['information_schema.tables'],
    };
  }

  if (directIntent.type === 'list-columns') {
    if (!directIntent?.table) return null;

    const safeLimit = Math.min(Math.max(Number(directIntent.limit || MAX_QUERY_LIMIT), 1), MAX_QUERY_LIMIT);
    const queryResult = await pool.query(
      `SELECT column_name AS columna
       FROM information_schema.columns
       WHERE table_schema = 'public'
         AND table_name = $1
       ORDER BY ordinal_position
       LIMIT ${safeLimit}`,
      [directIntent.table],
    );

    return {
      success: true,
      executionType: 'single',
      data: queryResult.rows || [],
      rowCount: Number(queryResult.rowCount || 0),
      message: queryResult.rowCount > 0
        ? `Se encontraron ${queryResult.rowCount} columnas en ${directIntent.table}.`
        : `No se encontraron columnas para ${directIntent.table}.`,
      source: ['information_schema.columns'],
    };
  }

  if (!directIntent?.table) return null;

  if (directIntent.type === 'count-rows') {
    const dateColumn = await resolveDateColumn(pool, directIntent.table);
    const timeWindow = directIntent.timeWindow && Number(directIntent.timeWindow.value) > 0
      ? directIntent.timeWindow
      : null;

    const whereClause = (dateColumn && timeWindow)
      ? `WHERE ${quoteIdentifier(dateColumn)} >= NOW() - ($1::int * INTERVAL '1 ${timeWindow.unit}')`
      : '';

    const queryText = [
      'SELECT COUNT(*)::int AS total',
      `FROM ${quoteIdentifier(directIntent.table)}`,
      whereClause,
    ].filter(Boolean).join('\n');

    const params = (dateColumn && timeWindow) ? [Number(timeWindow.value)] : [];
    const queryResult = await pool.query(queryText, params);
    const total = Number(queryResult.rows?.[0]?.total || 0);

    return {
      success: true,
      executionType: 'single',
      data: [{ total }],
      rowCount: 1,
      message: timeWindow
        ? `Se contó ${total} registros en ${directIntent.table} durante las ultimas ${timeWindow.value} ${timeWindow.unit === 'day' ? 'dias' : 'horas'}.`
        : `Se contó ${total} registros en ${directIntent.table}.`,
      source: [directIntent.table],
    };
  }

  if (directIntent.type === 'group-count') {
    const groupColumn = await resolveGroupByColumn(pool, directIntent.table, directIntent.groupByHint);
    if (!groupColumn) return null;

    const dateColumn = await resolveDateColumn(pool, directIntent.table);
    const timeWindow = directIntent.timeWindow && Number(directIntent.timeWindow.value) > 0
      ? directIntent.timeWindow
      : null;

    const whereClause = (dateColumn && timeWindow)
      ? `WHERE ${quoteIdentifier(dateColumn)} >= NOW() - ($1::int * INTERVAL '1 ${timeWindow.unit}')`
      : '';

    const safeLimit = Math.min(Math.max(Number(directIntent.limit || MAX_QUERY_LIMIT), 1), MAX_QUERY_LIMIT);
    const queryText = [
      `SELECT ${quoteIdentifier(groupColumn)}::text AS grupo, COUNT(*)::int AS total`,
      `FROM ${quoteIdentifier(directIntent.table)}`,
      whereClause,
      `GROUP BY ${quoteIdentifier(groupColumn)}`,
      'ORDER BY total DESC',
      `LIMIT ${safeLimit}`,
    ].filter(Boolean).join('\n');

    const params = (dateColumn && timeWindow) ? [Number(timeWindow.value)] : [];
    const queryResult = await pool.query(queryText, params);

    return {
      success: true,
      executionType: 'single',
      data: queryResult.rows || [],
      rowCount: Number(queryResult.rowCount || 0),
      message: `Conteo agrupado por ${groupColumn} en ${directIntent.table}.`,
      source: [directIntent.table],
    };
  }

  if (directIntent.type === 'recent-rows') {
    const dateColumn = await resolveDateColumn(pool, directIntent.table);
    if (!dateColumn) return null;

    const timeWindow = directIntent.timeWindow && Number(directIntent.timeWindow.value) > 0
      ? directIntent.timeWindow
      : { value: 24, unit: 'hour' };

    const safeLimit = Math.min(Math.max(Number(directIntent.limit || MAX_QUERY_LIMIT), 1), MAX_QUERY_LIMIT);
    const queryResult = await pool.query(
      `SELECT *
       FROM ${quoteIdentifier(directIntent.table)}
       WHERE ${quoteIdentifier(dateColumn)} >= NOW() - ($1::int * INTERVAL '1 ${timeWindow.unit}')
       ORDER BY ${quoteIdentifier(dateColumn)} DESC
       LIMIT ${safeLimit}`,
      [Number(timeWindow.value)],
    );

    return {
      success: true,
      executionType: 'single',
      data: queryResult.rows || [],
      rowCount: Number(queryResult.rowCount || 0),
      message: `Se obtuvieron ${Number(queryResult.rowCount || 0)} registros recientes de ${directIntent.table} (${timeWindow.value} ${timeWindow.unit === 'day' ? 'dias' : 'horas'}).`,
      source: [directIntent.table],
    };
  }

  if (directIntent.type === 'text-filter') {
    const identityColumn = await resolveIdentityColumn(pool, directIntent.table, directIntent.columnHint);
    if (!identityColumn) return null;

    const safeLimit = Math.min(Math.max(Number(directIntent.limit || MAX_QUERY_LIMIT), 1), MAX_QUERY_LIMIT);
    const filterValue = String(directIntent.value || '').trim();
    if (!filterValue) return null;

    const matchMode = String(directIntent.matchMode || 'contains');
    const valuePattern = matchMode === 'starts-with'
      ? `${filterValue}%`
      : (matchMode === 'ends-with' ? `%${filterValue}` : `%${filterValue}%`);

    const queryResult = await pool.query(
      `SELECT *
       FROM ${quoteIdentifier(directIntent.table)}
       WHERE ${quoteIdentifier(identityColumn)}::text ILIKE $1
       ORDER BY ${quoteIdentifier(identityColumn)}::text ASC
       LIMIT ${safeLimit}`,
      [valuePattern],
    );

    const matchLabel = matchMode === 'starts-with'
      ? 'comienza con'
      : (matchMode === 'ends-with' ? 'termina con' : 'contiene');

    return {
      success: true,
      executionType: 'single',
      data: queryResult.rows || [],
      rowCount: Number(queryResult.rowCount || 0),
      message: queryResult.rowCount > 0
        ? `Se encontraron ${queryResult.rowCount} registros en ${directIntent.table} cuyo ${identityColumn} ${matchLabel} "${filterValue}".`
        : `No se encontraron registros en ${directIntent.table} cuyo ${identityColumn} ${matchLabel} "${filterValue}".`,
      source: [directIntent.table],
    };
  }

  const tableExistsResult = await pool.query(
    `SELECT 1
     FROM information_schema.tables
     WHERE table_schema = 'public'
       AND table_name = $1
     LIMIT 1`,
    [directIntent.table],
  );

  if (!Array.isArray(tableExistsResult.rows) || tableExistsResult.rows.length === 0) {
    return null;
  }

  if (directIntent.type === 'id-search') {
    const idColumn = await resolveIdColumn(pool, directIntent.table, directIntent.columnHint);
    if (!idColumn) return null;

    const queryResult = await pool.query(
      `SELECT *
       FROM ${quoteIdentifier(directIntent.table)}
       WHERE ${quoteIdentifier(idColumn)}::text = $1
       LIMIT 1`,
      [String(directIntent.value || '')],
    );

    return {
      success: true,
      executionType: 'single',
      data: queryResult.rows || [],
      rowCount: Number(queryResult.rowCount || 0),
      message: queryResult.rowCount > 0
        ? `Se encontró ${queryResult.rowCount} resultado por ID en ${directIntent.table}.`
        : `No se encontraron resultados por ID en ${directIntent.table}.`,
      source: [directIntent.table],
    };
  }

  if (directIntent.type === 'identity-search') {
    const identityColumn = await resolveIdentityColumn(pool, directIntent.table, directIntent.columnHint);
    if (!identityColumn) return null;

    const queryResult = await pool.query(
      `SELECT *
       FROM ${quoteIdentifier(directIntent.table)}
       WHERE ${quoteIdentifier(identityColumn)}::text ILIKE $1
       LIMIT 1`,
      [`%${String(directIntent.value || '').trim()}%`],
    );

    return {
      success: true,
      executionType: 'single',
      data: queryResult.rows || [],
      rowCount: Number(queryResult.rowCount || 0),
      message: queryResult.rowCount > 0
        ? `Se encontró ${queryResult.rowCount} resultado para ${directIntent.value} en ${directIntent.table}.`
        : `No se encontraron resultados para ${directIntent.value} en ${directIntent.table}.`,
      source: [directIntent.table],
    };
  }

  if (directIntent.type === 'starts-with-search') {
    const identityColumn = await resolveIdentityColumn(pool, directIntent.table, directIntent.columnHint);
    if (!identityColumn) return null;

    const safeLimit = Math.min(Math.max(Number(directIntent.limit || MAX_QUERY_LIMIT), 1), MAX_QUERY_LIMIT);
    const startsWithValue = String(directIntent.value || '').trim();
    if (!startsWithValue) return null;

    const queryResult = await pool.query(
      `SELECT *
       FROM ${quoteIdentifier(directIntent.table)}
       WHERE ${quoteIdentifier(identityColumn)}::text ILIKE $1
       ORDER BY ${quoteIdentifier(identityColumn)}::text ASC
       LIMIT ${safeLimit}`,
      [`${startsWithValue}%`],
    );

    return {
      success: true,
      executionType: 'single',
      data: queryResult.rows || [],
      rowCount: Number(queryResult.rowCount || 0),
      message: queryResult.rowCount > 0
        ? `Se encontraron ${queryResult.rowCount} usuarios cuyo ${identityColumn} comienza con "${startsWithValue}".`
        : `No se encontraron usuarios cuyo ${identityColumn} comienza con "${startsWithValue}".`,
      source: [directIntent.table],
    };
  }

  if (directIntent.type === 'list-ids') {
    const idColumn = await resolveBestIdColumn(pool, directIntent.table);
    if (!idColumn) return null;

    const safeLimit = Math.min(Math.max(Number(directIntent.limit || MAX_QUERY_LIMIT), 1), MAX_QUERY_LIMIT);
    const queryResult = await pool.query(
      `SELECT ${quoteIdentifier(idColumn)} AS id
       FROM ${quoteIdentifier(directIntent.table)}
       WHERE ${quoteIdentifier(idColumn)} IS NOT NULL
       LIMIT ${safeLimit}`,
    );

    return {
      success: true,
      executionType: 'single',
      data: queryResult.rows || [],
      rowCount: Number(queryResult.rowCount || 0),
      message: `Se obtuvieron ${Number(queryResult.rowCount || 0)} IDs de ${directIntent.table}.`,
      source: [directIntent.table],
    };
  }

  const queryResult = await pool.query(
    `SELECT *
     FROM ${quoteIdentifier(directIntent.table)}
     LIMIT ${Math.min(Math.max(Number(directIntent.limit || MAX_QUERY_LIMIT), 1), MAX_QUERY_LIMIT)}`,
  );

  return {
    success: true,
    executionType: 'single',
    data: queryResult.rows || [],
    rowCount: Number(queryResult.rowCount || 0),
    message: `Se obtuvieron ${Number(queryResult.rowCount || 0)} registros de ${directIntent.table}.`,
    source: [directIntent.table],
  };
}

function preprocessNaturalQuery(rawText, maxLimit = MAX_QUERY_LIMIT) {
  const source = String(rawText || '').trim();
  if (!source) {
    return {
      queryText: source,
      limit: maxLimit,
      intent: 'none',
    };
  }

  let queryText = source;
  let limit = maxLimit;
  let intent = 'natural';

  const limitMatch = source.match(/\blimit\s+(\d{1,4})\b/i);
  if (limitMatch) {
    const parsed = Number(limitMatch[1]);
    if (Number.isFinite(parsed) && parsed > 0) {
      limit = Math.min(parsed, maxLimit);
    }
    queryText = queryText.replace(limitMatch[0], ' ').replace(/\s+/g, ' ').trim();
    intent = 'limit';
  }

  const idSearchMatch = source.match(/(?:busca(?:r)?\s+(?:el|la)?\s*)?([a-zA-Z_][a-zA-Z0-9_]*)\s+([0-9a-fA-F-]{8,}|\d{1,20})/i);
  if (idSearchMatch) {
    const column = String(idSearchMatch[1] || '').trim().toLowerCase();
    if (isReservedPseudoColumn(column)) {
      return { queryText, limit, intent };
    }
    const value = String(idSearchMatch[2] || '').trim();
    const userWithIdMatch = source.match(/\b(usuario|usuarios|user|users)\s+con\s+(?:el\s+)?(?:users?_)?(?:id|uuid)\s+([0-9a-fA-F-]{8,}|\d{1,20})\b/i);
    const entityFromPattern = userWithIdMatch ? normalizeEntityToken(userWithIdMatch[1]) : null;
    const entityFromColumn = column.includes('_') ? normalizeEntityToken(column.split('_')[0]) : null;
    const entityFromContext = detectEntityInText(source);
    const entity = entityFromPattern || entityFromColumn || entityFromContext;
    if (entity && value) {
      queryText = `${entity} ${value}`;
      limit = 1;
      intent = 'id-search';
    }
  }

  const looseIdSearchMatch = source.match(/\b(?:id|uuid|user_id|usuario_id)\b\s+([0-9a-fA-F-]{8,}|\d{1,20})\b/i)
    || source.match(/^\s*([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|\d{1,20})\s*$/i);
  if (looseIdSearchMatch) {
    const value = String(looseIdSearchMatch[1] || '').trim();
    const entityFromContext = detectEntityInText(source);
    const entity = entityFromContext || 'users';
    if (value) {
      queryText = `${entity} con id ${value}`;
      limit = 1;
      intent = 'id-search';
    }
  }

  return { queryText, limit, intent };
}

async function buildSchemaSummaryForAi(pool) {
  if (!pool || typeof pool.query !== 'function') return 'Sin esquema disponible';

  try {
    const result = await pool.query(
      `SELECT table_name, column_name
       FROM information_schema.columns
       WHERE table_schema = 'public'
       ORDER BY table_name, ordinal_position`,
    );

    const grouped = new Map();
    for (const row of result.rows || []) {
      const table = String(row.table_name || '').trim();
      const column = String(row.column_name || '').trim();
      if (!table || !column) continue;
      const current = grouped.get(table) || [];
      if (current.length < 10) current.push(column);
      grouped.set(table, current);
    }

    return [...grouped.entries()]
      .slice(0, 12)
      .map(([table, columns]) => `${table}: ${columns.join(', ')}`)
      .join('\n');
  } catch {
    return 'Sin esquema disponible';
  }
}

/**
 * Detects and executes meta-queries (list tables, describe table, show relations)
 * deterministically — no Ollama needed for these common patterns.
 * Returns { rows, resumenHumano, databaseId } or null if no match.
 */
async function detectAndExecuteMetaQuery(queryText, allDatabases, registry) {
  const q = String(queryText || '').toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '');

  // ── Detect target engine from query text ─────────────────────────────────
  const mentionsOracle = /\boracle\b/.test(q);
  const mentionsPostgres = /\bpostgres(?:ql)?\b|\bpg\b/.test(q);
  const mentionsMysql = /\bmysql\b/.test(q);
  const mentionsMssql = /\bmssql\b|\bsql\s*server\b/.test(q);
  const engineHint = mentionsOracle ? 'oracle' : mentionsPostgres ? 'postgres' : mentionsMysql ? 'mysql' : mentionsMssql ? 'mssql' : null;

  const dbs = Array.isArray(allDatabases) ? allDatabases.filter((d) => d.enabled) : [];

  // Find all target databases (if engine mentioned → filter, else all)
  const targetDbs = engineHint ? dbs.filter((d) => d.type === engineHint) : dbs;
  if (targetDbs.length === 0) return null;

  // ── 1. List tables ────────────────────────────────────────────────────────
  const isListTables = /\b(tablas?|tables?|entidades?)\b/.test(q)
    && /\b(lista|listar|dame|muestra|mostrar|ver|show|get|cuales?|que)\b/.test(q);
  if (isListTables) {
    const rows = [];
    for (const db of targetDbs) {
      try {
        let sql;
        if (db.type === 'oracle') sql = "SELECT table_name AS tabla FROM USER_TABLES ORDER BY table_name";
        else if (db.type === 'postgres') sql = "SELECT table_name AS tabla FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name";
        else if (db.type === 'mysql') sql = "SELECT table_name AS tabla FROM information_schema.tables WHERE table_schema=DATABASE() ORDER BY table_name";
        else if (db.type === 'mssql') sql = "SELECT table_name AS tabla FROM information_schema.tables WHERE table_schema='dbo' ORDER BY table_name";
        else continue;
        const dbRows = await registry.executeCompiledQuery({ databaseId: db.id, sql, params: [] });
        for (const r of dbRows || []) {
          rows.push({ tabla: String(r.TABLA ?? r.tabla ?? r.table_name ?? ''), base_de_datos: db.id, tipo: db.type });
        }
      } catch { /* skip unavailable DB */ }
    }
    if (rows.length > 0) {
      const dbNames = [...new Set(rows.map((r) => r.base_de_datos))].join(', ');
      return { rows, resumenHumano: `Se encontraron ${rows.length} tabla(s) en: ${dbNames}.`, databaseId: targetDbs[0].id };
    }
  }

  // ── 2. Describe table (columns) ───────────────────────────────────────────
  const describeMatch = q.match(/\b(?:descri(?:be?|bir|beme)|columnas?|estructura|campos?|schema de|esquema de)\b[^a-z0-9]*([a-z_][a-z0-9_]*)/);
  if (describeMatch) {
    const tableName = String(describeMatch[1] || '').toUpperCase();
    if (tableName.length > 1) {
      const rows = [];
      for (const db of targetDbs) {
        try {
          let sql;
          if (db.type === 'oracle') {
            sql = `SELECT column_name AS columna, data_type AS tipo, data_length AS longitud, nullable AS nulo FROM USER_TAB_COLUMNS WHERE table_name='${tableName}' ORDER BY column_id`;
          } else if (db.type === 'postgres') {
            sql = `SELECT column_name AS columna, data_type AS tipo, is_nullable AS nulo FROM information_schema.columns WHERE table_schema='public' AND table_name='${tableName.toLowerCase()}' ORDER BY ordinal_position`;
          } else if (db.type === 'mysql') {
            sql = `SELECT column_name AS columna, data_type AS tipo, is_nullable AS nulo FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='${tableName.toLowerCase()}' ORDER BY ordinal_position`;
          } else continue;
          const dbRows = await registry.executeCompiledQuery({ databaseId: db.id, sql, params: [] });
          for (const r of dbRows || []) rows.push({ ...r, base_de_datos: db.id });
        } catch { /* skip */ }
      }
      if (rows.length > 0) {
        return { rows, resumenHumano: `Estructura de la tabla ${tableName}: ${rows.length} columna(s).`, databaseId: targetDbs[0].id };
      }
    }
  }

  // ── 3. Foreign keys / relations ───────────────────────────────────────────
  const isRelations = /\b(relacion(?:es)?|foraneas?|foreign\s*key|referencias?|constraint|fk)\b/.test(q);
  if (isRelations) {
    const rows = [];
    for (const db of targetDbs) {
      try {
        let sql;
        if (db.type === 'oracle') {
          sql = `SELECT a.constraint_name AS restriccion, a.table_name AS tabla_origen, c_pk.table_name AS tabla_referencia FROM USER_CONSTRAINTS a JOIN USER_CONSTRAINTS c_pk ON a.r_constraint_name = c_pk.constraint_name WHERE a.constraint_type='R' ORDER BY a.table_name`;
        } else if (db.type === 'postgres') {
          sql = `SELECT tc.constraint_name AS restriccion, tc.table_name AS tabla_origen, ccu.table_name AS tabla_referencia, kcu.column_name AS columna FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name=kcu.constraint_name JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name=tc.constraint_name WHERE tc.constraint_type='FOREIGN KEY' ORDER BY tc.table_name`;
        } else continue;
        const dbRows = await registry.executeCompiledQuery({ databaseId: db.id, sql, params: [] });
        for (const r of dbRows || []) rows.push({ ...r, base_de_datos: db.id });
      } catch { /* skip */ }
    }
    if (rows.length > 0) {
      return { rows, resumenHumano: `Se encontraron ${rows.length} relación(es) entre tablas.`, databaseId: targetDbs[0].id };
    }
    // Even if 0 rows, this was a valid meta-query — return empty meaningful response
    if (targetDbs.length > 0) {
      return { rows: [], resumenHumano: 'No se encontraron relaciones (foreign keys) en las tablas del usuario.', databaseId: targetDbs[0].id };
    }
  }

  return null;
}

/**
 * Builds a schema summary from ALL registered databases for the AI context.
 * Postgres via information_schema, Oracle via USER_TAB_COLUMNS, MySQL via information_schema.
 */
async function buildMultiDbSchemaSummaryForAi(pool, registry) {
  const sections = [];

  // Postgres (primary pool)
  if (pool && typeof pool.query === 'function') {
    try {
      const result = await pool.query(
        `SELECT table_name, column_name FROM information_schema.columns
         WHERE table_schema = 'public' ORDER BY table_name, ordinal_position`,
      );
      const grouped = new Map();
      for (const row of result.rows || []) {
        const table = String(row.table_name || '').trim();
        const col = String(row.column_name || '').trim();
        if (!table || !col) continue;
        const curr = grouped.get(table) || [];
        if (curr.length < 8) curr.push(col);
        grouped.set(table, curr);
      }
      if (grouped.size > 0) {
        sections.push('--- PostgreSQL (pg_main) ---');
        for (const [t, cols] of [...grouped.entries()].slice(0, 15)) {
          sections.push(`${t}(${cols.join(', ')})`);
        }
      }
    } catch { /* best-effort */ }
  }

  // All other engines via registry
  if (registry && typeof registry.getDatabases === 'function') {
    for (const db of registry.getDatabases()) {
      if (!db.enabled || db.type === 'postgres') continue;
      try {
        let metaSql;
        if (db.type === 'oracle') {
          metaSql = 'SELECT table_name, column_name FROM USER_TAB_COLUMNS ORDER BY table_name, column_id';
        } else if (db.type === 'mysql') {
          metaSql = 'SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = DATABASE() ORDER BY table_name, ordinal_position';
        } else if (db.type === 'mssql') {
          metaSql = "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'dbo' ORDER BY table_name, ordinal_position";
        } else {
          continue;
        }
        const rows = await registry.executeCompiledQuery({ databaseId: db.id, sql: metaSql, params: [] });
        const grouped = new Map();
        for (const row of rows || []) {
          const table = String(row.TABLE_NAME || row.table_name || '').trim();
          const col = String(row.COLUMN_NAME || row.column_name || '').trim();
          if (!table || !col) continue;
          const curr = grouped.get(table) || [];
          if (curr.length < 8) curr.push(col);
          grouped.set(table, curr);
        }
        if (grouped.size > 0) {
          sections.push(`--- ${db.type.toUpperCase()} (${db.id}) ---`);
          for (const [t, cols] of [...grouped.entries()].slice(0, 15)) {
            sections.push(`${t}(${cols.join(', ')})`);
          }
        }
      } catch { /* best-effort: skip unavailable DB */ }
    }
  }

  return sections.length > 0 ? sections.join('\n') : 'Sin esquema disponible';
}

/**
 * Asks Ollama to generate a SELECT SQL for any user question using multi-DB schema context.
 * Returns { sql, databaseId, explanation } or null on failure.
 */
async function generateSqlWithOllama(queryText, registry, pgPool = null) {
  const src = String(queryText || '').trim();
  if (!src) return null;

  const allDbs = typeof registry?.getDatabases === 'function' ? registry.getDatabases() : [];
  const dbList = allDbs.filter((d) => d.enabled).map((d) => `${d.id} (${d.type})`).join(', ');
  const schema = await buildMultiDbSchemaSummaryForAi(pgPool, registry);

  const prompt = [
    'You are a SQL generator. Read the user question and return a JSON with the SQL to execute.',
    '',
    'RULES (follow exactly):',
    '1. Output ONLY a JSON object. No markdown. No explanation. No code blocks.',
    '2. JSON format: {"sql":"...","database_id":"...","explanation":"..."}',
    '3. sql: a single SELECT statement. Never INSERT/UPDATE/DELETE/DROP/ALTER.',
    '4. database_id: pick the best matching id from the list below.',
    '5. explanation: one short sentence in Spanish describing what was done.',
    '',
    'SQL REFERENCE BY DATABASE TYPE:',
    '  Oracle - list tables:     SELECT table_name FROM USER_TABLES ORDER BY table_name',
    '  Oracle - show table data: SELECT * FROM <table> WHERE ROWNUM <= 50',
    '  Oracle - describe table:  SELECT column_name, data_type, nullable FROM USER_TAB_COLUMNS WHERE table_name=UPPER(\'<table>\') ORDER BY column_id',
    '  Oracle - foreign keys:    SELECT constraint_name, table_name, r_constraint_name FROM USER_CONSTRAINTS WHERE constraint_type=\'R\'',
    '  Oracle - count rows:      SELECT COUNT(*) AS total FROM <table>',
    '  Postgres - list tables:   SELECT table_name FROM information_schema.tables WHERE table_schema=\'public\' ORDER BY table_name',
    '  Postgres - show table:    SELECT * FROM <table> LIMIT 50',
    '  Postgres - describe:      SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name=\'<table>\' ORDER BY ordinal_position',
    '  Postgres - count:         SELECT COUNT(*) AS total FROM <table>',
    '',
    `Available database IDs: ${dbList || 'none'}`,
    '',
    'Database schema:',
    schema,
    '',
    `User question: ${src}`,
    '',
    'JSON response:',
  ].join('\n');

  try {
    const response = await askOllama(prompt, { timeoutMs: 30000, model: process.env.OLLAMA_MODEL || undefined });
    console.log(`[AI-SQL-GEN] Ollama ok=${response?.ok} text="${String(response?.text || '').slice(0, 120)}"`);
    const aiJson = response?.json && typeof response.json === 'object' ? response.json : null;
    if (!response?.ok || !aiJson) {
      console.warn(`[AI-SQL-GEN] No valid JSON from Ollama. ok=${response?.ok} json=${JSON.stringify(aiJson)}`);
      return null;
    }

    const sql = String(aiJson.sql || '').trim();
    let databaseId = String(aiJson.database_id || '').trim();
    const explanation = String(aiJson.explanation || '').trim();

    console.log(`[AI-SQL-GEN] Parsed: sql="${sql.slice(0, 80)}" db="${databaseId}"`);

    if (!sql) return null;

    // Security: only SELECT or WITH (CTE)
    const norm = sql.replace(/\s+/g, ' ').trim().toLowerCase();
    if (!norm.startsWith('select') && !norm.startsWith('with')) return null;
    if (/\b(insert|update|delete|drop|truncate|alter|create|grant|revoke|exec|execute)\b/.test(norm)) return null;

    // Reject Oracle bind placeholders (:1 :varname) — causes NJS-098
    if (/:[a-zA-Z_]\w*/.test(sql) || /:\d+/.test(sql)) {
      console.warn(`[AI-SQL-GEN] Rejected: SQL contains bind placeholder: ${sql.slice(0, 80)}`);
      return null;
    }

    // Validate / fallback database_id
    const targetDb = allDbs.find((d) => d.id === databaseId && d.enabled);
    if (!targetDb) {
      const oracleMentioned = /oracle/i.test(src);
      const postgresMentioned = /postgres/i.test(src);
      const fallbackDb = oracleMentioned ? allDbs.find((d) => d.type === 'oracle' && d.enabled)
        : postgresMentioned ? allDbs.find((d) => d.type === 'postgres' && d.enabled)
        : allDbs.find((d) => d.enabled);
      if (!fallbackDb) return null;
      databaseId = fallbackDb.id;
      console.log(`[AI-SQL-GEN] db_id fallback → ${databaseId}`);
    }

    return { sql, databaseId, explanation };
  } catch (err) {
    console.warn(`[AI-SQL-GEN] Error: ${err instanceof Error ? err.message : err}`);
    return null;
  }
}

async function refineQuestionWithOllama(queryText, pool) {
  const source = String(queryText || '').trim();
  if (!source) return null;
  if (source.split(/\s+/).length < 4) return null;

  const schemaSummary = await buildSchemaSummaryForAi(pool);
  const prompt = [
    'Eres un copiloto de interpretación de consultas para un backend SQL.',
    'No inventes datos ni ejecutes SQL.',
    'Tu tarea es reescribir la pregunta del usuario para que sea más clara para un motor SQL determinístico.',
    'Responde SOLO JSON válido con este formato:',
    '{"query":"...","confianza":0.0}',
    '',
    `Pregunta original: ${source}`,
    'Esquema disponible:',
    schemaSummary,
  ].join('\n');

  try {
    const response = await askOllama(prompt, { timeoutMs: 3000, model: pickOllamaModel(source) });
    const aiJson = response?.json && typeof response.json === 'object' ? response.json : null;
    if (!response?.ok || !aiJson) return null;

    const rewritten = String(aiJson?.query || '').trim();
    const confidence = Number(aiJson?.confianza || 0);
    if (!rewritten) return null;
    if (rewritten.toLowerCase() === source.toLowerCase()) return null;
    if (!Number.isFinite(confidence) || confidence < 0.6) return null;

    return rewritten;
  } catch {
    return null;
  }
}

async function normalizeQuestionWithOllama(queryText, pool, defaultLimit = MAX_QUERY_LIMIT, registry = null) {
  const source = String(queryText || '').trim();
  if (!source) return null;

  const schemaSummary = registry
    ? await buildMultiDbSchemaSummaryForAi(pool, registry)
    : await buildSchemaSummaryForAi(pool);
  
  // Structured prompt with clear operation definitions
  const prompt = [
    'You are a database query parser. Classify the user query and return ONLY valid JSON.',
    'Operations:',
    '  list-tables = user asks about what tables/entities exist in the database (NOT rows from a table)',
    '  list-columns = user asks about columns/fields of a table',
    '  count = user wants a count/total number of rows',
    '  list = user wants all rows from a specific table (e.g. "show users", "get logs")',
    '  filter = user wants rows matching a specific value/name',
    '  rank = user wants top/most active records ranked by activity',
    '  search = user wants to search by keyword',
    `Available tables: ${schemaSummary}`,
    `User query: "${source}"`,
    'Return JSON only: {"operation":"list-tables|list-columns|count|list|filter|rank|search","table":"tablename_or_null","value":"filter_value_or_null","confidence":0.9}',
  ].join('\n');

  try {
    const response = await askOllama(prompt, { timeoutMs: 15000, model: pickOllamaModel(source) });
    const aiJson = response?.json && typeof response.json === 'object' ? response.json : null;
    
    if (!response?.ok || !aiJson) {
      return null;
    }

    let confidence = Number(aiJson.confidence ?? 0.6);
    if (!Number.isFinite(confidence)) confidence = 0.6;

    const operation = String(aiJson.operation ?? 'unknown').toLowerCase();
    console.log(`[Ollama] ✓ op=${operation}, conf=${confidence}`);

    // Map Ollama operation to intent
    const directIntent = mapOllamaOperationToIntent(aiJson, operation, defaultLimit, source);
    
    return {
      refinedQuery: source,
      directIntent: directIntent || null,
      advancedIntent: null,
      confidence: Math.max(confidence, 0.5),
    };
  } catch (error) {
    return null;
  }
}

// Tablas comunes conocidas — solo para desambiguación preferencial, NO para bloqueo.
// El motor valida contra el schema real; esta lista solo prioriza matches.
const KNOWN_DB_TABLES = new Set(['users', 'logs', 'sessions', 'comments', 'knowledge_base', 'query_history', 'error_logs', 'semantic_learning', 'roles']);

function normalizeAiTargetTable(aiJson, originalQueryText = '') {
  const rawTable = String(aiJson?.table || aiJson?.targetEntity || aiJson?.target || aiJson?.entity || '').trim();
  const fromRaw = resolveTableAliasToken(rawTable);

  // Preferir tabla conocida exacta
  if (fromRaw && KNOWN_DB_TABLES.has(fromRaw)) return fromRaw;

  // Preferir entidad detectada en la query original
  const fromQuery = detectEntityInText(originalQueryText);
  if (fromQuery && KNOWN_DB_TABLES.has(fromQuery)) return fromQuery;

  // Intentar alias conocidos
  const normalizedRaw = sanitizeIdentifierToken(rawTable).toLowerCase();
  if (normalizedRaw) {
    for (const [alias, table] of Object.entries(ENTITY_ALIASES)) {
      if (normalizedRaw.includes(alias) && KNOWN_DB_TABLES.has(table)) {
        return table;
      }
    }
  }

  // IMPORTANTE: si la tabla no está en la lista conocida pero es un identificador
  // válido (p.ej. tablas Oracle), pasarla igual para que el motor la valide
  // contra el schema real. NO bloquear por no estar en KNOWN_DB_TABLES.
  if (fromRaw) return fromRaw;
  if (normalizedRaw) return normalizedRaw;

  return '';
}

function mapOllamaOperationToIntent(aiJson, operation, limit, originalQueryText = '') {
  const op = String(operation || '').toLowerCase();
  const specifiedTable = normalizeAiTargetTable(aiJson, originalQueryText);
  
  if (op.includes('list-table')) {
    // If Ollama says list-tables but query points to a concrete entity, treat it as row listing.
    if (specifiedTable) {
      return { matched: true, type: 'simple-table', table: specifiedTable, limit };
    }
    return { matched: true, type: 'list-tables', table: null, limit };
  }
  if (op.includes('list-col')) {
    const table = specifiedTable || 'users';
    return { matched: true, type: 'list-columns', table, limit };
  }
  if (op.includes('count')) {
    const table = specifiedTable || 'users';
    return { matched: true, type: 'count-rows', table, limit: 1 };
  }
  if (op.includes('filter') || op.includes('search')) {
    const table = specifiedTable || 'users';
    const value = String(aiJson.value || '').trim();
    if (!value) return null;
    return {
      matched: true,
      type: 'text-filter',
      table,
      value,
      columnHint: 'username',
      matchMode: 'contains',
      limit,
    };
  }
  if (op.includes('rank')) {
    return {
      matched: true,
      type: 'ranked-activity-users',
      table: 'users',
      relatedTable: 'logs',
      timeWindowHours: 24,
      limit: Math.min(limit, 10),
      sortDirection: 'DESC',
    };
  }
  if (op === 'list') {
    const table = specifiedTable || 'users';
    return { matched: true, type: 'simple-table', table, limit };
  }
  
  return null;
}

/**
 * Builds a list-tables response aggregating tables from ALL registered databases.
 * Used when multi-DB is active so users can see tables from every engine.
 * @param {Array<{id:string, type:string, schema:{tables:Array<{name:string}>}}>} databases
 * @returns {object|null}
 */
function buildMultiDbListTablesResult(databases) {
  const rows = [];
  for (const db of databases || []) {
    for (const table of db.schema?.tables || []) {
      rows.push({ tabla: table.name, base_de_datos: db.id, tipo: db.type });
    }
  }
  if (rows.length === 0) return null;
  const dbCount = new Set(rows.map((r) => r.base_de_datos)).size;
  return {
    success: true,
    executionType: 'distributed',
    data: rows,
    rowCount: rows.length,
    message: `Se encontraron ${rows.length} tabla(s) en ${dbCount} base(s) de datos.`,
    source: [...new Set(rows.map((r) => r.base_de_datos))],
  };
}

function filterDatabasesByIds(databases, requestedIds = []) {
  const normalizedIds = new Set(
    (Array.isArray(requestedIds) ? requestedIds : [])
      .map((id) => String(id || '').trim().toLowerCase())
      .filter(Boolean)
  );

  if (normalizedIds.size === 0) {
    return Array.isArray(databases) ? databases : [];
  }

  return (Array.isArray(databases) ? databases : [])
    .filter((db) => normalizedIds.has(String(db?.id || '').trim().toLowerCase()));
}

function extractRequestedDatabasesFromText(text, availableDatabases = []) {
  const input = String(text || '').trim().toLowerCase();
  if (!input) return [];

  const ids = new Set();
  const mentionsOracle = /\boracle\b/.test(input);
  const mentionsPostgres = /\bpostgres(?:ql)?\b|\bpg\b/.test(input);
  const mentionsMySql = /\bmysql\b/.test(input);

  for (const db of availableDatabases || []) {
    const dbId = String(db?.id || '').trim();
    const dbType = String(db?.type || '').trim().toLowerCase();
    if (!dbId) continue;

    if (input.includes(dbId.toLowerCase())) {
      ids.add(dbId);
      continue;
    }

    if (mentionsOracle && dbType === 'oracle') ids.add(dbId);
    if (mentionsPostgres && dbType === 'postgres') ids.add(dbId);
    if (mentionsMySql && dbType === 'mysql') ids.add(dbId);
  }

  return [...ids];
}

async function executeRawSelectForRequestedDatabase(
  multiDatabaseEngine,
  sqlText,
  requestedDatabases = [],
  fallbackExecuteSafeSelect = null,
  routingOptions = {},
) {
  const requested = Array.isArray(requestedDatabases)
    ? requestedDatabases.map((id) => String(id || '').trim()).filter(Boolean)
    : [];
  const explicitDatabaseId = String(routingOptions?.databaseId || '').trim();
  const explicitDatabaseHint = String(routingOptions?.databaseHint || '').trim().toLowerCase();

  if (requested.length > 0) {
    const targetDatabaseId = requested[0];
    const rows = await multiDatabaseEngine.registry.executeCompiledQuery({
      databaseId: targetDatabaseId,
      sql: sqlText,
      params: [],
    });

    return {
      rows: Array.isArray(rows) ? rows : [],
      source: [targetDatabaseId],
      summary: `Consulta SQL ejecutada en ${targetDatabaseId}.`,
    };
  }

  if (explicitDatabaseId) {
    const rows = await multiDatabaseEngine.registry.executeCompiledQuery({
      databaseId: explicitDatabaseId,
      sql: sqlText,
      params: [],
    });

    return {
      rows: Array.isArray(rows) ? rows : [],
      source: [explicitDatabaseId],
      summary: `Consulta SQL ejecutada en ${explicitDatabaseId}.`,
    };
  }

  if (explicitDatabaseHint) {
    const allDatabases = multiDatabaseEngine.registry?.getDatabases?.() || [];
    const hinted = allDatabases.find(
      (db) => String(db?.type || '').trim().toLowerCase() === explicitDatabaseHint,
    );

    if (hinted?.id) {
      const rows = await multiDatabaseEngine.registry.executeCompiledQuery({
        databaseId: hinted.id,
        sql: sqlText,
        params: [],
      });

      return {
        rows: Array.isArray(rows) ? rows : [],
        source: [hinted.id],
        summary: `Consulta SQL ejecutada en ${hinted.id}.`,
      };
    }
  }

  if (typeof fallbackExecuteSafeSelect === 'function') {
    const result = await fallbackExecuteSafeSelect(sqlText, [], 5000, {
      allowCrossDatabase: true,
      allowUnknownTablesForTarget: true,
      databaseId: explicitDatabaseId,
      databaseHint: explicitDatabaseHint,
    });
    return {
      rows: Array.isArray(result?.rows) ? result.rows : [],
      source: [],
      summary: 'Consulta SQL ejecutada correctamente.',
    };
  }

  throw new Error('No se pudo resolver una base de datos destino para ejecutar SQL raw.');
}

function inferDatabaseHintFromSqlText(sqlText = '') {
  return detectSqlSyntaxEngine(String(sqlText || '')) || '';
}

/**
 * @param {import('../infrastructure/distributed/MultiDatabaseEngine.js').MultiDatabaseEngine} multiDatabaseEngine
 * @returns {express.Router}
 */
export function createQueryRouter(multiDatabaseEngine, options = {}) {
  const router = express.Router();
  const ensureDatabasesReady = typeof options?.ensureDatabasesReady === 'function'
    ? options.ensureDatabasesReady
    : null;
  const pool = options?.pool || null;
  const executeSafeSelect = typeof options?.executeSafeSelect === 'function'
    ? options.executeSafeSelect
    : null;
  const relevanceValidator = new DatabaseRelevanceValidator();

  /**
   * POST /api/query
   *
   * Body: { query: string, limit?: number, databases?: string[] }
   *
   * Respuesta normalizada:
   * {
   *   success: boolean,
   *   executionType: "single" | "distributed" | "unresolved",
   *   confidence: number | null,
   *   data: any[],
   *   rowCount: number,
   *   message: string | null,
   *   warnings: string[],
   *   sources: string[],
   *   suggestions: string[]
   * }
   */
  router.post('/', async (req, res) => {
    const trace = {
      interpretadoPor: 'deterministic',
      intencion: 'unknown',
      confianza: null,
    };

    // ── 1. Validación de input ───────────────────────────────────────────────
    const rawQuery = req.body?.query;

    if (!rawQuery || typeof rawQuery !== 'string' || !rawQuery.trim()) {
      return res.status(400).json({
        success: false,
        error: 'El campo "query" es requerido y debe ser un texto no vacío.',
      });
    }

    let queryText;
    try {
      queryText = sanitizeQueryText(rawQuery);
    } catch (error) {
      return res.status(400).json({
        success: false,
        error: error instanceof Error ? error.message : 'Input contiene patrones potencialmente peligrosos.',
      });
    }

    queryText = stripConversationalPrefix(queryText);

    if (queryText.length === 0) {
      return res.status(400).json({
        success: false,
        error: 'La consulta quedó vacía luego de sanitizar. Revisa el input.',
      });
    }

    // ── NEW: Validate if query is database-related ───────────────────────────
    // Skip validation for SQL-like queries (they're obviously DB-related)
    if (!isSqlLikeText(queryText)) {
      const relevanceValidation = relevanceValidator.validateRelevance(queryText);
      
      if (!relevanceValidation.isDbRelated) {
        console.log(`[RELEVANCE] Query rejected as non-DB-related: "${queryText}" (score: ${relevanceValidation.relevanceScore})`);
        return res.status(200).json(
          relevanceValidator.createNonDbQueryResponse(queryText, relevanceValidation)
        );
      }
      
      console.log(`[RELEVANCE] Query accepted as DB-related: "${queryText}" (score: ${relevanceValidation.relevanceScore})`);
    }

    if (detectErrorAnalysisInput(queryText)) {
      const analysis = analyzeErrorInput(queryText);
      const matches = await searchErrorLogs(pool, analysis, 10);
      const enrichedAnalysis = await enrichErrorAnalysisWithAi(analysis, matches);
      return res.status(200).json(buildErrorAnalysisResponse(enrichedAnalysis, matches));
    }

    if (BLOCKED_SQL_KEYWORDS.test(queryText)) {
      return res.status(400).json({
        success: false,
        error: 'Este endpoint solo acepta consultas de lectura en lenguaje natural. No se permiten operaciones de escritura.',
      });
    }

    if (isSqlLikeText(queryText) && !isSelectOnlySql(queryText)) {
      return res.status(400).json({
        success: false,
        error: 'Solo se permiten consultas de lectura (SELECT).',
      });
    }

    // ── 2. Opciones de ejecución ─────────────────────────────────────────────
    const rawLimit = Number(req.body?.limit);
    const requestedLimit = Number.isFinite(rawLimit) && rawLimit > 0 ? Math.min(rawLimit, MAX_QUERY_LIMIT) : MAX_QUERY_LIMIT;
    const preprocessed = preprocessNaturalQuery(queryText, requestedLimit);
    let effectiveQueryText = String(preprocessed.queryText || queryText || '').trim() || queryText;
    let limit = Number(preprocessed.limit || requestedLimit || MAX_QUERY_LIMIT);
    const explicitDatabases = Array.isArray(req.body?.databases) ? req.body.databases : [];
    const explicitDatabaseId = String(req.body?.databaseId || '').trim();
    const allDatabases = multiDatabaseEngine.registry?.getDatabases?.() || [];
    const inferredDatabases = extractRequestedDatabasesFromText(queryText, allDatabases);
    const databases = explicitDatabases.length > 0
      ? explicitDatabases
      : (explicitDatabaseId ? [explicitDatabaseId] : inferredDatabases);
    let forceDistributedSemantic = false;

    if (isSqlLikeText(queryText) && isSelectOnlySql(queryText)) {
      const sqlDirectiveId = extractDatabaseDirective(queryText);
      const sqlEngineHint = sqlDirectiveId ? '' : inferDatabaseHintFromSqlText(queryText);
      const rawRoutingOptions = sqlDirectiveId
        ? { databaseId: sqlDirectiveId }
        : (sqlEngineHint ? { databaseHint: sqlEngineHint } : {});

      console.log(buildEngineLog({
        engine: sqlEngineHint || null,
        queryType: detectQueryType(queryText),
        databaseId: sqlDirectiveId || sqlEngineHint || 'primary (fallback)',
        reason: sqlDirectiveId
          ? '-- database: directive'
          : (sqlEngineHint ? 'syntax markers detected' : 'no specific markers'),
        sql: queryText,
      }));

      const rawSqlExecution = await executeRawSelectForRequestedDatabase(
        multiDatabaseEngine,
        queryText,
        databases,
        executeSafeSelect,
        rawRoutingOptions,
      );

      const rawRows = rawSqlExecution.rows;
      let rawResumen = rawSqlExecution.summary;
      if (!rawResumen) {
        try {
          rawResumen = _resultExplainer.explain(rawRows)?.resumen || `Se obtuvieron ${rawRows.length} registros.`;
        } catch {
          rawResumen = `Se obtuvieron ${rawRows.length} registros.`;
        }
      }
      return res.status(200).json({
        resumenHumano: rawResumen,
        resultado: rawRows,
        metadata: {
          entidad: inferEntityName({ source: rawSqlExecution.source }, rawRows),
          total: rawRows.length,
          executionType: 'single-db',
          sources: Array.isArray(rawSqlExecution.source) ? rawSqlExecution.source : [],
          confidence: 1,
          trazabilidad: normalizeTrace({
            interpretadoPor: 'sql-raw',
            intencion: 'select',
            confianza: 1,
          }),
        },
      });
    }
    
    // ── 2A.5. OLLAMA PRIMARY HANDLER ─────────────────────────────────────────
    // For ALL natural-language queries: let Ollama generate and execute the SQL.
    // This runs BEFORE the semantic engine so misparses ("entidad 'de'") never happen.
    // If Ollama fails or returns nothing → fall through to the engine as backup.
    if (!isSqlLikeText(queryText)) {
      const _aiRegistry = multiDatabaseEngine.registry;
      try {
        const generated = await generateSqlWithOllama(queryText, _aiRegistry, pool);
        if (generated?.sql && generated?.databaseId) {
          console.log(`[AI-SQL-GEN] "${queryText}" → ${generated.databaseId}: ${generated.sql.slice(0, 100)}`);
          const genRows = await _aiRegistry.executeCompiledQuery({
            databaseId: generated.databaseId,
            sql: generated.sql,
            params: [],
          });
          if (Array.isArray(genRows)) {
            return res.status(200).json({
              resumenHumano: generated.explanation || (genRows.length > 0 ? `${genRows.length} resultado(s) encontrados.` : 'Consulta ejecutada sin resultados.'),
              resultado: genRows,
              metadata: {
                entidad: generated.databaseId,
                total: genRows.length,
                executionType: 'ai-generated-sql',
                sources: [generated.databaseId],
                confidence: 0.9,
                trazabilidad: normalizeTrace({ interpretadoPor: 'ai-sql-gen', intencion: 'generate-sql', confianza: 0.9 }),
              },
            });
          }
        }
      } catch (aiErr) {
        console.warn('[AI-SQL-GEN] Primary handler error, falling back to engine:', aiErr instanceof Error ? aiErr.message : aiErr);
      }
    }

    // ── 2B. NEW: AI-FIRST APPROACH ───────────────────────────────────────────
    // ALWAYS try Ollama first, regardless of complexity
    // This ensures ANY database-related query is handled by AI
    const deterministicDirectIntent = detectDirectSqlIntent(effectiveQueryText);
    const deterministicRankingIntent = detectRankingIntentFromText(effectiveQueryText, limit);
    const extractedIdentifierValue = extractIdentifierLiteral(effectiveQueryText) || extractIdentifierLiteral(queryText);
    const forcedIdentifierIntent = extractedIdentifierValue
      ? {
        matched: true,
        type: 'id-search',
        table: detectEntityInText(effectiveQueryText) || detectEntityInText(queryText) || 'users',
        value: extractedIdentifierValue,
        columnHint: 'id',
        limit: 1,
      }
      : null;
    const hasStrongDeterministicIdIntent = Boolean(
      deterministicDirectIntent?.matched
      && deterministicDirectIntent?.type === 'id-search'
      && String(deterministicDirectIntent?.value || '').trim().length > 0
    );
    const hasStrongDeterministicRankingIntent = Boolean(
      deterministicRankingIntent?.matched
      && deterministicRankingIntent?.type === 'ranked-activity-users'
    );

    let directIntent = null;
    let aiAttempted = false;
    
    console.log(`[AI-FIRST] Processing query: "${queryText}"`);
    
    if (hasStrongDeterministicIdIntent || forcedIdentifierIntent || hasStrongDeterministicRankingIntent) {
      directIntent = hasStrongDeterministicIdIntent
        ? deterministicDirectIntent
        : (forcedIdentifierIntent || deterministicRankingIntent);
      trace.interpretadoPor = 'deterministic';
      trace.intencion = String(directIntent?.type || 'id-search');
      trace.confianza = 1;
      console.log('[AI-FIRST] Deterministic strong intent locked before AI normalization');
    } else {
      // Try Ollama FIRST for everything else
      const aiNormalized = await normalizeQuestionWithOllama(queryText, pool, limit, multiDatabaseEngine.registry);
      if (aiNormalized && aiNormalized.confidence >= AI_NORMALIZATION_CONFIDENCE_THRESHOLD) {
        aiAttempted = true;
        console.log(`[AI-FIRST] Ollama succeeded with confidence ${aiNormalized.confidence}`);
        
        if (aiNormalized.advancedIntent?.matched) {
          directIntent = aiNormalized.advancedIntent;
          trace.interpretadoPor = 'ai-assisted';
          trace.intencion = String(aiNormalized.advancedIntent.type || 'unknown');
          trace.confianza = aiNormalized.confidence;
        } else if (aiNormalized.directIntent?.matched) {
          directIntent = aiNormalized.directIntent;
          trace.interpretadoPor = 'ai-assisted';
          trace.intencion = String(aiNormalized.directIntent.type || 'unknown');
          trace.confianza = aiNormalized.confidence;
        } else if (aiNormalized.refinedQuery) {
          // If AI refined query, update effective query and mark as ai-assisted
          effectiveQueryText = aiNormalized.refinedQuery;
          trace.interpretadoPor = 'ai-assisted';
          trace.intencion = 'query-normalization';
          trace.confianza = aiNormalized.confidence;
          
          // Try to infer intent from refined query
          const inferredDirectIntent = detectDirectSqlIntent(aiNormalized.refinedQuery);
          if (inferredDirectIntent?.matched) {
            directIntent = inferredDirectIntent;
            trace.intencion = String(inferredDirectIntent.type || 'unknown');
          } else {
            const inferredRankingIntent = detectRankingIntentFromText(aiNormalized.refinedQuery, limit);
            if (inferredRankingIntent?.matched) {
              directIntent = inferredRankingIntent;
              trace.intencion = String(inferredRankingIntent.type || 'unknown');
            }
          }
        }
      } else {
        console.log(`[AI-FIRST] Ollama not conclusive (${aiNormalized?.confidence || 'N/A'}), falling back to deterministic`);
      }
    }
    
    // ── 2C. Fallback: Deterministic only if AI failed ───────────────────────
    if (!directIntent || !trace.interpretadoPor || trace.interpretadoPor === 'deterministic') {
      if (deterministicDirectIntent?.matched) {
        directIntent = deterministicDirectIntent;
        if (!aiAttempted) {
          trace.interpretadoPor = 'deterministic';
        }
        trace.intencion = String(deterministicDirectIntent.type || 'unknown');
      } else {
        const rankingIntent = detectRankingIntentFromText(queryText, limit);
        if (rankingIntent?.matched) {
          directIntent = rankingIntent;
          if (!aiAttempted) {
            trace.interpretadoPor = 'deterministic';
          }
          trace.intencion = String(rankingIntent.type || 'unknown');
        }
      }
    }

    // ── 3. Garantizar bootstrap multi-db (si está configurado) ──────────────
    if (ensureDatabasesReady) {
      try {
        await ensureDatabasesReady();

        // Semantic multi-entity queries must pass through distributed resolution/merge.
        try {
          const resolutionPreview = multiDatabaseEngine.resolveEntitiesAcrossDatabases(effectiveQueryText, databases);
          const entityCount = Array.isArray(resolutionPreview?.entities) ? resolutionPreview.entities.length : 0;
          const hasRelation = Boolean(resolutionPreview?.intent?.conditions?.relation);
          forceDistributedSemantic = FORCE_DISTRIBUTED_MULTI_ENTITY && (hasRelation || entityCount >= 2);
        } catch {
          forceDistributedSemantic = false;
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : 'No se pudo inicializar el subsistema multi-base.';
        console.error('[POST /api/query] bootstrap error:', message);
        return res.status(503).json({
          success: false,
          error: message,
          message: 'El servicio de consulta inteligente no está listo todavía.',
        });
      }
    }

    // ── 4. Ejecución via MultiDatabaseEngine ─────────────────────────────────
    let engineResult;
    try {
      // When multiple databases are configured, bypass the direct postgres-only execution
      // for bulk-fetch intents so ALL databases are queried through the distributed engine.
      let allConfiguredDbs = multiDatabaseEngine.registry?.getDatabases?.() || [];
      const isMultiDb = allConfiguredDbs.length > 1;
      const MULTI_DB_PASSTHROUGH_INTENTS = new Set(['simple-table', 'simple-limit', 'list-tables', 'count-rows', 'group-count', 'recent-rows']);
      const hasRequestedDatabaseFilter = Array.isArray(databases) && databases.length > 0;
      const shouldBypassDirect = isMultiDb && (
        hasRequestedDatabaseFilter
        || (directIntent?.matched && MULTI_DB_PASSTHROUGH_INTENTS.has(directIntent.type))
      );

      if (forceDistributedSemantic) {
        engineResult = await multiDatabaseEngine.execute(effectiveQueryText, {
          limit,
          databases,
          forceDistributed: true,
        });
      } else {
        const advancedResult = shouldBypassDirect ? null : await tryExecuteAdvancedIntent(pool, directIntent);
        const directResult = advancedResult || (shouldBypassDirect ? null : await tryExecuteDeterministicDirectSql(pool, directIntent));
        if (directResult) {
          engineResult = directResult;
        } else if (shouldBypassDirect && directIntent?.type === 'list-tables') {
          if (typeof multiDatabaseEngine.registry?.introspectEmptySchemas === 'function') {
            try {
              await multiDatabaseEngine.registry.introspectEmptySchemas();
              allConfiguredDbs = multiDatabaseEngine.registry?.getDatabases?.() || allConfiguredDbs;
            } catch {
              // Best-effort only: if introspection fails, continue with current schema snapshot.
            }
          }
          // For list-tables in multi-DB: aggregate tables from requested DBs when provided,
          // otherwise use all registered databases.
          const listTablesDatabases = filterDatabasesByIds(allConfiguredDbs, databases);
          engineResult = buildMultiDbListTablesResult(listTablesDatabases) || await multiDatabaseEngine.execute(effectiveQueryText, { limit, databases });
        } else {
          engineResult = await multiDatabaseEngine.execute(effectiveQueryText, { limit, databases });

          const firstPassEmpty = Array.isArray(engineResult?.data) && engineResult.data.length === 0;
          const firstPassAmbiguous = !engineResult?.success || engineResult?.executionType === 'unresolved';

          if (firstPassEmpty || firstPassAmbiguous) {
            const aiRefinedQuery = await refineQuestionWithOllama(effectiveQueryText, pool);
            if (aiRefinedQuery) {
              const retryResult = await multiDatabaseEngine.execute(aiRefinedQuery, {
                limit,
                databases,
                forceDistributed: forceDistributedSemantic,
              });
              const retryHasData = Array.isArray(retryResult?.data) && retryResult.data.length > 0;
              const retryResolved = Boolean(retryResult?.success) && retryResult?.executionType !== 'unresolved';
              if (retryResolved || retryHasData) {
                trace.interpretadoPor = 'ai-refined';
                trace.intencion = String(trace.intencion || 'query-refinement');
                engineResult = {
                  ...retryResult,
                  message: String(retryResult?.message || '').trim() || `Consulta refinada con apoyo IA: ${aiRefinedQuery}`,
                };
              }
            }
          }
        }
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Error interno al ejecutar la consulta.';
      console.error('[POST /api/query] engine error:', message);

      if (String(message).toLowerCase().includes('no hay bases registradas')) {
        return res.status(503).json({
          success: false,
          error: message,
          message: 'No hay bases registradas para el motor distribuido. Reintenta en unos segundos.',
        });
      }

      return res.status(500).json({
        success: false,
        error: message,
      });
    }

    // ── 5. Mapeo de respuesta ─────────────────────────────────────────────────
    const engineUnresolved = !engineResult.success || engineResult.executionType === 'unresolved';
    const engineEmpty = Array.isArray(engineResult.data) && engineResult.data.length === 0;

    if (engineUnresolved || engineEmpty) {
      // Fallback A: deterministic meta-query (list tables, describe table, foreign keys)
      try {
        const registry = multiDatabaseEngine.registry;
        const allConfiguredDbsFallback = registry?.getDatabases?.() || [];
        const metaResult = await detectAndExecuteMetaQuery(queryText, allConfiguredDbsFallback, registry);
        if (metaResult) {
          return res.status(200).json({
            resumenHumano: metaResult.resumenHumano,
            resultado: metaResult.rows,
            metadata: {
              entidad: metaResult.databaseId,
              total: metaResult.rows.length,
              executionType: 'meta-query',
              sources: [metaResult.databaseId],
              confidence: 1,
              trazabilidad: normalizeTrace({ interpretadoPor: 'deterministic-meta', intencion: 'meta-query', confianza: 1 }),
            },
          });
        }
      } catch (metaError) {
        console.warn('[META-QUERY] Failed:', metaError instanceof Error ? metaError.message : metaError);
      }

      // Fallback B: Ollama SQL generation for free-form questions
      try {
        const registry = multiDatabaseEngine.registry;
        const generated = await generateSqlWithOllama(queryText, registry, pool);
        if (generated?.sql && generated?.databaseId) {
          console.log(`[AI-SQL-GEN] Generated SQL for "${queryText}" → ${generated.databaseId}: ${generated.sql.slice(0, 80)}`);
          const genRows = await registry.executeCompiledQuery({
            databaseId: generated.databaseId,
            sql: generated.sql,
            params: [],
          });
          if (Array.isArray(genRows)) {
            return res.status(200).json({
              resumenHumano: generated.explanation || `${genRows.length} resultado(s) encontrados.`,
              resultado: genRows,
              metadata: {
                entidad: generated.databaseId,
                total: genRows.length,
                executionType: 'ai-generated-sql',
                sources: [generated.databaseId],
                confidence: 0.85,
                trazabilidad: normalizeTrace({
                  interpretadoPor: 'ai-sql-gen',
                  intencion: 'generate-sql',
                  confianza: 0.85,
                }),
              },
            });
          }
        }
      } catch (genError) {
        console.warn('[AI-SQL-GEN] Fallback failed:', genError instanceof Error ? genError.message : genError);
      }

      if (engineUnresolved) return res.status(200).json(buildAmbiguityResponse(engineResult, trace));
      return res.status(200).json(buildEmptyResponse(queryText, engineResult.message, trace, engineResult));
    }

    return res.status(200).json(buildSuccessResponse(engineResult, trace));
  });

  return router;
}

export default createQueryRouter;
