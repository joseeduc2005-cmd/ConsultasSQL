/**
 * Decisiones que se toman mirando el texto, antes de hablar con ninguna base.
 *
 * Qué tabla parece ser la protagonista de lo que alguien escribió, si una sentencia
 * de escritura viene sin WHERE —y por tanto tocaría la tabla entera—, qué parámetros
 * necesita un script para poder correr, y cómo se presenta un panel a partir de unas
 * filas.
 *
 * Son reglas del negocio, no detalles de PostgreSQL. Aquí se pueden probar con
 * ejemplos raros sin levantar nada, que es lo que hacía falta sobre todo para la de
 * las escrituras sin WHERE.
 */
import { detectErrorAnalysisInput } from './errores.js';
import { resolveRequestedDatabasesForQuery } from '../bases/resolucion.js';
import { extractVariables } from '../flujos/pasos.js';
import { buildProductionSqlResponse } from '../presentacion/explicacion.js';
import { normalizeSchemaIdentifier } from '../sql/guardas.js';
import { normalizeStrictSqlRawPayload } from '../sql/relacional.js';

export function buildCrossDbSelectOptions(options = {}) {
  const explicitDatabaseId = String(options?.databaseId || '').trim();
  const requestedDatabases = Array.isArray(options?.requestedDatabases)
    ? options.requestedDatabases
    : [];
  const queryHintText = String(options?.queryHintText || '').trim();

  const resolvedRequested = resolveRequestedDatabasesForQuery(queryHintText, requestedDatabases);
  const resolvedDatabaseId = explicitDatabaseId || resolvedRequested[0] || '';

  return {
    allowCrossDatabase: true,
    databaseId: resolvedDatabaseId,
    allowUnknownTablesForTarget: true,
  };
}

export function buildAutoDashboard(rows = []) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return { chartType: 'none', reason: 'empty' };
  }

  const first = rows[0] || {};
  const keys = Object.keys(first);
  const numericKeys = keys.filter((k) => typeof first[k] === 'number');
  const dateKeys = keys.filter((k) => /(date|fecha|created_at|updated_at|time)/i.test(k));
  const categoryKeys = keys.filter((k) => typeof first[k] === 'string');

  if (numericKeys.some((k) => /count|total|sum|avg|max|min/i.test(k))) {
    return {
      chartType: 'bar',
      xKey: categoryKeys[0] || keys[0],
      yKey: numericKeys.find((k) => /count|total|sum/i.test(k)) || numericKeys[0],
    };
  }

  if (dateKeys.length > 0 && numericKeys.length > 0) {
    return {
      chartType: 'line',
      xKey: dateKeys[0],
      yKey: numericKeys[0],
    };
  }

  if (categoryKeys.length > 0 && numericKeys.length > 0) {
    return {
      chartType: 'pie',
      categoryKey: categoryKeys[0],
      valueKey: numericKeys[0],
    };
  }

  return { chartType: 'table' };
}

export function tableLooksLikeUsers(tableName) {
  const normalized = normalizeSchemaIdentifier(tableName);
  return normalized === 'users' || normalized === 'usuarios' || normalized.includes('user');
}

/**
 * Qué tabla parece ser la protagonista de lo que alguien escribió.
 *
 * Recibe la lista de tablas en vez de ir a buscarla: de dónde sale esa lista es
 * cosa de PostgreSQL, y este módulo no tiene por qué saberlo.
 */
export function detectMainTableFromText(text, tablas = []) {
  const normalized = String(text || '').toLowerCase();
  const aliases = {
    users: ['user', 'users', 'usuario', 'usuarios'],
    logs: ['log', 'logs', 'bitacora', 'bitacoras'],
    sessions: ['session', 'sessions', 'sesion', 'sesiones'],
    roles: ['role', 'roles', 'rol', 'roles'],
  };

  for (const tableName of tablas) {
    const tokens = aliases[tableName] || [tableName];
    if (tokens.some((token) => normalized.includes(token))) {
      return tableName;
    }
  }

  return tablas.find((t) => tableLooksLikeUsers(t)) || tablas[0] || '';
}

export function collectStructuredRequiredParams(script) {
  const requiredFromScript = Array.isArray(script?.parametros_requeridos)
    ? script.parametros_requeridos.map((item) => String(item || '').trim()).filter(Boolean)
    : [];

  const detectedFromPlaceholders = extractVariables(script)
    .map((item) => String(item || '').trim())
    .filter((item) => Boolean(item) && !item.includes('.'));

  return Array.from(new Set([...requiredFromScript, ...detectedFromPlaceholders]));
}

export const MANUAL_SQL_PLACEHOLDER_REGEX = /{{\s*([a-zA-Z0-9_.]+)\s*}}/g;

export function hasUnsafeMutationWithoutWhere(sqlText = '') {
  const cleaned = String(sqlText || '').trim().replace(/;\s*$/, '').trim();
  if (!/^(update|delete)\b/i.test(cleaned)) return false;
  // Sin ningún WHERE → inseguro
  if (!/\bwhere\b/i.test(cleaned)) return true;
  // WHERE trivialmente verdadero: 1=1, 1 = 1, true, 'a'='a', etc.
  const whereIdx = cleaned.toLowerCase().lastIndexOf('where');
  const afterWhere = cleaned.slice(whereIdx + 5).trim().replace(/;\s*$/, '').trim().toLowerCase();
  const trivial = [
    /^1\s*=\s*1$/,
    /^'[^']*'\s*=\s*'[^']*'$/,    // 'a'='a'
    /^true$/,
    /^1\s*=\s*1\s*(and\s+.*)?$/,  // WHERE 1=1 AND ...
  ];
  if (trivial.some((re) => re.test(afterWhere))) return true;
  return false;
}

export function detectErrorAnalysisMode(text) {
  return detectErrorAnalysisInput(text);
}

export function enforceStrictProductionSqlPayload(rawPayload) {
  const normalized = normalizeStrictSqlRawPayload(rawPayload);
  return buildProductionSqlResponse(normalized);
}

export function isStrictSqlResponsePath(requestPath = '') {
  const path = String(requestPath || '').toLowerCase();
  return [
    '/api/sql/manual',
    '/api/query',
    '/api/query/distributed',
    '/api/query/execute-generated',
  ].includes(path);
}
