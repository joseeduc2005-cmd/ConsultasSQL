/**
 * Armar el SQL que se va a ejecutar.
 *
 * De una definición estructurada —tabla, columnas, condiciones— sale la sentencia, con
 * sus marcadores y sus valores aparte. Es el punto donde una intención se convierte en
 * algo que una base entiende, y por eso importa que lo que se interpola venga del
 * esquema y los valores nunca se peguen dentro del texto.
 *
 * Dominio puro: entra una definición, sale un texto y una lista de parámetros. No
 * ejecuta nada ni sabe contra qué base irá.
 *
 * Queda fuera `buildCrossDbSelectOptions`, que necesita saber qué bases hay
 * registradas: eso es estado de la aplicación, no una regla del formato.
 */
import { ALLOWED_WHERE_OPERATORS, assertManualSqlAllowed, isPlainObject, parseQualifiedFieldRef, resolveWhereEntry } from '../scripts/validacion.js';

export const STRUCTURED_PLACEHOLDER_REGEX = /{{\s*([a-zA-Z0-9_.]+)\s*}}/g;

export function sanitizeStructuredParamValue(value) {
  if (Array.isArray(value)) {
    return value.map((item) => sanitizeStructuredParamValue(item));
  }

  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value).map(([key, nestedValue]) => [key, sanitizeStructuredParamValue(nestedValue)])
    );
  }

  if (typeof value === 'boolean' || typeof value === 'number') {
    return value;
  }

  return String(value ?? '')
    .substring(0, 255)
    .replace(/[\x00-\x1F\x7F]/g, '')
    .replace(/[\u200B-\u200D\uFEFF]/g, '')
    .trim();
}

export function inferStructuredParamSemantic(name) {
  const normalized = String(name || '').trim().toLowerCase();
  if (!normalized) return 'text';
  if (normalized.includes('date') || normalized.includes('fecha')) return 'date';
  if (normalized.includes('role') || normalized.includes('rol')) return 'role';
  if (normalized.includes('codigo') || normalized.includes('code')) return 'code';
  if (normalized.includes('log')) return 'log';
  if (normalized.includes('id')) return 'id';
  if (normalized.includes('name') || normalized.includes('user') || normalized.includes('usuario')) return 'user_text';
  return 'text';
}

export function validateStructuredParamByName(name, value) {
  if (isMissingParamValue(value)) {
    return;
  }

  const semantic = inferStructuredParamSemantic(name);
  const stringValue = typeof value === 'string' ? value : String(value ?? '');
  const trimmed = stringValue.trim();

  if (semantic === 'id' && /\s/.test(trimmed)) {
    throw new Error(`Parámetro inválido (${name}): no puede contener espacios`);
  }

  if (semantic === 'role') {
    if (!/^[\w\s-]{2,40}$/i.test(trimmed)) {
      throw new Error(`Parámetro inválido (${name}): formato de rol inválido`);
    }
  }

  if (semantic === 'code') {
    if (!/^[\w-]{1,60}$/i.test(trimmed)) {
      throw new Error(`Parámetro inválido (${name}): formato de código inválido`);
    }
  }

  if (semantic === 'date') {
    const parsed = new Date(trimmed);
    if (Number.isNaN(parsed.getTime())) {
      throw new Error(`Parámetro inválido (${name}): fecha inválida`);
    }
  }
}

export const MANUAL_SQL_CANONICAL_PLACEHOLDER_REGEX = /{{\s*([a-zA-Z0-9_.]+)\s*}}|:([a-zA-Z_][a-zA-Z0-9_]*)/g;

export const MANUAL_SQL_POSITIONAL_PLACEHOLDER_REGEX = /\$(\d+)/g;

export function isWriteConfirmed(rawParams = {}) {
  const flag = rawParams?.__write_confirmed;
  return flag === true || String(flag || '').trim().toLowerCase() === 'true';
}

export function collectManualSqlPlaceholderMetadata(sqlText = '') {
  const source = String(sqlText || '');
  const placeholderOrder = [];
  const userPlaceholderOrder = [];
  const contextPlaceholderOrder = [];
  const placeholderOccurrences = [];
  const seen = new Set();
  const seenUser = new Set();
  const seenContext = new Set();

  const isContextPlaceholder = (name) => /^(step\d+\.|loopRow\.)/i.test(String(name || '').trim());

  for (const match of source.matchAll(MANUAL_SQL_CANONICAL_PLACEHOLDER_REGEX)) {
    const [rawMatch = '', curlyVariable, namedVariable] = match;
    const offset = Number(match.index || 0);
    if (rawMatch.startsWith(':')) {
      const previousChar = offset > 0 ? source[offset - 1] : '';
      if (previousChar === ':') {
        continue;
      }
    }

    const variableName = String(curlyVariable || namedVariable || '').trim();
    if (!variableName) continue;
    placeholderOccurrences.push(variableName);

    if (isContextPlaceholder(variableName)) {
      if (!seenContext.has(variableName)) {
        seenContext.add(variableName);
        contextPlaceholderOrder.push(variableName);
      }
    } else if (!seenUser.has(variableName)) {
      seenUser.add(variableName);
      userPlaceholderOrder.push(variableName);
    }

    if (!seen.has(variableName)) {
      seen.add(variableName);
      placeholderOrder.push(variableName);
    }
  }

  return {
    placeholderOrder,
    userPlaceholderOrder,
    contextPlaceholderOrder,
    placeholderOccurrences,
  };
}

export function resolveWorkflowContextPlaceholder(path, workflowContext = {}, safeParams = {}) {
  const pathStr = String(path || '').trim();

  // loopRow.field → direct lookup in workflowContext.loopRow
  const loopRowMatch = pathStr.match(/^loopRow\.([a-zA-Z_][\w]*)$/i);
  if (loopRowMatch) {
    const loopRow = workflowContext?.loopRow;
    if (loopRow && typeof loopRow === 'object') {
      const val = loopRow[loopRowMatch[1]];
      if (!isMissingParamValue(val)) return val;
    }
    return undefined;
  }

  const resolved = resolveValueMultiSource(pathStr, workflowContext, safeParams);
  if (!isMissingParamValue(resolved)) {
    return resolved;
  }

  // If stepN stores an array, allow using first row field via stepN.field.
  const pathMatch = pathStr.match(/^(step\d+)\.([a-zA-Z_][\w]*)$/i);
  if (!pathMatch) {
    return undefined;
  }

  const [, stepKey, fieldName] = pathMatch;
  const stepValue = workflowContext?.[stepKey];
  if (Array.isArray(stepValue) && stepValue.length > 0) {
    const first = stepValue[0];
    if (first && typeof first === 'object') {
      return first[fieldName];
    }
  }

  return undefined;
}

export function normalizeManualSqlParamsInput(rawParams) {
  if (Array.isArray(rawParams)) {
    return rawParams.map((value, index) => {
      const normalized = sanitizeStructuredParamValue(value);
      if (isMissingParamValue(normalized)) {
        throw new Error(`Parámetro posicional inválido en posición ${index + 1}`);
      }
      return normalized;
    });
  }

  if (rawParams && typeof rawParams === 'object') {
    return Object.entries(rawParams).reduce((acc, [key, value]) => {
      if (key === '__write_confirmed') {
        acc[key] = value;
        return acc;
      }

      const normalized = sanitizeStructuredParamValue(value);
      if (!isMissingParamValue(normalized)) {
        acc[key] = normalized;
      }
      return acc;
    }, {});
  }

  return {};
}

export function countPositionalSqlPlaceholders(sqlText = '') {
  const indexes = Array.from(String(sqlText || '').matchAll(MANUAL_SQL_POSITIONAL_PLACEHOLDER_REGEX))
    .map((match) => Number(match[1]))
    .filter((value) => Number.isFinite(value) && value > 0);

  return indexes.length > 0 ? Math.max(...indexes) : 0;
}

export function isOptionalUserPlaceholder(varName = '') {
  const key = String(varName || '').trim();
  if (!key) return false;
  return !/^(step\d+\.|loopRow\.)/i.test(key);
}

export function inferManualPlaceholderCast(sqlText = '', variableName = '', offset = 0, placeholderText = '') {
  const source = String(sqlText || '');
  const varName = String(variableName || '').trim().toLowerCase();
  const token = String(placeholderText || '').trim();
  const leftWindow = source.slice(Math.max(0, offset - 120), offset);
  const rightWindow = source.slice(offset + token.length, Math.min(source.length, offset + token.length + 120));
  const around = `${leftWindow}${token}${rightWindow}`.toLowerCase();

  if (/\bilike\b/.test(around)) {
    return 'text';
  }

  const idSemantic = /(^id$|_id$|uuid|guid)/.test(varName);
  const idComparison = /([\w".]+id|uuid|guid)\s*(=|!=|<>|in\s*\(|any\s*\()/i.test(around)
    || /\b(=|!=|<>)\s*\{\{\s*[^}]+\s*\}\}|\b(=|!=|<>)\s*:[a-zA-Z_][a-zA-Z0-9_]*/i.test(around);
  if (idSemantic || idComparison) {
    return 'uuid';
  }

  const numericSemantic = /(count|total|cantidad|numero|num|qty|sum|avg|max|min|edad|anio|year|month|day)/.test(varName);
  const numericComparison = /\b(count\s*\(|sum\s*\(|avg\s*\()/.test(around)
    || /(>=|<=|>|<)\s*(\{\{|:)/.test(around)
    || /\bbetween\b/.test(around);
  if (numericSemantic || numericComparison) {
    return 'int';
  }

  return null;
}

export function applyCastToSqlPlaceholder(sqlPlaceholder = '', castType = null) {
  const placeholder = String(sqlPlaceholder || '').trim();
  const cast = String(castType || '').trim().toLowerCase();
  if (!placeholder) return placeholder;
  if (!cast) return placeholder;
  if (/(::\s*(text|uuid|int))$/i.test(placeholder)) return placeholder;
  if (cast === 'text' || cast === 'uuid' || cast === 'int') {
    return `${placeholder}::${cast}`;
  }
  return placeholder;
}

export function applyOptionalSqlFilterTransform(sqlText = '') {
  let sql = String(sqlText || '');

  // El filtro opcional: si no mandan el dato, no se filtra por el.
  //
  // El orden de los dos lados del OR no es cosmetico. Antes se generaba
  // «($1 IS NULL OR campo = $1)», y asi PostgreSQL no podia deducir el tipo del
  // parametro: lo resuelve por su primera aparicion, y en «$1 IS NULL» no hay nada
  // de donde deducirlo. El resultado era que toda consulta con un parametro fallaba
  // con «no se pudo determinar el tipo del parametro $1» — justamente el caso de un
  // articulo que pide una cedula.
  //
  // Poniendo la comparacion primero, el tipo sale de la columna. El significado es
  // el mismo porque el OR es conmutativo, y esta comprobado contra PostgreSQL: con
  // valor devuelve la fila, sin valor devuelve todas.

  // campo = :variable  ->  (campo = :variable OR :variable IS NULL)
  sql = sql.replace(
    /([\w".]+)\s*=\s*(\{\{\s*([a-zA-Z0-9_.]+)\s*\}\}|:([a-zA-Z_][a-zA-Z0-9_]*))(?!\s*\))/gi,
    (match, leftExpr, placeholderExpr, curlyName, namedName) => {
      const varName = String(curlyName || namedName || '').trim();
      if (!isOptionalUserPlaceholder(varName)) return match;
      return `(${leftExpr} = ${placeholderExpr} OR ${placeholderExpr} IS NULL)`;
    },
  );

  // campo ILIKE :variable  ->  (campo ILIKE '%' || :variable || '%' OR :variable IS NULL)
  sql = sql.replace(
    /([\w".]+)\s+ILIKE\s*(\{\{\s*([a-zA-Z0-9_.]+)\s*\}\}|:([a-zA-Z_][a-zA-Z0-9_]*))(?!\s*\|\|)/gi,
    (match, leftExpr, placeholderExpr, curlyName, namedName) => {
      const varName = String(curlyName || namedName || '').trim();
      if (!isOptionalUserPlaceholder(varName)) return match;
      return `(${leftExpr} ILIKE '%' || ${placeholderExpr} || '%' OR ${placeholderExpr} IS NULL)`;
    },
  );

  return sql;
}

export function buildManualSqlQuery(sqlText, rawParams = {}, workflowContext = {}, options = {}) {
  const writeConfirmed = isWriteConfirmed(rawParams);
  // trusted=true → artículo escrito por admin → sin restricciones de sintaxis
  // trusted=false (default) → consola directa → aplica restricciones
  assertManualSqlAllowed(sqlText, {
    allowMutations: true,
    writeConfirmed,
    assumeWriteConfirmed: Boolean(options?.assumeWriteConfirmed),
    trusted: Boolean(options?.trusted ?? true),  // artículos son trusted por defecto
  });

  const cleaned = applyOptionalSqlFilterTransform(String(sqlText || '').trim().replace(/;\s*$/, '').trim());
  const normalizedParams = normalizeManualSqlParamsInput(rawParams);
  const {
    placeholderOrder,
    userPlaceholderOrder,
    placeholderOccurrences,
  } = collectManualSqlPlaceholderMetadata(cleaned);

  if (placeholderOrder.length === 0) {
    // Flexible mode: if the query has no placeholders, run it as-is without binding params.
    // This allows multi-step workflows where some steps use params and others do not.
    const positionalCount = countPositionalSqlPlaceholders(cleaned);
    if (positionalCount === 0) {
      return {
        text: cleaned,
        values: [],
        placeholderOrder: [],
        placeholderOccurrences: [],
      };
    }

    if (Array.isArray(normalizedParams)) {
      if (positionalCount !== normalizedParams.length) {
        throw new Error(`La consulta espera ${positionalCount} parámetros posicionales y recibió ${normalizedParams.length}`);
      }
      return {
        text: cleaned,
        values: normalizedParams,
        placeholderOrder: [],
        placeholderOccurrences: [],
      };
    }

    // Support param_N convention: when the frontend detects $N placeholders, it sends
    // fields named param_1, param_2, ... — convert them to a positional array.
    if (normalizedParams && typeof normalizedParams === 'object') {
      const paramNKeys = Object.keys(normalizedParams)
        .filter((k) => /^param_\d+$/.test(k))
        .sort((a, b) => parseInt(a.slice(6), 10) - parseInt(b.slice(6), 10));
      if (paramNKeys.length === positionalCount) {
        return {
          text: cleaned,
          values: paramNKeys.map((k) => normalizedParams[k]),
          placeholderOrder: [],
          placeholderOccurrences: [],
        };
      }
    }

    throw new Error(`La consulta usa placeholders posicionales ($1..$N). Envía ${positionalCount} parámetros en arreglo.`);
  }

  const externalPlaceholderOrder = userPlaceholderOrder;
  const safeParams = Array.isArray(normalizedParams)
    ? externalPlaceholderOrder.reduce((acc, variableName, index) => {
        // Optional mode: missing user params become null instead of hard failure.
        acc[variableName] = index < normalizedParams.length ? normalizedParams[index] : null;
        return acc;
      }, {})
    : normalizedParams;

  const values = [];
  const variableIndexes = new Map();

  const registerPlaceholder = (variableName) => {
    const key = String(variableName || '').trim();
    if (!key) {
      throw new Error('Placeholder SQL inválido');
    }

    const resolvedValue = /^(step\d+\.|loopRow\.)/i.test(key)
      ? resolveWorkflowContextPlaceholder(key, workflowContext, safeParams)
      : safeParams[key];
    if (isMissingParamValue(resolvedValue)) {
      // Optional mode for user input placeholders: bind NULL and continue.
      if (isOptionalUserPlaceholder(key)) {
        if (!variableIndexes.has(key)) {
          values.push(null);
          variableIndexes.set(key, values.length);
        }
        return `$${variableIndexes.get(key)}`;
      }
      throw new Error(`Falta parámetro para SQL manual: ${key}`);
    }

    validateStructuredParamByName(key.split('.').pop(), resolvedValue);

    if (!variableIndexes.has(key)) {
      values.push(sanitizeStructuredParamValue(resolvedValue));
      variableIndexes.set(key, values.length);
    }

    return `$${variableIndexes.get(key)}`;
  };

  const text = cleaned.replace(MANUAL_SQL_CANONICAL_PLACEHOLDER_REGEX, (match, curlyVariable, namedVariable, offset, source) => {
    const variableName = String(curlyVariable || namedVariable || '').trim();
    if (!variableName) {
      throw new Error('Placeholder SQL inválido');
    }

    if (match.startsWith(':')) {
      const previousChar = offset > 0 ? String(source || '')[offset - 1] : '';
      if (previousChar === ':') {
        return match;
      }
    }

    const sqlPlaceholder = registerPlaceholder(variableName);
    // ::cast syntax is PostgreSQL-only — skip for MySQL, Oracle, etc.
    const isPostgresDialect = !options?.dbType || String(options.dbType).toLowerCase() === 'postgres';
    const castType = isPostgresDialect
      ? inferManualPlaceholderCast(String(source || ''), variableName, Number(offset || 0), match)
      : null;
    return applyCastToSqlPlaceholder(sqlPlaceholder, castType);
  });

  if (placeholderOccurrences.length > 0 && values.length === 0) {
    throw new Error('Consulta con placeholders pero sin parámetros utilizables');
  }

  if (placeholderOrder.length !== values.length) {
    throw new Error('Mismatch entre placeholders y parámetros preparados');
  }

  return { text, values, placeholderOrder, placeholderOccurrences };
}

export function isMissingParamValue(value) {
  return value === undefined || value === null || (typeof value === 'string' && value.trim() === '');
}

export function getValueByPath(source, path) {
  if (!source || typeof source !== 'object') {
    return undefined;
  }

  if (Object.prototype.hasOwnProperty.call(source, path)) {
    return source[path];
  }

  return String(path)
    .split('.')
    .reduce((acc, key) => (acc !== undefined && acc !== null ? acc[key] : undefined), source);
}

export function resolveValueMultiSource(path, context, params) {
  // Busca primero en context (datos internos del workflow), luego en params (entrada del usuario)
  const pathObj = getValueByPath(context, path);
  if (pathObj !== undefined) {
    return pathObj;
  }
  return getValueByPath(params, path);
}

export function resolveValue(value, context, params) {
  if (typeof value !== 'string') {
    return value;
  }

  const fullMatch = value.match(/^{{\s*([a-zA-Z0-9_.]+)\s*}}$/);
  if (fullMatch) {
    const resolved = resolveValueMultiSource(fullMatch[1], context, params);
    if (isMissingParamValue(resolved)) {
      throw new Error(`Falta parámetro: ${fullMatch[1]}`);
    }
    return sanitizeStructuredParamValue(resolved);
  }

  return value.replace(STRUCTURED_PLACEHOLDER_REGEX, (_, rawPath) => {
    const path = String(rawPath || '').trim();
    const resolved = resolveValueMultiSource(path, context, params);
    if (isMissingParamValue(resolved)) {
      throw new Error(`Falta parámetro: ${path}`);
    }
    return String(sanitizeStructuredParamValue(resolved));
  });
}

export function buildAnalyticSelectQuery(script, normalizedInputParams, workflowContext = {}) {
    const baseTable = String(script.tabla || '').trim().toLowerCase();
    const joins = Array.isArray(script.join) ? script.join : [];
    const groupByCols = Array.isArray(script.group_by) ? script.group_by.filter(Boolean) : [];
    const aggregates = Array.isArray(script.aggregates) ? script.aggregates : [];
    const orderByItems = Array.isArray(script.order_by) ? script.order_by : [];
    const havingItems = Array.isArray(script.having) ? script.having : [];

    // -- JOIN clauses ---------------------------------------------------------
    const joinClauses = joins.map((joinItem) => {
      const joinTable = String(joinItem.tabla || '').trim().toLowerCase();
      const onObject = joinItem.on || {};
      const onParts = Object.entries(onObject).map(([leftRef, rightRef]) => {
        const left = parseQualifiedFieldRef(leftRef, baseTable);
        const right = parseQualifiedFieldRef(rightRef);
        return `"${left.table}"."${left.column}" = "${right.table}"."${right.column}"`;
      });
      return `JOIN "${joinTable}" ON ${onParts.join(' AND ')}`;
    });

    // -- WHERE ----------------------------------------------------------------
    const resolvedWhere = isPlainObject(script.where)
      ? resolveDynamicScriptNode(script.where, normalizedInputParams, workflowContext)
      : {};
    const whereEntries = Object.entries(resolvedWhere || {});
    const whereValues = [];
    const whereConditions = whereEntries.map(([fieldRef, rawValue]) => {
      const { table, column } = parseQualifiedFieldRef(fieldRef, baseTable);
      const { op, value, fieldRef: rightFieldRef } = resolveWhereEntry(rawValue);
      if (rightFieldRef) {
        const right = parseQualifiedFieldRef(rightFieldRef, baseTable);
        return `"${table}"."${column}" ${op} "${right.table}"."${right.column}"`;
      }
      whereValues.push(value);
      return `"${table}"."${column}" ${op} $${whereValues.length}`;
    });
    const whereClause = whereConditions.length ? ` WHERE ${whereConditions.join(' AND ')}` : '';

    // -- SELECT list: group-by cols + aggregates ------------------------------
    const groupByColsSql = groupByCols.map((col) => {
      const ref = parseQualifiedFieldRef(col, baseTable);
      return `"${ref.table}"."${ref.column}"`;
    });

    const ALLOWED_AGG_FUNCS = new Set(['COUNT', 'COUNT DISTINCT', 'SUM', 'AVG', 'MAX', 'MIN']);
    const aggregateExpressions = aggregates.map((agg) => {
      const func = String(agg.func || 'COUNT').toUpperCase().trim();
      if (!ALLOWED_AGG_FUNCS.has(func)) throw new Error(`Función de agregación no permitida: ${func}`);
      const colRaw = String(agg.column || '*').trim();
      const alias = String(agg.alias || '').trim();
      let colSql;
      if (colRaw === '*') {
        colSql = '*';
      } else {
        const ref = parseQualifiedFieldRef(colRaw, baseTable);
        colSql = `"${ref.table}"."${ref.column}"`;
      }
      const expr = func === 'COUNT DISTINCT'
        ? `COUNT(DISTINCT ${colSql})`
        : `${func}(${colSql})`;
      return alias ? `${expr} AS "${alias}"` : expr;
    });

    const selectParts = [...groupByColsSql, ...aggregateExpressions];
    const selectSql = selectParts.length > 0 ? selectParts.join(', ') : '*';

    // -- GROUP BY -------------------------------------------------------------
    const groupBySql = groupByColsSql.length > 0
      ? ` GROUP BY ${groupByColsSql.join(', ')}`
      : '';

    // -- HAVING ---------------------------------------------------------------
    // Build HAVING by finding the aggregate expression for each alias
    const aggByAlias = {};
    aggregates.forEach((agg, i) => {
      if (agg.alias) aggByAlias[agg.alias] = aggregateExpressions[i].split(' AS ')[0]; // raw expr without alias
    });
    const havingConditions = havingItems
      .filter((h) => h.alias && aggByAlias[h.alias] && h.op && h.value !== '' && h.value !== undefined)
      .map((h) => {
        const op = ALLOWED_WHERE_OPERATORS.has(h.op) ? h.op : '=';
        const val = Number.isFinite(Number(h.value)) ? Number(h.value) : `'${String(h.value).replace(/'/g, "''")}'`;
        return `${aggByAlias[h.alias]} ${op} ${val}`;
      });
    const havingClause = havingConditions.length > 0 ? ` HAVING ${havingConditions.join(' AND ')}` : '';

    // -- ORDER BY -------------------------------------------------------------
    const orderBySql = orderByItems
      .filter((ob) => ob.column)
      .map((ob) => {
        const dir = String(ob.direction || 'ASC').toUpperCase() === 'DESC' ? 'DESC' : 'ASC';
        // If ordering by an alias (aggregate), use it directly; otherwise qualify
        if (aggByAlias[ob.column] !== undefined) return `"${ob.column}" ${dir}`;
        const ref = parseQualifiedFieldRef(ob.column, baseTable);
        return `"${ref.table}"."${ref.column}" ${dir}`;
      });
    const orderByClause = orderBySql.length > 0 ? ` ORDER BY ${orderBySql.join(', ')}` : '';

    const limitValue = normalizeLimitValue(script.limit, 0);
    const limitClause = limitValue > 0 ? ` LIMIT ${limitValue}` : '';

    return {
      baseTable,
      whereValues,
      query: `SELECT ${selectSql} FROM "${baseTable}" ${joinClauses.join(' ')}${whereClause}${groupBySql}${havingClause}${orderByClause}${limitClause}`.trim(),
    };
}

export function resolveDynamicScriptNode(node, params, context = {}) {
  if (typeof node === 'string') {
    return resolveValue(node, context, params);
  }

  if (Array.isArray(node)) {
    return node.map((item) => resolveDynamicScriptNode(item, params, context));
  }

  if (node && typeof node === 'object') {
    return Object.fromEntries(
      Object.entries(node).map(([key, value]) => [key, resolveDynamicScriptNode(value, params, context)])
    );
  }

  return node;
}

export function normalizeLimitValue(limitValue, defaultValue = 0) {
  if (limitValue === undefined || limitValue === null || limitValue === '') {
    return defaultValue;
  }

  const parsed = Number(limitValue);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error('Límite inválido');
  }

  return Math.min(Math.floor(parsed), 100);
}
