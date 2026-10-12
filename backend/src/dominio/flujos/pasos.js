/**
 * Las reglas de un flujo: condiciones, variables y el orden de los pasos.
 *
 * Un artículo puede ser más que una consulta: puede ser una secuencia de pasos con
 * condiciones («si el saldo es cero, salta»), bucles sobre las filas de un paso
 * anterior, y variables que llevan el resultado de un paso al siguiente. Esto es lo
 * que decide todo eso.
 *
 * Decidir no es ejecutar, y esa es la línea: aquí se resuelve si una condición se
 * cumple y qué valor tiene una variable. Quien después abra una conexión y lance la
 * sentencia es la capa de aplicación. Por eso esto es código puro y se puede probar
 * entero sin levantar una base — que es justo lo que no se podía hacer cuando vivía
 * dentro de app.js.
 */
import { detectSqlOperation } from '../scripts/validacion.js';
import { MANUAL_SQL_NAMED_PLACEHOLDER_REGEX, parseStructuredScript } from '../scripts/estructurado.js';
import { STRUCTURED_PLACEHOLDER_REGEX, getValueByPath, isMissingParamValue, resolveWorkflowContextPlaceholder, sanitizeStructuredParamValue } from '../sql/construccion.js';
import { stripLeadingSqlComments } from '../sql/guardas.js';

export function getManualSqlStepTitle(stepSql = '', stepNumber = 1) {
  const raw = String(stepSql || '');
  const commentMatch = raw.match(/^\s*--\s*Paso\s*\d+\s*:\s*([^\n\r]+)/i);
  if (commentMatch && commentMatch[1]) {
    return String(commentMatch[1]).trim();
  }

  const cleaned = stripLeadingSqlComments(raw).trim();
  const operation = detectSqlOperation(cleaned);
  if (operation === 'select') return `Paso ${stepNumber}: Consultando datos`;
  if (operation === 'update') return `Paso ${stepNumber}: Actualizando registros`;
  if (operation === 'insert') return `Paso ${stepNumber}: Insertando registros`;
  if (operation === 'delete') return `Paso ${stepNumber}: Eliminando registros`;
  return `Paso ${stepNumber}: Procesando`;
}

export function evaluateProceduralExpression(rawExpr, workflowContext = {}) {
  const expr = String(rawExpr || '').trim();
  if (!expr) return '';

  const placeholderMatch = expr.match(/^{{\s*([a-zA-Z0-9_.]+)\s*}}$/);
  if (placeholderMatch) {
    const resolved = resolveWorkflowContextPlaceholder(placeholderMatch[1], workflowContext, workflowContext);
    return isMissingParamValue(resolved) ? '' : resolved;
  }

  if (/^'.*'$/s.test(expr)) {
    return expr.replace(/^'(.*)'$/s, '$1').replace(/''/g, "'");
  }

  if (/^-?\d+(?:\.\d+)?$/.test(expr)) {
    return Number(expr);
  }

  if (/^(true|false)$/i.test(expr)) {
    return expr.toLowerCase() === 'true';
  }

  const contextual = resolveWorkflowContextPlaceholder(expr, workflowContext, workflowContext);
  if (!isMissingParamValue(contextual)) return contextual;

  if (Object.prototype.hasOwnProperty.call(workflowContext || {}, expr)) {
    return workflowContext[expr];
  }

  const lowered = expr.toLowerCase();
  if (Object.prototype.hasOwnProperty.call(workflowContext || {}, lowered)) {
    return workflowContext[lowered];
  }

  return expr;
}

export function formatRaiseNoticeMessage(template, args = []) {
  let index = 0;
  const text = String(template || '').replace(/%/g, () => {
    const value = index < args.length ? args[index] : '';
    index += 1;
    return String(value ?? '');
  });
  return text.trim();
}

export function evaluateSimpleCondition(expr, workflowContext) {
  // Handle simple patterns like "v_count > 0", "step1.count > 0" or "{{step1.count}} > 0"
  const clean = String(expr || '').trim().replace(/\{\{\s*([a-zA-Z0-9_.]+)\s*\}\}/g, '$1');
  const m = clean.match(/^([a-zA-Z_][\w.]*)\s*(>=|<=|!=|<>|=|>|<)\s*(.+)$/i);
  if (!m) return true; // can't evaluate → assume true

  const [, lhs, op, rhs] = m;

  const resolveToken = (tokenRaw) => {
    const token = String(tokenRaw || '').trim().replace(/^['"]|['"]$/g, '');
    if (token === '') return null;
    if (/^(true|false)$/i.test(token)) return token.toLowerCase() === 'true';
    if (/^-?\d+(?:\.\d+)?$/.test(token)) return Number(token);

    const contextual = resolveWorkflowContextPlaceholder(token, workflowContext, workflowContext);
    if (!isMissingParamValue(contextual)) return contextual;

    if (Object.prototype.hasOwnProperty.call(workflowContext || {}, token)) {
      return workflowContext[token];
    }
    if (Object.prototype.hasOwnProperty.call(workflowContext || {}, token.toLowerCase())) {
      return workflowContext[token.toLowerCase()];
    }
    return token;
  };

  const lhsVal = resolveToken(lhs);
  const rhsVal = resolveToken(rhs);

  const lNum = Number(lhsVal);
  const rNum = Number(rhsVal);
  const bothNumeric = Number.isFinite(lNum) && Number.isFinite(rNum);

  if (op === '>') return bothNumeric ? lNum > rNum : String(lhsVal) > String(rhsVal);
  if (op === '<') return bothNumeric ? lNum < rNum : String(lhsVal) < String(rhsVal);
  if (op === '>=') return bothNumeric ? lNum >= rNum : String(lhsVal) >= String(rhsVal);
  if (op === '<=') return bothNumeric ? lNum <= rNum : String(lhsVal) <= String(rhsVal);
  if (op === '=') return bothNumeric ? lNum === rNum : String(lhsVal) === String(rhsVal);
  if (op === '!=' || op === '<>') return bothNumeric ? lNum !== rNum : String(lhsVal) !== String(rhsVal);
  return true;
}

export function parseStepOrder(stepKey = '') {
  const match = String(stepKey || '').match(/^step(\d+)$/i);
  return match ? Number(match[1]) : Number.MAX_SAFE_INTEGER;
}

export function collectStructuredVariables(node, variables = new Set()) {
  if (typeof node === 'string') {
    let match;
    while ((match = STRUCTURED_PLACEHOLDER_REGEX.exec(node)) !== null) {
      variables.add(match[1]);
    }
    STRUCTURED_PLACEHOLDER_REGEX.lastIndex = 0;

    let sqlMatch;
    while ((sqlMatch = MANUAL_SQL_NAMED_PLACEHOLDER_REGEX.exec(node)) !== null) {
      variables.add(sqlMatch[2]);
    }
    MANUAL_SQL_NAMED_PLACEHOLDER_REGEX.lastIndex = 0;

    return variables;
  }

  if (Array.isArray(node)) {
    node.forEach((item) => collectStructuredVariables(item, variables));
    return variables;
  }

  if (node && typeof node === 'object') {
    Object.values(node).forEach((value) => collectStructuredVariables(value, variables));
  }

  return variables;
}

export function extractVariables(scriptJson) {
  const parsedScript = parseStructuredScript(scriptJson);
  if (!parsedScript || typeof parsedScript !== 'object') {
    return [];
  }

  return Array.from(collectStructuredVariables(parsedScript));
}

export function interpolateStructuredNode(node, rawParams) {
  if (typeof node === 'string') {
    const fullMatch = node.match(/^{{\s*([a-zA-Z0-9_]+)\s*}}$/);
    if (fullMatch) {
      return sanitizeStructuredParamValue(rawParams[fullMatch[1]]);
    }

    return node.replace(STRUCTURED_PLACEHOLDER_REGEX, (_, variableName) => {
      return String(sanitizeStructuredParamValue(rawParams[variableName]));
    });
  }

  if (Array.isArray(node)) {
    return node.map((item) => interpolateStructuredNode(item, rawParams));
  }

  if (node && typeof node === 'object') {
    return Object.fromEntries(
      Object.entries(node).map(([key, value]) => [key, interpolateStructuredNode(value, rawParams)])
    );
  }

  return node;
}

export function replaceVariables(scriptJson, inputs) {
  const parsedScript = parseStructuredScript(scriptJson);
  if (!parsedScript) {
    return null;
  }

  const safeInputs = Object.entries(inputs || {}).reduce((acc, [key, value]) => {
    acc[key] = sanitizeStructuredParamValue(value);
    return acc;
  }, {});

  return interpolateStructuredNode(parsedScript, safeInputs);
}

export function resolveTemplateVariable(path, source) {
  return getValueByPath(source, path);
}

export function isEmptyWorkflowValue(value) {
  if (value === undefined || value === null) return true;
  if (typeof value === 'string' && value.trim() === '') return true;
  if (Array.isArray(value)) return value.length === 0;
  if (typeof value === 'object') return Object.keys(value).length === 0;
  return false;
}

export function normalizeWorkflowStoredValue(stepResult) {
  if (stepResult && Array.isArray(stepResult.data)) {
    if (stepResult.data.length === 1) {
      return stepResult.data[0];
    }
    return stepResult.data;
  }

  if (stepResult && stepResult.data !== undefined) {
    return stepResult.data;
  }

  return stepResult;
}

export function evaluateValidationCondition(actual, condition, expected) {
  const normalized = String(condition || 'igual').trim().toLowerCase();

  if (normalized === 'igual') return actual === expected;
  if (normalized === 'diferente') return actual !== expected;
  if (normalized === 'mayor') return Number(actual) > Number(expected);
  if (normalized === 'menor') return Number(actual) < Number(expected);
  if (normalized === 'contiene') return String(actual ?? '').includes(String(expected ?? ''));

  throw new Error(`Condición no soportada: ${condition}`);
}

export function evaluateWorkflowValidationCondition(actual, condition, expected) {
  const normalized = String(condition || 'existe').trim().toLowerCase();

  if (normalized === 'existe') return !isEmptyWorkflowValue(actual);
  if (normalized === 'no_existe') return isEmptyWorkflowValue(actual);
  if (normalized === 'igual') {
    if (typeof actual === 'object' || typeof expected === 'object') {
      return JSON.stringify(actual) === JSON.stringify(expected);
    }
    return actual === expected;
  }

  throw new Error(`Condición no soportada: ${condition}`);
}
