/**
 * Juntar en un solo resultado lo que devolvieron varios pasos.
 *
 * Un flujo de cinco pasos devuelve cinco cosas, y a quien lo lanzó hay que darle una.
 * Esto decide cómo se juntan: qué se agrega, qué claves se conservan cuando dos pasos
 * devuelven un campo con el mismo nombre, y cómo se cuenta en castellano lo que acabó
 * pasando.
 *
 * Es código puro. No decide qué se ejecuta ni toca una base: solo mira lo que ya
 * ocurrió y lo presenta.
 */
import { parseStepOrder } from '../flujos/pasos.js';
import { buildHumanSqlExplanation } from './explicacion.js';

/**
 * If any step produced a detailed write explanation (UPDATE/DELETE/INSERT
 * antes→después), builds a per-step breakdown so the user sees exactly what
 * each step changed. Returns null when no step has write detail.
 */
export function buildPerStepWriteSummary(executions = []) {
  const list = Array.isArray(executions) ? executions : [];
  const detailedSteps = list.filter(
    (ex) => typeof ex.explicacion === 'string'
      && /^✅\s+(UPDATE|DELETE|INSERT)\b/i.test(ex.explicacion.trim()),
  );
  if (detailedSteps.length === 0) return null;

  const affectedTotal = list.reduce((sum, ex) => sum + Number(ex.affectedRows || 0), 0);
  const stepCount = list.length;
  const blocks = detailedSteps.map((ex, idx) => {
    const title = String(ex.title || `Paso ${idx + 1}`).trim();
    return `▸ ${title}\n${ex.explicacion.trim()}`;
  });
  const header = `El script ejecutó ${stepCount} ${stepCount === 1 ? 'paso' : 'pasos'}`
    + (affectedTotal > 0 ? ` y afectó ${affectedTotal} fila${affectedTotal !== 1 ? 's' : ''} en total.` : '.');
  return `${header}\n\n${blocks.join('\n\n')}`;
}

export function buildComplexWorkflowExplanation(steps, executions, finalResult) {
  const rows = finalResult?.data || [];
  const loopSteps = steps.filter((s) => s.type === 'for_loop');
  const affectedTotal = executions.reduce((sum, ex) => sum + (ex.affectedRows || 0), 0);

  // Per-step write detail (UPDATE/DELETE/INSERT antes→después)
  const perStepSummary = buildPerStepWriteSummary(executions);
  if (perStepSummary) return perStepSummary;

  if (loopSteps.length > 0 && affectedTotal > 0) {
    return `El script procesó registros en ${loopSteps.length} loop(s) y afectó ${affectedTotal} filas en total.`;
  }
  if (rows.length > 0) {
    return buildHumanSqlExplanation(rows);
  }
  if (affectedTotal > 0) {
    return `El script ejecutó ${executions.length} pasos y afectó ${affectedTotal} filas.`;
  }
  return `El script se ejecutó correctamente en ${executions.length} ${executions.length === 1 ? 'paso' : 'pasos'}.`;
}

export function extractObjectForAggregation(value) {
  if (Array.isArray(value)) {
    const firstObj = value.find((item) => item && typeof item === 'object' && !Array.isArray(item));
    return firstObj || null;
  }
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    return value;
  }
  return null;
}

export function addWithUniqueKey(target, key, value, stepKey) {
  const baseKey = String(key || '').trim();
  if (!baseKey) return;

  if (!(baseKey in target)) {
    target[baseKey] = value;
    return;
  }

  if (target[baseKey] === value) return;

  const safeStep = String(stepKey || 'step').toLowerCase();
  let candidate = `${baseKey}_${safeStep}`;
  if (!(candidate in target)) {
    target[candidate] = value;
    return;
  }

  let suffix = 2;
  while (`${candidate}_${suffix}` in target) suffix += 1;
  target[`${candidate}_${suffix}`] = value;
}

export function buildAggregatedResultFromStepPayload(stepPayload = {}) {
  const aggregated = {};
  let hasData = false;

  const entries = Object.entries(stepPayload || {})
    .sort((a, b) => parseStepOrder(a[0]) - parseStepOrder(b[0]));

  for (const [stepKey, stepValue] of entries) {
    const sourceObj = extractObjectForAggregation(stepValue);
    if (!sourceObj) continue;

    for (const [field, value] of Object.entries(sourceObj)) {
      if (value === undefined || value === null) continue;
      addWithUniqueKey(aggregated, field, value, stepKey);
      hasData = true;
    }
  }

  return hasData ? aggregated : null;
}

export function buildDistributedSyntheticSql(scripts = []) {
  return (scripts || [])
    .map((script) => {
      const db = String(script?.databaseId || '').trim() || 'unknown-db';
      const sql = String(script?.sql || '').trim();
      return `-- ${db}\n${sql}`;
    })
    .filter(Boolean)
    .join('\n\n');
}
