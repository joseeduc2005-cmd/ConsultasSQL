// ─── Execution result parsing and summarization ───────────────────────────────
import { ExecutionResultPayload, ProgressStep } from './types';

export function sanitizeExecutionData(value: unknown): unknown {
  if (Array.isArray(value)) return value.map((item) => sanitizeExecutionData(item));
  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .filter(([key]) => !['password', 'token', 'secret', 'authorization'].includes(key.toLowerCase()))
        .map(([key, nestedValue]) => [key, sanitizeExecutionData(nestedValue)])
    );
  }
  return value;
}

export function getFinalExecutionPayload(result: ExecutionResultPayload | null): ExecutionResultPayload | null {
  if (!result) return null;
  const final = result.resultado_final;
  if (final && typeof final === 'object' && !Array.isArray(final)) return final as ExecutionResultPayload;
  return result;
}

export function getResultRows(result: ExecutionResultPayload | null): Record<string, unknown>[] {
  const rows = getFinalExecutionPayload(result)?.data;
  if (!Array.isArray(rows)) return [];
  return rows.filter((item): item is Record<string, unknown> => Boolean(item && typeof item === 'object'));
}

function describeRecord(record: Record<string, unknown>): string[] {
  const details: string[] = [];
  for (const [key, value] of Object.entries(record)) {
    const lower = key.toLowerCase();
    if (/(^id$|_id$|token|password|secret)/.test(lower)) continue;
    if (value === null || value === undefined || String(value).trim() === '') continue;
    const label = key.replace(/_/g, ' ').toLowerCase();
    details.push(typeof value === 'boolean' ? `${label}: ${value ? 'sí' : 'no'}` : `${label}: ${String(value)}`);
    if (details.length >= 3) break;
  }
  return details;
}

export function buildExecutionFindings(result: ExecutionResultPayload | null): string[] {
  const rows = getResultRows(result);
  if (rows.length === 0) return [];
  const findings: string[] = [];
  for (const [key, value] of Object.entries(rows[0])) {
    const lower = key.toLowerCase();
    if (/(^id$|_id$|token|password|secret)/.test(lower)) continue;
    if (value === null || value === undefined || String(value).trim() === '') continue;
    const label = key.replace(/_/g, ' ');
    if (typeof value === 'boolean') findings.push(`${label}: ${value ? 'Sí' : 'No'}`);
    else if (/(date|fecha|time|created|updated)/i.test(key) && typeof value === 'string') {
      const parsed = new Date(value);
      findings.push(`${label}: ${Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleString('es-ES')}`);
    } else findings.push(`${label}: ${String(value)}`);
    if (findings.length >= 5) break;
  }
  return findings;
}

export function buildExecutionSummary(
  progressSteps: ProgressStep[],
  execSuccess: boolean,
  executionResult: ExecutionResultPayload | null,
): string {
  const combined = progressSteps.map((step) => step.message.toLowerCase()).join(' | ');
  const finalPayload = getFinalExecutionPayload(executionResult);
  const rows = getResultRows(executionResult);

  if (execSuccess && rows.length > 0) {
    const details = describeRecord(rows[0]);
    const base = rows.length === 1 ? 'El proceso finalizó correctamente y encontró 1 resultado.' : `El proceso finalizó correctamente y encontró ${rows.length} resultados.`;
    return details.length > 0 ? `${base} Resultado principal: ${details.join(', ')}.` : base;
  }
  if (execSuccess && typeof finalPayload?.affectedRows === 'number') {
    const count = finalPayload.affectedRows;
    return `El proceso finalizó correctamente y aplicó cambios en ${count} ${count === 1 ? 'fila' : 'filas'}.`;
  }
  if (execSuccess) {
    if (combined.includes('usuario no existe') || combined.includes('no se encontraron')) return 'La ejecución terminó correctamente, pero no se encontraron datos del usuario para aplicar cambios.';
    if (combined.includes('update ejecutado') || combined.includes('filas afectadas')) return 'El usuario fue validado y se aplicaron los cambios esperados sin errores.';
    if (combined.includes('validacion ok')) return 'Todas las validaciones del flujo se cumplieron y el proceso finalizó de forma correcta.';
    return 'El proceso finalizó correctamente y el resultado está listo.';
  }
  const backendMessage = String(executionResult?.message || '').trim();
  if (backendMessage) return backendMessage;
  if (combined.includes('falta parámetro')) return 'La ejecución falló porque faltan datos obligatorios en el formulario.';
  if (combined.includes('usuario no existe')) return 'La ejecución se detuvo porque el usuario indicado no existe en el sistema.';
  if (combined.includes('clave') || combined.includes('password') || combined.includes('contrase')) return 'Se detectó un problema relacionado con credenciales. Revisa usuario y contraseña.';
  if (combined.includes('login') || combined.includes('sesion')) return 'La ejecución reportó un problema en autenticación o sesión.';
  return 'No fue posible completar el proceso. Revisa los datos ingresados o intenta nuevamente.';
}

export function buildHumanExecutionExplanation(result: ExecutionResultPayload | null, fallbackSummary: string) {
  if (!result) return fallbackSummary;
  if (typeof result.resumenHumano === 'string' && result.resumenHumano.trim()) return result.resumenHumano.trim();
  if (typeof result.explicacion === 'string' && result.explicacion.trim()) return result.explicacion.trim();
  const rows = getResultRows(result);
  if (rows.length === 0) return fallbackSummary;
  const first = rows[0];
  const parts: string[] = [];
  const entries = Object.entries(first)
    .filter(([key, value]) => {
      const lower = key.toLowerCase();
      if (/(^id$|_id$|token|password|secret)/.test(lower)) return false;
      return value !== null && value !== undefined && String(value).trim() !== '';
    })
    .slice(0, 4);
  for (const [key, value] of entries) {
    const label = key.replace(/_/g, ' ').toLowerCase();
    if (typeof value === 'number' && /(count|total|cantidad|monto|importe|amount|price|precio|valor)/i.test(key)) {
      parts.push(`El total de ${label} es ${value}.`);
    } else if (typeof value === 'boolean') {
      parts.push(`${label}: ${value ? 'sí' : 'no'}.`);
    } else {
      parts.push(`${label}: ${String(value)}.`);
    }
  }
  if (parts.length === 0) return fallbackSummary;
  return parts.join(' ').replace(/\s+/g, ' ').trim();
}
