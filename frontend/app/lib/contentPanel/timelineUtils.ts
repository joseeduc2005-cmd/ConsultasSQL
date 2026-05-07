// ─── Timeline and human-readable progress utilities ───────────────────────────
import { ProgressStep, TimelineTone, ParsedTimelineStep, HumanExecutionStep, ExecutionResultPayload } from './types';

export function parseTimelineStep(step: ProgressStep): ParsedTimelineStep {
  const raw = (step.message || '').trim();
  const match = raw.match(/^\[STEP\s+(\d+)\]\s*(.+)$/i);
  const content = (match?.[2] || raw).trim();
  const upper = content.toUpperCase();
  if (step.type === 'error' || upper.includes('ERROR') || upper.includes('❌')) return { title: content || 'Error de ejecución', detail: raw, tone: 'error', icon: '❌' };
  if (upper.includes('VALIDACION')) return { title: content, detail: raw, tone: 'validation', icon: '✔' };
  if (upper.includes('SELECT')) return { title: content, detail: raw, tone: 'select', icon: '🔍' };
  if (upper.includes('UPDATE') || upper.includes('INSERT') || upper.includes('DELETE')) return { title: content, detail: raw, tone: 'update', icon: '⚙' };
  return { title: content || 'Paso de ejecución', detail: raw, tone: 'info', icon: '•' };
}

export function getTimelineToneClasses(tone: TimelineTone) {
  if (tone === 'select') return { row: 'border-blue-200 bg-blue-50/60', icon: 'bg-blue-600 text-white', title: 'text-blue-800' };
  if (tone === 'validation') return { row: 'border-emerald-200 bg-emerald-50/60', icon: 'bg-emerald-600 text-white', title: 'text-emerald-800' };
  if (tone === 'update') return { row: 'border-violet-200 bg-violet-50/60', icon: 'bg-violet-600 text-white', title: 'text-violet-800' };
  if (tone === 'error') return { row: 'border-red-200 bg-red-50/60', icon: 'bg-red-600 text-white', title: 'text-red-800' };
  return { row: 'border-slate-200 bg-slate-50/70', icon: 'bg-slate-500 text-white', title: 'text-slate-700' };
}

export function isTechnicalProgressMessage(message: string): boolean {
  const upper = String(message || '').trim().toUpperCase();
  return (
    upper.includes('SQL OPERATION') || upper.includes('SQL PARAMS') || upper.includes('SQL PLACEHOLDERS') ||
    upper.startsWith('[SQL]') || upper.startsWith('[PARAMS]') || upper.startsWith('[PARAM ') ||
    upper.includes('PLACEHOLDER') || upper.includes('CANONICAL') || upper.includes('PREPARED') ||
    upper.includes('MOTOR SQL') || upper.includes('WORKFLOW COMPLETADO') || upper.includes('SCRIPT EMPRESARIAL DETECTADO')
  );
}

function inferQueryTitle(sql: string): string {
  const upper = (sql || '').trim().toUpperCase();
  const lower = (sql || '').toLowerCase();
  if (/\bCOUNT\b/.test(upper)) return 'Análisis de actividad';
  if (/\bjoin\b.*\broles?\b|\broles?\b.*\bjoin\b/i.test(lower)) return 'Obtener rol';
  if (/\bselect\b.*\busers?\b|\bfrom\b.*\busers?\b/i.test(lower)) return 'Buscar usuario';
  if (/\bUPDATE\b/.test(upper)) return 'Actualizar registro';
  if (/\bINSERT\b/.test(upper)) return 'Insertar registro';
  if (/\bDELETE\b/.test(upper)) return 'Eliminar registro';
  if (/\bSELECT\b/.test(upper)) return 'Consultar datos';
  return 'Ejecutar query';
}

function inferHumanExecutionStep(rawText: string, index: number, total: number): HumanExecutionStep {
  const lower = String(rawText || '').trim().toLowerCase();
  if (/\bjoin\b.*\brol|\brol\b.*\bjoin\b|\broles?\b/.test(lower)) return { key: 'role', startLabel: 'Obteniendo rol del usuario...', completeLabel: 'Rol obtenido', tone: 'select', icon: '🔍' };
  if (/\bcount\b|\blogs?\b|\bm[eé]tric|\btotal\b|\ban[aá]lis|\bgroup by\b|\bsum\b|\bavg\b/.test(lower)) return { key: 'analysis', startLabel: 'Analizando datos...', completeLabel: 'Datos procesados', tone: 'validation', icon: '📊' };
  if (/\bupdate\b|\binsert\b|\bdelete\b|\bproces|\bactualiz|\bmodific/.test(lower)) return { key: 'processing', startLabel: 'Procesando registros...', completeLabel: 'Cambios aplicados', tone: 'update', icon: '⚙️' };
  if (/\bif\b|\bthen\b|\bvalid|\bverific|\bcondici/.test(lower)) return { key: 'validation', startLabel: 'Validando información...', completeLabel: 'Validación completada', tone: 'validation', icon: '🛡️' };
  if (index === total - 1 || /\bresultado\b|\bfinal\b|\bgenerando\b/.test(lower)) return { key: 'result', startLabel: 'Generando resultado...', completeLabel: 'Resultado listo', tone: 'info', icon: '📈' };
  if (/\busers?\b|\busername\b|\busuario\b|\bbuscando usuario\b/.test(lower)) return { key: 'user', startLabel: 'Buscando usuario...', completeLabel: 'Usuario encontrado', tone: 'select', icon: '🔍' };
  if (/\bselect\b|\bfrom\b/.test(lower)) return { key: `query-${index}`, startLabel: 'Consultando información...', completeLabel: 'Información obtenida', tone: 'select', icon: '🔍' };
  return { key: `generic-${index}`, startLabel: 'Procesando solicitud...', completeLabel: 'Proceso completado', tone: 'info', icon: '📌' };
}

function limitHumanExecutionSteps(steps: HumanExecutionStep[]): HumanExecutionStep[] {
  if (steps.length <= 5) return steps;
  const lastStep = steps[steps.length - 1];
  const head = steps.slice(0, 4);
  if (head.some((step) => step.key === lastStep.key)) return head;
  return [...head, lastStep];
}

export function buildHumanExecutionSteps(progressSteps: ProgressStep[], result: ExecutionResultPayload | null): HumanExecutionStep[] {
  const executedQueries = Array.isArray(result?.executedQueries) ? result.executedQueries : [];
  const rawItems = executedQueries.length > 0
    ? executedQueries.map((item) => String(item?.title || item?.executedQuery || '').trim()).filter(Boolean)
    : progressSteps.filter((step) => !isTechnicalProgressMessage(step.message) && step.type !== 'error').map((step) => String(step.message || '').replace(/^\[STEP\s+\d+\]\s*/i, '').trim()).filter(Boolean);

  const uniqueSteps: HumanExecutionStep[] = [];
  const seen = new Set<string>();
  rawItems.forEach((item, index) => {
    const step = inferHumanExecutionStep(item, index, rawItems.length);
    if (seen.has(step.key)) return;
    seen.add(step.key);
    uniqueSteps.push(step);
  });
  return limitHumanExecutionSteps(uniqueSteps);
}

export function buildAnnotatedSqlScript(executedQueries: Array<{ step?: string; title?: string; executedQuery?: string; executionMs?: number }>): string {
  if (!executedQueries || executedQueries.length === 0) return '';
  return executedQueries.map((item, index) => {
    const sql = String(item?.executedQuery || '').trim();
    return `-- Paso ${index + 1}: ${inferQueryTitle(sql)}\n${sql}`;
  }).join('\n\n');
}
