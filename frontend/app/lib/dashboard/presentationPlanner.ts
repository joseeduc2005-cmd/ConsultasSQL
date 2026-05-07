// ─── Presentation mode planning ───────────────────────────────────────────────
import { AutoDashboard, AutoQueryResponse, PresentationPlan } from './types';
import { inferDashboardFromRows } from './chartInference';

export function normalizeText(value: string): string {
  return String(value || '').toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '');
}

export function extractTraceIntent(result: AutoQueryResponse | null): string {
  const payload = result?.resultado;
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return 'unknown';
  const maybeTrace = (payload as Record<string, unknown>)?.trazabilidad;
  if (!maybeTrace || typeof maybeTrace !== 'object' || Array.isArray(maybeTrace)) return 'unknown';
  const intent = (maybeTrace as Record<string, unknown>)?.intencion;
  return String(intent || 'unknown').trim().toLowerCase();
}

export function inferPresentationPlan(
  rows: Record<string, unknown>[],
  userQuery: string,
  dashboardHint?: AutoDashboard,
  traceIntent = 'unknown',
): PresentationPlan {
  if (!rows.length) return { mode: 'text', dashboard: { chartType: 'none' }, headline: 'No hay datos para mostrar', reason: 'La consulta fue válida, pero no devolvió filas.' };

  const normalizedQuery = normalizeText(userQuery);
  const dashboard = dashboardHint || inferDashboardFromRows(rows, userQuery);
  const firstRow = rows[0] || {};
  const keys = Object.keys(firstRow);
  const looksLikeEntityListing = /(dame|listar|muestra|mostrar|trae|ensename|enseñame|lista).*(usuarios|users|logs|sesiones|roles|tablas)/.test(normalizedQuery)
    || /(usuarios|users|logs|sesiones|roles|tablas).*(base de datos|bd|database)/.test(normalizedQuery);
  const numericCount = keys.filter((k) => {
    const value = firstRow[k];
    return value !== null && value !== undefined && value !== '' && !Number.isNaN(Number(value));
  }).length;

  if (dashboard.chartType === 'kpi') return { mode: 'kpi', dashboard, headline: 'Resultado puntual detectado', reason: dashboard.reason || 'Una sola fila con métrica principal.' };

  if (/count-rows|list-columns/.test(traceIntent)) {
    return { mode: dashboard.chartType === 'none' ? 'table' : 'chart-table', dashboard, headline: traceIntent === 'count-rows' ? 'Conteo total' : 'Estructura de columnas', reason: 'La intención detectada sugiere vista estructurada.' };
  }

  const hasNumericSeries = rows.some((row) => Object.values(row || {}).some((value) => value !== null && value !== undefined && value !== '' && Number.isFinite(Number(value))));

  if ((/ranked|top|mas|menos/.test(normalizedQuery) || traceIntent.includes('ranked')) && hasNumericSeries) {
    return { mode: 'chart-table', dashboard: { ...dashboard, chartType: 'bar', reason: 'ranking detectado' }, headline: 'Ranking detectado', reason: 'Para comparar posiciones, barras es más legible.' };
  }
  if ((/ranked|top|mas|menos/.test(normalizedQuery) || traceIntent.includes('ranked')) && !hasNumericSeries) {
    return { mode: 'table', dashboard: { ...dashboard, chartType: 'none', reason: 'ranking sin métrica numérica clara' }, headline: 'Resultado listado', reason: 'No se detectó una métrica numérica confiable para graficar ranking.' };
  }

  if (looksLikeEntityListing && keys.length >= 5 && numericCount <= 1) {
    return { mode: 'table', dashboard: { ...dashboard, chartType: 'none', reason: 'listado detallado detectado' }, headline: 'Vista tabular recomendada', reason: 'La consulta solicita un listado detallado; tabla priorizada para lectura completa.' };
  }

  if (dashboard.chartType !== 'none') {
    return { mode: rows.length > 12 || numericCount > 0 ? 'chart-table' : 'chart', dashboard, headline: 'Visualización recomendada automáticamente', reason: dashboard.reason || 'Se detectó una estructura apta para visualización.' };
  }

  if (rows.length <= 6 && keys.length <= 4) {
    return { mode: 'text', dashboard, headline: 'Formato narrativo recomendado', reason: 'Pocos datos: lectura directa más rápida que gráfica.' };
  }

  return { mode: 'table', dashboard, headline: 'Tabla recomendada', reason: 'Estructura tabular densa o sin eje numérico claro.' };
}
