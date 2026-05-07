// ─── Chart type inference from row data ───────────────────────────────────────
import { AutoDashboard } from './types';
import { formatCellValue } from './displayUtils';

export const PIE_COLORS = ['#0ea5e9', '#22d3ee', '#14b8a6', '#10b981', '#f59e0b', '#f97316', '#8b5cf6', '#6366f1'];
export const HISTOGRAM_BAR_COLOR = '#06b6d4';

export function inferDashboardFromRows(rows: Record<string, unknown>[], userQuery = ''): AutoDashboard {
  if (!Array.isArray(rows) || rows.length < 1) return { chartType: 'none' };
  const keys = Object.keys(rows[0] || {});
  if (keys.length < 1) return { chartType: 'none' };

  const normalizedQuery = String(userQuery || '').toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '');
  const prefersPie = /(porcentaje|proporcion|distribucion|participacion|reparto|segmentacion)/.test(normalizedQuery);
  const prefersCount = /(cantidad|cuantos|cuantas|conteo|total|mas|menos|top)/.test(normalizedQuery);
  const prefersUserDimension = /(usuario|usuarios|user|users|nombre|username)/.test(normalizedQuery);

  const sample = rows.slice(0, Math.min(12, rows.length));
  const isUuidLike = (v: string) => /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(v);
  const isReadableCategoryValue = (value: unknown) => {
    if (value === null || value === undefined) return false;
    if (typeof value === 'number' || typeof value === 'boolean') return true;
    if (value instanceof Date) return true;
    if (typeof value === 'object') return false;
    const text = String(value).trim();
    if (!text || text === '[object Object]') return false;
    if (isUuidLike(text)) return false;
    return text.length <= 48;
  };

  const dateKeys = keys.filter((k) => {
    const lower = k.toLowerCase();
    if (/(date|fecha|time|hora|created_at|updated_at)/.test(lower)) return true;
    let parseable = 0;
    for (const row of sample) {
      const val = row?.[k];
      if (val === null || val === undefined || String(val).trim() === '') continue;
      if (Number.isFinite(Date.parse(String(val)))) parseable += 1;
    }
    return parseable >= Math.ceil(sample.length * 0.6);
  });

  const numericKeys = keys.filter((k) => {
    let numericCount = 0;
    for (const row of sample) {
      const val = row?.[k];
      if (val !== null && val !== undefined && val !== '' && !Number.isNaN(Number(val))) numericCount += 1;
    }
    return numericCount >= Math.ceil(sample.length * 0.6);
  });

  const booleanKeys = keys.filter((k) => {
    let boolCount = 0;
    for (const row of sample) {
      const val = row?.[k];
      if (typeof val === 'boolean') { boolCount += 1; continue; }
      const raw = String(val ?? '').trim().toLowerCase();
      if (['true', 'false', 'si', 'sí', 'no', '0', '1'].includes(raw)) boolCount += 1;
    }
    return boolCount >= Math.ceil(sample.length * 0.6);
  });

  const textKeys = keys.filter((k) => !numericKeys.includes(k));
  const readableTextKeys = textKeys.filter((k) => {
    let readable = 0;
    for (const row of sample) { if (isReadableCategoryValue(row?.[k])) readable += 1; }
    return readable >= Math.ceil(sample.length * 0.6);
  });

  const countDistinctValues = (key: string, maxScan = 200): number => {
    const distinct = new Set<string>();
    for (const row of rows.slice(0, Math.min(maxScan, rows.length))) {
      const formatted = formatCellValue(row?.[key]);
      if (!formatted || formatted === '—' || formatted === '[object Object]') continue;
      distinct.add(formatted.toLowerCase());
      if (distinct.size > 20) break;
    }
    return distinct.size;
  };
  const hasUsefulCategoryDiversity = (key: string, minDistinct = 2) => key ? countDistinctValues(key) >= minDistinct : false;

  if (rows.length === 1 && numericKeys.length >= 1) return { chartType: 'kpi', yKey: numericKeys[0], reason: 'resultado puntual numérico' };

  const priceMetricKeys = numericKeys.filter((k) => /(price|precio|precios|monto|importe|amount|cost|costo|total|valor)/i.test(k));
  if (priceMetricKeys.length > 0 && rows.length >= 8) return { chartType: 'histogram', yKey: priceMetricKeys[0], valueKey: priceMetricKeys[0], reason: 'métrica de precio/importe detectada, histograma recomendado' };

  if (booleanKeys.length > 0) {
    const boolKey = booleanKeys[0];
    const distinctBooleanValues = countDistinctValues(boolKey);
    if (distinctBooleanValues <= 1) {
      const altCategoryKey = readableTextKeys.find((key) => { if (key === boolKey) return false; const d = countDistinctValues(key); return d >= 2 && d <= 12; });
      if (altCategoryKey) return { chartType: 'bar', xKey: altCategoryKey, yKey: '__count__', categoryKey: altCategoryKey, valueKey: '__count__', reason: `estado booleano uniforme; se recomienda distribución por ${altCategoryKey}` };
      return { chartType: 'none', reason: 'estado booleano único detectado; sin distribución útil para graficar' };
    }
    return { chartType: 'pie', categoryKey: boolKey, valueKey: '__count__', reason: 'columna booleana detectada, distribución recomendada' };
  }

  if (dateKeys.length > 0 && numericKeys.length > 0) return { chartType: 'line', xKey: dateKeys[0], yKey: numericKeys[0], categoryKey: dateKeys[0], valueKey: numericKeys[0], reason: 'serie temporal detectada' };
  if (dateKeys.length > 0) return { chartType: 'line', xKey: dateKeys[0], yKey: '__count__', categoryKey: dateKeys[0], valueKey: '__count__', reason: 'fechas detectadas, tendencia temporal por frecuencia' };
  if (textKeys.length === 0 && numericKeys.length === 1) return { chartType: 'line', xKey: '__row__', yKey: numericKeys[0], categoryKey: '__row__', valueKey: numericKeys[0], reason: 'solo métrica numérica' };

  if (keys.length === 1 && readableTextKeys.length === 1) {
    if (!hasUsefulCategoryDiversity(readableTextKeys[0])) return { chartType: 'none', reason: 'solo hay una categoría visible; se evita gráfica poco útil' };
    return { chartType: prefersPie ? 'pie' : 'bar', xKey: readableTextKeys[0], yKey: '__count__', categoryKey: readableTextKeys[0], valueKey: '__count__', reason: 'categorías únicas con frecuencia' };
  }

  if (readableTextKeys.length === 0) return { chartType: 'none', reason: 'no hay categorías legibles para eje X' };

  const pickPreferred = (candidates: string[], patterns: RegExp[]) => {
    for (const pattern of patterns) { const found = candidates.find((k) => pattern.test(String(k || '').toLowerCase())); if (found) return found; }
    return candidates[0] || null;
  };

  const preferredX = pickPreferred(readableTextKeys, [
    ...(prefersUserDimension ? [/(username|user_name|nombre|name|email|correo)/] : []),
    /(username|user_name|nombre|name|email|correo|title|titulo|detalle|descripcion|role|rol|estado|status)/,
    /(id)$/,
  ]);

  if (numericKeys.length === 0) {
    if (!preferredX) return { chartType: 'none' };
    if (!hasUsefulCategoryDiversity(preferredX)) return { chartType: 'none', reason: `solo se detectó una categoría en ${preferredX}` };
    return { chartType: 'bar', xKey: preferredX, yKey: '__count__', categoryKey: preferredX, valueKey: '__count__', reason: 'sin métrica numérica, se usa frecuencia por categoría' };
  }

  const preferredY = pickPreferred(numericKeys, [/(total|count|cantidad|score|monto|importe|price|amount|activo|enabled|status|estado)/, /(id)$/]);
  if (!preferredX || !preferredY) return { chartType: 'none' };
  if (!hasUsefulCategoryDiversity(preferredX)) return { chartType: 'none', reason: `eje X sin diversidad suficiente (${preferredX})` };

  const defaultChartType = prefersPie ? 'pie' : 'bar';
  if (prefersCount && preferredX) return { chartType: defaultChartType, xKey: preferredX, yKey: '__count__', categoryKey: preferredX, valueKey: '__count__', reason: 'consulta de conteo/distribución' };
  return { chartType: defaultChartType, xKey: preferredX, yKey: preferredY, categoryKey: preferredX, valueKey: preferredY, reason: 'categoría + métrica numérica detectadas' };
}
