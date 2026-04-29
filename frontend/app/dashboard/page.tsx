// app/dashboard/page.tsx

'use client';

import { useEffect, useMemo, useState } from 'react';
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { withAuth } from '../components/ProtectedRoute';
import ThemeToggle from '../components/ThemeToggle';
import { useTheme } from '../components/ThemeProvider';
import UserIdentityBadge from '../components/UserIdentityBadge';
import { stripDangerousSqlTerms } from '../lib/sanitize';

type DashboardUser = {
  username: string;
  role: string;
};

type QueryHistoryRow = {
  id: number;
  username: string;
  user_role: string;
  query_text: string;
  generated_sql: string;
  execution_ms: number;
  row_count: number;
  was_cached: boolean;
  status: string;
  created_at: string;
};

type AutoDashboard = {
  chartType: 'bar' | 'line' | 'pie' | 'table' | 'none' | 'kpi' | 'histogram';
  xKey?: string;
  yKey?: string;
  categoryKey?: string;
  valueKey?: string;
  reason?: string;
};

type PresentationPlan = {
  mode: 'kpi' | 'chart' | 'table' | 'text' | 'chart-table';
  dashboard: AutoDashboard;
  headline: string;
  reason: string;
};

type AutoQueryResponse = {
  success: boolean;
  mode?: 'sql' | 'debug';
  sql?: string;
  resumenHumano?: string;
  resultado?: Record<string, unknown> | unknown[] | null;
  data?: Record<string, unknown>[];
  count?: number;
  cached?: boolean;
  dashboard?: AutoDashboard;
  error?: string;
};

type UiNotice = {
  open: boolean;
  title: string;
  message: string;
};

function hasDebugResultPayload(payload: unknown): payload is { resumenHumano?: string; resultado: unknown } {
  return Boolean(payload && typeof payload === 'object' && 'resultado' in payload);
}

function extractResultadoData(payload: unknown): { rows: Record<string, unknown>[]; total: number; entidad?: string } {
  if (!payload || typeof payload !== 'object') return { rows: [], total: 0 };
  const maybe = payload as { resultado?: unknown };

  if (Array.isArray(maybe.resultado)) {
    const rows = maybe.resultado as Record<string, unknown>[];
    return { rows, total: rows.length };
  }

  const resultado = maybe?.resultado && typeof maybe.resultado === 'object' && !Array.isArray(maybe.resultado)
    ? (maybe.resultado as Record<string, unknown>)
    : null;

  const rows = Array.isArray(resultado?.data)
    ? (resultado?.data as Record<string, unknown>[])
    : (resultado ? [resultado] : []);

  const looksLikeDebugObject = Boolean(resultado && (
    'tipo_error' in resultado
    || 'flujo_detectado' in resultado
    || 'traza' in resultado
    || 'solucion' in resultado
    || 'causa' in resultado
  ));

  if (looksLikeDebugObject && !Array.isArray(resultado?.data)) {
    return { rows: [], total: 0 };
  }

  const total = Number(resultado?.total || rows.length || 0);
  const entidad = resultado?.entidad ? String(resultado.entidad) : undefined;

  return { rows, total, entidad };
}

function extractClassicRows(payload: unknown): Record<string, unknown>[] {
  if (!payload || typeof payload !== 'object') return [];
  const maybe = payload as { data?: unknown; rows?: unknown };
  if (Array.isArray(maybe.data)) return maybe.data as Record<string, unknown>[];
  if (Array.isArray(maybe.rows)) return maybe.rows as Record<string, unknown>[];
  return [];
}

function buildHybridResult(payload: unknown, fallbackSql = ''): AutoQueryResponse {
  if (hasDebugResultPayload(payload)) {
    const debugPayload = payload as { resumenHumano?: unknown; resultado: unknown };
    const resultadoData = extractResultadoData(payload);

    if (resultadoData.rows.length > 0) {
      return {
        success: true,
        mode: 'sql',
        sql: fallbackSql || undefined,
        resumenHumano: String(debugPayload.resumenHumano || '').trim(),
        resultado: (debugPayload.resultado || null) as Record<string, unknown> | unknown[] | null,
        data: resultadoData.rows,
        count: resultadoData.total,
        cached: false,
      };
    }

    return {
      success: true,
      mode: 'debug',
      sql: fallbackSql || undefined,
      resumenHumano: String(debugPayload.resumenHumano || '').trim(),
      resultado: (debugPayload.resultado || null) as Record<string, unknown> | unknown[] | null,
      data: [],
      count: 0,
      cached: false,
    };
  }

  const maybe = (payload && typeof payload === 'object' ? payload : {}) as {
    success?: boolean;
    sql?: string;
    rowCount?: number;
    count?: number;
    cached?: boolean;
    dashboard?: AutoDashboard;
  };
  const rows = extractClassicRows(payload);

  return {
    success: maybe.success !== false,
    mode: 'sql',
    sql: String(maybe.sql || fallbackSql || '').trim() || undefined,
    data: rows,
    count: Number(maybe.rowCount || maybe.count || rows.length || 0),
    cached: Boolean(maybe.cached),
    dashboard: maybe.dashboard,
  };
}

function inferDashboardFromRows(rows: Record<string, unknown>[], userQuery = ''): AutoDashboard {
  if (!Array.isArray(rows) || rows.length < 1) return { chartType: 'none' };

  const keys = Object.keys(rows[0] || {});
  if (keys.length < 1) return { chartType: 'none' };

  const normalizedQuery = String(userQuery || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '');

  const prefersPie = /(porcentaje|proporcion|proporcion|distribucion|distribucion|participacion|reparto|segmentacion)/.test(normalizedQuery);
  const prefersCount = /(cantidad|cuantos|cuantas|conteo|total|mas|menos|top)/.test(normalizedQuery);
  const prefersUserDimension = /(usuario|usuarios|user|users|nombre|username)/.test(normalizedQuery);

  const sample = rows.slice(0, Math.min(12, rows.length));
  const isUuidLike = (value: string) => /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
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
      const ts = Date.parse(String(val));
      if (Number.isFinite(ts)) parseable += 1;
    }
    return parseable >= Math.ceil(sample.length * 0.6);
  });
  const numericKeys = keys.filter((k) => {
    let numericCount = 0;
    for (const row of sample) {
      const val = row?.[k];
      if (val !== null && val !== undefined && val !== '' && !Number.isNaN(Number(val))) {
        numericCount += 1;
      }
    }
    return numericCount >= Math.ceil(sample.length * 0.6);
  });

  const booleanKeys = keys.filter((k) => {
    let boolCount = 0;
    for (const row of sample) {
      const val = row?.[k];
      if (typeof val === 'boolean') {
        boolCount += 1;
        continue;
      }
      const raw = String(val ?? '').trim().toLowerCase();
      if (['true', 'false', 'si', 'sí', 'no', '0', '1'].includes(raw)) {
        boolCount += 1;
      }
    }
    return boolCount >= Math.ceil(sample.length * 0.6);
  });

  const textKeys = keys.filter((k) => !numericKeys.includes(k));
  const readableTextKeys = textKeys.filter((k) => {
    let readable = 0;
    for (const row of sample) {
      if (isReadableCategoryValue(row?.[k])) readable += 1;
    }
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

  const hasUsefulCategoryDiversity = (key: string, minDistinct = 2): boolean => {
    if (!key) return false;
    return countDistinctValues(key) >= minDistinct;
  };

  if (rows.length === 1 && numericKeys.length >= 1) {
    return {
      chartType: 'kpi',
      yKey: numericKeys[0],
      reason: 'resultado puntual numérico',
    };
  }

  const priceMetricKeys = numericKeys.filter((k) => /(price|precio|precios|monto|importe|amount|cost|costo|total|valor)/i.test(k));

  if (priceMetricKeys.length > 0 && rows.length >= 8) {
    return {
      chartType: 'histogram',
      yKey: priceMetricKeys[0],
      valueKey: priceMetricKeys[0],
      reason: 'métrica de precio/importe detectada, histograma recomendado',
    };
  }

  if (booleanKeys.length > 0) {
    const boolKey = booleanKeys[0];
    const distinctBooleanValues = countDistinctValues(boolKey);

    if (distinctBooleanValues <= 1) {
      const alternativeCategoryKey = readableTextKeys.find((key) => {
        if (key === boolKey) return false;
        const distinct = countDistinctValues(key);
        return distinct >= 2 && distinct <= 12;
      });

      if (alternativeCategoryKey) {
        return {
          chartType: 'bar',
          xKey: alternativeCategoryKey,
          yKey: '__count__',
          categoryKey: alternativeCategoryKey,
          valueKey: '__count__',
          reason: `estado booleano uniforme; se recomienda distribución por ${alternativeCategoryKey}`,
        };
      }

      return {
        chartType: 'none',
        reason: 'estado booleano único detectado; sin distribución útil para graficar',
      };
    }

    return {
      chartType: 'pie',
      categoryKey: boolKey,
      valueKey: '__count__',
      reason: 'columna booleana detectada, distribución recomendada',
    };
  }

  if (dateKeys.length > 0 && numericKeys.length > 0) {
    return {
      chartType: 'line',
      xKey: dateKeys[0],
      yKey: numericKeys[0],
      categoryKey: dateKeys[0],
      valueKey: numericKeys[0],
      reason: 'serie temporal detectada',
    };
  }

  if (dateKeys.length > 0) {
    return {
      chartType: 'line',
      xKey: dateKeys[0],
      yKey: '__count__',
      categoryKey: dateKeys[0],
      valueKey: '__count__',
      reason: 'fechas detectadas, tendencia temporal por frecuencia',
    };
  }

  if (textKeys.length === 0 && numericKeys.length === 1) {
    // Single numeric column: line chart over row index.
    return {
      chartType: 'line',
      xKey: '__row__',
      yKey: numericKeys[0],
      categoryKey: '__row__',
      valueKey: numericKeys[0],
      reason: 'solo métrica numérica',
    };
  }

  if (keys.length === 1 && readableTextKeys.length === 1) {
    if (!hasUsefulCategoryDiversity(readableTextKeys[0])) {
      return {
        chartType: 'none',
        reason: 'solo hay una categoría visible; se evita gráfica poco útil',
      };
    }

    // Single text column: frequency chart (count by category).
    return {
      chartType: prefersPie ? 'pie' : 'bar',
      xKey: readableTextKeys[0],
      yKey: '__count__',
      categoryKey: readableTextKeys[0],
      valueKey: '__count__',
      reason: 'categorías únicas con frecuencia',
    };
  }

  if (readableTextKeys.length === 0) return { chartType: 'none', reason: 'no hay categorías legibles para eje X' };

  const pickPreferred = (candidates: string[], patterns: RegExp[]) => {
    for (const pattern of patterns) {
      const found = candidates.find((k) => pattern.test(String(k || '').toLowerCase()));
      if (found) return found;
    }
    return candidates[0] || null;
  };

  const preferredX = pickPreferred(readableTextKeys, [
    ...(prefersUserDimension ? [/(username|user_name|nombre|name|email|correo)/] : []),
    /(username|user_name|nombre|name|email|correo|title|titulo|detalle|descripcion|role|rol|estado|status)/,
    /(id)$/,
  ]);

  if (numericKeys.length === 0) {
    if (!preferredX) return { chartType: 'none' };
    if (!hasUsefulCategoryDiversity(preferredX)) {
      return {
        chartType: 'none',
        reason: `solo se detectó una categoría en ${preferredX}`,
      };
    }
    return {
      chartType: 'bar',
      xKey: preferredX,
      yKey: '__count__',
      categoryKey: preferredX,
      valueKey: '__count__',
      reason: 'sin métrica numérica, se usa frecuencia por categoría',
    };
  }

  const preferredY = pickPreferred(numericKeys, [
    /(total|count|cantidad|score|monto|importe|price|amount|activo|enabled|status|estado)/,
    /(id)$/,
  ]);
  if (!preferredX || !preferredY) return { chartType: 'none' };
  if (!hasUsefulCategoryDiversity(preferredX)) {
    return {
      chartType: 'none',
      reason: `eje X sin diversidad suficiente (${preferredX})`,
    };
  }

  const defaultChartType = prefersPie ? 'pie' : 'bar';

  if (prefersCount && preferredX) {
    return {
      chartType: defaultChartType,
      xKey: preferredX,
      yKey: '__count__',
      categoryKey: preferredX,
      valueKey: '__count__',
      reason: 'consulta de conteo/distribución',
    };
  }

  return {
    chartType: defaultChartType,
    xKey: preferredX,
    yKey: preferredY,
    categoryKey: preferredX,
    valueKey: preferredY,
    reason: 'categoría + métrica numérica detectadas',
  };
}

function normalizeText(value: string): string {
  return String(value || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '');
}

function extractTraceIntent(result: AutoQueryResponse | null): string {
  const payload = result?.resultado;
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return 'unknown';
  const maybeTrace = (payload as Record<string, unknown>)?.trazabilidad;
  if (!maybeTrace || typeof maybeTrace !== 'object' || Array.isArray(maybeTrace)) return 'unknown';
  const intent = (maybeTrace as Record<string, unknown>)?.intencion;
  return String(intent || 'unknown').trim().toLowerCase();
}

function inferPresentationPlan(
  rows: Record<string, unknown>[],
  userQuery: string,
  dashboardHint?: AutoDashboard,
  traceIntent = 'unknown',
): PresentationPlan {
  if (!rows.length) {
    return {
      mode: 'text',
      dashboard: { chartType: 'none' },
      headline: 'No hay datos para mostrar',
      reason: 'La consulta fue válida, pero no devolvió filas.',
    };
  }

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

  if (dashboard.chartType === 'kpi') {
    return {
      mode: 'kpi',
      dashboard,
      headline: 'Resultado puntual detectado',
      reason: dashboard.reason || 'Una sola fila con métrica principal.',
    };
  }

  if (/count-rows|list-columns/.test(traceIntent)) {
    return {
      mode: dashboard.chartType === 'none' ? 'table' : 'chart-table',
      dashboard,
      headline: traceIntent === 'count-rows' ? 'Conteo total' : 'Estructura de columnas',
      reason: 'La intención detectada sugiere vista estructurada.',
    };
  }

  const hasNumericSeries = rows.some((row) =>
    Object.values(row || {}).some((value) => value !== null && value !== undefined && value !== '' && Number.isFinite(Number(value)))
  );

  if ((/ranked|top|mas|menos/.test(normalizedQuery) || traceIntent.includes('ranked')) && hasNumericSeries) {
    return {
      mode: 'chart-table',
      dashboard: { ...dashboard, chartType: 'bar', reason: 'ranking detectado' },
      headline: 'Ranking detectado',
      reason: 'Para comparar posiciones, barras es más legible.',
    };
  }

  if ((/ranked|top|mas|menos/.test(normalizedQuery) || traceIntent.includes('ranked')) && !hasNumericSeries) {
    return {
      mode: 'table',
      dashboard: { ...dashboard, chartType: 'none', reason: 'ranking sin métrica numérica clara' },
      headline: 'Resultado listado',
      reason: 'No se detectó una métrica numérica confiable para graficar ranking.',
    };
  }

  if (looksLikeEntityListing && keys.length >= 5 && numericCount <= 1) {
    return {
      mode: 'table',
      dashboard: { ...dashboard, chartType: 'none', reason: 'listado detallado detectado' },
      headline: 'Vista tabular recomendada',
      reason: 'La consulta solicita un listado detallado; tabla priorizada para lectura completa.',
    };
  }

  if (dashboard.chartType !== 'none') {
    return {
      mode: rows.length > 12 || numericCount > 0 ? 'chart-table' : 'chart',
      dashboard,
      headline: 'Visualización recomendada automáticamente',
      reason: dashboard.reason || 'Se detectó una estructura apta para visualización.',
    };
  }

  if (rows.length <= 6 && keys.length <= 4) {
    return {
      mode: 'text',
      dashboard,
      headline: 'Formato narrativo recomendado',
      reason: 'Pocos datos: lectura directa más rápida que gráfica.',
    };
  }

  return {
    mode: 'table',
    dashboard,
    headline: 'Tabla recomendada',
    reason: 'Estructura tabular densa o sin eje numérico claro.',
  };
}

function formatCellValue(value: unknown): string {
  if (value === null || value === undefined || value === '') return '—';
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value.toLocaleString('es-PE');
  }
  if (typeof value === 'boolean') return value ? 'Sí' : 'No';
  const raw = String(value);
  const timestamp = Date.parse(raw);
  if (Number.isFinite(timestamp) && /(t|\d{4}-\d{2}-\d{2})/i.test(raw)) {
    return new Date(timestamp).toLocaleString('es-PE');
  }
  return raw;
}

function normalizeBooleanValue(value: unknown): boolean | null {
  if (typeof value === 'boolean') return value;
  const raw = String(value ?? '').trim().toLowerCase();
  if (['true', '1', 'si', 'sí', 'yes', 'y'].includes(raw)) return true;
  if (['false', '0', 'no', 'n'].includes(raw)) return false;
  return null;
}

function truncateLabel(value: string, max = 18): string {
  const text = String(value || '').trim();
  if (text.length <= max) return text;
  return `${text.slice(0, max - 1)}…`;
}

function buildResultInsights(rows: Record<string, unknown>[], userQuery: string): string[] {
  if (!rows.length) return ['No se encontraron registros para la consulta.'];

  const insights: string[] = [];
  insights.push(`Se encontraron ${rows.length} ${rows.length === 1 ? 'registro' : 'registros'}.`);

  const first = rows[0] || {};
  const keys = Object.keys(first);
  const visibleKeys = keys.filter((k) => !/(password|token|secret)/i.test(k));

  const numericKey = visibleKeys.find((k) => {
    let numeric = 0;
    for (const row of rows.slice(0, Math.min(20, rows.length))) {
      const value = row?.[k];
      if (value !== null && value !== undefined && value !== '' && Number.isFinite(Number(value))) {
        numeric += 1;
      }
    }
    return numeric >= Math.ceil(Math.min(20, rows.length) * 0.7);
  });

  if (numericKey) {
    const values = rows
      .map((row) => Number(row?.[numericKey]))
      .filter((v) => Number.isFinite(v));
    if (values.length > 0) {
      const max = Math.max(...values);
      const min = Math.min(...values);
      const avg = values.reduce((acc, v) => acc + v, 0) / values.length;
      insights.push(`Métrica ${numericKey}: min ${formatCellValue(min)}, max ${formatCellValue(max)}, promedio ${formatCellValue(Math.round(avg * 100) / 100)}.`);
    }
  }

  const candidateLabel = visibleKeys.find((k) => /(usuario|user|username|nombre|name|rol|role|estado|status)/i.test(k)) || visibleKeys[0];
  if (candidateLabel) {
    const labels = rows
      .map((row) => formatCellValue(row?.[candidateLabel]))
      .filter((text) => text && text !== '—' && text !== '[object Object]')
      .slice(0, 3);
    if (labels.length > 0) {
      insights.push(`Ejemplos en ${candidateLabel}: ${labels.join(', ')}.`);
    }
  }

  const normalizedQuery = normalizeText(userQuery);
  if (/usuarios|user/.test(normalizedQuery)) {
    insights.push('La tabla de usuarios se mostró en formato tabular para facilitar comparación campo a campo.');
  }

  return insights.slice(0, 4);
}

const PIE_COLORS = ['#0ea5e9', '#22d3ee', '#14b8a6', '#10b981', '#f59e0b', '#f97316', '#8b5cf6', '#6366f1'];
const HISTOGRAM_BAR_COLOR = '#06b6d4';

function normalizeUser(user: unknown): DashboardUser | null {
  if (!user || typeof user !== 'object') return null;
  const maybe = user as Record<string, unknown>;
  const username = String(maybe.username || '').trim();
  const role = String(maybe.role || '').trim().toLowerCase();
  if (!username) return null;
  return { username, role: role || 'user' };
}

function DashboardPage() {
  const { theme } = useTheme();
  const isLightTheme = theme === 'light';
  const [currentUser, setCurrentUser] = useState<DashboardUser | null>(null);
  const [query, setQuery] = useState('');
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const [result, setResult] = useState<AutoQueryResponse | null>(null);
  const [loadingQuery, setLoadingQuery] = useState(false);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState<UiNotice>({ open: false, title: '', message: '' });

  const [history, setHistory] = useState<QueryHistoryRow[]>([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historySearch, setHistorySearch] = useState('');
  const [historyStatus, setHistoryStatus] = useState<'all' | 'ok' | 'error'>('all');
  const [clearingHistory, setClearingHistory] = useState(false);

  const isDebugMode = result?.mode === 'debug';
  const debugResult = (isDebugMode && result?.resultado && typeof result.resultado === 'object' && !Array.isArray(result.resultado)
    ? result.resultado
    : {}) as Record<string, unknown>;
  const debugFlow = Array.isArray(debugResult?.flujo_detectado)
    ? (debugResult.flujo_detectado as unknown[]).map((item) => String(item))
    : debugResult?.flujo_detectado
      ? [String(debugResult.flujo_detectado)]
      : [];
  const debugTrace = Array.isArray(debugResult?.traza) ? (debugResult.traza as Array<Record<string, unknown>>) : [];

  useEffect(() => {
    const raw = localStorage.getItem('user');
    if (!raw) {
      setCurrentUser(null);
      return;
    }

    try {
      setCurrentUser(normalizeUser(JSON.parse(raw)));
    } catch {
      setCurrentUser(null);
    }
  }, []);

  useEffect(() => {
    const text = stripDangerousSqlTerms(query);
    if (!text) {
      setSuggestions([]);
      return;
    }

    const timer = window.setTimeout(async () => {
      try {
        const token = localStorage.getItem('token');
        const role = localStorage.getItem('userRole') || currentUser?.role || 'user';
        const response = await fetch(`/api/query/suggestions?q=${encodeURIComponent(text)}`, {
          headers: {
            ...(token ? { Authorization: `Bearer ${token}` } : {}),
            'x-user-role': role,
          },
        });
        const payload = await response.json();
        if (response.ok && payload?.success && Array.isArray(payload?.suggestions)) {
          setSuggestions(payload.suggestions);
        }
      } catch {
        // Suggestions fail silently
      }
    }, 250);

    return () => window.clearTimeout(timer);
  }, [query, currentUser?.role]);

  const runQuery = async () => {
    setError('');
    setNotice({ open: false, title: '', message: '' });

    const text = stripDangerousSqlTerms(query);
    if (!text) {
      setError('Escribe una consulta.');
      return;
    }

    setLoadingQuery(true);
    try {
      const token = localStorage.getItem('token');
      const role = localStorage.getItem('userRole') || currentUser?.role || 'user';
      console.info('[dashboard][query] passthrough dispatch', { query: text });

      // Primary path: use the intelligent NL endpoint directly
      const intelligentResponse = await fetch('/api/query', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
          'x-user-role': role,
        },
        body: JSON.stringify({ query: text, limit: 80 }),
      });

      const intelligentPayload = await intelligentResponse.json();
      if (intelligentResponse.ok && (intelligentPayload?.success || hasDebugResultPayload(intelligentPayload))) {
        const hybrid = buildHybridResult(intelligentPayload);
        const rowCount = Number(hybrid?.count || 0);

        setResult(hybrid);

        if (hybrid.mode === 'sql' && rowCount === 0) {
          setNotice({
            open: true,
            title: 'Sin resultados en la base de datos',
            message: String(intelligentPayload?.message || 'Lo solicitado no se encuentra en la base de datos actual.'),
          });
        }

        void loadHistory();
        return;
      }

      // Compatibility fallback: legacy generate -> execute flow
      const generateResponse = await fetch('/api/query/generate', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
          'x-user-role': role,
        },
        body: JSON.stringify({ text, query: text, limit: 80, offset: 0 }),
      });

      const generatePayload = await generateResponse.json();

      if (generateResponse.ok && hasDebugResultPayload(generatePayload)) {
        setResult(buildHybridResult(generatePayload));
        void loadHistory();
        return;
      }

      if (!generateResponse.ok || !generatePayload?.success || !generatePayload?.query?.sql) {
        const fallbackMessage = String(
          intelligentPayload?.message
          || intelligentPayload?.error
          || generatePayload?.error
          || 'No se pudo interpretar la consulta.'
        );
        setError(
          fallbackMessage
        );
        setNotice({
          open: true,
          title: 'Consulta no encontrada',
          message: fallbackMessage,
        });
        return;
      }

      const response = await fetch('/api/query/execute-generated', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
          'x-user-role': role,
        },
        body: JSON.stringify({ sql: generatePayload.query.sql, query: text }),
      });

      const payload = await response.json();

      if (response.ok && hasDebugResultPayload(payload)) {
        setResult(buildHybridResult(payload, generatePayload.query.sql));
        void loadHistory();
        return;
      }

      if (!response.ok || payload?.success === false) {
        setError(payload?.error || 'No se pudo ejecutar la consulta.');
        setNotice({
          open: true,
          title: 'Error al ejecutar',
          message: String(payload?.error || 'No se pudo ejecutar la consulta.'),
        });
        return;
      }

      const hybrid = buildHybridResult(payload, generatePayload.query.sql);
      setResult(hybrid);

      if (hybrid.mode === 'sql' && Number(hybrid?.count || 0) === 0) {
        setNotice({
          open: true,
          title: 'Sin resultados en la base de datos',
          message: String(payload?.message || 'Lo solicitado no se encuentra en la base de datos actual.'),
        });
      }

      void loadHistory();
    } catch {
      setError('Error de red ejecutando consulta.');
      setNotice({
        open: true,
        title: 'Error de red',
        message: 'No se pudo conectar con el servidor. Verifica que backend y frontend estén activos.',
      });
    } finally {
      setLoadingQuery(false);
    }
  };

  const loadHistory = async () => {
    if (!['admin', 'superadmin'].includes(currentUser?.role || '')) return;

    setHistoryLoading(true);
    try {
      const token = localStorage.getItem('token');
      const role = localStorage.getItem('userRole') || currentUser?.role || 'user';
      const response = await fetch('/api/query-history?limit=80', {
        headers: {
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
          'x-user-role': role,
        },
      });
      const payload = await response.json();
      if (response.ok && payload?.success && Array.isArray(payload?.data)) {
        setHistory(payload.data);
      }
    } catch {
      // history load is best effort
    } finally {
      setHistoryLoading(false);
    }
  };

  useEffect(() => {
    void loadHistory();
  }, [currentUser?.role]);

  const metrics = useMemo(() => {
    const rows = Array.isArray(result?.data) ? result.data : [];
    const count = Number(result?.count || rows.length || 0);
    const executionMs = history[0]?.execution_ms || 0;
    const cachedRate = history.length > 0
      ? Math.round((history.filter((h) => h.was_cached).length / history.length) * 100)
      : 0;
    return {
      rows: count,
      executionMs,
      cachedRate,
      totalQueries: history.length,
    };
  }, [result, history]);

  const traceIntent = useMemo(() => extractTraceIntent(result), [result]);

  const presentationPlan = useMemo(() => {
    const rows = Array.isArray(result?.data) ? result.data : [];
    return inferPresentationPlan(rows, query, result?.dashboard, traceIntent);
  }, [result, query, traceIntent]);

  const resultInsights = useMemo(() => {
    const rows = Array.isArray(result?.data) ? result.data : [];
    return buildResultInsights(rows, query);
  }, [result, query]);

  const activeDashboard = presentationPlan.dashboard;
  const chartTypeLabel = {
    bar: 'Barras',
    line: 'Tendencia',
    pie: 'Distribución',
    histogram: 'Histograma',
    kpi: 'KPI',
    table: 'Tabla',
    none: 'Resumen',
  }[String(activeDashboard?.chartType || 'none')] || 'Resumen';

  const chartRows = useMemo(() => {
    const rows = Array.isArray(result?.data) ? result.data : [];
    const dashboard = activeDashboard;
    if (!rows.length || !dashboard) return [];

    if (dashboard.chartType === 'bar' || dashboard.chartType === 'line') {
      const xKey = String(dashboard.xKey || 'label');
      const yKey = String(dashboard.yKey || 'value');

      if (yKey === '__count__') {
        const grouped = new Map<string, number>();
        for (const row of rows.slice(0, 500)) {
          const boolValue = normalizeBooleanValue(row?.[xKey]);
          const key = boolValue === null ? formatCellValue(row?.[xKey]) : (boolValue ? 'Sí' : 'No');
          if (!key || key === '—' || key === '[object Object]') continue;
          grouped.set(key, (grouped.get(key) || 0) + 1);
        }
        return [...grouped.entries()]
          .sort((a, b) => b[1] - a[1])
          .slice(0, 12)
          .map(([name, value]) => ({ name, value }));
      }

      return rows.slice(0, 24).map((row, index) => ({
        name: xKey === '__row__' ? `#${index + 1}` : formatCellValue(row?.[xKey]),
        value: Number(row?.[yKey] ?? 0),
      })).filter((entry) => Number.isFinite(entry.value) && entry.name !== '—' && entry.name !== '[object Object]');
    }

    if (dashboard.chartType === 'histogram') {
      const metricKey = String(dashboard.yKey || dashboard.valueKey || 'value');
      const values = rows
        .map((row) => Number(row?.[metricKey]))
        .filter((value) => Number.isFinite(value));

      if (!values.length) return [];

      const min = Math.min(...values);
      const max = Math.max(...values);
      if (min === max) {
        return [{ name: `${formatCellValue(min)}`, value: values.length }];
      }

      const bins = Math.max(5, Math.min(10, Math.round(Math.sqrt(values.length))));
      const width = (max - min) / bins;
      const counts = Array.from({ length: bins }, () => 0);

      for (const value of values) {
        const rawIndex = Math.floor((value - min) / width);
        const idx = Math.min(bins - 1, Math.max(0, rawIndex));
        counts[idx] += 1;
      }

      return counts.map((count, index) => {
        const start = min + width * index;
        const end = index === bins - 1 ? max : (start + width);
        return {
          name: `${formatCellValue(Math.round(start * 100) / 100)} - ${formatCellValue(Math.round(end * 100) / 100)}`,
          value: count,
        };
      });
    }

    if (dashboard.chartType === 'pie') {
      const categoryKey = String(dashboard.categoryKey || 'category');
      const valueKey = String(dashboard.valueKey || 'value');
      if (valueKey === '__count__') {
        const grouped = new Map<string, number>();
        for (const row of rows.slice(0, 500)) {
          const boolValue = normalizeBooleanValue(row?.[categoryKey]);
          const key = boolValue === null ? formatCellValue(row?.[categoryKey]) : (boolValue ? 'Sí' : 'No');
          if (!key || key === '—' || key === '[object Object]') continue;
          grouped.set(key, (grouped.get(key) || 0) + 1);
        }

        const sorted = [...grouped.entries()].sort((a, b) => b[1] - a[1]);
        const top = sorted.slice(0, 6).map(([name, value]) => ({ name, value }));
        const others = sorted.slice(6).reduce((acc, [, value]) => acc + value, 0);
        if (others > 0) top.push({ name: 'Otros', value: others });
        return top;
      }

      return rows.slice(0, 24).map((row) => ({
        name: formatCellValue(row?.[categoryKey]),
        value: Number(row?.[valueKey] ?? 0),
      })).filter((entry) => Number.isFinite(entry.value) && entry.name !== '—' && entry.name !== '[object Object]');
    }

    return [];
  }, [result, activeDashboard]);

  const visualizationSummary = useMemo(() => {
    const rows = Array.isArray(result?.data) ? result.data : [];
    const firstRow = rows[0] || {};
    const columns = Object.keys(firstRow);
    return {
      rowCount: rows.length,
      columnCount: columns.length,
      columns: columns.slice(0, 8),
      preview: Object.entries(firstRow).slice(0, 6),
    };
  }, [result]);

  const filteredHistory = useMemo(() => {
    const q = historySearch.trim().toLowerCase();
    return history.filter((row) => {
      const statusOk = historyStatus === 'all' ? true : row.status === historyStatus;
      if (!statusOk) return false;
      if (!q) return true;
      return [row.username, row.user_role, row.query_text, row.generated_sql, row.status]
        .map((field) => String(field || '').toLowerCase())
        .join(' ')
        .includes(q);
    });
  }, [history, historySearch, historyStatus]);

  const clearHistory = async () => {
    setClearingHistory(true);
    try {
      const token = localStorage.getItem('token');
      const role = localStorage.getItem('userRole') || currentUser?.role || 'user';
      const response = await fetch('/api/query-history', {
        method: 'DELETE',
        headers: {
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
          'x-user-role': role,
        },
      });
      const payload = await response.json();
      if (!response.ok || !payload?.success) {
        setError(payload?.error || 'No se pudo limpiar historial.');
        return;
      }
      setHistory([]);
      setHistorySearch('');
      setHistoryStatus('all');
    } catch {
      setError('Error de red limpiando historial.');
    } finally {
      setClearingHistory(false);
    }
  };

  const statusClass = (status: string) => {
    if (status === 'ok') return isLightTheme ? 'border-emerald-300 bg-emerald-50 text-emerald-700' : 'border-emerald-500/40 bg-emerald-500/15 text-emerald-300';
    return isLightTheme ? 'border-red-300 bg-red-50 text-red-700' : 'border-red-500/40 bg-red-500/15 text-red-300';
  };

  const shellClass = isLightTheme
    ? 'dashboard-shell min-h-screen bg-gradient-to-br from-sky-100 via-blue-100 to-cyan-100 text-slate-900'
    : 'dashboard-shell min-h-screen bg-[#0f172a] text-slate-100';
  const panelClass = isLightTheme
    ? 'dashboard-panel dashboard-elevate dashboard-animate rounded-2xl border border-sky-200 p-4'
    : 'dashboard-panel dashboard-elevate dashboard-animate rounded-2xl border border-slate-700/70 p-4';
  const panelHeaderClass = isLightTheme
    ? 'dashboard-panel dashboard-panel-hero dashboard-animate relative z-40 overflow-visible rounded-2xl border border-sky-200 p-5'
    : 'dashboard-panel dashboard-panel-hero dashboard-animate relative z-40 overflow-visible rounded-2xl border border-slate-700/70 p-5';

  return (
    <div className={shellClass}>
      {notice.open && (
        <div className="fixed inset-0 z-[120] flex items-center justify-center bg-slate-950/45 px-4">
          <div className={`w-full max-w-md rounded-2xl border p-4 shadow-2xl ${isLightTheme ? 'border-amber-300 bg-white text-slate-900' : 'border-amber-500/40 bg-slate-900 text-slate-100'}`}>
            <p className={`text-sm font-semibold ${isLightTheme ? 'text-amber-700' : 'text-amber-300'}`}>{notice.title}</p>
            <p className={`mt-2 text-sm ${isLightTheme ? 'text-slate-600' : 'text-slate-300'}`}>{notice.message}</p>
            <div className="mt-4 flex justify-end">
              <button
                type="button"
                onClick={() => setNotice({ open: false, title: '', message: '' })}
                className={`rounded-lg px-3 py-1.5 text-xs font-semibold ${isLightTheme ? 'bg-amber-100 text-amber-800 hover:bg-amber-200' : 'bg-amber-500/20 text-amber-200 hover:bg-amber-500/30'}`}
              >
                Entendido
              </button>
            </div>
          </div>
        </div>
      )}

      <div className="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
        <header className={panelHeaderClass}>
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <p className={`text-xs font-semibold uppercase tracking-[0.2em] ${isLightTheme ? 'text-sky-700' : 'text-cyan-300'}`}>Generador SQL</p>
              <h1 className="dashboard-gradient-text mt-1 text-2xl font-semibold">Panel SQL Automático</h1>
            </div>
            <div className="ml-auto flex items-center gap-3">
              <ThemeToggle />
              <UserIdentityBadge
                username={currentUser?.username || 'usuario'}
                role={currentUser?.role || 'user'}
                isLightTheme={isLightTheme}
                primaryActionLabel="Ir a Categorías"
                onPrimaryAction={() => {
                  window.location.href = '/admin';
                }}
                onLogout={() => {
                  localStorage.removeItem('token');
                  localStorage.removeItem('user');
                  window.location.href = '/login';
                }}
              />
            </div>
          </div>
        </header>

        <section className="mt-5 grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
          {[
            { label: 'Filas retornadas', value: metrics.rows },
            { label: 'Tiempo última query', value: `${metrics.executionMs} ms` },
            { label: 'Cache hit rate', value: `${metrics.cachedRate}%` },
            { label: 'Consultas registradas', value: metrics.totalQueries },
          ].map((metric) => (
            <div key={metric.label} className={`${panelClass} relative overflow-hidden`}>
              <div className={`pointer-events-none absolute right-[-28px] top-[-28px] h-20 w-20 rounded-full ${isLightTheme ? 'bg-sky-200/60' : 'bg-cyan-500/20'}`} />
              <p className={`text-xs uppercase tracking-wide ${isLightTheme ? 'text-slate-500' : 'text-slate-400'}`}>{metric.label}</p>
              <p className={`mt-2 text-2xl font-semibold ${isLightTheme ? 'text-sky-700' : 'text-cyan-200'}`}>{metric.value}</p>
            </div>
          ))}
        </section>

        <section className="mt-5 grid grid-cols-1 gap-5 2xl:grid-cols-[1.08fr_0.92fr]">
          <div className={panelClass}>
            <p className={`text-sm font-semibold ${isLightTheme ? 'text-sky-700' : 'text-cyan-200'}`}>Panel SQL automático</p>
            <p className={`mt-1 text-xs ${isLightTheme ? 'text-slate-500' : 'text-slate-400'}`}>Lenguaje natural -&gt; SQL -&gt; Resultado -&gt; Dashboard automático</p>

            <div className="mt-3 grid grid-cols-1 gap-2 lg:grid-cols-[1fr_auto]">
              <textarea
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="¿Qué quieres consultar?"
                className={`min-h-[96px] w-full rounded-2xl border px-4 py-3 text-sm ${isLightTheme ? 'border-sky-200 bg-white text-slate-900 placeholder:text-slate-400' : 'border-slate-700 bg-slate-950/80 text-slate-100 placeholder:text-slate-500'}`}
              />
              <button
                onClick={runQuery}
                disabled={loadingQuery}
                className="btn-accent rounded-2xl px-4 py-3 text-sm font-semibold text-white disabled:opacity-60"
              >
                {loadingQuery ? 'Consultando...' : 'Ejecutar consulta'}
              </button>
            </div>

            {suggestions.length > 0 && (
              <div className="mt-2 flex flex-wrap gap-2">
                {suggestions.map((item) => (
                  <button
                    key={`sug-${item}`}
                    onClick={() => setQuery(stripDangerousSqlTerms(item))}
                    className={`rounded-full border px-2.5 py-1 text-xs transition ${isLightTheme ? 'border-sky-200 bg-white text-sky-800 hover:border-sky-400 hover:text-sky-900' : 'border-slate-700 bg-slate-800 text-slate-300 hover:border-cyan-500/50 hover:text-cyan-200'}`}
                  >
                    {item}
                  </button>
                ))}
              </div>
            )}

            {error && (
              <div className="mt-3 rounded-xl border border-red-500/40 bg-red-500/10 px-3 py-2 text-xs text-red-300">
                {error}
              </div>
            )}

            <div className="mt-3 space-y-2">
              <div className="flex justify-end">
                <div className={`max-w-[92%] whitespace-pre-wrap break-words rounded-2xl rounded-br-md border px-3 py-2 text-sm ${isLightTheme ? 'border-sky-200 bg-sky-50 text-sky-950' : 'border-cyan-500/30 bg-cyan-500/10 text-cyan-100'}`}>
                  {query}
                </div>
              </div>

              {result?.sql && !isDebugMode && (
                <div className="flex justify-start">
                  <div className={`max-w-[92%] rounded-2xl rounded-bl-md border p-3 text-xs ${isLightTheme ? 'border-slate-200 bg-white text-slate-800' : 'border-slate-700 bg-slate-950 text-slate-200'}`}>
                    <p className={`mb-1 font-semibold ${isLightTheme ? 'text-sky-700' : 'text-cyan-300'}`}>SQL generado</p>
                    <pre className="whitespace-pre-wrap break-all">{result.sql}</pre>
                  </div>
                </div>
              )}

              {isDebugMode && (
                <div className="flex justify-start">
                  <div className={`max-w-[92%] rounded-2xl rounded-bl-md border p-3 text-xs ${isLightTheme ? 'border-amber-200 bg-amber-50 text-slate-800' : 'border-amber-500/30 bg-amber-500/10 text-slate-200'}`}>
                    <p className={`mb-1 font-semibold ${isLightTheme ? 'text-amber-700' : 'text-amber-300'}`}>Resumen de debugging</p>
                    <p className="whitespace-pre-wrap break-words">{result?.resumenHumano || 'Análisis de error completado.'}</p>
                  </div>
                </div>
              )}

              {!isDebugMode && result && (
                <div className="flex justify-start">
                  <div className={`max-w-[92%] rounded-2xl rounded-bl-md border p-3 text-xs ${isLightTheme ? 'border-emerald-200 bg-emerald-50 text-slate-800' : 'border-emerald-500/30 bg-emerald-500/10 text-slate-200'}`}>
                    <p className={`mb-1 font-semibold ${isLightTheme ? 'text-emerald-700' : 'text-emerald-300'}`}>
                      IA de presentación
                    </p>
                    <p className="break-words font-medium">{presentationPlan.headline}</p>
                    <p className="mt-1 break-words opacity-90">{presentationPlan.reason}</p>
                    {result?.resumenHumano ? (
                      <p className="mt-2 whitespace-pre-wrap break-words rounded-lg border border-emerald-300/40 bg-white/60 px-2 py-1.5 text-[11px] leading-relaxed dark:bg-slate-900/50">
                        {result.resumenHumano}
                      </p>
                    ) : null}
                  </div>
                </div>
              )}
            </div>
          </div>

          <div className={panelClass}>
            <p className={`text-sm font-semibold ${isLightTheme ? 'text-sky-700' : 'text-cyan-200'}`}>Visualización automática</p>
            <p className={`mt-1 break-words text-[11px] ${isLightTheme ? 'text-slate-500' : 'text-slate-400'}`}>Modo: {presentationPlan.mode} · {presentationPlan.reason}</p>
            <div className="mt-2 flex flex-wrap gap-1.5">
              <span className={`rounded-full border px-2.5 py-0.5 text-[10px] font-semibold ${isLightTheme ? 'border-sky-200 bg-sky-50 text-sky-700' : 'border-cyan-500/30 bg-cyan-500/10 text-cyan-200'}`}>
                {chartTypeLabel}
              </span>
              <span className={`rounded-full border px-2.5 py-0.5 text-[10px] ${isLightTheme ? 'border-slate-200 bg-white text-slate-600' : 'border-slate-700 bg-slate-900 text-slate-300'}`}>
                {visualizationSummary.rowCount} filas
              </span>
              <span className={`rounded-full border px-2.5 py-0.5 text-[10px] ${isLightTheme ? 'border-slate-200 bg-white text-slate-600' : 'border-slate-700 bg-slate-900 text-slate-300'}`}>
                {visualizationSummary.columnCount} columnas
              </span>
            </div>
            <div className="mt-3 h-[320px] min-h-0">
              {loadingQuery ? (
                <div className="space-y-2">
                  <div className="h-8 animate-pulse rounded bg-slate-800" />
                  <div className="h-56 animate-pulse rounded bg-slate-800" />
                </div>
              ) : activeDashboard?.chartType === 'kpi' && Array.isArray(result?.data) && result.data.length > 0 ? (
                <div className={`h-full overflow-auto rounded-2xl border p-4 ${isLightTheme ? 'border-emerald-200 bg-emerald-50/60 text-slate-700' : 'border-emerald-500/30 bg-emerald-500/10 text-slate-100'}`}>
                  <p className={`text-xs uppercase tracking-wide ${isLightTheme ? 'text-emerald-700' : 'text-emerald-300'}`}>KPI principal</p>
                  <p className="mt-3 text-4xl font-bold">
                    {formatCellValue((result.data[0] as Record<string, unknown>)?.[String(activeDashboard?.yKey || Object.keys(result.data[0] || {})[0] || '')])}
                  </p>
                  <p className="mt-2 text-xs opacity-80">{String(activeDashboard?.yKey || 'valor')}</p>
                  <div className="mt-4 grid grid-cols-2 gap-2 text-xs">
                    {Object.entries(result.data[0] || {}).slice(0, 4).map(([key, value]) => (
                      <div key={`kpi-extra-${key}`} className={`rounded-lg border px-2 py-1.5 ${isLightTheme ? 'border-emerald-200 bg-white' : 'border-emerald-500/20 bg-slate-900/40'}`}>
                        <p className="opacity-70">{key}</p>
                        <p className="font-semibold">{formatCellValue(value)}</p>
                      </div>
                    ))}
                  </div>
                </div>
              ) : activeDashboard?.chartType === 'bar' && chartRows.length > 0 ? (
                <div className={`h-full rounded-2xl border p-2 ${isLightTheme ? 'border-sky-200 bg-gradient-to-b from-white to-sky-50/60' : 'border-slate-700 bg-slate-950/55'}`}>
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={chartRows}>
                      <CartesianGrid stroke="#334155" strokeDasharray="3 3" />
                      <XAxis dataKey="name" tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={false} tickLine={false} tickFormatter={(value) => truncateLabel(String(value), 14)} interval={0} angle={-10} textAnchor="end" height={48} />
                      <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={false} tickLine={false} />
                      <Tooltip contentStyle={{ background: '#020617', border: '1px solid #334155', borderRadius: 12, color: '#e2e8f0' }} />
                      <Legend wrapperStyle={{ color: '#cbd5e1' }} />
                      <Bar dataKey="value" name="Valor" radius={[8, 8, 2, 2]}>
                        {chartRows.map((entry, idx) => (
                          <Cell key={`bar-cell-${entry.name}-${idx}`} fill={PIE_COLORS[idx % PIE_COLORS.length]} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              ) : activeDashboard?.chartType === 'line' && chartRows.length > 0 ? (
                <div className={`h-full rounded-2xl border p-2 ${isLightTheme ? 'border-sky-200 bg-gradient-to-b from-white to-sky-50/60' : 'border-slate-700 bg-slate-950/55'}`}>
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={chartRows}>
                      <defs>
                        <linearGradient id="lineFill" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor="#22d3ee" stopOpacity={0.4} />
                          <stop offset="95%" stopColor="#22d3ee" stopOpacity={0} />
                        </linearGradient>
                      </defs>
                      <CartesianGrid stroke="#334155" strokeDasharray="3 3" />
                      <XAxis dataKey="name" tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={false} tickLine={false} tickFormatter={(value) => truncateLabel(String(value), 14)} />
                      <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={false} tickLine={false} />
                      <Tooltip contentStyle={{ background: '#020617', border: '1px solid #334155', borderRadius: 12, color: '#e2e8f0' }} />
                      <Area type="monotone" dataKey="value" stroke="#22d3ee" fill="url(#lineFill)" strokeWidth={3} />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>
              ) : activeDashboard?.chartType === 'pie' && chartRows.length > 0 ? (
                <div className={`h-full rounded-2xl border p-2 ${isLightTheme ? 'border-sky-200 bg-gradient-to-b from-white to-sky-50/60' : 'border-slate-700 bg-slate-950/55'}`}>
                  <ResponsiveContainer width="100%" height="100%">
                    <PieChart>
                      <Tooltip contentStyle={{ background: '#020617', border: '1px solid #334155', borderRadius: 12, color: '#e2e8f0' }} />
                      <Legend wrapperStyle={{ color: '#cbd5e1' }} formatter={(value) => truncateLabel(String(value), 18)} />
                      <Pie data={chartRows} dataKey="value" nameKey="name" outerRadius={90} innerRadius={38} labelLine={false}>
                        {chartRows.map((entry, idx) => (
                          <Cell key={`pie-cell-${entry.name}-${idx}`} fill={PIE_COLORS[idx % PIE_COLORS.length]} />
                        ))}
                      </Pie>
                    </PieChart>
                  </ResponsiveContainer>
                </div>
              ) : activeDashboard?.chartType === 'histogram' && chartRows.length > 0 ? (
                <div className={`h-full rounded-2xl border p-2 ${isLightTheme ? 'border-sky-200 bg-gradient-to-b from-white to-sky-50/60' : 'border-slate-700 bg-slate-950/55'}`}>
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={chartRows}>
                      <CartesianGrid stroke="#334155" strokeDasharray="3 3" />
                      <XAxis dataKey="name" tick={{ fill: '#94a3b8', fontSize: 10 }} axisLine={false} tickLine={false} interval={0} angle={-20} textAnchor="end" height={56} tickFormatter={(value) => truncateLabel(String(value), 20)} />
                      <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={false} tickLine={false} />
                      <Tooltip contentStyle={{ background: '#020617', border: '1px solid #334155', borderRadius: 12, color: '#e2e8f0' }} />
                      <Legend wrapperStyle={{ color: '#cbd5e1' }} />
                      <Bar dataKey="value" name="Frecuencia" radius={[8, 8, 2, 2]} fill={HISTOGRAM_BAR_COLOR} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              ) : result && !isDebugMode && Array.isArray(result?.data) && result.data.length > 0 ? (
                <div className={`h-full overflow-auto rounded-2xl border p-3 ${isLightTheme ? 'border-sky-200 bg-sky-50/50 text-slate-700' : 'border-slate-700 bg-slate-950/50 text-slate-200'}`}>
                  <p className={`text-xs font-semibold uppercase tracking-wide ${isLightTheme ? 'text-sky-700' : 'text-cyan-300'}`}>
                    Resumen automático
                  </p>
                  <div className="mt-2 grid grid-cols-2 gap-2 text-xs">
                    <div className={`rounded-lg border px-2 py-1.5 ${isLightTheme ? 'border-sky-200 bg-white' : 'border-slate-700 bg-slate-900'}`}>
                      <p className="opacity-70">Filas</p>
                      <p className="font-semibold">{visualizationSummary.rowCount}</p>
                    </div>
                    <div className={`rounded-lg border px-2 py-1.5 ${isLightTheme ? 'border-sky-200 bg-white' : 'border-slate-700 bg-slate-900'}`}>
                      <p className="opacity-70">Columnas</p>
                      <p className="font-semibold">{visualizationSummary.columnCount}</p>
                    </div>
                  </div>

                  {visualizationSummary.columns.length > 0 && (
                    <div className="mt-3">
                      <p className="text-[11px] font-semibold opacity-80">Campos detectados</p>
                      <div className="mt-1 flex flex-wrap gap-1.5">
                        {visualizationSummary.columns.map((col) => (
                          <span
                            key={`viz-col-${col}`}
                            className={`rounded-full border px-2 py-0.5 text-[10px] ${isLightTheme ? 'border-sky-200 bg-white' : 'border-slate-700 bg-slate-900'}`}
                          >
                            {col}
                          </span>
                        ))}
                      </div>
                    </div>
                  )}

                  {visualizationSummary.preview.length > 0 && (
                    <div className="mt-3 space-y-1 text-xs">
                      <p className="text-[11px] font-semibold opacity-80">Vista rápida (primera fila)</p>
                      {visualizationSummary.preview.map(([key, value]) => (
                        <div key={`viz-preview-${key}`} className="flex items-start justify-between gap-3">
                          <span className="opacity-70">{key}</span>
                          <span className="max-w-[62%] whitespace-normal break-words text-right font-medium">{String(value ?? '')}</span>
                        </div>
                      ))}
                    </div>
                  )}

                  {resultInsights.length > 0 && (
                    <div className="mt-3 space-y-1.5 text-xs">
                      <p className="text-[11px] font-semibold opacity-80">Hallazgos clave</p>
                      {resultInsights.map((insight, idx) => (
                        <p key={`insight-${idx}`} className={`rounded-md border px-2 py-1 ${isLightTheme ? 'border-sky-200 bg-white' : 'border-slate-700 bg-slate-900'}`}>
                          {insight}
                        </p>
                      ))}
                    </div>
                  )}
                </div>
              ) : isDebugMode ? (
                <div className={`h-full overflow-auto rounded-2xl border p-3 ${isLightTheme ? 'border-amber-200 bg-amber-50/60 text-slate-700' : 'border-amber-500/30 bg-amber-500/10 text-slate-200'}`}>
                  <p className={`text-xs font-semibold uppercase tracking-wide ${isLightTheme ? 'text-amber-700' : 'text-amber-300'}`}>
                    Modo debugging automático
                  </p>
                  <div className="mt-2 space-y-1 text-xs">
                    <p><span className="font-semibold">Tipo error:</span> {String(debugResult?.tipo_error || 'N/A')}</p>
                    <p><span className="font-semibold">Archivo origen:</span> {String(debugResult?.archivo_origen || debugResult?.archivo || 'N/A')}</p>
                    <p><span className="font-semibold">Línea:</span> {String(debugResult?.linea ?? 'N/A')}</p>
                    <p><span className="font-semibold">Capa:</span> {String(debugResult?.capa || 'N/A')}</p>
                    <p><span className="font-semibold">Flujo:</span> {debugFlow.length > 0 ? debugFlow.join(' → ') : 'N/A'}</p>
                  </div>
                  {debugTrace.length > 0 && (
                    <div className="mt-3">
                      <p className="text-[11px] font-semibold opacity-90">Traza</p>
                      <ul className="mt-1 space-y-1 text-[11px]">
                        {debugTrace.map((frame, index) => {
                          const fileName = String(frame?.archivo || frame?.raw || '').trim();
                          const line = Number.isFinite(Number(frame?.linea)) ? Number(frame.linea) : null;
                          return (
                            <li key={`trace-viz-${index}`}>
                              {fileName ? `${fileName}${line !== null ? `:${line}` : ''}` : `frame-${index + 1}`}
                            </li>
                          );
                        })}
                      </ul>
                    </div>
                  )}
                </div>
              ) : (
                <div className={`flex h-full items-center justify-center rounded-2xl border border-dashed text-sm ${isLightTheme ? 'border-sky-200 bg-sky-50/40 text-sky-700' : 'border-slate-700 bg-slate-950/40 text-slate-500'}`}>
                  Ejecuta una consulta para visualizar resultados
                </div>
              )}
            </div>
          </div>
        </section>

        <section className="mt-5 grid grid-cols-1 gap-5 2xl:grid-cols-[1.22fr_0.78fr]">
          <div className={panelClass}>
            <div className="mb-3 flex items-center justify-between">
              <p className={`text-sm font-semibold ${isLightTheme ? 'text-sky-700' : 'text-cyan-200'}`}>Resultado de consulta</p>
              <span className={`rounded-full border px-2.5 py-1 text-xs ${isLightTheme ? 'border-sky-200 bg-sky-50 text-slate-600' : 'border-slate-700 bg-slate-900 text-slate-300'}`}>
                {isDebugMode ? 'modo debugging' : `${result?.count || 0} filas ${result?.cached ? '· cache' : ''}`}
              </span>
            </div>
            {isDebugMode ? (
              <div className={`rounded-xl border px-4 py-3 text-xs ${isLightTheme ? 'border-amber-200 bg-amber-50 text-slate-700' : 'border-amber-500/30 bg-amber-500/10 text-slate-200'}`}>
                <p className="mb-2 font-semibold">Resultado de análisis</p>
                <p><span className="font-semibold">Tipo:</span> {String(debugResult?.tipo_error || 'N/A')}</p>
                <p><span className="font-semibold">Archivo origen:</span> {String(debugResult?.archivo_origen || debugResult?.archivo || 'N/A')}</p>
                <p><span className="font-semibold">Línea:</span> {String(debugResult?.linea ?? 'N/A')}</p>
                <p><span className="font-semibold">Capa:</span> {String(debugResult?.capa || 'N/A')}</p>
                <p><span className="font-semibold">Flujo:</span> {debugFlow.length > 0 ? debugFlow.join(' → ') : 'N/A'}</p>
                {debugTrace.length > 0 && (
                  <div className="mt-2">
                    <p className="font-semibold">Traza</p>
                    <ul className="mt-1 space-y-1 text-[11px]">
                      {debugTrace.map((frame, index) => {
                        const fileName = String(frame?.archivo || frame?.raw || '').trim();
                        const line = Number.isFinite(Number(frame?.linea)) ? Number(frame.linea) : null;
                        return (
                          <li key={`trace-table-${index}`}>
                            {fileName ? `${fileName}${line !== null ? `:${line}` : ''}` : `frame-${index + 1}`}
                          </li>
                        );
                      })}
                    </ul>
                  </div>
                )}
              </div>
            ) : Array.isArray(result?.data) && result.data.length > 0 ? (
              <div className={`max-h-[420px] overflow-x-auto overflow-y-auto rounded-xl border ${isLightTheme ? 'border-sky-200 bg-white/70' : 'border-slate-700 bg-slate-950/40'}`}>
                <table className="min-w-full text-xs sm:min-w-[760px]">
                  <thead>
                    <tr className={`sticky top-0 z-10 ${isLightTheme ? 'bg-sky-50/95' : 'bg-slate-950/95'}`}>
                      {Object.keys(result.data![0]).map((col) => (
                        <th key={col} className={`max-w-[200px] whitespace-normal break-words px-3 py-2.5 text-left text-[10px] font-semibold uppercase tracking-wider ${isLightTheme ? 'text-slate-500' : 'text-slate-400'}`}>
                          {col}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {result.data.slice(0, 100).map((row, i) => (
                      <tr
                        key={i}
                        className={`border-t transition-colors ${
                          isLightTheme
                            ? `border-sky-100 ${i % 2 === 0 ? '' : 'bg-sky-50/40'} hover:bg-sky-100/60`
                            : `border-slate-800 ${i % 2 === 0 ? '' : 'bg-slate-800/30'} hover:bg-slate-700/40`
                        }`}
                      >
                        {Object.keys(result.data![0]).map((col) => {
                          const rawValue = (row as Record<string, unknown>)?.[col];
                          const boolValue = normalizeBooleanValue(rawValue);

                          return (
                            <td key={col} className={`max-w-[200px] whitespace-normal break-words px-3 py-2 align-top ${isLightTheme ? 'text-slate-700' : 'text-slate-200'}`}>
                              {boolValue === null ? (
                                <span className="leading-relaxed">{formatCellValue(rawValue)}</span>
                              ) : (
                                <span className={`inline-flex rounded-full border px-2 py-0.5 text-[10px] font-semibold ${boolValue
                                  ? (isLightTheme ? 'border-emerald-300 bg-emerald-50 text-emerald-700' : 'border-emerald-500/40 bg-emerald-500/15 text-emerald-300')
                                  : (isLightTheme ? 'border-slate-300 bg-slate-100 text-slate-700' : 'border-slate-600 bg-slate-800 text-slate-300')
                                }`}>
                                  {boolValue ? 'Sí' : 'No'}
                                </span>
                              )}
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className={`rounded-xl border px-4 py-8 text-center text-xs ${isLightTheme ? 'border-sky-200 text-slate-400' : 'border-slate-700 text-slate-500'}`}>
                {result ? 'Sin resultados para esta consulta' : 'Ejecuta una consulta para ver resultados'}
              </div>
            )}
          </div>

          <div className={panelClass}>
            <div className="flex items-center justify-between">
              <p className={`text-sm font-semibold ${isLightTheme ? 'text-sky-700' : 'text-cyan-200'}`}>Historial pro</p>
              <button
                onClick={clearHistory}
                disabled={clearingHistory}
                className={`rounded-lg border px-2 py-1 text-[10px] font-semibold transition ${isLightTheme ? 'border-rose-300 bg-rose-50 text-rose-700 hover:bg-rose-100' : 'border-rose-500/40 bg-rose-500/10 text-rose-300 hover:bg-rose-500/20'} disabled:opacity-60`}
              >
                {clearingHistory ? 'Limpiando...' : 'Borrar'}
              </button>
            </div>
            <div className="mt-3 flex gap-2">
              <input
                value={historySearch}
                onChange={(e) => setHistorySearch(e.target.value)}
                placeholder="Buscar usuario o SQL..."
                className={`w-full rounded-xl border px-3 py-2 text-xs ${isLightTheme ? 'border-sky-200 bg-white text-slate-900 placeholder:text-slate-400' : 'border-slate-700 bg-slate-950/80 text-slate-100 placeholder:text-slate-500'}`}
              />
              <select
                value={historyStatus}
                onChange={(e) => setHistoryStatus(e.target.value as 'all' | 'ok' | 'error')}
                className={`rounded-xl border px-3 py-2 text-xs ${isLightTheme ? 'border-sky-200 bg-white text-slate-700' : 'border-slate-700 bg-slate-950/80 text-slate-200'}`}
              >
                <option value="all">Todos</option>
                <option value="ok">OK</option>
                <option value="error">Error</option>
              </select>
            </div>

            <div className={`mt-3 max-h-[360px] overflow-auto rounded-xl border ${isLightTheme ? 'border-sky-200 bg-white/70' : 'border-slate-700 bg-slate-950/50'}`}>
              {historyLoading ? (
                <div className="space-y-2 p-3">
                  {[1, 2, 3, 4].map((i) => (
                    <div key={`hist-skeleton-${i}`} className="h-8 animate-pulse rounded bg-slate-800" />
                  ))}
                </div>
              ) : (
                <table className="min-w-full text-xs">
                  <thead className={isLightTheme ? 'sticky top-0 bg-sky-50/95 text-slate-600' : 'sticky top-0 bg-slate-950 text-slate-300'}>
                    <tr>
                      <th className="px-2 py-2 text-left">Usuario</th>
                      <th className="px-2 py-2 text-left">ms</th>
                      <th className="px-2 py-2 text-left">Estado</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredHistory.map((row) => (
                      <tr key={row.id} className={`border-t transition ${isLightTheme ? 'border-sky-100 hover:bg-sky-50/70' : 'border-slate-800 hover:bg-slate-800/60'}`}>
                        <td className={`px-2 py-2 ${isLightTheme ? 'text-slate-700' : 'text-slate-200'}`}>{row.username}</td>
                        <td className={`px-2 py-2 ${isLightTheme ? 'text-slate-600' : 'text-slate-300'}`}>{row.execution_ms}</td>
                        <td className={`px-2 py-2 ${isLightTheme ? 'text-slate-600' : 'text-slate-300'}`}>
                          <span className={`rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase ${statusClass(row.status)}`}>
                            {row.status}
                          </span>
                          {row.was_cached ? (
                            <span className="ml-1 rounded-full border border-cyan-500/40 bg-cyan-500/15 px-2 py-0.5 text-[10px] font-semibold uppercase text-cyan-300">
                              cache
                            </span>
                          ) : null}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        </section>
      </div>
    </div>
  );
}

export default withAuth(DashboardPage, ['user', 'admin', 'superadmin']);

