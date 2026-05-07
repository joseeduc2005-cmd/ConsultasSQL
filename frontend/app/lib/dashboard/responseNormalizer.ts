// ─── API response normalization ───────────────────────────────────────────────
import { AutoQueryResponse, AutoDashboard, DashboardUser } from './types';

export function hasDebugResultPayload(payload: unknown): payload is { resumenHumano?: string; resultado: unknown } {
  return Boolean(payload && typeof payload === 'object' && 'resultado' in payload);
}

export function extractResultadoData(payload: unknown): { rows: Record<string, unknown>[]; total: number; entidad?: string } {
  if (!payload || typeof payload !== 'object') return { rows: [], total: 0 };
  const maybe = payload as { resultado?: unknown };

  if (Array.isArray(maybe.resultado)) {
    const rows = maybe.resultado as Record<string, unknown>[];
    return { rows, total: rows.length };
  }

  const resultado = maybe?.resultado && typeof maybe.resultado === 'object' && !Array.isArray(maybe.resultado)
    ? (maybe.resultado as Record<string, unknown>) : null;
  const rows = Array.isArray(resultado?.data) ? (resultado?.data as Record<string, unknown>[]) : (resultado ? [resultado] : []);

  const looksLikeDebugObject = Boolean(resultado && (
    'tipo_error' in resultado || 'flujo_detectado' in resultado || 'traza' in resultado || 'solucion' in resultado || 'causa' in resultado
  ));
  if (looksLikeDebugObject && !Array.isArray(resultado?.data)) return { rows: [], total: 0 };

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

export function buildHybridResult(payload: unknown, fallbackSql = ''): AutoQueryResponse {
  if (hasDebugResultPayload(payload)) {
    const debugPayload = payload as { resumenHumano?: unknown; resultado: unknown };
    const resultadoData = extractResultadoData(payload);

    if (resultadoData.rows.length > 0) {
      return {
        success: true, mode: 'sql', sql: fallbackSql || undefined,
        resumenHumano: String(debugPayload.resumenHumano || '').trim(),
        resultado: (debugPayload.resultado || null) as Record<string, unknown> | unknown[] | null,
        data: resultadoData.rows, count: resultadoData.total, cached: false,
      };
    }

    return {
      success: true, mode: 'debug', sql: fallbackSql || undefined,
      resumenHumano: String(debugPayload.resumenHumano || '').trim(),
      resultado: (debugPayload.resultado || null) as Record<string, unknown> | unknown[] | null,
      data: [], count: 0, cached: false,
    };
  }

  const maybe = (payload && typeof payload === 'object' ? payload : {}) as {
    success?: boolean; sql?: string; rowCount?: number; count?: number; cached?: boolean; dashboard?: AutoDashboard;
  };
  const rows = extractClassicRows(payload);
  return {
    success: maybe.success !== false, mode: 'sql',
    sql: String(maybe.sql || fallbackSql || '').trim() || undefined,
    data: rows, count: Number(maybe.rowCount || maybe.count || rows.length || 0),
    cached: Boolean(maybe.cached), dashboard: maybe.dashboard,
  };
}

export function normalizeUser(user: unknown): DashboardUser | null {
  if (!user || typeof user !== 'object') return null;
  const maybe = user as Record<string, unknown>;
  const username = String(maybe.username || '').trim();
  const role = String(maybe.role || '').trim().toLowerCase();
  if (!username) return null;
  return { username, role: role || 'user' };
}
