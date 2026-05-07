// ─── Cell formatting and result insight generation ────────────────────────────

export function formatCellValue(value: unknown): string {
  if (value === null || value === undefined || value === '') return '—';
  if (typeof value === 'number' && Number.isFinite(value)) return value.toLocaleString('es-PE');
  if (typeof value === 'boolean') return value ? 'Sí' : 'No';
  const raw = String(value);
  const timestamp = Date.parse(raw);
  if (Number.isFinite(timestamp) && /(t|\d{4}-\d{2}-\d{2})/i.test(raw)) {
    return new Date(timestamp).toLocaleString('es-PE');
  }
  return raw;
}

export function normalizeBooleanValue(value: unknown): boolean | null {
  if (typeof value === 'boolean') return value;
  const raw = String(value ?? '').trim().toLowerCase();
  if (['true', '1', 'si', 'sí', 'yes', 'y'].includes(raw)) return true;
  if (['false', '0', 'no', 'n'].includes(raw)) return false;
  return null;
}

export function truncateLabel(value: string, max = 18): string {
  const text = String(value || '').trim();
  if (text.length <= max) return text;
  return `${text.slice(0, max - 1)}…`;
}

function normalizeText(value: string): string {
  return String(value || '').toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '');
}

export function buildResultInsights(rows: Record<string, unknown>[], userQuery: string): string[] {
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
      if (value !== null && value !== undefined && value !== '' && Number.isFinite(Number(value))) numeric += 1;
    }
    return numeric >= Math.ceil(Math.min(20, rows.length) * 0.7);
  });

  if (numericKey) {
    const values = rows.map((row) => Number(row?.[numericKey])).filter((v) => Number.isFinite(v));
    if (values.length > 0) {
      const max = Math.max(...values);
      const min = Math.min(...values);
      const avg = values.reduce((acc, v) => acc + v, 0) / values.length;
      insights.push(`Métrica ${numericKey}: min ${formatCellValue(min)}, max ${formatCellValue(max)}, promedio ${formatCellValue(Math.round(avg * 100) / 100)}.`);
    }
  }

  const candidateLabel = visibleKeys.find((k) => /(usuario|user|username|nombre|name|rol|role|estado|status)/i.test(k)) || visibleKeys[0];
  if (candidateLabel) {
    const labels = rows.map((row) => formatCellValue(row?.[candidateLabel])).filter((text) => text && text !== '—' && text !== '[object Object]').slice(0, 3);
    if (labels.length > 0) insights.push(`Ejemplos en ${candidateLabel}: ${labels.join(', ')}.`);
  }

  if (/usuarios|user/.test(normalizeText(userQuery))) {
    insights.push('La tabla de usuarios se mostró en formato tabular para facilitar comparación campo a campo.');
  }

  return insights.slice(0, 4);
}
