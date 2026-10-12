/**
 * Cómo se le cuenta a una persona lo que hizo una consulta.
 *
 * Explicar un SQL en castellano, dar forma al resultado, decidir qué avisos del motor
 * vale la pena enseñar. Nada de esto decide qué se ejecuta: decide qué se lee después,
 * que es lo que la gente juzga.
 *
 * Es dominio puro —entra un dato, sale texto— y por eso se puede probar sin levantar
 * nada, cosa que estando dentro de `app.js` no pasaba.
 *
 * Queda fuera a propósito `buildNaturalLanguageSuggestions`: lee la caché del
 * aprendizaje semántico, que es estado mutable que `app.js` escribe en veinticinco
 * sitios. Traérsela aquí haría que el dominio dependiera de cuándo alguien la refrescó.
 */

export function buildHumanSqlExplanation(rows = []) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return 'La consulta se ejecutó correctamente pero no devolvió registros.';
  }

  const first = rows[0] || {};

  // ── MongoDB mutation responses ────────────────────────────────────────────
  if ('insertedId' in first) {
    const id = String(first.insertedId || '').slice(0, 24);
    return `✅ Documento insertado correctamente. ID: ${id}`;
  }
  if ('insertedCount' in first) {
    const n = Number(first.insertedCount || 0);
    return `✅ Se insertaron ${n} documento${n !== 1 ? 's' : ''} correctamente.`;
  }
  if ('modifiedCount' in first || 'matchedCount' in first) {
    const matched = Number(first.matchedCount ?? 0);
    const modified = Number(first.modifiedCount ?? 0);
    const upserted = Number(first.upsertedCount ?? 0);
    return `✅ Documentos encontrados: ${matched}, modificados: ${modified}${upserted > 0 ? `, insertados (upsert): ${upserted}` : ''}.`;
  }
  if ('deletedCount' in first) {
    const n = Number(first.deletedCount || 0);
    return `✅ Se eliminaron ${n} documento${n !== 1 ? 's' : ''}.`;
  }

  const normalizeLabel = (key) => String(key || '')
    .replace(/^[_\W]+|[_\W]+$/g, '')
    .replace(/_/g, ' ')
    .toLowerCase();

  const inferFieldRole = (key) => {
    const k = String(key || '').toLowerCase();
    if (/(_id$|^id$|uuid|guid)/.test(k)) return 'identifier';
    if (/(count|total|cantidad|qty|numero|num|sum|avg|max|min|monto|importe|valor|amount|price|precio|saldo)/.test(k)) return 'metric';
    if (/(status|estado|clasificacion|class|tipo|type|rol|role)/.test(k)) return 'status';
    if (/(name|username|nombre|titulo|title)/.test(k)) return 'entity';
    if (/(date|fecha|time|hora|timestamp|created|updated)/.test(k)) return 'temporal';
    if (typeof first[k] === 'number') return 'metric';
    return 'text';
  };

  const formatMetricSentence = (key, value) => {
    const k = String(key || '').toLowerCase();
    const n = Number(value);
    const hasNumber = Number.isFinite(n);
    if (!hasNumber) return `El valor de ${normalizeLabel(key)} es ${String(value)}.`;

    if (/(session|sesion)/.test(k)) {
      if (n <= 0) return 'No tiene sesiones registradas.';
      if (n === 1) return 'Tiene 1 sesión registrada.';
      return `Tiene ${n} sesiones registradas.`;
    }
    if (/(user|usuario)/.test(k)) {
      if (n <= 0) return 'No existen usuarios en el resultado.';
      if (n === 1) return 'Existe 1 usuario en el sistema.';
      return `Existen ${n} usuarios en el sistema.`;
    }
    if (/(log|actividad|evento|evento)/.test(k)) {
      if (n <= 0) return 'No hay actividad registrada.';
      if (n === 1) return 'Hay 1 actividad registrada.';
      return `Hay ${n} actividades registradas.`;
    }
    if (/(monto|importe|amount|price|precio|saldo|valor)/.test(k)) {
      return `El monto total es ${n}.`;
    }

    return `El total de ${normalizeLabel(key)} es ${n}.`;
  };

  const usableEntries = Object.entries(first)
    .filter(([key, value]) => value !== null && value !== undefined && String(value).trim?.() !== '')
    .map(([key, value]) => ({ key, value, role: inferFieldRole(key) }))
    .filter((item) => item.role !== 'identifier' && !/^(__|step\d+\.|v_|p_|l_)/i.test(item.key));

  if (usableEntries.length === 0) {
    return `La consulta se ejecutó correctamente y devolvió ${rows.length} ${rows.length === 1 ? 'registro' : 'registros'}.`;
  }

  const entityEntry = usableEntries.find((entry) => entry.role === 'entity' && typeof entry.value === 'string');
  const statusEntry = usableEntries.find((entry) => entry.role === 'status');
  const metricEntries = usableEntries.filter((entry) => entry.role === 'metric').slice(0, 2);
  const temporalEntry = usableEntries.find((entry) => entry.role === 'temporal');

  const parts = [];

  if (entityEntry) {
    parts.push(`Entidad principal: ${String(entityEntry.value)}.`);
  }

  if (statusEntry) {
    parts.push(`${normalizeLabel(statusEntry.key)}: ${String(statusEntry.value)}.`);
  }

  for (const metric of metricEntries) {
    parts.push(formatMetricSentence(metric.key, metric.value));
  }

  if (temporalEntry) {
    const parsed = new Date(String(temporalEntry.value));
    const formatted = Number.isNaN(parsed.getTime())
      ? String(temporalEntry.value)
      : parsed.toLocaleString('es-ES');
    parts.push(`${normalizeLabel(temporalEntry.key)}: ${formatted}.`);
  }

  if (parts.length === 0) {
    return `La consulta se ejecutó correctamente y devolvió ${rows.length} ${rows.length === 1 ? 'registro' : 'registros'}.`;
  }

  return parts.join(' ').replace(/\s+/g, ' ').trim();
}

export function sanitizeProductionResultObject(input = {}) {
  const source = input && typeof input === 'object' && !Array.isArray(input) ? input : {};
  const blockedKey = /^(workflow|steps?|debug|script|sql|query|executed|placeholder|context|internal|step\d+|__|v_|p_|l_)/i;
  const blockedStepSuffix = /_step\d+$/i;
  const sensitiveKey = /(password|passwd|pwd|hash|token|secret|apikey|api_key|salt|refresh_token|access_token)/i;
  const compact = {};
  const canonicalMap = new Map();

  const normalizeCanonicalKey = (key = '') => String(key || '').trim().replace(/_step\d+$/i, '').toLowerCase();
  const isPrimitive = (value) => ['string', 'number', 'boolean'].includes(typeof value);
  const isUsefulArray = (value) => Array.isArray(value)
    && value.length > 0
    && value.length <= 5
    && value.every((item) => isPrimitive(item) && String(item).length <= 80);

  for (const [rawKey, rawValue] of Object.entries(source)) {
    const key = String(rawKey || '').trim();
    if (!key || blockedKey.test(key) || blockedStepSuffix.test(key) || sensitiveKey.test(key)) continue;
    if (rawValue === null || rawValue === undefined) continue;
    if (typeof rawValue === 'string' && rawValue.trim() === '') continue;

    const canonical = normalizeCanonicalKey(key);
    if (!canonical) continue;

    // Keep only useful primitives/small arrays; discard large nested payloads.
    if (!(isPrimitive(rawValue) || isUsefulArray(rawValue))) continue;
    if (typeof rawValue === 'string' && rawValue.length > 240) continue;

    const existingKey = canonicalMap.get(canonical);
    if (!existingKey) {
      canonicalMap.set(canonical, key);
      compact[key] = rawValue;
      continue;
    }

    // Prefer non-empty scalar values when duplicate semantic keys appear.
    const existingValue = compact[existingKey];
    const existingIsEmpty = existingValue === '' || existingValue === null || existingValue === undefined;
    if (existingIsEmpty && rawValue !== null && rawValue !== undefined && String(rawValue).trim() !== '') {
      compact[existingKey] = rawValue;
    }
  }

  const keys = Object.keys(compact);
  if (keys.length <= 14) {
    return compact;
  }

  // For very wide rows, project top-priority fields similar to what users expect in SQL grids.
  const priorityOrder = [
    'username', 'usuario', 'nombre', 'email',
    'role', 'rol', 'estado', 'activo', 'bloqueado',
    'total', 'total_logs', 'total_sesiones', 'sesiones_activas', 'comentarios_principales',
    'count', 'row_count',
    'id', 'user_id', 'role_id',
    'created_at', 'fecha', 'updated_at',
  ];

  const projected = {};
  const lowered = Object.entries(compact).map(([k, v]) => [k, String(k).toLowerCase(), v]);

  for (const preferred of priorityOrder) {
    const hit = lowered.find(([, lower]) => lower === preferred);
    if (hit) projected[hit[0]] = hit[2];
  }

  for (const [key, lowerKey, value] of lowered) {
    if (Object.prototype.hasOwnProperty.call(projected, key)) continue;
    if (/^(total_|count|promedio|max|min|avg|sum|sesiones|logs|comentarios)/i.test(lowerKey)) {
      projected[key] = value;
    }
    if (Object.keys(projected).length >= 12) break;
  }

  return Object.keys(projected).length > 0 ? projected : compact;
}

export function filterRelevantNotices(notices = []) {
  const blocked = [
    /^=+.*=+$/i,
    /^\s*\[step\s*\d+\]/i,
    /^\s*paso\s*\d+\s*:/i,
    /^\s*\[sql/i,
    /^\s*\[(operation|query|sql operation|sql placeholders|sql params)\]/i,
    /^\s*(inicio|fin)\b/i,
    /^\s*(debug|traza|trace)\b/i,
    /^\s*(motor|workflow|script interpretado)\b/i,
  ];

  const unique = new Set();
  const output = [];

  for (const raw of Array.isArray(notices) ? notices : []) {
    let msg = String(raw || '').trim();
    if (!msg) continue;
    msg = msg.replace(/^NOTICE:\s*/i, '').trim();
    if (!msg) continue;
    // Strip leading emojis / symbols so technical lines like "🧠 Motor SQL..."
    // are still caught by the blocked patterns below.
    const testMsg = msg.replace(/^[^\w[]+/, '').trim();
    if (blocked.some((pattern) => pattern.test(msg) || pattern.test(testMsg))) continue;

    const dedupeKey = msg.toLowerCase();
    if (unique.has(dedupeKey)) continue;
    unique.add(dedupeKey);
    output.push(msg);
  }

  return output;
}

export function buildProductionSummary({ rows = [], semantic = {}, notices = [], fallback = '' } = {}) {
  const cleanNotices = filterRelevantNotices(notices);
  const semanticFromNotices = extractSemanticMetricsFromNotices(cleanNotices);
  const mergedSemantic = {
    ...(semantic && typeof semantic === 'object' && !Array.isArray(semantic) ? semantic : {}),
    ...semanticFromNotices,
  };

  const s = mergedSemantic;
  const totalUsuarios = Number(s.total_usuarios);
  const totalLogs = Number(s.total_logs);
  const totalSesiones = Number(s.total_sesiones);
  const sesionesActivas = Number(s.sesiones_activas);
  const parts = [];

  if (Number.isFinite(totalUsuarios)) {
    parts.push(`Se analizaron ${totalUsuarios} ${totalUsuarios === 1 ? 'usuario' : 'usuarios'}.`);
  }

  if (Number.isFinite(totalLogs) || Number.isFinite(totalSesiones) || Number.isFinite(sesionesActivas)) {
    const logsText = Number.isFinite(totalLogs) ? `${totalLogs} ${totalLogs === 1 ? 'log' : 'logs'} registrados` : 'logs no disponibles';
    const sesionesText = Number.isFinite(sesionesActivas)
      ? `${sesionesActivas} ${sesionesActivas === 1 ? 'sesion activa' : 'sesiones activas'}`
      : Number.isFinite(totalSesiones)
        ? `${totalSesiones} ${totalSesiones === 1 ? 'sesion registrada' : 'sesiones registradas'}`
        : 'sesiones no disponibles';
    parts.push(`Hay ${logsText} y ${sesionesText}.`);
  }

  // Raw notices are already human-readable — return them directly before
  // attempting a synthetic sentence from partial metric extraction.
  if (cleanNotices.length > 0) {
    return cleanNotices.join('\n').trim();
  }

  if (parts.length > 0) {
    return parts.join(' ');
  }

  if (String(fallback || '').trim()) {
    return String(fallback || '').trim();
  }

  return buildHumanSqlExplanation(rows);
}

export function buildProductionSqlResponse({
  success = true,
  rows = [],
  notices = [],
  semanticCandidate = null,
  explicacion = '',
  message = '',
} = {}) {
  void success;
  const data = Array.isArray(rows) ? rows : [];
  const semanticBase = semanticCandidate && typeof semanticCandidate === 'object' && !Array.isArray(semanticCandidate)
    ? semanticCandidate
    : (data[0] && typeof data[0] === 'object' ? data[0] : {});
  const semanticFromNotices = extractSemanticMetricsFromNotices(notices);
  const cleanedSemantic = sanitizeProductionResultObject({
    ...(semanticBase && typeof semanticBase === 'object' && !Array.isArray(semanticBase) ? semanticBase : {}),
    ...semanticFromNotices,
  });
  const cleanedRows = data.filter(
    (row) => row !== null && row !== undefined && typeof row === 'object' && !Array.isArray(row),
  );

  const semanticKeys = Object.keys(cleanedSemantic);
  void semanticKeys;
  const resultado = cleanedRows;
  const cleanNotices = filterRelevantNotices(notices);
  const resumenHumano = buildProductionSummary({
    rows: data,
    semantic: cleanedSemantic,
    notices: cleanNotices,
    fallback: explicacion || message,
  });

  const payload = {
    resumenHumano,
    resultado,
  };

  // Fall back to the generic message ONLY when no better summary exists.
  // buildProductionSummary already returns `explicacion` (e.g. the write
  // before/after detail) as its fallback, so we must not clobber it here.
  if (!String(payload.resumenHumano || '').trim() && String(message || '').trim()) {
    payload.resumenHumano = String(message || '').trim();
  }

  return payload;
}

export function extractSemanticMetricsFromNotices(lines = []) {
  const metrics = {};

  for (const lineRaw of lines || []) {
    const line = String(lineRaw || '').trim();
    if (!line) continue;

    // Examples: "Total logs: 2", "Total sesiones: 1", "Max logs: 5"
    const metricMatch = line.match(/^\s*([a-zA-Z_\s]+?)\s*:\s*(-?\d+(?:\.\d+)?)\s*$/i);
    if (!metricMatch) continue;

    const label = String(metricMatch[1] || '')
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[^a-z0-9\s_]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
    const value = Number(metricMatch[2]);
    if (!Number.isFinite(value)) continue;

    let key = label
      .replace(/\btotal\b/g, 'total')
      .replace(/\bpromedio\b/g, 'promedio')
      .replace(/\bmaximo\b|\bmax\b/g, 'max')
      .replace(/\bsesiones activas\b/g, 'sesiones_activas')
      .replace(/\s+/g, '_');

    if (key === 'total_usuarios' || key === 'usuarios_total') key = 'total_usuarios';
    if (key === 'total_logs' || key === 'logs_total') key = 'total_logs';
    if (key === 'total_sesiones' || key === 'sesiones_total') key = 'total_sesiones';
    if (key === 'total_activos' || key === 'activos_total') key = 'total_activos';
    if (key === 'max_logs' || key === 'logs_max') key = 'max_logs';
    if (key === 'promedio_logs' || key === 'logs_promedio' || key === 'avg_logs') key = 'promedio_logs';

    if (key) metrics[key] = value;
  }

  return metrics;
}
