import path from 'path';
import { askOllama } from '../ai/OllamaClient.js';

const ERROR_AI_CONFIDENCE_THRESHOLD = 0.6;

const ERROR_TRIGGER_RE = /\b(error|exception|failed|failure|refused|denied|timeout|timed\s+out|undefined|null|cannot|syntax\s+error|connection|authentication|unauthorized|forbidden|trace|stack|traceback|fatal|panic|crash)\b/i;

const MESSAGE_COLUMNS = ['mensaje', 'message', 'msg', 'error', 'descripcion', 'description', 'detail', 'details', 'exception', 'reason'];
const STACK_COLUMNS = ['stack', 'stacktrace', 'trace', 'traceback', 'detalle', 'details'];
const MODULE_COLUMNS = ['modulo', 'module', 'source', 'origin', 'component', 'service'];
const CONTEXT_COLUMNS = ['contexto', 'context', 'scope', 'feature', 'domain'];
const FILE_COLUMNS = ['archivo', 'file', 'filename', 'path', 'source_file'];
const LINE_COLUMNS = ['linea', 'line', 'line_number', 'line_no'];
const DATE_COLUMNS = ['fecha', 'created_at', 'timestamp', 'date', 'fecha_hora', 'logged_at', 'occurred_at'];
const LAYER_HINTS = {
  Controller: ['controller', 'route', 'router', 'endpoint', 'handler'],
  Service: ['service', 'usecase', 'workflow', 'manager'],
  Repository: ['repository', 'repo', 'dao', 'store'],
  Database: ['database', 'sql', 'postgres', 'mysql', 'oracle', 'db', 'connection', 'pool'],
  Frontend: ['component', 'view', 'page', 'screen', 'frontend', 'ui'],
};
const ORIGIN_PRIORITY = ['Database', 'Repository', 'Service', 'Backend', 'Controller', 'Frontend'];

const ERROR_PATTERNS = [
  {
    regex: /connection\s+refused/i,
    type: 'connection refused',
    cause: 'conexión rechazada a base de datos o servicio no disponible',
    solution: 'verificar que PostgreSQL o el servicio objetivo esté activo y revisar host/puerto en .env',
    context: 'base de datos',
  },
  {
    regex: /password\s+authentication\s+failed/i,
    type: 'password authentication failed',
    cause: 'credenciales incorrectas o usuario de base de datos inválido',
    solution: 'revisar DB_USER y DB_PASSWORD en .env y validar acceso manual a la base de datos',
    context: 'autenticación',
  },
  {
    regex: /undefined\s+is\s+not\s+a\s+function/i,
    type: 'undefined is not a function',
    cause: 'se invocó una función inexistente o una dependencia no se inicializó correctamente',
    solution: 'revisar la importación o inicialización de la función y validar el valor antes de invocarla',
    context: 'javascript',
  },
  {
    regex: /cannot\s+read\s+(properties|property)\s+of\s+(undefined|null)/i,
    type: 'cannot read property',
    cause: 'se intentó acceder a una propiedad de un valor null o undefined',
    solution: 'validar el objeto antes de leer sus propiedades y revisar el origen del dato faltante',
    context: 'javascript',
  },
  {
    regex: /syntax\s+error/i,
    type: 'syntax error',
    cause: 'la sentencia o el archivo contiene un error de sintaxis',
    solution: 'revisar la sintaxis cerca de la línea reportada y comparar con la estructura esperada del lenguaje o SQL',
    context: 'sintaxis',
  },
  {
    regex: /timeout/i,
    type: 'timeout',
    cause: 'la operación excedió el tiempo máximo permitido',
    solution: 'revisar conectividad, índices y tiempos de espera configurados; optimizar la operación si aplica',
    context: 'rendimiento',
  },
  {
    regex: /denied|forbidden|unauthorized/i,
    type: 'access denied',
    cause: 'el usuario o servicio no tiene permisos suficientes',
    solution: 'revisar roles, permisos y credenciales del usuario afectado',
    context: 'autorización',
  },
];

const STOPWORDS = new Set([
  'error', 'exception', 'failed', 'refused', 'denied', 'timeout', 'undefined', 'null', 'cannot',
  'syntax', 'connection', 'the', 'and', 'for', 'with', 'from', 'this', 'that', 'una', 'para', 'con',
  'los', 'las', 'del', 'por', 'que', 'line', 'stack', 'trace', 'at', 'near', 'module',
]);

function unique(values = []) {
  return [...new Set(values.filter(Boolean))];
}

function normalizeWhitespace(value = '') {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function normalizeToken(value = '') {
  return normalizeWhitespace(value)
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9_./\\-]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function quoteIdentifier(identifier) {
  return `"${String(identifier || '').replace(/"/g, '""')}"`;
}

function pickColumn(columns = [], candidates = []) {
  const normalizedColumns = (columns || []).map((column) => String(column || '').toLowerCase());
  return candidates.find((candidate) => normalizedColumns.includes(candidate)) || null;
}

function extractFileAndLine(text = '') {
  const match = String(text || '').match(/([A-Za-z]:[\\/][^\s:()]+|[A-Za-z0-9_./\\-]+\.(?:js|ts|jsx|tsx|mjs|cjs|java|cs|py|rb|go|php|kt|swift|sql))(?:[:(](\d+))?/i);
  if (!match) {
    return { archivo: null, linea: null };
  }

  return {
    archivo: path.basename(match[1]),
    linea: match[2] ? Number(match[2]) : null,
  };
}

function extractStackFrames(text = '') {
  const lines = String(text || '')
    .split(/\r?\n/)
    .map((line) => String(line || '').trim())
    .filter(Boolean);

  const frames = [];

  for (const line of lines) {
    let match = line.match(/^at\s+(.+?)\s+\(([^()]+)\)$/i);
    if (!match) {
      match = line.match(/^at\s+([^()]+)$/i);
      if (match) {
        const location = parseLocationParts(String(match[1] || '').trim());
        frames.push({
          raw: line,
          funcion: null,
          archivo: location.fileName,
          linea: location.line,
          modulo: location.fileName
            ? path.basename(location.fileName, path.extname(location.fileName))
            : inferModuleName(location.fileRaw, location.fileName),
          capa: inferLayer({ modulo: location.fileRaw, archivo: location.fileName || '', text: line }),
        });
        continue;
      }
    }

    if (match) {
      const functionName = String(match[1] || '').trim();
      const location = parseLocationParts(String(match[2] || '').trim());
      frames.push({
        raw: line,
        funcion: functionName || null,
        archivo: location.fileName,
        linea: location.line,
        modulo: inferModuleName(functionName || location.fileRaw, location.fileName),
        capa: inferLayer({ modulo: functionName, archivo: location.fileName || '', text: line }),
      });
      continue;
    }

    const javaLike = line.match(/^at\s+([A-Za-z0-9_.$]+)\(([A-Za-z0-9_.$-]+):(\d+)\)$/i);
    if (javaLike) {
      const functionName = String(javaLike[1] || '').trim();
      const fileName = path.basename(String(javaLike[2] || '').trim());
      frames.push({
        raw: line,
        funcion: functionName,
        archivo: fileName,
        linea: Number(javaLike[3]),
        modulo: inferModuleName(functionName, fileName),
        capa: inferLayer({ modulo: functionName, archivo: fileName, text: line }),
      });
      continue;
    }

    const pyLike = line.match(/^File\s+"([^"]+)",\s+line\s+(\d+),\s+in\s+(.+)$/i);
    if (pyLike) {
      const fileName = path.basename(String(pyLike[1] || '').trim());
      const functionName = String(pyLike[3] || '').trim();
      frames.push({
        raw: line,
        funcion: functionName || null,
        archivo: fileName,
        linea: Number(pyLike[2]),
        modulo: inferModuleName(functionName, fileName),
        capa: inferLayer({ modulo: functionName, archivo: fileName, text: line }),
      });
    }
  }

  return frames;
}

function parseLocationParts(location = '') {
  const source = String(location || '').trim();
  if (!source) {
    return { fileRaw: '', fileName: null, line: null };
  }

  const tokens = source.split(':');
  if (tokens.length >= 3 && /^\d+$/.test(tokens[tokens.length - 1]) && /^\d+$/.test(tokens[tokens.length - 2])) {
    const fileRaw = tokens.slice(0, -2).join(':').trim();
    return {
      fileRaw,
      fileName: fileRaw.includes('.') ? path.basename(fileRaw) : null,
      line: Number(tokens[tokens.length - 2]),
    };
  }

  if (tokens.length >= 2 && /^\d+$/.test(tokens[tokens.length - 1])) {
    const fileRaw = tokens.slice(0, -1).join(':').trim();
    return {
      fileRaw,
      fileName: fileRaw.includes('.') ? path.basename(fileRaw) : null,
      line: Number(tokens[tokens.length - 1]),
    };
  }

  return {
    fileRaw: source,
    fileName: source.includes('.') ? path.basename(source) : null,
    line: null,
  };
}

function isControllerLikeLayer(layer = '') {
  return String(layer || '').trim().toLowerCase() === 'controller';
}

function findOriginFrame(frames = []) {
  if (!Array.isArray(frames) || frames.length === 0) return { frame: null, index: -1 };

  for (const preferredLayer of ORIGIN_PRIORITY) {
    const index = frames.findIndex((frame) => String(frame?.capa || '') === preferredLayer);
    if (index >= 0) {
      if (preferredLayer !== 'Controller') {
        return { frame: frames[index], index };
      }
      break;
    }
  }

  const nonControllerIndex = frames.findIndex((frame) => !isControllerLikeLayer(frame?.capa));
  if (nonControllerIndex >= 0) {
    return { frame: frames[nonControllerIndex], index: nonControllerIndex };
  }

  return { frame: frames[0], index: 0 };
}

function buildFlow(frames = []) {
  if (!Array.isArray(frames) || frames.length === 0) return [];

  const flow = [];
  const normalizedLayers = frames
    .map((frame) => String(frame?.capa || '').trim())
    .filter(Boolean)
    .map((layer) => (layer === 'Backend' ? 'Service' : layer));

  for (const layer of normalizedLayers.slice().reverse()) {
    if (flow[flow.length - 1] !== layer) {
      flow.push(layer);
    }
  }

  return flow.length > 0 ? [flow.join(' -> ')] : [];
}

function classifyErrorDomain(text = '', layer = 'Backend') {
  const source = normalizeToken(text);
  if (/connection|authentication|timeout|econnrefused|enotfound|pool|postgres|mysql|oracle|database|db/.test(source)) {
    return 'error de conexión';
  }
  if (/syntax|invalid|duplicate key|foreign key|constraint|relation .* does not exist/.test(source)) {
    return 'error de datos';
  }
  if (/undefined|null|not a function|cannot read|referenceerror|typeerror/.test(source)) {
    return 'error de lógica';
  }
  if (/docker|network|dns|infra|kubernetes|memory|cpu|disk|filesystem/.test(source)) {
    return 'error de infraestructura';
  }
  if (layer === 'Database' || layer === 'Repository') return 'error de datos';
  if (layer === 'Service') return 'error de lógica';
  return 'error de infraestructura';
}

function inferModuleName(text = '', archivo = null) {
  const classLikeMatch = String(text || '').match(/\b([A-Z][A-Za-z0-9_]{2,})\b/g);
  if (Array.isArray(classLikeMatch) && classLikeMatch.length > 0) {
    const preferred = classLikeMatch.find((token) => !ERROR_TRIGGER_RE.test(token));
    if (preferred) return preferred;
  }

  const pathMatch = String(text || '').match(/(?:at|in|from)\s+([A-Za-z0-9_./\\-]+)(?::\d+)?/i);
  if (pathMatch?.[1]) {
    const normalizedPath = String(pathMatch[1]).replace(/\\/g, '/').split('/').filter(Boolean);
    const lastSegment = normalizedPath[normalizedPath.length - 1] || '';
    if (lastSegment && !lastSegment.includes('.')) return lastSegment;
  }

  if (archivo) return path.basename(archivo, path.extname(archivo));
  return null;
}

function inferLayer({ modulo = '', archivo = '', text = '' } = {}) {
  const source = normalizeToken(`${modulo} ${archivo} ${text}`);
  for (const [layer, hints] of Object.entries(LAYER_HINTS)) {
    if (hints.some((hint) => source.includes(hint))) {
      return layer;
    }
  }
  return 'Backend';
}

function inferPattern(text = '') {
  return ERROR_PATTERNS.find((pattern) => pattern.regex.test(String(text || ''))) || null;
}

function isWeakInferenceText(value = '') {
  const normalized = normalizeToken(value);
  if (!normalized) return true;
  return /no se pudo determinar|no determinada|desconocid|revisar .* stack trace/.test(normalized);
}

function extractKeywords(text = '', archivo = null, modulo = null) {
  const source = String(text || '');
  const pattern = inferPattern(source);
  const words = unique(
    normalizeWhitespace(source)
      .split(/[^A-Za-z0-9_./-]+/)
      .map((word) => String(word || '').trim())
      .filter((word) => word.length >= 3)
      .filter((word) => !STOPWORDS.has(word.toLowerCase()))
      .slice(0, 12),
  );

  return unique([
    pattern?.type || null,
    modulo || null,
    archivo ? path.basename(archivo, path.extname(archivo)) : null,
    ...words,
  ]).slice(0, 8);
}

function summarizeRecurringPatterns(rows = []) {
  const counts = new Map();
  for (const row of rows) {
    const message = normalizeWhitespace(row?.mensaje || row?.stack || row?.contexto || '');
    if (!message) continue;
    const key = message.toLowerCase().slice(0, 80);
    counts.set(key, (counts.get(key) || 0) + 1);
  }

  return [...counts.entries()]
    .filter(([, count]) => count > 1)
    .sort((left, right) => right[1] - left[1])
    .slice(0, 3)
    .map(([pattern, count]) => ({ patron: pattern, frecuencia: count }));
}

async function discoverHistoricalSources(pool) {
  if (!pool || typeof pool.query !== 'function') return [];

  const result = await pool.query(`
    SELECT table_schema, table_name, column_name
    FROM information_schema.columns
    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
    ORDER BY table_schema, table_name, ordinal_position
  `);

  const grouped = new Map();
  for (const row of result.rows || []) {
    const key = `${row.table_schema}.${row.table_name}`;
    const entry = grouped.get(key) || {
      schema: row.table_schema,
      table: row.table_name,
      columns: [],
    };
    entry.columns.push(String(row.column_name || '').toLowerCase());
    grouped.set(key, entry);
  }

  return [...grouped.values()]
    .map((source) => {
      const messageColumn = pickColumn(source.columns, MESSAGE_COLUMNS);
      const stackColumn = pickColumn(source.columns, STACK_COLUMNS);
      const moduleColumn = pickColumn(source.columns, MODULE_COLUMNS);
      const contextColumn = pickColumn(source.columns, CONTEXT_COLUMNS);
      const fileColumn = pickColumn(source.columns, FILE_COLUMNS);
      const lineColumn = pickColumn(source.columns, LINE_COLUMNS);
      const dateColumn = pickColumn(source.columns, DATE_COLUMNS);
      const normalizedTable = normalizeToken(source.table);

      let score = 0;
      if (messageColumn) score += 4;
      if (stackColumn) score += 2;
      if (dateColumn) score += 2;
      if (moduleColumn) score += 1;
      if (contextColumn) score += 1;
      if (fileColumn) score += 1;
      if (lineColumn) score += 1;
      if (/(error|log|exception|incident|failure|trace)/.test(normalizedTable)) score += 2;

      return {
        ...source,
        score,
        messageColumn,
        stackColumn,
        moduleColumn,
        contextColumn,
        fileColumn,
        lineColumn,
        dateColumn,
      };
    })
    .filter((source) => source.messageColumn && source.score >= 4)
    .sort((left, right) => right.score - left.score)
    .slice(0, 5);
}

async function searchHistorySource(pool, source, analysis, limit) {
  const searchFields = [source.messageColumn, source.stackColumn, source.moduleColumn].filter(Boolean);
  if (searchFields.length === 0) {
    return { rows: [], frequency: 0, source: `${source.schema}.${source.table}` };
  }

  const { clause, values } = buildSearchWhere(analysis?.keywords || []);
  const whereClause = clause === 'TRUE'
    ? 'TRUE'
    : clause
      .replace(/mensaje/g, quoteIdentifier(source.messageColumn))
      .replace(/COALESCE\(stack, ''\)/g, `COALESCE(${quoteIdentifier(source.stackColumn || source.messageColumn)}, '')`)
      .replace(/COALESCE\(modulo, ''\)/g, `COALESCE(${quoteIdentifier(source.moduleColumn || source.messageColumn)}, '')`);

  const columns = [
    `${quoteIdentifier(source.messageColumn)} AS mensaje`,
    `${quoteIdentifier(source.stackColumn || source.messageColumn)} AS stack`,
    `${quoteIdentifier(source.moduleColumn || source.messageColumn)} AS modulo`,
    `${quoteIdentifier(source.contextColumn || source.messageColumn)} AS contexto`,
    `${quoteIdentifier(source.fileColumn || source.messageColumn)} AS archivo`,
    `${quoteIdentifier(source.lineColumn || source.messageColumn)} AS linea`,
    source.dateColumn ? `${quoteIdentifier(source.dateColumn)} AS fecha` : 'NULL AS fecha',
  ];
  const qualifiedTable = `${quoteIdentifier(source.schema)}.${quoteIdentifier(source.table)}`;

  const rowsResult = await pool.query(
    `SELECT ${columns.join(', ')}
     FROM ${qualifiedTable}
     WHERE ${whereClause}
     ORDER BY ${source.dateColumn ? quoteIdentifier(source.dateColumn) : '1'} DESC
     LIMIT ${Math.max(1, Math.min(Number(limit) || 10, 25))}`,
    values,
  );

  const countResult = await pool.query(
    `SELECT COUNT(*) AS total
     FROM ${qualifiedTable}
     WHERE ${whereClause}`,
    values,
  );

  return {
    rows: rowsResult.rows || [],
    frequency: Number(countResult.rows?.[0]?.total || 0),
    source: `${source.schema}.${source.table}`,
  };
}

export function detectErrorAnalysisInput(text = '') {
  return ERROR_TRIGGER_RE.test(String(text || ''));
}

export function analyzeErrorInput(text = '') {
  const rawText = String(text || '');
  const normalized = normalizeWhitespace(rawText);
  const pattern = inferPattern(normalized);
  const { archivo, linea } = extractFileAndLine(normalized);
  const modulo = inferModuleName(normalized, archivo);
  const capa = inferLayer({ modulo, archivo, text: normalized });
  const keywords = extractKeywords(normalized, archivo, modulo);
  const stackFrames = extractStackFrames(rawText);
  const stackDetected = stackFrames.length >= 2;
  const originInfo = findOriginFrame(stackFrames);
  const originFrame = originInfo.frame;
  const archivoOrigen = originFrame?.archivo || archivo || null;
  const lineaOrigen = Number.isFinite(Number(originFrame?.linea)) ? Number(originFrame.linea) : (linea || null);
  const moduloOrigen = originFrame?.modulo || modulo || null;
  const capaOrigen = originFrame?.capa || capa;
  const nivelFallo = originInfo.index >= 0 ? originInfo.index + 1 : (lineaOrigen ? 1 : null);
  const flujoDetectado = buildFlow(stackFrames);
  const dominioError = classifyErrorDomain(normalized, capaOrigen);

  return {
    rawText: normalized,
    tipo_error: pattern?.type || 'error desconocido',
    causa: pattern?.cause || 'no se pudo determinar una causa exacta con el texto recibido',
    solucion: pattern?.solution || 'revisar el módulo afectado, el stack trace y la configuración relacionada con el error',
    contexto: pattern?.context || 'backend',
    archivo,
    linea,
    modulo,
    capa,
    keywords,
    stack_detectado: stackDetected,
    niveles_traza: stackFrames.length,
    traza: stackFrames,
    archivo_origen: archivoOrigen,
    linea_origen: lineaOrigen,
    modulo_origen: moduloOrigen,
    capa_origen: capaOrigen,
    nivel_fallo: nivelFallo,
    flujo_detectado: flujoDetectado,
    error_dominio: dominioError,
    propagacion_detectada: stackFrames.length > 1,
  };
}

function buildSearchWhere(keywords = []) {
  const terms = unique((keywords || []).map((item) => normalizeWhitespace(item)).filter(Boolean)).slice(0, 8);
  if (terms.length === 0) {
    return { clause: 'TRUE', values: [] };
  }

  const values = [];
  const parts = [];
  for (const term of terms) {
    const pattern = `%${term}%`;
    values.push(pattern);
    const indexA = values.length;
    values.push(pattern);
    const indexB = values.length;
    values.push(pattern);
    const indexC = values.length;
    parts.push(`(mensaje ILIKE $${indexA} OR COALESCE(stack, '') ILIKE $${indexB} OR COALESCE(modulo, '') ILIKE $${indexC})`);
  }

  return {
    clause: parts.join(' OR '),
    values,
  };
}

export async function searchErrorLogs(pool, analysis, limit = 10) {
  if (!pool || typeof pool.query !== 'function') {
    return { rows: [], frequency: 0, sources: [], patrones: [], confidence: 0.35 };
  }

  try {
    const sources = await discoverHistoricalSources(pool);
    if (sources.length === 0) {
      return { rows: [], frequency: 0, sources: [], patrones: [], confidence: 0.35 };
    }

    const sourceResults = [];
    for (const source of sources) {
      try {
        sourceResults.push(await searchHistorySource(pool, source, analysis, limit));
      } catch {
        // Ignore incompatible tables and keep scanning the rest.
      }
    }

    const rows = sourceResults.flatMap((result) => (result.rows || []).map((row) => ({ ...row, fuente: result.source })));
    const frequency = sourceResults.reduce((total, result) => total + Number(result.frequency || 0), 0);
    const patrones = summarizeRecurringPatterns(rows);

    return {
      rows: rows.slice(0, Math.max(1, Math.min(Number(limit) || 10, 25))),
      frequency,
      sources: unique(sourceResults.map((result) => result.source)),
      patrones,
      confidence: Math.min(0.98, frequency > 0 ? 0.6 + Math.min(frequency, 20) * 0.02 : 0.35),
    };
  } catch {
    return { rows: [], frequency: 0, sources: [], patrones: [], confidence: 0.35 };
  }
}

export async function enrichErrorAnalysisWithAi(analysis, matches = { rows: [], frequency: 0 }) {
  const sourceAnalysis = analysis && typeof analysis === 'object' ? { ...analysis } : {};
  const frequency = Number(matches?.frequency || 0);
  const confidence = Number(matches?.confidence || (frequency > 0 ? 0.7 : 0.35));
  const shouldUseAi = confidence < ERROR_AI_CONFIDENCE_THRESHOLD
    || isWeakInferenceText(sourceAnalysis?.causa)
    || isWeakInferenceText(sourceAnalysis?.solucion);

  if (!shouldUseAi) {
    return sourceAnalysis;
  }

  const traceSummary = Array.isArray(sourceAnalysis?.flujo_detectado) && sourceAnalysis.flujo_detectado.length > 0
    ? sourceAnalysis.flujo_detectado.join('; ')
    : 'No detectado';
  const traceLines = Array.isArray(sourceAnalysis?.traza) && sourceAnalysis.traza.length > 0
    ? sourceAnalysis.traza
      .slice(0, 8)
      .map((frame, index) => {
        const layer = String(frame?.capa || '').trim() || 'Backend';
        const file = String(frame?.archivo || '').trim() || 'desconocido';
        const line = Number.isFinite(Number(frame?.linea)) ? Number(frame.linea) : '?';
        return `${index + 1}. ${layer} - ${file}:${line}`;
      })
      .join('\n')
    : 'Sin traza disponible';

  const prompt = [
    'Eres un experto en debugging backend.',
    'Analiza el error y responde EXCLUSIVAMENTE con JSON válido.',
    'Formato estricto:',
    '{"causa":"...","solucion":"..."}',
    '',
    `Error: ${String(sourceAnalysis?.rawText || '')}`,
    `Tipo: ${String(sourceAnalysis?.tipo_error || 'error desconocido')}`,
    `Modulo origen: ${String(sourceAnalysis?.modulo_origen || sourceAnalysis?.modulo || 'desconocido')}`,
    `Archivo origen: ${String(sourceAnalysis?.archivo_origen || sourceAnalysis?.archivo || 'desconocido')}`,
    `Linea origen: ${String(sourceAnalysis?.linea_origen || sourceAnalysis?.linea || 'desconocida')}`,
    `Capa origen: ${String(sourceAnalysis?.capa_origen || sourceAnalysis?.capa || 'Backend')}`,
    `Flujo detectado: ${traceSummary}`,
    'Traza resumida:',
    traceLines,
  ].join('\n');

  try {
    const aiResponse = await askOllama(prompt, { timeoutMs: 3000 });
    const aiJson = aiResponse?.json && typeof aiResponse.json === 'object' ? aiResponse.json : null;
    if (!aiResponse?.ok || !aiJson) {
      return sourceAnalysis;
    }

    const causa = normalizeWhitespace(aiJson?.causa || '');
    const solucion = normalizeWhitespace(aiJson?.solucion || '');

    return {
      ...sourceAnalysis,
      causa: causa || sourceAnalysis?.causa,
      solucion: solucion || sourceAnalysis?.solucion,
    };
  } catch {
    return sourceAnalysis;
  }
}

export function buildErrorAnalysisResponse(analysis, matches = { rows: [], frequency: 0 }) {
  const firstMatch = Array.isArray(matches?.rows) && matches.rows.length > 0 ? matches.rows[0] : null;
  const frequency = Number(matches?.frequency || 0);
  const modulo = analysis?.modulo_origen || analysis?.modulo || firstMatch?.modulo || null;
  const archivo = analysis?.archivo_origen || analysis?.archivo || firstMatch?.archivo || null;
  const linea = Number.isFinite(Number(analysis?.linea_origen))
    ? Number(analysis.linea_origen)
    : Number.isFinite(Number(analysis?.linea))
      ? Number(analysis.linea)
      : Number(firstMatch?.linea || 0) || null;
  const capa = analysis?.capa_origen || analysis?.capa || firstMatch?.capa || inferLayer({ modulo, archivo, text: `${analysis?.rawText || ''} ${firstMatch?.contexto || ''}` });
  const contexto = analysis?.contexto || firstMatch?.contexto || 'backend';
  const confidence = Number(matches?.confidence || (frequency > 0 ? 0.7 : 0.35));
  const patrones = Array.isArray(matches?.patrones) ? matches.patrones : [];
  const fuentesHistoricas = Array.isArray(matches?.sources) ? matches.sources : unique([firstMatch?.fuente].filter(Boolean));
  const nivelFallo = Number.isFinite(Number(analysis?.nivel_fallo)) ? Number(analysis.nivel_fallo) : (linea ? 1 : null);
  const flujoDetectado = Array.isArray(analysis?.flujo_detectado) ? analysis.flujo_detectado : [];
  const dominio = analysis?.error_dominio || classifyErrorDomain(analysis?.rawText || '', capa);
  const propagacionDetectada = Boolean(analysis?.propagacion_detectada);

  const result = {
    tipo_error: analysis?.tipo_error || 'error desconocido',
    tipo_dominio: dominio,
    modulo,
    capa,
    archivo_origen: archivo,
    linea,
    nivel_fallo: nivelFallo,
    flujo_detectado: flujoDetectado,
    propagacion_detectada: propagacionDetectada,
    frecuencia: frequency,
    confianza: Number(confidence.toFixed(2)),
    ocurrio_antes: frequency > 0,
    causa: analysis?.causa || 'causa no determinada',
    archivo,
    contexto,
    solucion: analysis?.solucion || 'revisar el stack trace y el módulo afectado',
    ...(Array.isArray(analysis?.traza) && analysis.traza.length > 0 ? { traza: analysis.traza } : {}),
    ...(fuentesHistoricas.length > 0 ? { fuentes_historicas: fuentesHistoricas } : {}),
    ...(patrones.length > 0 ? { patrones_recurrentes: patrones } : {}),
  };

  const locationText = modulo
    ? `en el módulo ${modulo}`
    : archivo
      ? `en el archivo ${archivo}`
      : 'en el sistema backend';
  const frequencyText = frequency > 0
    ? `Este error ya ocurrió ${frequency} ${frequency === 1 ? 'vez' : 'veces'} anteriormente y la confianza de la inferencia aumentó a ${Math.round(confidence * 100)}%.`
    : 'No se encontraron coincidencias históricas; se aplicó inferencia universal basada en el texto recibido.';
  const traceText = Array.isArray(flujoDetectado) && flujoDetectado.length > 0
    ? ` Flujo detectado: ${flujoDetectado[0]}.`
    : '';
  const propagationText = propagacionDetectada
    ? ` Se observó propagación del error desde el nivel ${nivelFallo || 1} hacia capas superiores.`
    : '';
  const sourceText = fuentesHistoricas.length > 0
    ? ` Fuentes analizadas: ${fuentesHistoricas.join(', ')}.`
    : '';

  return {
    resumenHumano: `Se detectó un error ${result.tipo_error} ${locationText}. ${frequencyText}${traceText}${propagationText}${sourceText} Causa probable: ${result.causa}. Solución recomendada: ${result.solucion}.`,
    resultado: result,
  };
}