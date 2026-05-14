// ─── Script validation, builder, and SQL/JSON utilities ───────────────────────
import { generateScriptJson, validateWorkflow as validateWorkflowDefinition } from '@/lib/workflowBuilder';
import {
  SolutionType, WorkflowRow, WorkflowValidationResult, BuilderFormState,
  ScriptCreationMode,
} from './adminTypes';
import {
  isDatabaseSolutionType, isScriptSolutionType, getAllowedActionTypes,
  adaptRowsForSolutionType, normalizeWorkflowRows, createEmptyWorkflowRow,
} from './workflowUtils';

// ── Primitive value parser ────────────────────────────────────────────────────
export function parsePrimitiveValue(rawValue: string) {
  const trimmed = String(rawValue || '').trim();
  if (!trimmed) return '';
  if (/^\{\{.+\}\}$/.test(trimmed)) return trimmed;
  if (/^-?\d+(\.\d+)?$/.test(trimmed)) return Number(trimmed);
  if (/^(true|false)$/i.test(trimmed)) return trimmed.toLowerCase() === 'true';
  if (/^null$/i.test(trimmed)) return null;
  return trimmed;
}

// ── Workflow validation ───────────────────────────────────────────────────────
export function validateWorkflowRows(rows: WorkflowRow[], solutionType: SolutionType): WorkflowValidationResult {
  const normalizedRows = normalizeWorkflowRows(rows);
  const rowErrors: Record<number, string[]> = {};
  const allowedTypes = getAllowedActionTypes(solutionType);

  if (normalizedRows.length === 0) {
    return { valid: false, generalError: 'Debes agregar al menos un paso al workflow.', rowErrors: {} };
  }

  normalizedRows.forEach((row, index) => {
    const errors: string[] = [];
    if (!row.descripcion.trim()) errors.push('La descripción es obligatoria.');
    if (!row.tipo) errors.push('Debes seleccionar un tipo.');
    if (row.tipo && !allowedTypes.includes(row.tipo)) errors.push(`El tipo ${row.tipo} no está permitido.`);
    if (row.tipo && row.tipo !== 'validacion' && !row.tabla) errors.push('Debes seleccionar una tabla.');

    if (row.tipo === 'select' || row.tipo === 'insert') {
      if (!row.config.campo.trim()) errors.push('El campo es obligatorio.');
      if (!row.config.valor.trim()) errors.push('El valor es obligatorio.');
    }
    if (row.tipo === 'update') {
      if (!row.config.campoObjetivo.trim()) errors.push('El campo a actualizar es obligatorio.');
      if (!row.config.valorNuevo.trim()) errors.push('El valor nuevo es obligatorio.');
      if (!row.config.condicionCampo.trim()) errors.push('La condición where es obligatoria.');
      if (!row.config.condicionValor.trim()) errors.push('El valor de la condición es obligatorio.');
    }
    if (row.tipo === 'delete') {
      if (!row.config.condicionCampo.trim()) errors.push('El campo condición es obligatorio.');
      if (!row.config.condicionValor.trim()) errors.push('El valor condición es obligatorio.');
    }
    if (isScriptSolutionType(solutionType) && row.tipo === 'validacion') {
      if (!row.config.variable.trim()) errors.push('La variable a validar es obligatoria.');
      if (!row.config.condicion) errors.push('La condición es obligatoria.');
      if (row.config.condicion === 'igual' && !row.config.valorValidacion.trim()) {
        errors.push('Debes indicar el valor esperado.');
      }
    }
    if (errors.length > 0) rowErrors[index] = errors;
  });

  const valid = Object.keys(rowErrors).length === 0;
  return { valid, generalError: valid ? '' : 'Completa los campos requeridos antes de guardar.', rowErrors };
}

// ── Workflow JSON builder ─────────────────────────────────────────────────────
export function buildWorkflowScriptJson(rows: WorkflowRow[], solutionType: SolutionType) {
  const normalizedRows = adaptRowsForSolutionType(rows, solutionType);
  const workflow = normalizedRows.map((row) => {
    if (isScriptSolutionType(solutionType) && row.tipo === 'validacion') {
      return {
        tipo: 'validacion', descripcion: row.descripcion.trim(),
        variable: row.config.variable.trim(), condicion: row.config.condicion || 'existe',
        ...(row.config.condicion === 'igual' ? { valor: parsePrimitiveValue(row.config.valorValidacion) } : {}),
        ...(row.config.mensajeError.trim() ? { mensaje_error: row.config.mensajeError.trim() } : {}),
      };
    }
    const base = {
      tipo: row.tipo, descripcion: row.descripcion.trim(), tabla: row.tabla,
      ...(isScriptSolutionType(solutionType) && row.config.guardarEn.trim() ? { guardar_en: row.config.guardarEn.trim() } : {}),
    };
    if (row.tipo === 'select') return { ...base, where: { [row.config.campo.trim()]: parsePrimitiveValue(row.config.valor) } };
    if (row.tipo === 'insert') return { ...base, data: { [row.config.campo.trim()]: parsePrimitiveValue(row.config.valor) } };
    if (row.tipo === 'update') return {
      ...base,
      set: { [row.config.campoObjetivo.trim()]: parsePrimitiveValue(row.config.valorNuevo) },
      where: { [row.config.condicionCampo.trim()]: parsePrimitiveValue(row.config.condicionValor) },
    };
    return { ...base, where: { [row.config.condicionCampo.trim()]: parsePrimitiveValue(row.config.condicionValor) } };
  });

  if (solutionType === 'script') return { modo: 'script', origen: 'visual-workflow-builder', workflow };
  return workflow.length === 1 ? workflow[0] : { workflow };
}

// ── SQL / JSON parsing helpers ────────────────────────────────────────────────
// Only block truly destructive DDL that cannot be justified in any admin article.
// The admin panel is a trusted context — the backend handles engine-specific validation.
const SQL_FORBIDDEN_KEYWORDS = /\b(drop\s+database|drop\s+schema|drop\s+tablespace|revoke|create\s+role|create\s+user)\b/i;
const PLSQL_MARKER_RE = /\b(DECLARE|EXCEPTION|ELSIF)\b|\bIF\b[\s\S]{0,600}\bTHEN\b|\bFOR\b\s+\w+\s+IN\b|\bBEGIN\b[\s\S]{5,}\bEND\b/i;

export function isComplexSqlScript(sql: string) { return PLSQL_MARKER_RE.test(String(sql || '')); }

export function parseJsonText(value: string): { ok: true; parsed: Record<string, unknown> } | { ok: false; error: string } {
  try {
    const parsed = JSON.parse(value);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return { ok: false, error: 'El JSON debe ser un objeto.' };
    return { ok: true, parsed };
  } catch { return { ok: false, error: 'JSON inválido.' }; }
}

/**
 * Universal SQL validator for the admin panel.
 * Accepts ANY SQL statement that is:
 *  - Non-empty
 *  - Not a raw MongoDB document (caught upstream by BSON detector)
 *  - Not explicitly destructive (DROP DATABASE / DROP SCHEMA)
 * Everything else passes through to the target DB engine.
 */
export function validateManualSqlInput(sourceText: string): { ok: true; sql: string } | { ok: false; error: string } {
  const sql = String(sourceText || '').trim();
  if (!sql) return { ok: false, error: 'Debes ingresar SQL válido.' };

  // Complex procedural scripts (PL/SQL, PL/pgSQL) — always accepted
  if (isComplexSqlScript(sql)) return { ok: true, sql };

  const statements = sql.split(';').map((s) => s.trim()).filter(Boolean);
  if (statements.length === 0) return { ok: false, error: 'Debes ingresar al menos una sentencia SQL.' };

  for (const stmt of statements) {
    const cleaned = stmt.replace(/^(?:\s*(?:--[^\n]*(?:\r?\n|$)|\/\*[\s\S]*?\*\/))+/, '').trim().replace(/;\s*$/, '').trim();
    if (SQL_FORBIDDEN_KEYWORDS.test(cleaned)) {
      return { ok: false, error: 'SQL contiene operaciones destructivas no permitidas (DROP DATABASE / DROP SCHEMA).' };
    }
  }
  // Accept any other SQL — SELECT, INSERT, UPDATE, DELETE, SHOW, USE, PRAGMA, CALL,
  // EXEC, MERGE, SET, CREATE TABLE, ALTER TABLE, TRUNCATE, etc.
  return { ok: true, sql: statements.join(';\n\n') };
}

export function convertSqlSelectToWorkflow(sourceText: string): { ok: true; parsed: Record<string, unknown> } | { ok: false; error: string } {
  const validation = validateManualSqlInput(sourceText);
  if (!validation.ok) return { ok: false, error: validation.error };
  return { ok: true, parsed: { modo: 'script', origen: 'manual-sql', sql: validation.sql } };
}

// ── MongoDB shell syntax parser ───────────────────────────────────────────────
// Converts: db.collection.operation([args]) → { modo, origen, collection, operation, pipeline/filter }
const MONGO_SHELL_RE = /^db\.([a-zA-Z0-9_]+)\.(aggregate|find|findOne|findone|count|distinct|updateOne|updateMany|deleteOne|deleteMany|insertOne|insertMany)\s*\(([\s\S]*)\)\s*;?\s*$/i;

// ── BSON type normalizer ─────────────────────────────────────────────────────
// Converts MongoDB shell BSON constructors to plain JSON-compatible values.
// ObjectId('abc') → "abc",  NumberInt('4') → 4,  ISODate('...') → "...",  etc.
export function normalizeBsonTypes(text: string): string {
  return text
    .replace(/\bObjectId\s*\(\s*['"]([^'"]*)['"]\s*\)/g, '"$1"')
    .replace(/\bNumberInt\s*\(\s*['"]?(-?\d+)['"]?\s*\)/g, '$1')
    .replace(/\bNumberLong\s*\(\s*['"]?(-?\d+)['"]?\s*\)/g, '$1')
    .replace(/\bNumberDecimal\s*\(\s*['"]?(-?[\d.]+)['"]?\s*\)/g, '$1')
    .replace(/\bDouble\s*\(\s*['"]?(-?[\d.eE+\-]+)['"]?\s*\)/g, '$1')
    .replace(/\bISODate\s*\(\s*['"]([^'"]*)['"]\s*\)/g, '"$1"')
    .replace(/\bnew\s+Date\s*\(\s*['"]([^'"]*)['"]\s*\)/g, '"$1"')
    .replace(/\bBinData\s*\([^)]*\)/g, '"[BinData]"')
    .replace(/\bTimestamp\s*\([^)]*\)/g, '0')
    .replace(/\bUUID\s*\(\s*['"]([^'"]*)['"]\s*\)/g, '"$1"');
}

/** Convert MongoDB shell JS-object syntax to valid JSON string.
 *  Handles: BSON types, unquoted keys, single-quoted strings.
 */
function normalizeMongoArgsToJson(raw: string): string {
  return normalizeBsonTypes(raw)
    .replace(/([{,]\s*)([a-zA-Z_$][a-zA-Z0-9_$]*)\s*:/g, '$1"$2":')   // quote unquoted keys
    .replace(/:\s*'([^'\\]*(\\.[^'\\]*)*)'/g, ': "$1"');               // single→double quotes
}

export function parseMongoShellSyntax(text: string): { ok: true; parsed: Record<string, unknown> } | { ok: false; error: string } {
  const trimmed = String(text || '').trim();
  const match = trimmed.match(MONGO_SHELL_RE);
  if (!match) return { ok: false, error: 'No reconocido como MongoDB shell (db.collection.operation(...)).' };

  const [, collection, operation, argsRaw] = match;
  const op = operation.toLowerCase();
  const args = argsRaw.trim() || '{}';

  // Try to normalize shell syntax to valid JSON so stored data is always parseable
  let normalizedArgs = args;
  try {
    JSON.parse(args); // already valid JSON — no change needed
  } catch {
    try {
      const fixed = normalizeMongoArgsToJson(args);
      JSON.parse(fixed); // verify normalization worked
      normalizedArgs = fixed;
    } catch {
      normalizedArgs = args; // keep raw — backend has its own relaxed parser
    }
  }

  const result: Record<string, unknown> = {
    modo: 'script',
    origen: 'manual-sql',
    collection,
    operation: op,
    databaseId: 'mongo_local',
    target_database: 'mongo_local',
  };

  if (op === 'aggregate') {
    result.pipeline = normalizedArgs;
  } else {
    result.filter = normalizedArgs;
  }

  return { ok: true, parsed: result };
}

export function validateScriptAgainstSchema(script: Record<string, unknown>): string[] {
  const errors: string[] = [];
  if (!String(script.tipo || '').trim()) errors.push('tipo requerido');
  const table = String(script.tabla || '').trim();
  if (!table && !Array.isArray(script.workflow) && !Array.isArray(script.join) && !script.accion) {
    errors.push('tabla requerida');
  }
  return errors;
}

export function inferCreationModeFromScript(scriptSource: unknown): ScriptCreationMode {
  if (!scriptSource || typeof scriptSource !== 'object' || Array.isArray(scriptSource)) return 'visual_builder';
  const script = scriptSource as Record<string, unknown>;
  if (Array.isArray(script.workflow)) return 'visual_builder';
  if (script.tipo || script.workflow || script.join || script.accion) return 'json_unificado';
  return 'codigo_manual';
}

export function computeScriptForMode(form: BuilderFormState) {
  const mode = form.script_creation_mode;

  if (isDatabaseSolutionType(form.tipo_solucion) && mode === 'codigo_manual') {
    return { valid: false, error: 'Código Manual solo está disponible en modo Script.', scriptJson: null };
  }

  if (mode === 'visual_builder') {
    const validation = validateWorkflowDefinition(form.workflow_rows as any, form.tipo_solucion as any);
    if (!validation.isValid) return { valid: false, error: validation.errors?.[0] || 'Workflow inválido', scriptJson: null };
    const visualScript = form.script_json ?? generateScriptJson(form.workflow_rows as any, form.tipo_solucion as any);
    return { valid: Boolean(visualScript), error: visualScript ? '' : 'No se pudo generar script', scriptJson: visualScript };
  }

  const rawText = mode === 'json_unificado' ? form.unified_json_text : form.manual_json_text;
  if (!rawText.trim()) {
    return { valid: false, error: mode === 'codigo_manual' ? 'Debes ingresar JSON o SQL SELECT.' : 'Debes ingresar un JSON.', scriptJson: null };
  }

  // ── BSON type detection ───────────────────────────────────────────────────
  // If the text contains BSON constructors (ObjectId, NumberInt, ISODate, etc.)
  // and does NOT start with db.collection.operation(), it is a raw MongoDB document
  // (copy-pasted from Compass). Reject immediately with a helpful message —
  // never let it reach any SQL/JSON parser which would throw confusing errors.
  const hasBsonTypes = /\b(ObjectId|NumberInt|NumberLong|NumberDecimal|Double|ISODate|BinData|Timestamp|UUID)\s*\(/.test(rawText);
  if (hasBsonTypes && !/^db\./.test(rawText.trim())) {
    return {
      valid: false,
      error: '⚠ Pegaste un documento MongoDB (con ObjectId/NumberInt/ISODate). '
        + 'Para consultar usa: db.conciliacion.find({institutionCode:4}) '
        + 'o db.conciliacion.aggregate([{$match:{institutionCode:4}}])',
      scriptJson: null,
    };
  }

  // Pre-process: normalize BSON type constructors so MongoDB shell queries
  // with BSON types in filter/pipeline values are properly converted to JSON.
  const sourceText = hasBsonTypes ? normalizeBsonTypes(rawText) : rawText;

  const parsed = parseJsonText(sourceText);
  if (!parsed.ok) {
    if (mode === 'codigo_manual') {
      // Try MongoDB shell syntax first: db.collection.operation(...)
      const mongoShell = parseMongoShellSyntax(sourceText);
      if (mongoShell.ok) return { valid: true, error: '', scriptJson: mongoShell.parsed };
      // Fall back to SQL validation
      const converted = convertSqlSelectToWorkflow(sourceText);
      if (!converted.ok) return { valid: false, error: converted.error, scriptJson: null };
      return { valid: true, error: '', scriptJson: converted.parsed };
    }
    return { valid: false, error: parsed.error, scriptJson: null };
  }

  if (mode === 'json_unificado') {
    const errs = validateScriptAgainstSchema(parsed.parsed);
    if (!parsed.parsed.tipo && !Array.isArray(parsed.parsed.workflow) && !parsed.parsed.accion) {
      errs.push('El JSON debe incluir tipo, accion o workflow');
    }
    if (errs.length > 0) return { valid: false, error: errs[0], scriptJson: null };
  }

  if (mode === 'codigo_manual' && !parsed.parsed.workflow && !parsed.parsed.tipo && !parsed.parsed.accion) {
    // Accept manual-sql format: { modo: 'script', origen: 'manual-sql', sql/pipeline/collection: '...' }
    // MongoDB articles use 'pipeline' + 'collection' + 'operation' instead of 'sql'
    const isOriginManualSql =
      String(parsed.parsed.modo || '').toLowerCase() === 'script' &&
      String(parsed.parsed.origen || '').toLowerCase() === 'manual-sql';
    const hasSqlPayload =
      String(parsed.parsed.sql || '').trim().length > 0 ||
      String(parsed.parsed.pipeline || '').trim().length > 0 ||
      (typeof parsed.parsed.collection === 'string' && parsed.parsed.collection.trim().length > 0);
    if (!isOriginManualSql || !hasSqlPayload) {
      return { valid: false, error: 'Código Manual requiere workflow, tipo, accion — o formato manual-sql (modo+origen+sql/pipeline).', scriptJson: null };
    }
  }

  return { valid: true, error: '', scriptJson: parsed.parsed };
}
