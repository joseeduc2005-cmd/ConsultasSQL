// ─── Script structure analysis and field extraction ───────────────────────────
import { KnowledgeArticle } from '../../types';
import { ExecutionField } from './types';

const SCRIPT_VARIABLE_REGEX = /{{\s*([a-zA-Z0-9_.]+)\s*}}/g;
const SQL_NAMED_PARAM_REGEX = /(^|[^:]):([a-zA-Z_][a-zA-Z0-9_]*)/g;
const PLSQL_MARKER_RE = /\b(DECLARE|EXCEPTION|ELSIF)\b|\bIF\b[\s\S]{0,600}\bTHEN\b|\bFOR\b\s+\w+\s+IN\b|\bBEGIN\b[\s\S]{5,}\bEND\b/i;

export function parseStructuredScript(scriptJson: KnowledgeArticle['script_json']) {
  if (!scriptJson) return null;
  if (typeof scriptJson === 'object') return scriptJson;
  try { return JSON.parse(scriptJson); } catch { return null; }
}

function collectScriptVariables(node: unknown, variables = new Set<string>()) {
  if (typeof node === 'string') {
    let match;
    while ((match = SCRIPT_VARIABLE_REGEX.exec(node)) !== null) variables.add(match[1]);
    SCRIPT_VARIABLE_REGEX.lastIndex = 0;
    let sqlMatch;
    while ((sqlMatch = SQL_NAMED_PARAM_REGEX.exec(node)) !== null) variables.add(sqlMatch[2]);
    SQL_NAMED_PARAM_REGEX.lastIndex = 0;
    return variables;
  }
  if (Array.isArray(node)) { node.forEach((item) => collectScriptVariables(item, variables)); return variables; }
  if (node && typeof node === 'object') Object.values(node).forEach((value) => collectScriptVariables(value, variables));
  return variables;
}

function extractVariables(scriptJson: unknown): string[] {
  const parsed = typeof scriptJson === 'object' ? scriptJson : parseStructuredScript(scriptJson as KnowledgeArticle['script_json']);
  if (!parsed || typeof parsed !== 'object') return [];
  return Array.from(collectScriptVariables(parsed));
}

export function isComplexSqlScript(sql: string): boolean {
  return PLSQL_MARKER_RE.test(String(sql || ''));
}

function extractInputVarsFromPlSql(sql: string): string[] {
  const vars = new Set<string>();
  for (const m of String(sql || '').matchAll(/{{\s*([a-zA-Z0-9_.]+)\s*}}/g)) {
    const name = m[1];
    if (!/^(step\d+\.|loopRow\.)/i.test(name)) vars.add(name);
  }
  for (const m of String(sql || '').matchAll(/(?<![:])[:]([a-zA-Z_][a-zA-Z0-9_]*)/g)) vars.add(m[1]);
  return Array.from(vars);
}

function inferScriptFieldType(name: string): ExecutionField['type'] {
  const n = name.toLowerCase();
  if (n.includes('date') || n.includes('fecha')) return 'date';
  if (n.includes('password') || n.includes('contrasena') || n.includes('clave')) return 'password';
  if (n.includes('email') || n.includes('correo')) return 'email';
  if (/^(es_|is_|has_|incluir_|activo|enabled|flag)/.test(n)) return 'checkbox';
  if (/(codigo|code|horas|dias|minutos|monto|amount|cantidad|numero|count|edad)/.test(n) && !n.includes('id')) return 'number';
  return 'text';
}

function formatScriptFieldLabel(name: string) {
  const n = String(name || '').trim().toLowerCase();
  if (n === 'user_id' || n === 'usuario_id') return 'Ingrese ID de usuario';
  if (n.includes('identificacion')) return 'Ingrese identificación';
  if (n.includes('codigo') || n === 'code') return 'Ingrese código';
  if (n === 'username' || n === 'usuario' || n === 'user') return 'Ingrese nombre de usuario';
  if (n === 'role' || n === 'rol') return 'Ingrese rol';
  if (n.includes('date') || n.includes('fecha')) return 'Ingrese fecha';
  return `Ingrese ${n.replace(/_/g, ' ')}`;
}

// SQL write keywords — DML, DDL and privilege/procedure calls that can change the system.
const SQL_WRITE_KEYWORDS = /\b(insert|update|delete|merge|upsert|replace|truncate|drop|alter|create|grant|revoke|call|exec|execute)\b/i;
// MongoDB write operations — JSON format, shell syntax, or parsed `operation` field.
const MONGO_WRITE_OPS = /\b(insertone|insertmany|updateone|updatemany|replaceone|deleteone|deletemany|findandmodify|findoneandupdate|findoneanddelete|findoneandreplace|bulkwrite)\b/i;

/**
 * Detects whether a raw SQL / MongoDB text performs a write operation.
 * Strips SQL comments first so leading `-- comment`, `/* ... *​/`, `BEGIN;` or
 * whitespace cannot hide a real mutation keyword (the original bug).
 */
export function sqlTextHasMutation(rawSql: string): boolean {
  const noComments = String(rawSql || '')
    .replace(/\/\*[\s\S]*?\*\//g, ' ')   // block comments
    .replace(/--[^\n]*/g, ' ');           // line comments
  if (!noComments.trim()) return false;
  // MongoDB write ops — checked before stripping quotes (Mongo JSON/shell relies on them).
  if (MONGO_WRITE_OPS.test(noComments)) return true;
  // SQL — strip single-quoted string literals so data values can't trigger false positives.
  const sqlOnly = noComments.replace(/'(?:[^']|'')*'/g, "''");
  return SQL_WRITE_KEYWORDS.test(sqlOnly);
}

export function scriptHasMutation(node: unknown): boolean {
  if (!node || typeof node !== 'object') return false;
  if (Array.isArray(node)) return node.some((item) => scriptHasMutation(item));
  const current = node as Record<string, unknown>;

  // Visual workflow rows
  const tipo = String(current.tipo || '').trim().toLowerCase();
  if (tipo === 'update' || tipo === 'delete' || tipo === 'insert') return true;

  // MongoDB shell-parsed format: { operation: 'updateone', ... }
  const operation = String(current.operation || '').trim().toLowerCase();
  if (operation && MONGO_WRITE_OPS.test(operation)) return true;

  // Manual SQL / MongoDB text — check every field that can carry a query,
  // regardless of modo/origen, so any storage format is covered.
  for (const key of ['sql', 'pipeline', 'filter', 'query', 'statement']) {
    const val = current[key];
    if (typeof val === 'string' && sqlTextHasMutation(val)) return true;
  }

  return Object.values(current).some((value) => scriptHasMutation(value));
}

/**
 * Siempre false: el sistema ya no escribe.
 *
 * Esto pintaba una casilla de «confirmo que esto modifica datos» antes de ejecutar.
 * Un articulo ahora solo consulta —el backend rechaza cualquier UPDATE, INSERT o
 * DELETE—, asi que pedir esa confirmacion seria pedirla para algo que no va a pasar.
 *
 * Se deja la funcion para no tocar quien la llama, y se deja scriptTieneEscritura
 * exportada aparte: sirve para avisar de que un articulo viejo trae una escritura y
 * por tanto ya no se va a poder ejecutar.
 */
export function scriptRequiresWriteConfirmation(_article: { script_json?: KnowledgeArticle['script_json'] } | null) {
  return false;
}

/** Si un articulo guardado trae una escritura, que el sistema ya no ejecuta. */
export function scriptTieneEscritura(article: { script_json?: KnowledgeArticle['script_json'] } | null) {
  return scriptHasMutation(parseStructuredScript(article?.script_json));
}

export function getScriptExecutionFields(article: { script_json?: KnowledgeArticle['script_json'] } | null): ExecutionField[] {
  const parsed = parseStructuredScript(article?.script_json);
  if (!parsed || typeof parsed !== 'object') return [];
  const scriptObject = parsed as Record<string, unknown>;
  const sqlText = String(scriptObject.sql || '').trim();

  if (
    String(scriptObject.modo || '').trim().toLowerCase() === 'script' &&
    String(scriptObject.origen || '').trim().toLowerCase() === 'manual-sql' &&
    isComplexSqlScript(sqlText)
  ) {
    const fromSql = extractInputVarsFromPlSql(sqlText);
    const required = Array.isArray(scriptObject.parametros_requeridos)
      ? scriptObject.parametros_requeridos.map((item) => String(item || '').trim()).filter(Boolean) : [];
    const merged = Array.from(new Set([...required, ...fromSql]));
    return merged.map((name) => ({ name, label: formatScriptFieldLabel(name), type: inferScriptFieldType(name), required: false }));
  }

  const required = Array.isArray(scriptObject.parametros_requeridos)
    ? scriptObject.parametros_requeridos.map((item) => String(item || '').trim()).filter(Boolean) : [];
  const autoDetected = extractVariables(parsed).map((item) => String(item || '').trim()).filter((item) => Boolean(item) && !item.includes('.'));
  const merged = Array.from(new Set([...required, ...autoDetected]));

  if (merged.length === 0) {
    // Fallback: detect $N positional placeholders in the SQL.
    // The frontend uses param_N as the field name so the backend can reconstruct the array.
    const sql = String(scriptObject.sql || '');
    const positionalNums = Array.from(new Set(
      Array.from(sql.matchAll(/\$(\d+)/g)).map((m) => parseInt(m[1], 10))
    )).sort((a, b) => a - b);
    if (positionalNums.length > 0) {
      return positionalNums.map((n) => ({
        name: `param_${n}`,
        label: `Parámetro ${n}`,
        type: 'text' as ExecutionField['type'],
        required: true,
      }));
    }
  }

  return merged.map((name) => ({ name, label: formatScriptFieldLabel(name), type: inferScriptFieldType(name), required: false }));
}

export function getScriptPreviewText(article: { script_json?: KnowledgeArticle['script_json'] } | null): string {
  const parsed = parseStructuredScript(article?.script_json);
  if (!parsed || typeof parsed !== 'object') return '';
  const script = parsed as Record<string, unknown>;
  if (String(script.modo || '').trim().toLowerCase() === 'script') {
    const origen = String(script.origen || '').trim().toLowerCase();
    if (origen === 'manual-sql') {
      const sql = String(script.sql || '').trim();
      if (sql) return isComplexSqlScript(sql) ? `-- [Motor PL/SQL híbrido]\n-- Este script será interpretado y ejecutado paso a paso.\n\n${sql}` : sql;
    }
    return JSON.stringify(parsed, null, 2);
  }
  return JSON.stringify(parsed, null, 2);
}

export function getBusinessScriptDetails(article: { script_json?: KnowledgeArticle['script_json'] } | null) {
  const parsed = parseStructuredScript(article?.script_json);
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return null;
  const script = parsed as Record<string, unknown>;
  if (String(script.modo || '').trim().toLowerCase() !== 'script') return null;
  const pasos = Array.isArray(script.pasos)
    ? script.pasos.map((step, index) => {
        const item = step as Record<string, unknown>;
        return { orden: Number(item.orden) > 0 ? Number(item.orden) : index + 1, descripcion: String(item.descripcion || ''), accion: String(item.accion || '') };
      }).sort((a, b) => a.orden - b.orden)
    : [];
  return { script: String(script.script || ''), descripcion: String(script.descripcion || ''), pasos };
}

export function inferArticleExecutionTarget(article: { script_json?: KnowledgeArticle['script_json'] } | null): { databaseId?: string } {
  const parsed = parseStructuredScript(article?.script_json);
  const p = parsed as Record<string, unknown> | null;
  const id = String(
    p?.target_database || p?.databaseId || p?.database_id ||
    p?.targetDatabaseId || p?.target_database_id || ''
  ).trim();
  return id ? { databaseId: id } : {};
}
