/**
 * Saber si una sentencia escribe o solo lee.
 *
 * De esto depende qué se le muestra a un cliente sin permiso de escritura, si hace
 * falta confirmación antes de lanzarla y si se guarda una foto de las filas antes de
 * tocarlas. Se mira el texto del SQL y también la operación de Mongo, porque el
 * sistema habla con las dos.
 *
 * Es código puro, y conviene que lo sea: una comprobación que decide si algo cambia
 * datos tiene que poder probarse con cien ejemplos raros sin tocar una base.
 */
import { parseQualifiedFieldRef, resolveWhereEntry } from '../scripts/validacion.js';

/**
 * Detects the operation type of a MongoDB JSON query.
 * Returns 'select' for read operations and 'write' for DML.
 */
export function detectMongoOperation(jsonText = '') {
  try {
    const op = JSON.parse(String(jsonText || '').trim());
    if (op?.find !== undefined || op?.findOne !== undefined || op?.aggregate || op?.countDocuments !== undefined || op?.distinct !== undefined) return 'select';
    if (op?.insertOne || op?.insertMany || op?.updateOne || op?.updateMany || op?.replaceOne || op?.deleteOne || op?.deleteMany) return 'insert';
  } catch {
    // not valid JSON — treat as read
  }
  return 'select';
}

/**
 * Extracts the target table + WHERE clause from a single-statement
 * UPDATE / DELETE / INSERT so we can snapshot affected rows.
 * Returns null when the statement can't be parsed (multi-statement, exotic SQL).
 */
export function parseWriteTarget(sqlText = '') {
  const sql = String(sqlText || '')
    .replace(/\/\*[\s\S]*?\*\//g, ' ')
    .replace(/--[^\n]*/g, ' ')
    .replace(/;\s*$/, '')
    .replace(/\s+/g, ' ')
    .trim();
  // Bail on multi-statement scripts — the snapshot would be ambiguous.
  if (/;\s*\S/.test(sql)) return null;

  let m = sql.match(/^update\s+([a-zA-Z_][\w.$"]*)\s+set\s+[\s\S]*?(?:\s+where\s+([\s\S]+?))?(?:\s+returning\b[\s\S]*)?$/i);
  if (m) return { operation: 'update', table: m[1].replace(/"/g, ''), whereClause: (m[2] || '').trim() };

  m = sql.match(/^delete\s+from\s+([a-zA-Z_][\w.$"]*)(?:\s+where\s+([\s\S]+?))?(?:\s+returning\b[\s\S]*)?$/i);
  if (m) return { operation: 'delete', table: m[1].replace(/"/g, ''), whereClause: (m[2] || '').trim() };

  m = sql.match(/^insert\s+into\s+([a-zA-Z_][\w.$"]*)/i);
  if (m) return { operation: 'insert', table: m[1].replace(/"/g, ''), whereClause: '' };

  return null;
}

/**
 * Re-numbers the $N placeholders inside a WHERE clause so it can run as a
 * standalone SELECT (the original numbering references the full statement).
 */
export function renumberWhereParams(whereClause = '', allValues = []) {
  const map = new Map();
  const values = [];
  const clause = String(whereClause || '').replace(/\$(\d+)/g, (_, n) => {
    const old = Number(n);
    if (!map.has(old)) { map.set(old, values.length + 1); values.push(allValues[old - 1]); }
    return '$' + map.get(old);
  });
  return { clause, values };
}

export const SQL_WRITE_KEYWORDS_RE = /\b(insert|update|delete|merge|upsert|replace|truncate|drop|alter|create|grant|revoke|call|exec|execute)\b/i;

export const MONGO_WRITE_OPS_RE = /\b(insertone|insertmany|updateone|updatemany|replaceone|deleteone|deletemany|findandmodify|findoneandupdate|findoneanddelete|findoneandreplace|bulkwrite)\b/i;

/**
 * Detects whether a raw SQL / MongoDB text performs a write operation.
 * Strips SQL comments first so leading `-- comment`, block comments, `BEGIN;`
 * or whitespace cannot hide a real mutation keyword.
 */
export function sqlTextContainsMutation(rawSql) {
  const noComments = String(rawSql || '')
    .replace(/\/\*[\s\S]*?\*\//g, ' ')   // block comments
    .replace(/--[^\n]*/g, ' ');           // line comments
  if (!noComments.trim()) return false;
  // MongoDB write ops — checked before stripping quotes (Mongo JSON/shell relies on them).
  if (MONGO_WRITE_OPS_RE.test(noComments)) return true;
  // SQL — strip single-quoted string literals so data values can't trigger false positives.
  const sqlOnly = noComments.replace(/'(?:[^']|'')*'/g, "''");
  return SQL_WRITE_KEYWORDS_RE.test(sqlOnly);
}

export function scriptContainsMutation(node) {
  if (!node || typeof node !== 'object') {
    return false;
  }

  if (Array.isArray(node)) {
    return node.some((item) => scriptContainsMutation(item));
  }

  const current = node;
  const stepType = String(current.tipo || '').trim().toLowerCase();
  if (['update', 'delete', 'insert', 'merge', 'truncate', 'drop', 'alter', 'ddl'].includes(stepType)) {
    return true;
  }

  // MongoDB shell-parsed format: { operation: 'updateone', ... }
  const operation = String(current.operation || '').trim().toLowerCase();
  if (operation && MONGO_WRITE_OPS_RE.test(operation)) {
    return true;
  }

  // Manual SQL / MongoDB text — check every field that can carry a query,
  // regardless of modo/origen, so any storage format is covered.
  for (const key of ['sql', 'pipeline', 'filter', 'query', 'statement']) {
    const val = current[key];
    if (typeof val === 'string' && sqlTextContainsMutation(val)) {
      return true;
    }
  }

  return Object.values(current).some((value) => scriptContainsMutation(value));
}

export function buildWhereClause(whereObject, startIndex = 1, logic = 'AND') {
  const safeLogic = ['AND', 'OR'].includes(String(logic).toUpperCase())
    ? String(logic).toUpperCase()
    : 'AND';
  const whereFields = Object.keys(whereObject || {});
  const whereValues = [];
  const conditions = whereFields.map((field, index) => {
    const { op, value, fieldRef } = resolveWhereEntry(whereObject[field]);

    if (fieldRef) {
      const right = parseQualifiedFieldRef(fieldRef);
      return `"${field}" ${op} "${right.table}"."${right.column}"`;
    }

    whereValues.push(value);
    return `"${field}" ${op} $${startIndex + whereValues.length - 1}`;
  });
  const whereClause = whereFields.length
    ? ` WHERE ${conditions.join(` ${safeLogic} `)}`
    : '';

  return { whereFields, whereValues, whereClause };
}
