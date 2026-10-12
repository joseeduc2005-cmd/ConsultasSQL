/**
 * Normalizar texto para usarlo como clave.
 *
 * Tres funciones cortas repartidas por `app.js` que hacen lo mismo con pequeñas
 * diferencias: quitar tildes, pasar a minúsculas y dejar algo que se pueda comparar o
 * guardar. Juntas se ve que son la misma idea, y las diferencias dejan de parecer
 * casualidad: una conserva el guion bajo porque los nombres de columna lo llevan, la
 * otra no porque es para un nombre de proceso.
 */

export function normalizeSemanticLearningTerm(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9_]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

export function dedupeSemanticValues(values = []) {
  return [...new Set((values || []).map((item) => String(item || '').trim()).filter(Boolean))];
}

export function slugifyProcessName(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

export function normalizeGeneratedSelectSql(sqlText) {
  const input = String(sqlText || '');
  if (!input) return input;

  // Repair common generator drift: ILIKE over non-text columns without explicit cast.
  // Example: "t1"."activo" ILIKE '%x%' -> "t1"."activo"::text ILIKE '%x%'
  return input.replace(/((?:"[^"]+"|[a-zA-Z_][\w]*)\.(?:"[^"]+"|[a-zA-Z_][\w]*))(?!::text)\s+ILIKE/gi, '$1::text ILIKE');
}
