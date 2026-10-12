/**
 * Las guardas que deciden qué SQL se deja pasar.
 *
 * Esto es lo más crítico del backend: lo que separa una consulta legítima de una
 * inyección. Estaba dentro de `app.js`, en medio de 7.700 líneas, sin exportar y sin
 * una sola prueba — ni siquiera se podía importar para probarlo, porque importar ese
 * archivo arranca el servidor.
 *
 * Sacado tal cual, byte por byte, para que las pruebas midan lo que el sistema hace hoy
 * y no lo que creemos que hace. Cualquier cambio de comportamiento viene después, con
 * la red ya puesta.
 */

export const SQL_BLOCKED_KEYWORDS = [
  'drop',
  'truncate',
  'alter',
  'create',
  'grant',
  'revoke',
  'comment',
];

export function normalizeSchemaIdentifier(value) {
  return String(value || '').trim().toLowerCase();
}

/**
 * Removes string literals, quoted identifiers and comments from SQL so that
 * keyword/operator detection is never fooled by their textual contents
 * (e.g. SELECT 'please update your profile' must NOT look like an UPDATE).
 */
export function stripSqlStringsAndComments(sqlText = '') {
  return String(sqlText || '')
    .replace(/\/\*[\s\S]*?\*\//g, ' ')      // block comments
    .replace(/--[^\n]*/g, ' ')               // line comments
    .replace(/'(?:[^']|'')*'/g, "''")        // single-quoted strings
    .replace(/`(?:[^`]|``)*`/g, '``')        // backtick identifiers (MySQL)
    .replace(/"(?:[^"]|"")*"/g, '""');       // double-quoted identifiers
}

/**
 * Qué posiciones del texto caen dentro de una cadena o de un comentario.
 *
 * Devuelve una marca de la misma longitud que el SQL, con un 1 en cada carácter que no
 * es código. Hace falta la longitud original: lo que se necesita saber es si un marcador
 * que está en la posición N es de verdad un marcador, y recortar el texto —como hace
 * `stripSqlStringsAndComments`— mueve justo eso.
 *
 * Existe porque un `:algo` dentro de un texto se tomaba por un parámetro: el `:MI` de
 * `TO_CHAR(fecha, 'HH24:MI')` destrozaba el formato y añadía un parámetro fantasma. En
 * Oracle eso es como se escribe una hora, así que caía en cualquier informe.
 */
export function marcarLiteralesYComentarios(sqlText = '') {
  const texto = String(sqlText || '');
  const dentro = new Uint8Array(texto.length);
  let i = 0;
  const marcar = (desde, hasta) => { for (let k = desde; k < hasta && k < texto.length; k += 1) dentro[k] = 1; };

  while (i < texto.length) {
    const c = texto[i];
    const siguiente = texto[i + 1];

    if (c === '-' && siguiente === '-') {
      const fin = texto.indexOf('\n', i);
      const hasta = fin === -1 ? texto.length : fin;
      marcar(i, hasta);
      i = hasta;
      continue;
    }
    if (c === '/' && siguiente === '*') {
      const fin = texto.indexOf('*/', i + 2);
      const hasta = fin === -1 ? texto.length : fin + 2;
      marcar(i, hasta);
      i = hasta;
      continue;
    }
    if (c === "'" || c === '"' || c === '`') {
      let j = i + 1;
      while (j < texto.length) {
        // La comilla repetida es un escape, no el cierre: 'O''Brien' es una sola cadena.
        if (texto[j] === c && texto[j + 1] === c) { j += 2; continue; }
        if (texto[j] === c) { j += 1; break; }
        j += 1;
      }
      marcar(i, j);
      i = j;
      continue;
    }
    // Bloque con dólares de PostgreSQL: $$ … $$ o $etiqueta$ … $etiqueta$
    if (c === '$') {
      const etiqueta = texto.slice(i).match(/^\$[A-Za-z_][A-Za-z0-9_]*\$|^\$\$/);
      if (etiqueta) {
        const cierre = texto.indexOf(etiqueta[0], i + etiqueta[0].length);
        const hasta = cierre === -1 ? texto.length : cierre + etiqueta[0].length;
        marcar(i, hasta);
        i = hasta;
        continue;
      }
    }
    i += 1;
  }
  return dentro;
}

export function stripLeadingSqlComments(sqlText = '') {
  return String(sqlText || '')
    .replace(/^(?:\s*(?:--[^\n]*(?:\r?\n|$)|\/\*[\s\S]*?\*\/))+/, '')
    .trim();
}

/**
 * Splits a multi-statement SQL script into individual statements on `;`,
 * while correctly ignoring semicolons that appear inside:
 *   - single-quoted strings  '...'   ('' = escaped quote)
 *   - double-quoted identifiers "..." ("" = escaped)
 *   - backtick identifiers (MySQL)  `...`
 *   - line comments  -- ...
 *   - block comments (de barra-asterisco a asterisco-barra)
 *   - dollar-quoted blocks (PostgreSQL)  $$...$$  or  $tag$...$tag$
 * This lets arbitrarily long / complex scripts (DBeaver-style) be split safely.
 */
export function splitManualSqlSteps(sqlText = '') {
  const text = String(sqlText || '').trim();
  if (!text) return [];
  // MongoDB JSON queries start with '{' — never split them, return as single step
  if (text.startsWith('{')) return [text];

  const statements = [];
  let current = '';
  let i = 0;
  const n = text.length;

  while (i < n) {
    const ch = text[i];
    const next = text[i + 1];

    // ── line comment: -- ... \n ──
    if (ch === '-' && next === '-') {
      const eol = text.indexOf('\n', i);
      const end = eol === -1 ? n : eol;
      current += text.slice(i, end);
      i = end;
      continue;
    }
    // ── block comment: /* ... */ ──
    if (ch === '/' && next === '*') {
      const close = text.indexOf('*/', i + 2);
      const end = close === -1 ? n : close + 2;
      current += text.slice(i, end);
      i = end;
      continue;
    }
    // ── single-quoted string ──
    if (ch === "'") {
      let j = i + 1;
      while (j < n) {
        if (text[j] === "'" && text[j + 1] === "'") { j += 2; continue; }
        if (text[j] === "'") { j += 1; break; }
        j += 1;
      }
      current += text.slice(i, j);
      i = j;
      continue;
    }
    // ── double-quoted identifier ──
    if (ch === '"') {
      let j = i + 1;
      while (j < n) {
        if (text[j] === '"' && text[j + 1] === '"') { j += 2; continue; }
        if (text[j] === '"') { j += 1; break; }
        j += 1;
      }
      current += text.slice(i, j);
      i = j;
      continue;
    }
    // ── backtick identifier (MySQL) ──
    if (ch === '`') {
      const close = text.indexOf('`', i + 1);
      const end = close === -1 ? n : close + 1;
      current += text.slice(i, end);
      i = end;
      continue;
    }
    // ── dollar-quoted block (PostgreSQL): $$...$$ or $tag$...$tag$ ──
    if (ch === '$') {
      const tagMatch = text.slice(i, i + 64).match(/^\$([a-zA-Z_]\w*)?\$/);
      if (tagMatch) {
        const tag = tagMatch[0];
        const close = text.indexOf(tag, i + tag.length);
        const end = close === -1 ? n : close + tag.length;
        current += text.slice(i, end);
        i = end;
        continue;
      }
    }
    // ── statement terminator ──
    if (ch === ';') {
      const trimmed = current.trim();
      if (trimmed) statements.push(trimmed);
      current = '';
      i += 1;
      continue;
    }

    current += ch;
    i += 1;
  }

  const tail = current.trim();
  if (tail) statements.push(tail);
  return statements;
}

export function containsBlockedSql(sqlText) {
  const stripped = String(sqlText || '').trimEnd().replace(/;\s*$/, '');
  const normalized = stripLeadingSqlComments(stripped.trim()).toLowerCase();
  if (!normalized.startsWith('select') && !normalized.startsWith('with')) return true;
  // Reject stacked statements (SQL injection) — but a ; inside a string literal
  // or comment is NOT a separator, so use the real tokenizing splitter.
  if (splitManualSqlSteps(stripped).length > 1) return true;
  // Check blocked keywords against code only — string/comment contents removed
  // so legitimate SELECTs with words like "update" in literals aren't blocked.
  const codeOnly = stripSqlStringsAndComments(normalized);
  return SQL_BLOCKED_KEYWORDS.some((word) => codeOnly.includes(`${word} `));
}

export function extractSqlReferencedTables(sqlText) {
  const sourceSql = String(sqlText || '');
  const refs = Array.from(sourceSql.matchAll(
    /\b(?:from|join)\s+((?:"[^"]+"|[a-zA-Z0-9_]+)(?:\s*\.\s*(?:"[^"]+"|[a-zA-Z0-9_]+))?)/gi,
  ));

  const resolvedRefs = new Set();

  refs.forEach((match) => {
    const qualified = String(match?.[1] || '').trim();
    if (!qualified) return;

    const parts = qualified
      .split('.')
      .map((part) => part.trim())
      .filter(Boolean)
      .map((part) => part.replace(/^"|"$/g, ''));

    if (parts.length === 0) return;

    // Preserve schema-qualified names for multi-database allow-lists
    // (e.g. system.comments), and include unqualified fallback (comments).
    const qualifiedName = normalizeSchemaIdentifier(parts.join('.'));
    const tableName = normalizeSchemaIdentifier(parts[parts.length - 1]);

    if (qualifiedName) resolvedRefs.add(qualifiedName);
    if (tableName) resolvedRefs.add(tableName);
  });

  return [...resolvedRefs];
}

export function getDatabaseSchemaTableNames(database) {
  return (database?.schema?.tables || [])
    .map((table) => normalizeSchemaIdentifier(table?.name || table))
    .filter(Boolean);
}
