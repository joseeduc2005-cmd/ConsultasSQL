/**
 * PlSqlInterpreter.js
 *
 * Interprets PL/SQL-like procedural scripts and transforms them into
 * executable SQL workflow step arrays.
 *
 * Supported constructs:
 *   - DECLARE blocks (variable declarations)
 *   - BEGIN...END wrappers
 *   - SELECT ... INTO varname FROM ... (variable assignment)
 *   - FOR varname IN (SELECT ...) LOOP ... END LOOP (loop simulation)
 *   - IF condition THEN ... [ELSE ...] END IF (conditional steps)
 *   - Regular SQL statements: SELECT / INSERT / UPDATE / DELETE
 *
 * Philosophy: INTERPRET → TRANSFORM → PREPARE (no direct execution here)
 */

// ----- Detection -----

const PLSQL_MARKER_RE =
  /DO\s*\$\$|\bDECLARE\b|\bBEGIN\b|\bFOR\b\s+\w+\s+IN\b|\bLOOP\b|\bIF\b[\s\S]{0,600}\bTHEN\b|\bSELECT\b[\s\S]{1,600}\bINTO\b|\bINTO\b|\bRAISE\s+NOTICE\b|\bEXCEPTION\b|\bELSIF\b/i;

const PROCEDURAL_KEYWORD_RE = /DO\s*\$\$|\bDECLARE\b|\bBEGIN\b|\bFOR\b|\bLOOP\b|\bIF\b|\bINTO\b|\bRAISE\s+NOTICE\b/i;

/**
 * Returns true if the SQL text appears to be a complex procedural script.
 */
export function isComplexSqlScript(sqlText) {
  return PLSQL_MARKER_RE.test(String(sqlText || ''));
}

export function shouldForceProceduralMode(sqlText) {
  return PROCEDURAL_KEYWORD_RE.test(String(sqlText || ''));
}

// ----- Comment stripping -----

function stripComments(src) {
  let result = '';
  let i = 0;
  const s = String(src || '');

  while (i < s.length) {
    // String literal
    if (s[i] === "'") {
      result += s[i++];
      while (i < s.length) {
        if (s[i] === "'" && s[i + 1] === "'") {
          result += "''";
          i += 2;
        } else if (s[i] === "'") {
          result += s[i++];
          break;
        } else {
          result += s[i++];
        }
      }
      continue;
    }
    // Line comment
    if (s[i] === '-' && s[i + 1] === '-') {
      while (i < s.length && s[i] !== '\n') i++;
      result += ' ';
      continue;
    }
    // Block comment
    if (s[i] === '/' && s[i + 1] === '*') {
      i += 2;
      while (i < s.length && !(s[i] === '*' && s[i + 1] === '/')) i++;
      i += 2;
      result += ' ';
      continue;
    }
    result += s[i++];
  }
  return result;
}

// ----- DECLARE block -----

/**
 * Parses the DECLARE block and returns a Map<lowercaseVarName, { type, default }>.
 */
function parseDeclareBlock(src) {
  const vars = new Map();
  const m = src.match(/\bDECLARE\b([\s\S]*?)\bBEGIN\b/i);
  if (!m) return vars;

  for (const line of m[1].split(';')) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    // Match: varname TYPE [:= default]
    const vm = trimmed.match(
      /^([a-z_@#][a-z0-9_@#$]*)\s+([\w%()]+(?:\s+[\w%()]+)?)\s*(?::=\s*(.*))?$/i,
    );
    if (vm) {
      vars.set(vm[1].toLowerCase(), {
        type: vm[2].trim(),
        default: vm[3]?.trim() || null,
      });
    }
  }
  return vars;
}

// ----- Body extraction -----

/**
 * Checks whether the source has a BEGIN keyword.
 */
function hasBeginBlock(src) {
  return /\bBEGIN\b/i.test(src);
}

/**
 * Extracts the content inside the outermost BEGIN...END block.
 * Tracks nesting depth for BEGIN/LOOP/IF/CASE.
 */
function extractBodyContent(src) {
  const beginMatch = src.match(/\bBEGIN\b/i);
  if (!beginMatch) return src;

  const contentStart = (beginMatch.index ?? 0) + beginMatch[0].length;
  let depth = 0;
  let i = contentStart;

  while (i < src.length) {
    // Skip string literals
    if (src[i] === "'") {
      i++;
      while (i < src.length) {
        if (src[i] === "'" && src[i + 1] === "'") { i += 2; continue; }
        if (src[i] === "'") { i++; break; }
        i++;
      }
      continue;
    }

    // Check for keywords at word boundary
    if (/[A-Za-z_]/.test(src[i])) {
      let wordEnd = i;
      while (wordEnd < src.length && /[A-Za-z0-9_]/.test(src[wordEnd])) wordEnd++;
      const word = src.slice(i, wordEnd).toUpperCase();

      if (['BEGIN', 'LOOP', 'CASE', 'IF'].includes(word)) {
        depth++;
        i = wordEnd;
        continue;
      }
      if (word === 'END') {
        if (depth === 0) return src.slice(contentStart, i).trim();
        depth--;
        i = wordEnd;
        continue;
      }
      i = wordEnd;
      continue;
    }
    i++;
  }

  return src.slice(contentStart).trim();
}

// ----- Statement splitter -----

/**
 * Splits a PL/SQL body into top-level statement strings,
 * respecting BEGIN/LOOP/IF/CASE nesting and string literals.
 * Returns an array of raw statement strings (without trailing semicolons).
 */
function splitTopLevelStatements(body) {
  const statements = [];
  let current = '';
  let depth = 0;
  let i = 0;
  const s = String(body || '');

  while (i < s.length) {
    // String literal
    if (s[i] === "'") {
      current += s[i++];
      while (i < s.length) {
        if (s[i] === "'" && s[i + 1] === "'") { current += "''"; i += 2; continue; }
        if (s[i] === "'") { current += s[i++]; break; }
        current += s[i++];
      }
      continue;
    }

    // Keyword nesting
    if (/[A-Za-z_]/.test(s[i])) {
      let wordEnd = i;
      while (wordEnd < s.length && /[A-Za-z0-9_]/.test(s[wordEnd])) wordEnd++;

      // Only act on word boundary
      const prevChar = i > 0 ? s[i - 1] : ' ';
      if (!/\w/.test(prevChar)) {
        const word = s.slice(i, wordEnd).toUpperCase();
        if (['BEGIN', 'LOOP', 'CASE', 'IF'].includes(word)) depth++;
        else if (word === 'END') { if (depth > 0) depth--; }
      }

      current += s.slice(i, wordEnd);
      i = wordEnd;
      continue;
    }

    // Semicolon at depth 0 → end of statement
    if (s[i] === ';' && depth === 0) {
      const trimmed = current.trim();
      if (trimmed) statements.push(trimmed);
      current = '';
      i++;
      continue;
    }

    current += s[i++];
  }

  const remaining = current.trim();
  if (remaining) statements.push(remaining);
  return statements;
}

// ----- Statement classifiers -----

/**
 * Detects the SQL operation keyword of a statement.
 */
function detectOperation(sql) {
  const first = sql.trim().toUpperCase().match(/^([A-Z]+)/)?.[1] || '';
  if (first === 'SELECT') return 'select';
  if (first === 'INSERT') return 'insert';
  if (first === 'UPDATE') return 'update';
  if (first === 'DELETE') return 'delete';
  if (first === 'FOR') return 'for_loop';
  if (first === 'IF') return 'conditional';
  return 'unknown';
}

function splitTopLevelCsv(input) {
  const items = [];
  let current = '';
  let depth = 0;
  let i = 0;
  const s = String(input || '');

  while (i < s.length) {
    if (s[i] === "'") {
      current += s[i++];
      while (i < s.length) {
        if (s[i] === "'" && s[i + 1] === "'") { current += "''"; i += 2; continue; }
        if (s[i] === "'") { current += s[i++]; break; }
        current += s[i++];
      }
      continue;
    }

    if (s[i] === '(') {
      depth++;
      current += s[i++];
      continue;
    }
    if (s[i] === ')') {
      if (depth > 0) depth--;
      current += s[i++];
      continue;
    }

    if (s[i] === ',' && depth === 0) {
      const t = current.trim();
      if (t) items.push(t);
      current = '';
      i++;
      continue;
    }

    current += s[i++];
  }

  const rest = current.trim();
  if (rest) items.push(rest);
  return items;
}

function normalizeIntoVarName(rawName) {
  let name = String(rawName || '').trim().toLowerCase();
  if (!name) return '';

  // Remove common procedural prefixes repeatedly: v_, p_, l_
  while (/^(v_|p_|l_)/i.test(name)) {
    name = name.replace(/^(v_|p_|l_)/i, '');
  }

  name = name
    .replace(/^[^a-z0-9_]+/i, '')
    .replace(/[^a-z0-9_]+/gi, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '');

  return name;
}

function isGenericSemanticName(name) {
  const n = String(name || '').trim().toLowerCase();
  if (!n) return true;
  if (n.length <= 1) return true;
  if (/^into_\d+$/i.test(n)) return true;
  return [
    'x', 'y', 'z',
    'tmp', 'temp',
    'var', 'variable',
    'value', 'valor',
    'data', 'dato',
    'item',
    'res', 'result', 'resultado',
  ].includes(n);
}

function extractSelectAlias(colExpr) {
  const expr = String(colExpr || '').trim();
  if (!expr) return '';

  const explicitAlias = expr.match(/\bAS\s+"?([a-zA-Z_][a-zA-Z0-9_]*)"?\s*$/i);
  if (explicitAlias) return explicitAlias[1].toLowerCase();

  const tokenAlias = expr.match(/(?:\.\s*)?"?([a-zA-Z_][a-zA-Z0-9_]*)"?\s*$/i);
  if (tokenAlias) return tokenAlias[1].toLowerCase();

  return '';
}

function dedupeSemanticNames(names) {
  const counters = new Map();
  return names.map((baseName, idx) => {
    let base = String(baseName || '').trim().toLowerCase();
    if (!base) base = `resultado_${idx + 1}`;

    const seen = counters.get(base) || 0;
    counters.set(base, seen + 1);
    if (seen === 0) return base;
    return `${base}_${seen + 1}`;
  });
}

/**
 * Parses a SELECT...INTO statement.
 * Returns { cleanSql, intoVars, selectedFields } or null.
 */
function parseSelectInto(sql) {
  // SELECT col1 [AS alias1], col2... INTO var1, var2 FROM table WHERE ...
  const m = sql.match(/^SELECT\s+([\s\S]+?)\s+INTO\s+([\w\s,]+?)\s+FROM\b([\s\S]+)$/i);
  if (!m) return null;

  const rawCols = m[1];
  const rawVars = m[2];
  const rest = m[3];

  const selectedCols = splitTopLevelCsv(rawCols);
  const intoVars = splitTopLevelCsv(rawVars).map((v) => v.trim().toLowerCase());
  if (intoVars.length === 0 || selectedCols.length === 0) return null;

  const inferredFieldNames = intoVars.map((intoVar, idx) => {
    const fromInto = normalizeIntoVarName(intoVar);
    const fromAlias = normalizeIntoVarName(extractSelectAlias(selectedCols[idx] || selectedCols[0] || ''));

    if (fromInto && !isGenericSemanticName(fromInto)) return fromInto;
    if (fromAlias && !isGenericSemanticName(fromAlias)) return fromAlias;
    if (fromInto) return fromInto;
    if (fromAlias) return fromAlias;
    return `resultado_${idx + 1}`;
  });

  const selectedFields = dedupeSemanticNames(inferredFieldNames);
  const aliasedCols = selectedCols.map((col, idx) => {
    const alias = selectedFields[idx] || selectedFields[0];
    return `${col.trim()} AS ${alias}`;
  });

  const cleanSql = `SELECT ${aliasedCols.join(', ')} FROM${rest}`;

  return { cleanSql, intoVars, selectedFields };
}

/**
 * Extracts a FOR...IN(SELECT...)LOOP...END LOOP block from the beginning of text.
 * Returns { loopVar, selectSql, bodyText, consumedLength } or null.
 */
function extractForLoop(text) {
  const forRe = /^\s*FOR\s+(\w+)\s+IN\s*\(/i;
  const m = forRe.exec(text);
  if (!m) return null;

  const loopVar = m[1].toLowerCase();
  let i = m[0].length;

  // Collect SELECT inside balanced parentheses
  let parenDepth = 1;
  const selectStart = i;
  while (i < text.length && parenDepth > 0) {
    if (text[i] === "'") {
      i++;
      while (i < text.length && text[i] !== "'") i++;
      i++;
      continue;
    }
    if (text[i] === '(') parenDepth++;
    else if (text[i] === ')') parenDepth--;
    i++;
  }
  const selectSql = text.slice(selectStart, i - 1).trim();

  // Expect LOOP keyword
  const loopKwRe = /^\s*LOOP\s*/i;
  const loopKwMatch = loopKwRe.exec(text.slice(i));
  if (!loopKwMatch) return null;
  i += loopKwMatch[0].length;

  // Collect body until matching END LOOP
  const bodyStart = i;
  let depth = 0;

  while (i < text.length) {
    if (text[i] === "'") {
      i++;
      while (i < text.length && text[i] !== "'") i++;
      i++;
      continue;
    }

    if (/[A-Za-z_]/.test(text[i])) {
      let wordEnd = i;
      while (wordEnd < text.length && /[A-Za-z0-9_]/.test(text[wordEnd])) wordEnd++;
      const word = text.slice(i, wordEnd).toUpperCase();

      if (['LOOP', 'BEGIN', 'CASE', 'IF'].includes(word)) {
        depth++;
        i = wordEnd;
        continue;
      }
      if (word === 'END') {
        const after = text.slice(wordEnd).trimStart();
        if (/^LOOP\b/i.test(after)) {
          if (depth === 0) {
            const bodyText = text.slice(bodyStart, i).trim();
            const skipEnd = text.slice(wordEnd).match(/^\s*LOOP\s*;?\s*/i);
            const consumed = wordEnd + (skipEnd?.[0].length || 0);
            return { loopVar, selectSql, bodyText, consumedLength: consumed };
          }
          depth--;
        }
        i = wordEnd;
        continue;
      }
      i = wordEnd;
      continue;
    }
    i++;
  }
  return null;
}

/**
 * Extracts an IF...THEN...END IF block from the beginning of text.
 * Returns { condition, thenBody, elseBody, consumedLength } or null.
 */
function extractIfBlock(text) {
  const ifRe = /^\s*IF\s+([\s\S]+?)\s+THEN\b/i;
  const m = ifRe.exec(text);
  if (!m) return null;

  const condition = m[1].trim();
  let i = m[0].length;

  // Collect body until END IF, tracking ELSIF / ELSE
  let depth = 0;
  let thenEnd = i;
  let elseStart = -1;

  while (i < text.length) {
    if (text[i] === "'") {
      i++;
      while (i < text.length && text[i] !== "'") i++;
      i++;
      continue;
    }

    if (/[A-Za-z_]/.test(text[i])) {
      let wordEnd = i;
      while (wordEnd < text.length && /[A-Za-z0-9_]/.test(text[wordEnd])) wordEnd++;
      const word = text.slice(i, wordEnd).toUpperCase();

      if (['BEGIN', 'LOOP', 'CASE', 'IF'].includes(word)) {
        depth++;
        i = wordEnd;
        continue;
      }
      if (word === 'ELSE' && depth === 0) {
        thenEnd = i;
        elseStart = wordEnd;
        // skip whitespace
        while (elseStart < text.length && /\s/.test(text[elseStart])) elseStart++;
        i = elseStart;
        continue;
      }
      if (word === 'END') {
        const after = text.slice(wordEnd).trimStart();
        if (/^IF\b/i.test(after)) {
          if (depth === 0) {
            const thenBody = text.slice(m[0].length, thenEnd === m[0].length ? i : thenEnd).trim();
            const elseBody = elseStart !== -1 ? text.slice(elseStart, i).trim() : null;
            const skipEnd = text.slice(wordEnd).match(/^\s*IF\s*;?\s*/i);
            const consumed = wordEnd + (skipEnd?.[0].length || 0);
            return { condition, thenBody, elseBody, consumedLength: consumed };
          }
          depth--;
        }
        i = wordEnd;
        continue;
      }
      i = wordEnd;
      continue;
    }
    i++;
  }
  return null;
}

// ----- Human title inference -----

function inferTitle(sql, operation, stepIndex) {
  const ordinal = stepIndex + 1;
  const upper = sql.toUpperCase();

  if (operation === 'for_loop') {
    const tbl = sql.match(/\bFROM\s+(\w+)/i)?.[1];
    return `Paso ${ordinal}: Procesar registros${tbl ? ` de ${tbl}` : ''}`;
  }
  if (operation === 'conditional') {
    return `Paso ${ordinal}: Evaluación condicional`;
  }
  if (operation === 'select') {
    if (upper.includes('COUNT')) return `Paso ${ordinal}: Contar registros`;
    if (upper.includes('EXISTS')) return `Paso ${ordinal}: Verificar existencia`;
    const tbl = sql.match(/\bFROM\s+(\w+)/i)?.[1];
    return tbl ? `Paso ${ordinal}: Consultar ${tbl}` : `Paso ${ordinal}: Consultar datos`;
  }
  if (operation === 'insert') {
    const tbl = sql.match(/\bINTO\s+(\w+)/i)?.[1];
    return tbl ? `Paso ${ordinal}: Insertar en ${tbl}` : `Paso ${ordinal}: Insertar registro`;
  }
  if (operation === 'update') {
    const tbl = sql.match(/\bUPDATE\s+(\w+)/i)?.[1];
    return tbl ? `Paso ${ordinal}: Actualizar ${tbl}` : `Paso ${ordinal}: Actualizar registro`;
  }
  if (operation === 'delete') {
    const tbl = sql.match(/\bFROM\s+(\w+)/i)?.[1];
    return tbl ? `Paso ${ordinal}: Eliminar de ${tbl}` : `Paso ${ordinal}: Eliminar registros`;
  }
  return `Paso ${ordinal}: Ejecutar consulta`;
}

// ----- Variable resolution -----

/**
 * Replaces declared-variable references in SQL with {{stepN.field}} placeholders.
 * varResolution: Map<varName, { stepIndex, field }>
 */
function applyVarResolution(sql, varResolution) {
  let result = sql;
  for (const [varName, info] of varResolution.entries()) {
    if (info.stepIndex !== undefined) {
      const placeholder = `{{step${info.stepIndex + 1}.${info.field}}}`;
      const re = new RegExp(`(?<![\\w.])${escRe(varName)}(?![\\w.])`, 'gi');
      result = result.replace(re, placeholder);
    }
  }
  return result;
}

/**
 * Replaces loop variable field access (e.g. registro.id) with {{loopRow.id}} in inner SQL.
 */
export function applyLoopVarResolution(sql, loopVarName) {
  const re = new RegExp(`(?<![\\w.])${escRe(loopVarName)}\\.([a-zA-Z_]\\w*)(?![\\w.])`, 'gi');
  return sql.replace(re, (_, field) => `{{loopRow.${field}}}`);
}

function escRe(str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Extracts {{variable}} names (excluding step/loopRow references) from a SQL string.
 * These are candidate user-input variables.
 */
function extractInputVarsFromSql(sql) {
  const vars = new Set();
  for (const m of sql.matchAll(/\{\{\s*([a-zA-Z0-9_.]+)\s*\}\}/g)) {
    const name = m[1];
    if (!/^step\d+\.|^loopRow\./i.test(name)) vars.add(name);
  }
  return vars;
}

// ----- Core statement processor -----

/**
 * Processes a list of raw statement strings into WorkflowStep objects.
 * Mutates steps array and varResolution map.
 */
function processStatements(rawStatements, steps, declaredVars, varResolution, loopCtxVar = null) {
  let i = 0;
  while (i < rawStatements.length) {
    const stmt = rawStatements[i].trim();
    if (!stmt) { i++; continue; }

    const opRaw = detectOperation(stmt);

    // ---- RAISE NOTICE ----
    const raiseNotice = parseRaiseNoticeStatement(stmt);
    if (raiseNotice) {
      const resolvedArgs = raiseNotice.args.map((arg) => {
        const withLoopCtx = loopCtxVar ? applyLoopVarResolution(arg, loopCtxVar) : arg;
        return applyVarResolution(withLoopCtx, varResolution);
      });

      steps.push({
        type: 'notice',
        operation: 'notice',
        noticeTemplate: raiseNotice.template,
        noticeArgs: resolvedArgs,
        title: `Paso ${steps.length + 1}: Mensaje de script`,
        requiresWrite: false,
        inputVars: resolvedArgs.flatMap((arg) => Array.from(extractInputVarsFromSql(String(arg || '')))),
      });
      i++;
      continue;
    }

    // ---- Variable assignment (accumulator / derived values) ----
    const assignment = parseAssignmentStatement(stmt);
    if (assignment) {
      const normalizedTarget = normalizeIntoVarName(assignment.targetVar) || assignment.targetVar;
      let assignmentExpr = assignment.expression;

      // If self-reference appears before first resolution, default it to 0.
      if (!varResolution.has(assignment.targetVar.toLowerCase())) {
        const selfRe = new RegExp(`(?<![\\w.])${escRe(assignment.targetVar)}(?![\\w.])`, 'gi');
        assignmentExpr = assignmentExpr.replace(selfRe, '0');
      }

      const resolvedExpr = applyVarResolution(
        loopCtxVar ? applyLoopVarResolution(assignmentExpr, loopCtxVar) : assignmentExpr,
        varResolution,
      );

      const syntheticSql = `SELECT (${resolvedExpr}) AS ${normalizedTarget}`;
      const stepIdx = steps.length;

      steps.push({
        type: 'assignment',
        sql: syntheticSql,
        title: `Paso ${stepIdx + 1}: Calcular ${normalizedTarget}`,
        operation: 'assignment',
        targetVar: assignment.targetVar,
        requiresWrite: false,
        inputVars: Array.from(extractInputVarsFromSql(syntheticSql)),
      });

      varResolution.set(assignment.targetVar.toLowerCase(), { stepIndex: stepIdx, field: normalizedTarget });
      i++;
      continue;
    }

    // ---- FOR LOOP ----
    if (opRaw === 'for_loop' || /^\s*FOR\s+\w+\s+IN\s*\(/i.test(stmt)) {
      const loopData = extractForLoop(stmt + ';'); // add ; to help termination
      if (loopData) {
        const resolvedSelect = applyVarResolution(loopData.selectSql, varResolution);
        const bodyRaw = splitTopLevelStatements(loopData.bodyText);
        const bodySteps = [];
        processStatements(bodyRaw, bodySteps, declaredVars, varResolution, loopData.loopVar);

        steps.push({
          type: 'for_loop',
          sql: resolvedSelect,
          loopVar: loopData.loopVar,
          bodySteps,
          title: inferTitle(loopData.selectSql, 'for_loop', steps.length),
          operation: 'select',
          requiresWrite: bodySteps.some((s) => s.requiresWrite),
          inputVars: Array.from(extractInputVarsFromSql(resolvedSelect)),
        });
        i++;
        continue;
      }
    }

    // ---- IF BLOCK ----
    if (opRaw === 'conditional' || /^\s*IF\s+[\s\S]{1,600}\bTHEN\b/i.test(stmt)) {
      const ifData = extractIfBlock(stmt + ';');
      if (ifData) {
        const thenRaw = splitTopLevelStatements(ifData.thenBody);
        const elseRaw = ifData.elseBody ? splitTopLevelStatements(ifData.elseBody) : [];
        const thenSteps = [];
        const elseSteps = [];
        processStatements(thenRaw, thenSteps, declaredVars, varResolution, loopCtxVar);
        processStatements(elseRaw, elseSteps, declaredVars, varResolution, loopCtxVar);

        // Try to extract a condition SQL (SELECT COUNT(*) or SELECT EXISTS)
        const condSqlRaw = buildConditionSql(ifData.condition, varResolution);

        steps.push({
          type: 'conditional',
          conditionExpr: applyVarResolution(ifData.condition, varResolution),
          conditionSql: condSqlRaw,
          thenSteps,
          elseSteps,
          title: inferTitle('', 'conditional', steps.length),
          operation: 'conditional',
          requiresWrite: thenSteps.some((s) => s.requiresWrite) || elseSteps.some((s) => s.requiresWrite),
          inputVars: Array.from(extractInputVarsFromSql(condSqlRaw || '')),
        });
        i++;
        continue;
      }
    }

    // ---- SELECT INTO ----
    if (opRaw === 'select') {
      const selectInto = parseSelectInto(stmt);
      if (selectInto) {
        const resolvedSql = applyVarResolution(
          loopCtxVar ? applyLoopVarResolution(selectInto.cleanSql, loopCtxVar) : selectInto.cleanSql,
          varResolution,
        );

        const stepIdx = steps.length;
        // Register variable resolution for each INTO var
        selectInto.intoVars.forEach((varName, idx) => {
          const field = selectInto.selectedFields[idx] || selectInto.selectedFields[0] || varName;
          varResolution.set(varName, { stepIndex: stepIdx, field });
        });

        steps.push({
          type: 'select',
          sql: resolvedSql,
          intoVars: selectInto.intoVars,
          selectedFields: selectInto.selectedFields,
          title: inferTitle(resolvedSql, 'select', stepIdx),
          operation: 'select',
          requiresWrite: false,
          inputVars: Array.from(extractInputVarsFromSql(resolvedSql)),
        });
        i++;
        continue;
      }
    }

    // ---- Regular SQL statement ----
    if (['select', 'insert', 'update', 'delete'].includes(opRaw)) {
      const withLoopCtx = loopCtxVar ? applyLoopVarResolution(stmt, loopCtxVar) : stmt;
      const resolvedSql = applyVarResolution(withLoopCtx, varResolution);

      steps.push({
        type: opRaw === 'select' ? 'select' : 'mutation',
        sql: resolvedSql,
        title: inferTitle(resolvedSql, opRaw, steps.length),
        operation: opRaw,
        requiresWrite: opRaw !== 'select',
        inputVars: Array.from(extractInputVarsFromSql(resolvedSql)),
      });
    }

    i++;
  }
}

/**
 * Tries to build a verifiable SQL condition from a PL/SQL IF condition expression.
 * e.g. "v_count > 0" → uses step context
 *      "NOT EXISTS (SELECT ...)" → wrap in SELECT
 */
function buildConditionSql(condExpr, varResolution) {
  const expr = String(condExpr || '').trim();

  // If the condition is itself a subquery check
  if (/^\(?SELECT\b/i.test(expr)) {
    return `SELECT CASE WHEN (${expr}) THEN 1 ELSE 0 END AS condition_result`;
  }
  if (/\bEXISTS\b/i.test(expr)) {
    return `SELECT CASE WHEN ${applyVarResolution(expr, varResolution)} THEN 1 ELSE 0 END AS condition_result`;
  }

  // Numeric comparison like "v_count > 0" - resolved at runtime via step context
  return null;
}

function parseAssignmentStatement(sql) {
  const m = String(sql || '').trim().match(/^([a-z_][a-z0-9_@#$]*)\s*:=\s*([\s\S]+)$/i);
  if (!m) return null;
  const targetVar = String(m[1] || '').trim().toLowerCase();
  const expression = String(m[2] || '').trim().replace(/;\s*$/, '');
  if (!targetVar || !expression) return null;
  return { targetVar, expression };
}

function buildInferredProjectionStep(declaredVars = new Map(), stepIndex = 0) {
  return {
    type: 'select',
    sql: 'SELECT CURRENT_TIMESTAMP AS generated_at',
    title: inferTitle('SELECT inferido desde DECLARE', 'select', stepIndex),
    operation: 'select',
    requiresWrite: false,
    inputVars: [],
    inferred: true,
  };
}

function parseRaiseNoticeStatement(sql) {
  const source = String(sql || '').trim().replace(/;\s*$/, '');
  const match = source.match(/^RAISE\s+NOTICE\s+([\s\S]+)$/i);
  if (!match) return null;

  const payload = String(match[1] || '').trim();
  if (!payload) return null;

  const parts = splitTopLevelCsv(payload);
  if (parts.length === 0) return null;

  const rawTemplate = String(parts[0] || '').trim();
  const template = rawTemplate
    .replace(/^'(.*)'$/s, '$1')
    .replace(/''/g, "'");

  const args = parts.slice(1).map((item) => String(item || '').trim()).filter(Boolean);
  return { template, args };
}

function toOutputAlias(rawName) {
  const normalized = normalizeIntoVarName(rawName);
  if (!normalized) return '';

  if (/(^|_)contador($|_)/i.test(normalized)) return 'total_usuarios';
  if (/(^|_)total(_|$).*usuarios?/i.test(normalized) || /usuarios?_total/i.test(normalized)) return 'total_usuarios';
  if (/(^|_)total(_|$).*logs?/i.test(normalized) || /logs?_total/i.test(normalized)) return 'total_logs';
  if (/(^|_)total(_|$).*sesiones?/i.test(normalized) || /sesiones?_total/i.test(normalized)) return 'total_sesiones';
  if (/(^|_)total(_|$).*activos?/i.test(normalized) || /activos?_total/i.test(normalized)) return 'total_activos';
  if (/(^|_)max(_|$).*logs?/i.test(normalized) || /logs?_max/i.test(normalized)) return 'max_logs';
  if (/(^|_)(promedio|avg|media)(_.*)?logs?/i.test(normalized) || /logs?_(promedio|avg|media)/i.test(normalized)) return 'promedio_logs';

  return normalized;
}

function buildFinalProjectionFromVarResolution(varResolution = new Map(), stepIndex = 0) {
  const projected = [];

  for (const [varName, info] of varResolution.entries()) {
    if (!info || info.stepIndex === undefined) continue;
    const field = String(info.field || '').trim();
    if (!field) continue;

    projected.push({
      alias: toOutputAlias(varName || field),
      placeholder: `{{step${Number(info.stepIndex) + 1}.${field}}}`,
      order: Number(info.stepIndex),
    });
  }

  const unique = new Map();
  for (const item of projected.sort((a, b) => a.order - b.order)) {
    if (!item.alias) continue;
    if (!unique.has(item.alias)) unique.set(item.alias, item.placeholder);
  }

  const semanticPriority = [
    'total_usuarios',
    'total_logs',
    'total_sesiones',
    'total_activos',
    'max_logs',
    'promedio_logs',
  ];

  const aliases = Array.from(unique.keys());
  aliases.sort((a, b) => {
    const ia = semanticPriority.indexOf(a);
    const ib = semanticPriority.indexOf(b);
    if (ia === -1 && ib === -1) return a.localeCompare(b);
    if (ia === -1) return 1;
    if (ib === -1) return -1;
    return ia - ib;
  });

  if (aliases.length === 0) return null;

  const cols = aliases
    .slice(0, 20)
    .map((alias) => `${unique.get(alias)} AS ${alias}`)
    .join(', ');

  return {
    type: 'select',
    sql: `SELECT ${cols}`,
    title: inferTitle('SELECT resumen procedural', 'select', stepIndex),
    operation: 'select',
    requiresWrite: false,
    inputVars: [],
    generatedFinalSelect: true,
  };
}

// ----- Main export -----

/**
 * Parse a PL/SQL script into a structured workflow.
 *
 * @param {string} sqlText
 * @returns {{
 *   steps: WorkflowStep[],
 *   inputVars: string[],
 *   declaredVars: string[],
 *   requiresConfirmation: boolean
 * }}
 */
export function parsePlSqlScript(sqlText) {
  const sourceText = String(sqlText || '').trim();
  const src = stripComments(sourceText);

  const declaredVars = parseDeclareBlock(src);
  const body = hasBeginBlock(src) ? extractBodyContent(src) : src;
  const rawStatements = splitTopLevelStatements(body);

  const steps = [];
  const varResolution = new Map();

  processStatements(rawStatements, steps, declaredVars, varResolution);

  const forcedProcedural = shouldForceProceduralMode(sourceText) || isComplexSqlScript(sourceText);
  if (forcedProcedural) {
    const finalSelectStep = buildFinalProjectionFromVarResolution(varResolution, steps.length)
      || buildInferredProjectionStep(declaredVars, steps.length);
    steps.push(finalSelectStep);
  }

  const hasExecutableStep = steps.some((s) => ['select', 'insert', 'update', 'delete', 'assignment', 'for_loop', 'conditional'].includes(String(s.operation || '')));
  if (!hasExecutableStep) {
    // Never fallback to SELECT 1. Infer a projection from DECLARE vars instead.
    steps.push(buildInferredProjectionStep(declaredVars, steps.length));
  }

  // Collect all input variables referenced across all steps
  const allInputVars = new Set();
  collectInputVars(steps, allInputVars);

  return {
    steps,
    inputVars: Array.from(allInputVars),
    declaredVars: Array.from(declaredVars.keys()),
    requiresConfirmation: steps.some((s) => s.requiresWrite),
  };
}

function collectInputVars(steps, set) {
  for (const step of steps) {
    for (const v of (step.inputVars || [])) set.add(v);
    if (step.bodySteps) collectInputVars(step.bodySteps, set);
    if (step.thenSteps) collectInputVars(step.thenSteps, set);
    if (step.elseSteps) collectInputVars(step.elseSteps, set);
  }
}
