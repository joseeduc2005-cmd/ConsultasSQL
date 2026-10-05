import { ensureDynamicColumnAllowed, isPlainObject, parseQualifiedFieldRef, resolveWhereEntry } from '../scripts/validacion.js';
import { normalizeLimitValue, resolveDynamicScriptNode } from './construccion.js';

/**
 * El armado de consultas relacionales: SELECT con JOIN, y las escrituras con su
 * explicación en castellano.
 *
 * Es la parte del motor que convierte una definición de artículo —tabla, uniones,
 * campos, condiciones— en una sentencia con sus parámetros. Lo que más importa no es
 * que el texto salga bien formado, sino que ningún valor acabe pegado dentro del SQL:
 * los valores viajan siempre como $1, $2, y eso es lo que separa una consulta de una
 * inyección.
 *
 * Es código puro. No sabe de PostgreSQL, no abre conexiones y no espera a nada, así
 * que se puede probar entero sin levantar una base.
 *
 * Dos cosas cambiaron al traerlo aquí, y las dos por el mismo motivo: este código
 * miraba por su cuenta el caché del esquema de PostgreSQL —el tipo de una columna y
 * la lista de tablas permitidas—, y eso lo ataba a la infraestructura.
 *
 * Ahora las recibe en un solo parámetro, que es el puerto:
 *
 *   esquema.validarTipo(tabla, columna, valor)   lanza si el valor no es del tipo
 *                                                que esa columna admite
 *   esquema.permiteTabla(nombre)                 si se puede unir con esa tabla
 *   esquema.columnasDe(nombre)                   qué columnas tiene esa tabla
 *
 * Sin valor por defecto, a propósito. Si alguien se olvida de pasarlo, revienta en la
 * primera comprobación. Con un valor por defecto se quedaría sin validar en silencio,
 * y para una comprobación que corre antes de escribir en producción ese es el peor
 * fallo posible: el que no se nota.
 */
export function normalizeStrictSqlRawPayload(rawPayload) {
  const raw = rawPayload && typeof rawPayload === 'object' && !Array.isArray(rawPayload)
    ? rawPayload
    : {};

  const rows = Array.isArray(raw.rows)
    ? raw.rows
    : Array.isArray(raw.data)
      ? raw.data
      : (raw.resultado && typeof raw.resultado === 'object' && !Array.isArray(raw.resultado) && Array.isArray(raw.resultado.data))
        ? raw.resultado.data
      : Array.isArray(raw.resultado)
        ? raw.resultado
        : [];

  const semanticCandidate =
    (raw.resultado && typeof raw.resultado === 'object' && !Array.isArray(raw.resultado) ? raw.resultado : null)
    || (raw.resultadoSemantico && typeof raw.resultadoSemantico === 'object' && !Array.isArray(raw.resultadoSemantico) ? raw.resultadoSemantico : null)
    || (raw.resultado_final && typeof raw.resultado_final === 'object' && !Array.isArray(raw.resultado_final) ? raw.resultado_final : null)
    || (raw.final && typeof raw.final === 'object' && !Array.isArray(raw.final) ? raw.final : null)
    || (rows[0] && typeof rows[0] === 'object' && !Array.isArray(rows[0]) ? rows[0] : {});

  const noticesFromArray = Array.isArray(raw.notices) ? raw.notices : [];
  const noticesFromSummary = String(raw.resumenHumano || raw.explicacion || raw.explanation || '')
    .split(/\r?\n/)
    .map((line) => String(line || '').trim())
    .filter(Boolean);
  const notices = noticesFromArray.length > 0 ? noticesFromArray : noticesFromSummary;

  const explicacion = String(
    raw.resumenHumano
    || raw.explicacion
    || raw.explanation
    || raw.message
    || raw.error
    || '',
  ).trim();

  const message = String(raw.message || raw.error || '').trim();

  return {
    rows,
    notices,
    semanticCandidate,
    explicacion,
    message,
  };
}

/** Formats a value for the before/after detail lines. */
export function formatDetailValue(v) {
  if (v === null || v === undefined) return 'NULL';
  if (v instanceof Date) return v.toISOString();
  if (typeof v === 'object') {
    const s = JSON.stringify(v);
    return s.length > 80 ? s.slice(0, 80) + '…' : s;
  }
  const s = String(v);
  return s.length > 80 ? s.slice(0, 80) + '…' : s;
}

/** Picks a likely identifier column from a row, for before/after matching. */
export function pickRowIdentifier(row = {}) {
  for (const k of ['id', 'ID', 'Id', '_id', 'uuid', 'UUID', 'codigo', 'CODIGO']) {
    if (row && Object.prototype.hasOwnProperty.call(row, k)) return { key: k, value: row[k] };
  }
  return null;
}

/**
 * Builds a human, multi-line explanation for a write operation, including a
 * per-row before→after diff (UPDATE), deleted rows (DELETE) or inserted rows (INSERT).
 */
export function buildWriteDetailExplanation({ operation, table, affectedRows, beforeRows = [], afterRows = [], text = '' }) {
  const op = String(operation || '').toLowerCase();
  const preview = String(text || '').replace(/\s+/g, ' ').trim();
  const queryPreview = preview.length > 160 ? preview.slice(0, 160) + '…' : preview;
  const n = Number(affectedRows || 0);
  const plural = (count) => (count === 1 ? '' : 's');
  const lines = [];

  if (op === 'update') {
    lines.push(`✅ UPDATE ejecutado en \`${table}\` — ${n} fila${plural(n)} afectada${plural(n)}.`);
    if (n === 0) {
      lines.push('ℹ️ Ninguna fila coincidió con el criterio WHERE — no se modificó ningún dato.');
    } else if (beforeRows.length && afterRows.length) {
      const afterById = new Map();
      for (const r of afterRows) {
        const id = pickRowIdentifier(r);
        if (id) afterById.set(String(id.value), r);
      }
      lines.push('📋 Cambios aplicados (antes → después):');
      let shown = 0;
      for (const before of beforeRows) {
        if (shown >= 20) { lines.push(`  … y ${beforeRows.length - shown} fila${plural(beforeRows.length - shown)} más`); break; }
        const id = pickRowIdentifier(before);
        const after = id ? afterById.get(String(id.value)) : null;
        if (!after) continue;
        const changes = [];
        for (const key of Object.keys(before)) {
          const b = formatDetailValue(before[key]);
          const a = formatDetailValue(after[key]);
          if (b !== a) changes.push(`${key}: ${b} → ${a}`);
        }
        if (changes.length) {
          const lbl = id ? `${id.key}=${formatDetailValue(id.value)}` : `fila ${shown + 1}`;
          lines.push(`  • ${lbl} — ${changes.join('  |  ')}`);
          shown++;
        }
      }
      if (shown === 0) lines.push('  • Las filas coincidieron pero los valores nuevos eran iguales a los anteriores.');
    }
  } else if (op === 'delete') {
    lines.push(`✅ DELETE ejecutado en \`${table}\` — ${n} fila${plural(n)} eliminada${plural(n)}.`);
    if (n === 0) {
      lines.push('ℹ️ Ninguna fila coincidió con el criterio WHERE — no se eliminó ningún dato.');
    } else if (beforeRows.length) {
      lines.push('🗑️ Registros eliminados:');
      beforeRows.slice(0, 20).forEach((r) => {
        const entries = Object.entries(r).slice(0, 6).map(([k, v]) => `${k}=${formatDetailValue(v)}`);
        lines.push(`  • ${entries.join(', ')}`);
      });
      if (beforeRows.length > 20) lines.push(`  … y ${beforeRows.length - 20} más`);
    }
  } else if (op === 'insert') {
    lines.push(`✅ INSERT ejecutado en \`${table}\` — ${n} fila${plural(n)} insertada${plural(n)}.`);
    if (afterRows.length) {
      lines.push('📥 Registros insertados:');
      afterRows.slice(0, 20).forEach((r) => {
        const entries = Object.entries(r).slice(0, 8).map(([k, v]) => `${k}=${formatDetailValue(v)}`);
        lines.push(`  • ${entries.join(', ')}`);
      });
    }
  } else {
    lines.push(`✅ ${op.toUpperCase()} ejecutado correctamente — ${n} fila${plural(n)} afectada${plural(n)}.`);
  }

  lines.push(`📝 SQL: ${queryPreview}`);
  return lines.join('\n');
}

/**
 * La regla: un valor tiene que ser del tipo que la columna dice.
 *
 * Antes esta funcion buscaba ella misma el tipo en el cache del esquema, y eso la
 * ataba a PostgreSQL. Ahora recibe el tipo ya resuelto: la regla es del negocio, de
 * donde sale el tipo no es asunto suyo.
 */
export function asegurarTipoDeColumna(tipoEsperado, field, value) {
  if (value === null || value === undefined) {
    return;
  }

  if (!tipoEsperado) {
    return;
  }

  if (tipoEsperado === 'string' && typeof value !== 'string') {
    throw new Error(`Tipo invalido para ${field}`);
  }

  if (tipoEsperado === 'boolean' && typeof value !== 'boolean') {
    throw new Error(`Tipo invalido para ${field}`);
  }

  if (tipoEsperado === 'number' && (typeof value !== 'number' || !Number.isFinite(value))) {
    throw new Error(`Tipo invalido para ${field}`);
  }

  if (tipoEsperado === 'string_or_number' && typeof value !== 'string' && typeof value !== 'number') {
    throw new Error(`Tipo invalido para ${field}`);
  }
}

export function buildRelationalMutationQuery(script, normalizedInputParams, workflowContext, startIndex, esquema) {
  workflowContext = workflowContext || {};
  startIndex = startIndex || 1;
  const baseTable = String(script.tabla || '').trim().toLowerCase();
  const joins = Array.isArray(script.join) ? script.join : [];
  const baseAlias = 'rb';

  const toQualifiedSql = (fieldRef) => {
    const ref = parseQualifiedFieldRef(fieldRef, baseTable);
    const tableSql = ref.table === baseTable ? baseAlias : ref.table;
    return `"${tableSql}"."${ref.column}"`;
  };

  const joinSql = joins
    .map((joinItem) => {
      const joinTable = String(joinItem.tabla || '').trim().toLowerCase();
      const onObject = joinItem.on || {};
      const onParts = Object.entries(onObject).map(([leftRef, rightRef]) => (
        `${toQualifiedSql(leftRef)} = ${toQualifiedSql(rightRef)}`
      ));
      return `JOIN "${joinTable}" ON ${onParts.join(' AND ')}`;
    })
    .join(' ');

  const resolvedWhere = isPlainObject(script.where)
    ? resolveDynamicScriptNode(script.where, normalizedInputParams, workflowContext)
    : {};

  const whereValues = [];
  const whereConditions = Object.entries(resolvedWhere || {}).map(([fieldRef, rawValue]) => {
    const leftSql = toQualifiedSql(fieldRef);
    const { op, value, fieldRef: rightFieldRef } = resolveWhereEntry(rawValue);

    if (rightFieldRef) {
      return `${leftSql} ${op} ${toQualifiedSql(rightFieldRef)}`;
    }

    const ref = parseQualifiedFieldRef(fieldRef, baseTable);
    esquema.validarTipo(ref.table, ref.column, value);
    whereValues.push(value);
    return `${leftSql} ${op} $${startIndex + whereValues.length - 1}`;
  });

  const logic = ['AND', 'OR'].includes(String(script.logic || '').toUpperCase())
    ? String(script.logic).toUpperCase()
    : 'AND';

  const whereClause = whereConditions.length
    ? ` WHERE ${whereConditions.join(` ${logic} `)} AND "${baseAlias}".ctid = "b".ctid`
    : ` WHERE "${baseAlias}".ctid = "b".ctid`;

  return {
    baseTable,
    baseAlias,
    joinSql,
    whereClause,
    whereValues,
    resolvedWhere,
  };
}

export function buildSelectProjectionSql(script, baseTable) {
  const rootColumns = Array.isArray(script?.columnas)
    ? script.columnas.map((entry) => String(entry || '').trim()).filter(Boolean)
    : [];

  const joinColumns = Array.isArray(script?.join)
    ? script.join.flatMap((joinItem) => {
        const joinTable = String(joinItem?.tabla || '').trim().toLowerCase();
        const cols = Array.isArray(joinItem?.columnas) ? joinItem.columnas : [];
        return cols
          .map((entry) => String(entry || '').trim())
          .filter(Boolean)
          .map((column) => ({ table: joinTable, column }));
      })
    : [];

  const projection = [
    ...rootColumns.map((column) => ({ table: baseTable, column })),
    ...joinColumns,
  ];

  if (projection.length === 0) {
    return `"${baseTable}".*`;
  }

  return projection
    .map(({ table, column }) => `"${table}"."${column}"`)
    .join(', ');
}

export function buildRelationalSelectQuery(script, normalizedInputParams, workflowContext, esquema) {
  workflowContext = workflowContext || {};
  const baseTable = String(script.tabla || '').trim().toLowerCase();
  const joins = Array.isArray(script.join) ? script.join : [];

    const joinClauses = joins.map((joinItem) => {
      const joinTable = String(joinItem.tabla || '').trim().toLowerCase();
      const onObject = joinItem.on || {};

      const onParts = Object.entries(onObject).map(([leftRef, rightRef]) => {
      const left = parseQualifiedFieldRef(leftRef, baseTable);
      const right = parseQualifiedFieldRef(rightRef);

      return `"${left.table}"."${left.column}" = "${right.table}"."${right.column}"`;
    });

    return `JOIN "${joinTable}" ON ${onParts.join(' AND ')}`;
  });

  const resolvedWhere = isPlainObject(script.where)
    ? resolveDynamicScriptNode(script.where, normalizedInputParams, workflowContext)
    : {};

  const whereEntries = Object.entries(resolvedWhere || {});
  const whereValues = [];
  const whereConditions = whereEntries.map(([fieldRef, rawValue]) => {
    const { table, column } = parseQualifiedFieldRef(fieldRef, baseTable);
    const { op, value, fieldRef: rightFieldRef } = resolveWhereEntry(rawValue);

    if (rightFieldRef) {
      const right = parseQualifiedFieldRef(rightFieldRef, baseTable);
      if (!esquema.permiteTabla(right.table)) {
        throw new Error(`WHERE: tabla no permitida (${right.table})`);
      }
      ensureDynamicColumnAllowed(right.table, [right.column], esquema);
      return `"${table}"."${column}" ${op} "${right.table}"."${right.column}"`;
    }

    esquema.validarTipo(table, column, value);
    whereValues.push(value);
    return `"${table}"."${column}" ${op} $${whereValues.length}`;
  });

  // Support AND / OR logic between conditions (default: AND)
  const logic = ['AND', 'OR'].includes(String(script.logic || '').toUpperCase())
    ? String(script.logic).toUpperCase()
    : 'AND';

  const whereClause = whereConditions.length ? ` WHERE ${whereConditions.join(` ${logic} `)}` : '';
  const limitValue = normalizeLimitValue(script.limit, 0);
  const limitClause = limitValue > 0 ? ` LIMIT ${limitValue}` : '';
  const projectionSql = buildSelectProjectionSql(script, baseTable);

  return {
    baseTable,
    resolvedWhere,
    whereValues,
    query: `SELECT ${projectionSql} FROM "${baseTable}" ${joinClauses.join(' ')}${whereClause}${limitClause}`,
  };
}