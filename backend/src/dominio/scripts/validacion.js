/**
 * Qué cuenta como una definición de script válida.
 *
 * Un artículo ejecutable guarda su script como un objeto: de dónde salen los datos, qué
 * columnas se tocan, qué condiciones se aplican. Antes de ejecutar nada, esto comprueba
 * que ese objeto describa algo permitido — y «permitido» aquí significa dentro de lo que
 * alguien autorizó, no dentro de lo que la base aceptaría.
 *
 * Veinte funciones y listas que vivían sueltas en `app.js`. Son dominio puro: reciben un
 * objeto y lanzan si no sirve. Juntas forman las reglas del formato, que es algo que
 * conviene poder leer de una sentada.
 */
import { isComplexSqlScript, parsePlSqlScript } from '../sql/plsql.js';
import { getBusinessActionMetadata, getRequiredParamsForActions } from '../acciones/catalogo.js';
import { asegurarConsultaDeMongo } from '../mongo/consulta.js';
import { normalizeSchemaIdentifier, splitManualSqlSteps, stripLeadingSqlComments, stripSqlStringsAndComments } from '../sql/guardas.js';

export const ALLOWED_WHERE_OPERATORS = new Set(['=', '!=', '>', '<', '>=', '<=', 'LIKE']);

/**
 * Qué puede hacer el script de un artículo.
 *
 * Solo leer. Antes la lista era ['update', 'insert', 'delete', 'select'] y el sistema
 * sabía escribir en producción con una confirmación; se decidió que no lo haga. Un
 * artículo pide los datos que le falten —una cédula, un número de transacción— y
 * devuelve lo que encuentra, y ahí acaba lo que puede hacer.
 *
 * Que la lista sea de uno es la garantía: añadir una escritura obliga a escribirla
 * aquí, y entonces es la decisión de alguien y no un descuido.
 */
export const DYNAMIC_SCRIPT_ALLOWED_TYPES = ['select'];

/** Lo que el sistema sabía hacer y ya no hace. Está para poder explicarlo al rechazar. */
export const DYNAMIC_SCRIPT_RETIRED_TYPES = ['update', 'insert', 'delete'];

export const MANUAL_SQL_BLOCKED_KEYWORDS = /\b(insert|update|delete|drop|alter|truncate|create|grant|revoke)\b/i;

export const WORKFLOW_ALLOWED_STEP_TYPES = ['select', 'update', 'insert', 'delete', 'validacion'];

export function isPlainObject(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

export function buildRelationKey(fromTable, fromColumn, toTable, toColumn) {
  return `${normalizeSchemaIdentifier(fromTable)}.${normalizeSchemaIdentifier(fromColumn)}=>${normalizeSchemaIdentifier(toTable)}.${normalizeSchemaIdentifier(toColumn)}`;
}

export function detectSqlOperation(sqlText = '') {
  const cleaned = stripLeadingSqlComments(String(sqlText || '').trim().replace(/;\s*$/, '').trim()).toLowerCase();
  if (cleaned.startsWith('select')) return 'select';
  // WITH ... AS (...) SELECT → CTE → select
  if (/^with\s/.test(cleaned)) return 'select';
  // (SELECT ...) UNION (SELECT ...) — valido, y sin esto quedaba como 'unknown'.
  if (/^\(+\s*select\b/.test(cleaned)) return 'select';
  if (cleaned.startsWith('update')) return 'update';
  if (cleaned.startsWith('delete')) return 'delete';
  if (cleaned.startsWith('insert')) return 'insert';
  // MERGE INTO — Oracle, MSSQL, PostgreSQL 15+
  if (cleaned.startsWith('merge')) return 'merge';
  // Stored procedures
  if (/^(call|exec(ute)?)\s/.test(cleaned)) return 'call';
  // DDL — DROP, CREATE, ALTER, GRANT, REVOKE, TRUNCATE, COMMENT, RENAME...
  // En artículos de admin: completamente válidos. En consola: bloqueados.
  if (/^(truncate|drop|create|alter|grant|revoke|comment|rename)\s/.test(cleaned)) return 'ddl';
  return 'unknown';
}

export function parseQualifiedFieldRef(fieldRef, defaultTable = '') {
  const raw = String(fieldRef || '').trim();
  if (!raw) {
    throw new Error('Referencia de campo inválida');
  }

  const parts = raw.split('.').map((part) => part.trim()).filter(Boolean);
  if (parts.length === 1) {
    if (!defaultTable) {
      throw new Error(`Campo ${raw} requiere tabla explícita`);
    }
    return { table: defaultTable, column: parts[0] };
  }

  if (parts.length !== 2) {
    throw new Error(`Campo calificado inválido: ${raw}`);
  }

  return { table: parts[0].toLowerCase(), column: parts[1] };
}

export function hasRelationBetweenFields(left, right, esquema) {
  const direct = buildRelationKey(left.table, left.column, right.table, right.column);
  const reverse = buildRelationKey(right.table, right.column, left.table, left.column);
  return esquema.hayRelacion(direct) || esquema.hayRelacion(reverse);
}

export function normalizeDynamicScriptType(type) {
  return String(type || '').trim().toLowerCase();
}

export function normalizeBusinessStep(step, index) {
  if (!step || typeof step !== 'object') {
    throw new Error(`Paso ${index + 1} inválido`);
  }

  const ordenRaw = Number(step.orden);
  const orden = Number.isFinite(ordenRaw) && ordenRaw > 0 ? Math.floor(ordenRaw) : index + 1;
  const descripcion = String(step.descripcion || '').trim();
  const accion = String(step.accion || '').trim();

  if (!descripcion) {
    throw new Error(`Paso ${orden} sin descripción`);
  }

  if (!accion) {
    throw new Error(`Paso ${orden} sin acción`);
  }

  return { orden, descripcion, accion };
}

/**
 * Normalize a WHERE value that may be either a plain scalar or an extended
 * operator object from the builder:
 *   { op: '!=', value: 5 }  or  { op: '=', field: 'employees.user_id' }
 * Returns { op, value, fieldRef } and rejects unknown operators.
 */
export function resolveWhereEntry(rawValue) {
  if (
    rawValue !== null &&
    typeof rawValue === 'object' &&
    !Array.isArray(rawValue) &&
    rawValue.op != null
  ) {
    const op = String(rawValue.op).trim().toUpperCase();
    if (!ALLOWED_WHERE_OPERATORS.has(op)) {
      throw new Error(`Operador WHERE no permitido: ${rawValue.op}`);
    }

    const hasField = typeof rawValue.field === 'string' && rawValue.field.trim() !== '';
    if (hasField) {
      return { op, value: null, fieldRef: String(rawValue.field).trim() };
    }

    return { op, value: rawValue.value ?? null, fieldRef: null };
  }
  return { op: '=', value: rawValue, fieldRef: null };
}

export function ensureDynamicColumnAllowed(tableName, columns, esquema) {
  const allowedColumns = esquema.columnasDe(tableName);
  for (const columnName of columns) {
    if (!allowedColumns.includes(columnName)) {
      throw new Error(`Columna no permitida: ${columnName}`);
    }
  }
}

export function ensureDynamicJoinRelationAllowed(left, right, contextLabel, esquema) {
  contextLabel = contextLabel || 'JOIN';
  if (left.table === right.table && left.column === right.column) {
    return;
  }

  if (!hasRelationBetweenFields(left, right, esquema)) {
    throw new Error(
      `${contextLabel}: no existe relación FK entre ${left.table}.${left.column} y ${right.table}.${right.column}`
    );
  }
}

export function assertDynamicScriptAllowed(script, esquema) {
  const type = normalizeDynamicScriptType(script.tipo);
  const tableName = String(script.tabla || '').trim().toLowerCase();

  if (!DYNAMIC_SCRIPT_ALLOWED_TYPES.includes(type)) {
    if (DYNAMIC_SCRIPT_RETIRED_TYPES.includes(type)) {
      throw new Error(
        `El sistema ya no ejecuta ${type.toUpperCase()}: un artículo solo consulta. `
        + 'Use un script de tipo select con los campos que haya que pedir.'
      );
    }
    throw new Error(`Operación no permitida: ${type || '(sin tipo)'}. Solo se admite select.`);
  }

  if (!esquema.permiteTabla(tableName)) {
    throw new Error('Tabla no permitida');
  }

  const setObject = isPlainObject(script.set) ? script.set : null;
  const whereObject = isPlainObject(script.where) ? script.where : null;
  const dataObject = isPlainObject(script.data) ? script.data : null;

  if (type === 'update') {
    if (!setObject || Object.keys(setObject).length === 0) {
      throw new Error('UPDATE requiere un objeto set válido');
    }
    if (!whereObject || Object.keys(whereObject).length === 0) {
      throw new Error('UPDATE requiere un objeto where válido');
    }
    ensureDynamicColumnAllowed(tableName, Object.keys(setObject), esquema);
    for (const fieldRef of Object.keys(whereObject)) {
      const { table, column } = parseQualifiedFieldRef(fieldRef, tableName);
      if (!esquema.permiteTabla(table)) {
        throw new Error(`WHERE: tabla no permitida (${table})`);
      }
      ensureDynamicColumnAllowed(table, [column], esquema);
    }
  }

  if (type === 'insert') {
    if (!dataObject || Object.keys(dataObject).length === 0) {
      throw new Error('INSERT requiere un objeto data válido');
    }
    ensureDynamicColumnAllowed(tableName, Object.keys(dataObject), esquema);
  }

  if (type === 'delete' || type === 'select') {
    if (type === 'delete' && (!whereObject || Object.keys(whereObject).length === 0)) {
      throw new Error(`${type.toUpperCase()} requiere un objeto where válido`);
    }
    if (whereObject && Object.keys(whereObject).length > 0) {
      // Support both plain "columna" keys and qualified "tabla.columna" keys
      for (const fieldRef of Object.keys(whereObject)) {
        const { table, column } = parseQualifiedFieldRef(fieldRef, tableName);
        if (!esquema.permiteTabla(table)) {
          throw new Error(`WHERE: tabla no permitida (${table})`);
        }
        ensureDynamicColumnAllowed(table, [column], esquema);
      }
    }

    if (type === 'select' && Array.isArray(script.columnas)) {
      const parsedColumns = script.columnas
        .map((entry) => parseQualifiedFieldRef(String(entry || '').trim(), tableName));

      parsedColumns.forEach(({ table, column }) => {
        if (!esquema.permiteTabla(table)) {
          throw new Error(`SELECT: tabla no permitida (${table})`);
        }
        ensureDynamicColumnAllowed(table, [column], esquema);
      });
    }
  }

  return {
    type,
    tableName,
    setObject,
    dataObject,
    whereObject,
  };
}

/**
 * Valida que un SQL sea ejecutable en el contexto dado.
 *
 * trusted=true  → artículos escritos por admins: NO se bloquea ningún SQL.
 *                 El control de acceso es la visibilidad del artículo (visible_para).
 * trusted=false → consola SQL directa: se aplican restricciones de seguridad.
 */
/**
 * Las operaciones que el sistema ya no ejecuta, sea cual sea la base.
 *
 * «ddl» incluye TRUNCATE, DROP, ALTER y CREATE: detectSqlOperation los agrupa bajo ese
 * nombre, y por eso TRUNCATE se colaba cuando la lista decia 'truncate'.
 */
export const OPERACIONES_QUE_ESCRIBEN = new Set([
  'update', 'delete', 'insert', 'merge', 'truncate', 'ddl', 'call',
]);

/**
 * Lo que una consulta no puede llevar aunque empiece por SELECT.
 *
 * Las tres escriben o bloquean, y las tres se leen como 'select':
 *
 *   - un CTE cuyo cuerpo es un INSERT, UPDATE, DELETE o MERGE — en PostgreSQL eso
 *     escribe de verdad, y `WITH x AS (DELETE ...) SELECT * FROM x` borraba sin que la
 *     puerta se enterara;
 *   - `SELECT ... INTO copia`, que crea una tabla;
 *   - `FOR UPDATE` y sus variantes, que no escriben pero toman candados sobre las filas
 *     y en produccion dejan a otros esperando.
 *
 * Se mira sobre el texto sin literales ni comentarios: si no, un `WHERE accion =
 * 'delete'` quedaria prohibido, que es la clase de falso positivo que obliga a la gente
 * a buscarle la vuelta a la herramienta.
 */
const EFECTOS_EN_UNA_CONSULTA = [
  [/\(\s*(insert|update|delete|merge)\b/i, 'un CTE que escribe'],
  [/\binto\b/i, 'un SELECT ... INTO, que crea una tabla'],
  [/\bfor\s+(update|share|no\s+key\s+update|key\s+share)\b/i, 'un FOR UPDATE, que bloquea las filas'],
];

/**
 * Se niega si lo que llega no es, entera, una consulta.
 *
 * Esto tenia una lista de lo prohibido y dejaba pasar el resto, y `detectSqlOperation`
 * devuelve 'unknown' para lo que no empieza por una palabra que conozca. O sea que todo
 * lo que no reconocia entraba — y lo que no reconoce es justo lo que mas escribe: un
 * bloque `BEGIN ... UPDATE ... END` de Oracle, un `DO $$ ... $$` de PostgreSQL, un
 * `REPLACE INTO` de MySQL, un `COPY ... FROM`, un `SET session_replication_role`. La
 * base de la cooperativa es Oracle, donde el bloque PL/SQL es la forma normal de
 * escribir, asi que el agujero estaba abierto justo donde mas se usa.
 *
 * Ahora es al reves: solo pasa lo que se reconoce como consulta. Lo que la puerta no
 * sabe leer no entra, que es como tiene que fallar una puerta.
 *
 * Y se miran todas las sentencias, no la primera: «SELECT 1; DROP TABLE socios» pasaba
 * entera. Partirlas aqui dentro y no en cada sitio que llama es lo que evita que el
 * proximo que use esta funcion se olvide de hacerlo.
 */
export function asegurarSoloConsulta(sqlText) {
  const bruto = String(sqlText || '').trim();
  if (!bruto) return;

  // Mongo no es SQL, pero tiene que pasar por una puerta igual.
  //
  // Aqui habia un `return`: «es Mongo, ya lo mira su propia puerta». Para un articulo
  // con `collection` y `operation` como campos sueltos es cierto. Pero la consulta
  // tambien se puede escribir entera dentro de `sql`, y entonces baja por este camino y
  // no habia ninguna otra puerta detras. Un `deleteMany` escrito asi se guardaba sin que
  // nadie mirara nada.
  if (bruto.startsWith('{')) {
    asegurarConsultaDeMongo(bruto);
    return;
  }

  for (const sentencia of splitManualSqlSteps(bruto)) {
    const limpio = stripLeadingSqlComments(sentencia.replace(/;\s*$/, '').trim());
    if (!limpio) continue;

    const operacion = detectSqlOperation(limpio);
    if (operacion !== 'select') {
      // Un nombre que se entienda cuando lo hay; cuando no, decir que no se reconoce es
      // mas util que inventarse una categoria.
      const que = operacion === 'unknown'
        ? 'Esto no se reconoce como una consulta'
        : `El sistema no ejecuta ${operacion.toUpperCase()}`;
      throw new Error(
        `${que}: un articulo solo consulta, contra cualquier base. `
        + 'Escriba un SELECT con los campos que haya que pedir.'
      );
    }

    const sinLiterales = stripSqlStringsAndComments(limpio);
    for (const [patron, descripcion] of EFECTOS_EN_UNA_CONSULTA) {
      if (patron.test(sinLiterales)) {
        throw new Error(
          `Esa consulta lleva ${descripcion}, y eso no es consultar. `
          + 'Escriba un SELECT con los campos que haya que pedir.'
        );
      }
    }
  }
}

export function assertManualSqlAllowed(sqlText, options = {}) {
  const {
    allowMutations = false,
    writeConfirmed = false,
    assumeWriteConfirmed = false,
    trusted = false,       // true = artículo de admin, sin restricciones de sintaxis
  } = options;

  const raw = String(sqlText || '').trim();
  if (!raw) throw new Error('SQL manual vacío');

  const cleaned = stripLeadingSqlComments(raw.replace(/;\s*$/, '').trim());
  const operation = detectSqlOperation(cleaned);

  const WRITE_OPS = new Set(['update', 'delete', 'insert', 'merge', 'truncate']);
  const PROC_OPS  = new Set(['call']);

  // ── Ninguna escritura, venga de donde venga ────────────────────────────────
  //
  // Va ANTES del modo confiable a proposito. El modo confiable —el de los articulos
  // de admin— hacia `if (trusted) return;` y se saltaba todo, asi que un articulo con
  // UPDATE se guardaba sin que nadie lo mirara. Ser de admin permite escribir SQL
  // raro; no permite escribir en la base.
  //
  // Vale para cualquier base conectada, no solo PostgreSQL: por aqui pasa todo el SQL
  // manual, sea cual sea el motor al que vaya.
  asegurarSoloConsulta(cleaned);

  // ── Modo confiable (articulos de admin): sin restricciones de sintaxis ──────
  if (trusted) return;


  if (!['select', ...WRITE_OPS, ...PROC_OPS].includes(operation)) {
    throw new Error('Operación no permitida en la consola. Usa SELECT, INSERT, UPDATE, DELETE, MERGE, CALL.');
  }

  if ((WRITE_OPS.has(operation) || PROC_OPS.has(operation)) && !allowMutations) {
    throw new Error('Solo se permite SQL SELECT en este contexto');
  }

  if ((WRITE_OPS.has(operation) || PROC_OPS.has(operation)) && !(writeConfirmed || assumeWriteConfirmed)) {
    throw new Error('Esta acción modificará datos. Confirma la ejecución para continuar.');
  }

  // Keyword check against code only — a "update"/"delete" word inside a string
  // literal of a SELECT must not trip this guard.
  if (operation === 'select' && MANUAL_SQL_BLOCKED_KEYWORDS.test(stripSqlStringsAndComments(cleaned))) {
    throw new Error('SQL contiene palabras bloqueadas');
  }

  // Reject stacked statements — but a ; inside a string literal or comment is
  // NOT a separator, so use the real tokenizing splitter.
  if (!PROC_OPS.has(operation) && operation !== 'merge' && splitManualSqlSteps(cleaned).length > 1) {
    throw new Error('Solo se permite una consulta por ejecución en la consola');
  }
}

export function validateWorkflowBuilderDefinition(script, esquema) {
  if (!script || typeof script !== 'object' || Array.isArray(script)) {
    throw new Error('script_json de workflow inválido');
  }

  const workflow = Array.isArray(script.workflow) ? script.workflow : [];
  if (workflow.length === 0) {
    throw new Error('El workflow requiere al menos un paso');
  }

  workflow.forEach((rawStep, index) => {
    if (!isPlainObject(rawStep)) {
      throw new Error(`Paso ${index + 1} inválido`);
    }

    const stepType = normalizeDynamicScriptType(rawStep.tipo);
    if (!WORKFLOW_ALLOWED_STEP_TYPES.includes(stepType)) {
      throw new Error(`Operación no soportada: ${stepType || 'desconocida'}`);
    }

    if (stepType === 'validacion') {
      const variable = String(rawStep.variable || '').trim();
      if (!variable) {
        throw new Error(`Paso ${index + 1}: la validación requiere variable`);
      }

      const condition = String(rawStep.condicion || 'existe').trim().toLowerCase();
      if (!['existe', 'no_existe', 'igual'].includes(condition)) {
        throw new Error(`Paso ${index + 1}: condición de validación no soportada`);
      }

      if (condition === 'igual' && !Object.prototype.hasOwnProperty.call(rawStep, 'valor')) {
        throw new Error(`Paso ${index + 1}: la condición igual requiere valor`);
      }
      return;
    }

    assertDynamicScriptAllowed(rawStep, esquema);
  });
}

export function validateRelationalScriptDefinition(script, esquema) {
  if (!script || typeof script !== 'object' || Array.isArray(script)) {
    throw new Error('Script relacional inválido');
  }

  const type = normalizeDynamicScriptType(script.tipo);
  if (type !== 'select') {
    throw new Error('Script relacional solo soporta tipo select');
  }

  const baseTable = String(script.tabla || '').trim().toLowerCase();
  if (!esquema.permiteTabla(baseTable)) {
    throw new Error(`Tabla base no permitida: ${baseTable || 'vacía'}`);
  }

  const joins = Array.isArray(script.join) ? script.join : [];
  if (joins.length === 0) {
    throw new Error('Script relacional requiere al menos un join');
  }

  joins.forEach((joinItem, index) => {
    if (!isPlainObject(joinItem)) {
      throw new Error(`Join ${index + 1} inválido`);
    }

    const joinTable = String(joinItem.tabla || '').trim().toLowerCase();
    if (!esquema.permiteTabla(joinTable)) {
      throw new Error(`Join ${index + 1}: tabla no permitida (${joinTable || 'vacía'})`);
    }

    const onObject = isPlainObject(joinItem.on) ? joinItem.on : null;
    if (!onObject || Object.keys(onObject).length === 0) {
      throw new Error(`Join ${index + 1}: requiere objeto on`);
    }

    Object.entries(onObject).forEach(([leftRef, rightRef]) => {
      const left = parseQualifiedFieldRef(leftRef, baseTable);
      const right = parseQualifiedFieldRef(rightRef);

      if (!esquema.permiteTabla(left.table)) {
        throw new Error(`Join ${index + 1}: tabla no permitida en ON (${left.table})`);
      }
      if (!esquema.permiteTabla(right.table)) {
        throw new Error(`Join ${index + 1}: tabla no permitida en ON (${right.table})`);
      }

      ensureDynamicColumnAllowed(left.table, [left.column], esquema);
      ensureDynamicColumnAllowed(right.table, [right.column], esquema);
      ensureDynamicJoinRelationAllowed(left, right, `Join ${index + 1}`, esquema);
    });

    if (Array.isArray(joinItem.columnas)) {
      joinItem.columnas.forEach((columnName) => {
        const col = String(columnName || '').trim();
        if (col) ensureDynamicColumnAllowed(joinTable, [col], esquema);
      });
    }
  });

  if (Array.isArray(script.columnas)) {
    script.columnas.forEach((columnName) => {
      const col = String(columnName || '').trim();
      if (col) ensureDynamicColumnAllowed(baseTable, [col], esquema);
    });
  }

  const whereObject = isPlainObject(script.where) ? script.where : null;
  if (whereObject) {
    Object.entries(whereObject).forEach(([fieldRef, rawValue]) => {
      const { table, column } = parseQualifiedFieldRef(fieldRef, baseTable);
      if (!esquema.permiteTabla(table)) {
        throw new Error(`WHERE: tabla no permitida (${table})`);
      }
      ensureDynamicColumnAllowed(table, [column], esquema);

      const { fieldRef: rightFieldRef } = resolveWhereEntry(rawValue);
      if (rightFieldRef) {
        const right = parseQualifiedFieldRef(rightFieldRef, baseTable);
        if (!esquema.permiteTabla(right.table)) {
          throw new Error(`WHERE: tabla no permitida (${right.table})`);
        }
        ensureDynamicColumnAllowed(right.table, [right.column], esquema);
      }
    });
  }
}

export function validateRelationalMutationScriptDefinition(script, mutationType, esquema) {
  // Esta funcion validaba los UPDATE y DELETE relacionales de un articulo. El sistema
  // ya no escribe, asi que lo unico que queda de ella es negarse.
  //
  // Se deja en vez de borrarla, y rechazando en vez de devolviendo: si algun camino
  // que no se vio todavia la llama, falla aqui y no ejecuta nada. Su cuerpo —noventa
  // lineas que comprobaban columnas y uniones de una escritura— se quito porque el
  // linter lo marco como inalcanzable, que es lo que era.
  void script; void mutationType; void esquema;
  throw new Error(
    `El sistema ya no ejecuta ${String(mutationType || '').toUpperCase()} relacionales: `
    + 'un articulo solo consulta.'
  );
}

export function validateBusinessScriptDefinition(script, esquema) {
  if (!script || typeof script !== 'object' || Array.isArray(script)) {
    throw new Error('script_json empresarial inválido');
  }

  if (
    String(script.modo || '').trim().toLowerCase() === 'script'
    && String(script.origen || '').trim().toLowerCase() === 'manual-sql'
  ) {
    // MongoDB: colección + pipeline/filter, sin campo sql.
    //
    // Esto hacía `return` sin mirar nada, con el motivo de que no hay SQL que validar.
    // Era verdad y no bastaba: lo que había que mirar era la operación, y sin eso un
    // artículo con `deleteMany` se guardaba tan tranquilo. La regla es que el sistema no
    // escribe en ninguna base, y Mongo es una base.
    const hasMongoPayload =
      String(script.collection || '').trim().length > 0 &&
      (String(script.pipeline || '').trim().length > 0 ||
       String(script.filter   || '').trim().length > 0 ||
       String(script.operation || '').trim().length > 0);
    if (hasMongoPayload) {
      asegurarConsultaDeMongo(script);
      return;
    }

    const sqlText = String(script.sql || '').trim();
    if (!sqlText) throw new Error('SQL manual vacío');

    // Ninguna escritura, y aqui arriba del todo.
    //
    // Antes la comprobacion vivia mas abajo y las dos ramas siguientes se la saltaban:
    // un INSERT con parentesis o un MERGE los considera complejos isComplexSqlScript,
    // y esa rama hacia return sin mirar nada. Por eso un articulo con INSERT se
    // guardaba tan tranquilo. Comprobar primero y una sola vez es lo que lo cierra
    // para cualquier forma de SQL y cualquier base conectada.
    splitManualSqlSteps(sqlText).forEach((paso) => asegurarSoloConsulta(paso));

    // Complex PL/SQL scripts: validate via interpreter
    if (isComplexSqlScript(sqlText)) {
      const parsed = parsePlSqlScript(sqlText);
      if (parsed.steps.length === 0) {
        throw new Error('El script procedural no generó pasos ejecutables');
      }
      return;
    }

    // Standard multi-step SQL
    const sqlSteps = splitManualSqlSteps(sqlText);
    if (sqlSteps.length === 0) throw new Error('SQL manual vacío');
      // allowMutations: false. Antes era true, con el motivo «articulos admin ->
      // cualquier SQL valido (DROP, ALTER, MERGE, etc.)», y por eso un articulo con
      // un UPDATE se guardaba sin problema y solo fallaba al ejecutarlo. Mejor
      // negarse al guardar: nadie construye un articulo entero para descubrir
      // despues que no corre. trusted=true se mantiene: el SQL lo escribe un admin.
    sqlSteps.forEach((stepSql) => {
      assertManualSqlAllowed(stepSql, { allowMutations: false, writeConfirmed: false, trusted: true });
    });
    return;
  }

  if (Array.isArray(script.workflow)) {
    validateWorkflowBuilderDefinition(script, esquema);
    return;
  }

  if (Array.isArray(script.join)) {
    validateRelationalScriptDefinition(script, esquema);
    return;
  }

  const dynamicType = normalizeDynamicScriptType(script.tipo);
  if (dynamicType) {
    assertDynamicScriptAllowed(script, esquema);
    return;
  }

  if (String(script.modo || '').trim().toLowerCase() !== 'script') {
    throw new Error('El script empresarial debe incluir modo="script"');
  }

  const scriptName = String(script.script || '').trim();
  if (!scriptName) {
    throw new Error('El script empresarial requiere nombre de proceso');
  }

  const steps = Array.isArray(script.pasos) ? script.pasos : [];
  if (steps.length === 0) {
    throw new Error('El script empresarial requiere al menos un paso');
  }

  const normalizedSteps = steps.map((step, index) => normalizeBusinessStep(step, index));
  const actionNames = normalizedSteps.map((step) => step.accion);

  for (const actionName of actionNames) {
    const metadata = getBusinessActionMetadata(actionName);
    if (!metadata) {
      throw new Error(`Acción no permitida: ${actionName}`);
    }
  }

  const requiredFromActions = getRequiredParamsForActions(actionNames);
  const requiredFromScript = Array.isArray(script.parametros_requeridos)
    ? script.parametros_requeridos.map((item) => String(item || '').trim()).filter(Boolean)
    : [];

  for (const paramName of requiredFromActions) {
    if (!requiredFromScript.includes(paramName)) {
      throw new Error(`El script empresarial debe declarar el parámetro requerido: ${paramName}`);
    }
  }
}
