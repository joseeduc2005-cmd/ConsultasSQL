/**
 * Workflow Builder Utilities - Complete Production Implementation
 * Handles workflow creation, validation, and JSON generation
 */

// ============================================================
// CONSTANTS & TYPE DEFINITIONS
// ============================================================

export const SOLUTION_TYPES = {
  DATABASE: 'database',
  SCRIPT: 'script',
  LECTURA: 'lectura'
};

export const ACTION_TYPES = {
  SELECT: 'select',
  UPDATE: 'update',
  INSERT: 'insert',
  DELETE: 'delete',
  VALIDACION: 'validacion'
};

export const SELECT_MODES = {
  SIMPLE: 'simple',
  RELACIONAL: 'relacional',
  ANALITICO: 'analitico',
};

export const ALLOWED_ACTION_TYPES = {
  [SOLUTION_TYPES.DATABASE]: ['select', 'update', 'insert', 'delete'],
  [SOLUTION_TYPES.SCRIPT]: ['select', 'update', 'insert', 'delete', 'validacion']
};

export const ALLOWED_TABLES = [];

const PLACEHOLDER_PATTERN = /^\{\{[^{}]+\}\}$/;

const isPlaceholderValue = (value) => {
  if (value === null || value === undefined) return false;
  return PLACEHOLDER_PATTERN.test(String(value).trim());
};

const parseScriptValue = (rawValue) => {
  const trimmed = String(rawValue ?? '').trim();
  if (!trimmed) return '';
  if (isPlaceholderValue(trimmed)) return trimmed;
  if (/^-?\d+(\.\d+)?$/.test(trimmed)) return Number(trimmed);
  if (/^(true|false)$/i.test(trimmed)) return trimmed.toLowerCase() === 'true';
  if (/^null$/i.test(trimmed)) return null;
  return trimmed;
};

const getInsertPairs = (config = {}) => {
  if (Array.isArray(config.insert_pairs)) {
    return config.insert_pairs;
  }

  if (config.columnas_valores && typeof config.columnas_valores === 'object' && !Array.isArray(config.columnas_valores)) {
    return Object.entries(config.columnas_valores).map(([columna, valor]) => ({ columna, valor: String(valor ?? '') }));
  }

  return [];
};

const getJoinPairs = (config = {}) => {
  if (!Array.isArray(config.join_pairs)) return [];

  return config.join_pairs
    .map((pair) => ({
      tabla: String(pair?.tabla || '').trim().toLowerCase(),
      base_columna: String(pair?.base_columna || '').trim(),
      join_columna: String(pair?.join_columna || '').trim(),
    }))
    .filter((pair) => pair.tabla || pair.base_columna || pair.join_columna);
};

const buildRelationalJoinFromPairs = (joinPairs = [], baseTable = '') => {
  if (!baseTable) return [];

  const groupedByTable = new Map();

  (Array.isArray(joinPairs) ? joinPairs : [])
    .filter((pair) => pair.tabla && pair.base_columna && pair.join_columna)
    .forEach((pair) => {
      const joinTable = String(pair.tabla || '').trim().toLowerCase();
      if (!joinTable) return;

      if (!groupedByTable.has(joinTable)) {
        groupedByTable.set(joinTable, {
          tabla: joinTable,
          on: {},
        });
      }

      const leftRef = `${baseTable}.${pair.base_columna}`;
      const rightRef = `${joinTable}.${pair.join_columna}`;
      groupedByTable.get(joinTable).on[leftRef] = rightRef;
    });

  return Array.from(groupedByTable.values());
};

const getSelectConditionPairs = (config = {}) => {
  if (Array.isArray(config.where_pairs)) {
    return config.where_pairs
      .map((pair) => ({
        tabla: String(pair?.tabla || '').trim(),
        columna: String(pair?.columna || '').trim(),
        operador: String(pair?.operador || '=').trim() || '=',
        valor: String(pair?.valor ?? '').trim(),
        compare_type: String(pair?.compare_type || 'value').trim() || 'value',
        right_tabla: String(pair?.right_tabla || '').trim(),
        right_columna: String(pair?.right_columna || '').trim(),
      }))
      .filter((pair) => pair.columna || pair.valor || pair.right_columna);
  }

  // Legacy fallback: single columna/valor
  if (config.columna || config.valor) {
    return [{
      tabla: '',
      columna: String(config.columna || '').trim(),
      operador: '=',
      valor: String(config.valor ?? '').trim(),
      compare_type: 'value',
      right_tabla: '',
      right_columna: '',
    }];
  }

  return [];
};

const getSelectedColumns = (config = {}, baseTable = '') => {
  const raw = Array.isArray(config.selected_columns)
    ? config.selected_columns
    : Array.isArray(config.columnas)
      ? config.columnas
      : [];

  return raw
    .map((entry) => String(entry || '').trim())
    .filter(Boolean)
    .map((entry) => {
      if (entry.includes('.')) return entry;
      return baseTable ? `${baseTable}.${entry}` : entry;
    });
};

const getSelectMode = (config = {}) => {
  const explicitMode = String(config.select_mode || '').trim();
  if (Object.values(SELECT_MODES).includes(explicitMode)) {
    return explicitMode;
  }

  const queryMode = String(config.query_mode || '').trim();
  if (queryMode === 'analitico') return SELECT_MODES.ANALITICO;
  if (queryMode === 'compuesta') return SELECT_MODES.RELACIONAL;
  if (queryMode === 'normal') return SELECT_MODES.SIMPLE;

  return SELECT_MODES.RELACIONAL;
};

/**
 * Build the WHERE key for a condition pair.
 * - JOIN queries: "tabla.columna" so the backend can route each condition to
 *   the correct table alias (handled by parseQualifiedFieldRef).
 * - Simple (non-JOIN) queries: plain "columna" — the backend expects bare
 *   column names for single-table WHERE clauses.
 */
const buildWhereKey = (pair, baseTable, isJoin = false) => {
  if (!isJoin) return pair.columna;
  const tabla = pair.tabla || baseTable || '';
  const columna = pair.columna;
  if (tabla && columna) return `${tabla}.${columna}`;
  return columna;
};

/**
 * Apply a scalar WHERE filter with operator support.
 * Returns a value usable in the where object for '=' operators and stores
 * extended operator info for non-'=' operators.
 */
const buildWhereValue = (pair) => {
  if (pair.compare_type === 'column' && pair.right_columna) {
    const rhsField = pair.right_tabla
      ? `${pair.right_tabla}.${pair.right_columna}`
      : pair.right_columna;
    return { op: pair.operador || '=', field: rhsField };
  }

  if (pair.operador === '=') return parseScriptValue(pair.valor);
  // For non-= operators, encode as { op, value } to preserve semantics
  return { op: pair.operador, value: parseScriptValue(pair.valor) };
};

// ============================================================
// ADAPTER FUNCTIONS - Convert between type systems
// ============================================================

/**
 * Convert admin page WorkflowRow format to internal format
 */
export const convertAdminRowToInternalRow = (adminRow) => {
  if (!adminRow) return null;

  const internalConfig = {};

  // Map common fields
  if (adminRow.config?.campo) internalConfig.columna = adminRow.config.campo;
  if (adminRow.config?.valor) internalConfig.valor = adminRow.config.valor;
  if (adminRow.config?.campoObjetivo) internalConfig.columna_actualizar = adminRow.config.campoObjetivo;
  if (adminRow.config?.valorNuevo) internalConfig.valor_nuevo = adminRow.config.valorNuevo;
  if (adminRow.config?.condicionCampo) internalConfig.columna_condicion = adminRow.config.condicionCampo;
  if (adminRow.config?.condicionValor) internalConfig.valor_condicion = adminRow.config.condicionValor;
  if (adminRow.config?.variable) internalConfig.variable = adminRow.config.variable;
  if (adminRow.config?.condicion) internalConfig.condicion = adminRow.config.condicion;
  if (adminRow.config?.valorValidacion) internalConfig.valor = adminRow.config.valorValidacion;
  if (adminRow.config?.guardarEn) internalConfig.guardar_en = adminRow.config.guardarEn;
  if (adminRow.config?.mensajeError) internalConfig.mensaje_error = adminRow.config.mensajeError;
  if (adminRow.config?.query_mode) internalConfig.query_mode = adminRow.config.query_mode;
  if (Array.isArray(adminRow.config?.join_pairs)) internalConfig.join_pairs = adminRow.config.join_pairs;
  if (Array.isArray(adminRow.config?.where_pairs)) internalConfig.where_pairs = adminRow.config.where_pairs;

  return {
    orden: adminRow.orden,
    descripcion: adminRow.descripcion,
    tipo: adminRow.tipo,
    tabla: adminRow.tabla,
    config: internalConfig
  };
};

/**
 * Convert internal format back to admin page WorkflowRow format
 */
export const convertInternalRowToAdminRow = (internalRow) => {
  if (!internalRow) return null;

  const adminConfig = {};

  // Map back
  if (internalRow.config?.columna) adminConfig.campo = internalRow.config.columna;
  if (internalRow.config?.valor) adminConfig.valor = internalRow.config.valor;
  if (internalRow.config?.columna_actualizar) adminConfig.campoObjetivo = internalRow.config.columna_actualizar;
  if (internalRow.config?.valor_nuevo) adminConfig.valorNuevo = internalRow.config.valor_nuevo;
  if (internalRow.config?.columna_condicion) adminConfig.condicionCampo = internalRow.config.columna_condicion;
  if (internalRow.config?.valor_condicion) adminConfig.condicionValor = internalRow.config.valor_condicion;
  if (internalRow.config?.variable) adminConfig.variable = internalRow.config.variable;
  if (internalRow.config?.condicion) adminConfig.condicion = internalRow.config.condicion;
  if (internalRow.config?.valor) adminConfig.valorValidacion = internalRow.config.valor;
  if (internalRow.config?.guardar_en) adminConfig.guardarEn = internalRow.config.guardar_en;
  if (internalRow.config?.mensaje_error) adminConfig.mensajeError = internalRow.config.mensaje_error;
  if (internalRow.config?.query_mode) adminConfig.query_mode = internalRow.config.query_mode;
  if (Array.isArray(internalRow.config?.join_pairs)) adminConfig.join_pairs = internalRow.config.join_pairs;
  if (Array.isArray(internalRow.config?.where_pairs)) adminConfig.where_pairs = internalRow.config.where_pairs;

  return {
    orden: internalRow.orden,
    descripcion: internalRow.descripcion,
    tipo: internalRow.tipo,
    tabla: internalRow.tabla,
    config: adminConfig
  };
};

/**
 * Batch convert admin rows to internal format
 */
export const convertAdminRowsToInternal = (adminRows) => {
  return adminRows
    .map(convertAdminRowToInternalRow)
    .filter(Boolean);
};

/**
 * Batch convert internal rows back to admin format
 */
export const convertInternalRowsToAdmin = (internalRows) => {
  return internalRows
    .map(convertInternalRowToAdminRow)
    .filter(Boolean);
};

// ============================================================
// EMPTY ROW FACTORY
// ============================================================

export const createEmptyWorkflowRow = (orden = 1) => ({
  orden,
  descripcion: '',
  tipo: '',
  tabla: '',
  config: {}
});

// ============================================================
// VALIDATION FUNCTIONS
// ============================================================

/**
 * Get allowed action types for a given solution type
 */
export const getAllowedActionTypes = (solutionType) => {
  return ALLOWED_ACTION_TYPES[solutionType] || [];
};

/**
 * Check if action type is valid for solution type
 */
export const isActionTypeAllowed = (actionType, solutionType) => {
  return getAllowedActionTypes(solutionType).includes(actionType);
};

/**
 * Validate a single workflow row
 */
export const validateWorkflowRow = (row, solutionType, schema = {}) => {
  const errors = [];
  const availableTables = Object.keys(schema || {});
  const columnsForTable = row.tabla ? (schema[row.tabla] || []) : [];
  const requiresTable = row.tipo && row.tipo !== ACTION_TYPES.VALIDACION;

  const requireColumn = (columnValue, message) => {
    if (!columnValue) {
      errors.push(message);
      return;
    }

    if (requiresTable && columnsForTable.length > 0 && !columnsForTable.includes(columnValue)) {
      errors.push(`${message} (${columnValue}) no pertenece a la tabla seleccionada`);
    }
  };

  if (!row.tipo) {
    errors.push('Debes elegir el tipo de paso');
  } else if (!isActionTypeAllowed(row.tipo, solutionType)) {
    errors.push(`El tipo ${row.tipo} no está disponible en este modo`);
  }

  if (requiresTable && !row.tabla) {
    errors.push('Debes seleccionar una tabla');
  }

  if (requiresTable && row.tabla && availableTables.length > 0 && !availableTables.includes(row.tabla)) {
    errors.push(`La tabla ${row.tabla} no está disponible`);
  }

  if (!row.descripcion?.trim()) {
    errors.push('Debes escribir una descripción');
  }

  // Validate config based on type
  if (row.tipo) {
    const configErrors = validateConfigForActionType(
      row.tipo,
      row.config,
      solutionType,
      { requireColumn, tableName: row.tabla, columnsForTable, schema }
    );
    errors.push(...configErrors);
  }

  return errors;
};

/**
 * Validate complete workflow
 */
export const validateWorkflow = (rows, solutionType, schema = {}) => {
  const errors = [];
  const rowErrors = {};

  // Filter out empty rows
  const nonEmptyRows = rows.filter(row => row.tipo || row.tabla || row.descripcion?.trim());

  if (nonEmptyRows.length === 0) {
    return { isValid: false, errors: ['Al menos un paso es requerido'], rowErrors: { 0: ['Al menos un paso es requerido'] } };
  }

  // Validate each row
  nonEmptyRows.forEach((row, index) => {
    const currentRowErrors = validateWorkflowRow(row, solutionType, schema);
    if (currentRowErrors.length > 0) {
      rowErrors[index] = currentRowErrors;
      errors.push(`Paso ${index + 1}: ${currentRowErrors.join(', ')}`);
    }
  });

  if (solutionType === SOLUTION_TYPES.SCRIPT) {
    const validacionSteps = nonEmptyRows.filter(row => row.tipo === ACTION_TYPES.VALIDACION);
    if (validacionSteps.length === 0) {
      console.warn('Advertencia: Modo script sin pasos de validación');
    }
  }

  return {
    isValid: errors.length === 0,
    errors,
    rowErrors
  };
};

/**
 * Validate config based on action type
 */
const validateConfigForActionType = (actionType, config, solutionType, context = {}) => {
  const errors = [];
  const requireColumn = context.requireColumn || (() => {});
  const tableName = context.tableName;
  const columnsForTable = Array.isArray(context.columnsForTable) ? context.columnsForTable : [];
  const schema = context.schema || {};

  if (!config) return ['Configuración no válida'];

  switch (actionType) {
    case ACTION_TYPES.SELECT:
      const selectMode = getSelectMode(config);
      const conditionPairs = getSelectConditionPairs(config);
      const selectedColumns = getSelectedColumns(config, tableName);
      const aggregates = Array.isArray(config.aggregates) ? config.aggregates : [];

      if (selectMode === SELECT_MODES.ANALITICO) {
        if (aggregates.length === 0) {
          errors.push('Debes agregar al menos una agregación en modo analítico');
        }

        aggregates.forEach((agg, index) => {
          if (!agg?.func) {
            errors.push(`Falta función de agregación (${index + 1})`);
          }
          if (!agg?.column) {
            errors.push(`Falta columna de agregación (${index + 1})`);
          }
        });

        break;
      }

      if (conditionPairs.length === 0) {
        errors.push('Agrega al menos una condición');
      }
      if (selectedColumns.length === 0) {
        errors.push('Debes seleccionar al menos una columna para mostrar');
      }

      conditionPairs.forEach((pair, index) => {
        if (!pair.tabla && tableName) {
          errors.push(`Debes seleccionar una tabla (condición ${index + 1})`);
        }

        if (!pair.columna) {
          errors.push(`Falta columna (condición ${index + 1})`);
        } else if (!pair.tabla) {
          // Only validate against base-table columns when no explicit tabla given
          requireColumn(pair.columna, `Falta columna (condición ${index + 1})`);
        }

        if (pair.compare_type === 'column') {
          if (!pair.right_columna) {
            errors.push(`Falta columna relacionada (condición ${index + 1})`);
          }
        } else if (pair.valor === '' || pair.valor === undefined) {
          errors.push(`Falta valor (condición ${index + 1})`);
        }
      });

      const queryMode = config.query_mode === 'compuesta' ? 'compuesta' : 'normal';
      if (queryMode === 'compuesta') {
        const joinPairs = getJoinPairs(config);
        if (joinPairs.length === 0) {
          errors.push('Si usas JOIN, debes conectar las tablas');
        }

        joinPairs.forEach((pair, index) => {
        const joinColumns = Array.isArray(schema[pair.tabla]) ? schema[pair.tabla] : [];

        if (!pair.tabla) {
          errors.push('Si usas JOIN, debes conectar las tablas');
        } else if (Object.keys(schema).length > 0 && !schema[pair.tabla]) {
          errors.push(`La tabla de la relación ${index + 1} no es válida`);
        }

        if (!pair.base_columna) {
          errors.push('Si usas JOIN, debes conectar las tablas');
        } else if (columnsForTable.length > 0 && !columnsForTable.includes(pair.base_columna)) {
          errors.push(`La columna principal de la relación ${index + 1} no es válida`);
        }

        if (!pair.join_columna) {
          errors.push('Si usas JOIN, debes conectar las tablas');
        } else if (joinColumns.length > 0 && !joinColumns.includes(pair.join_columna)) {
          errors.push(`La columna relacionada de la relación ${index + 1} no es válida`);
        }

        if (!tableName) {
          errors.push('Primero selecciona la tabla');
        }
        });
      }

      if (solutionType === SOLUTION_TYPES.SCRIPT && !config.guardar_en) {
        errors.push('Debes indicar dónde guardar el resultado');
      }
      break;

    case ACTION_TYPES.UPDATE:
      requireColumn(config.columna_actualizar, 'Debes elegir la columna a actualizar');
      if (!config.valor_nuevo) errors.push('Debes escribir el nuevo valor');
      if (config.query_mode === 'compuesta') {
        const joinPairs = getJoinPairs(config);
        if (joinPairs.length === 0) {
          errors.push('Si usas JOIN, debes conectar las tablas');
        }

        const conditionPairs = getSelectConditionPairs(config);
        if (conditionPairs.length === 0) {
          errors.push('Agrega al menos una condición');
        }

        conditionPairs.forEach((pair, index) => {
          if (!pair.columna) errors.push(`Falta columna (condición ${index + 1})`);
          if (pair.compare_type === 'column') {
            if (!pair.right_columna) errors.push(`Falta columna relacionada (condición ${index + 1})`);
          } else if (pair.valor === '' || pair.valor === undefined) {
            errors.push(`Falta valor (condición ${index + 1})`);
          }
        });
      } else {
        requireColumn(config.columna_condicion, 'Debes elegir la columna para buscar el registro');
        if (!config.valor_condicion) errors.push('Debes escribir el valor para encontrar el registro');
      }
      break;

    case ACTION_TYPES.INSERT:
      if (getInsertPairs(config).length === 0) {
        errors.push('Debes agregar al menos una columna con su valor');
      }
      getInsertPairs(config).forEach((pair, index) => {
        requireColumn(pair.columna, `Debes elegir la columna ${index + 1}`);
        if (pair.valor === undefined || pair.valor === null || String(pair.valor).trim() === '') {
          errors.push(`Debes escribir el valor ${index + 1}`);
        }
      });
      break;

    case ACTION_TYPES.DELETE:
      if (config.query_mode === 'compuesta') {
        const joinPairs = getJoinPairs(config);
        if (joinPairs.length === 0) {
          errors.push('Si usas JOIN, debes conectar las tablas');
        }

        const conditionPairs = getSelectConditionPairs(config);
        if (conditionPairs.length === 0) {
          errors.push('Agrega al menos una condición');
        }

        conditionPairs.forEach((pair, index) => {
          if (!pair.columna) errors.push(`Falta columna (condición ${index + 1})`);
          if (pair.compare_type === 'column') {
            if (!pair.right_columna) errors.push(`Falta columna relacionada (condición ${index + 1})`);
          } else if (pair.valor === '' || pair.valor === undefined) {
            errors.push(`Falta valor (condición ${index + 1})`);
          }
        });
      } else {
        requireColumn(config.columna_condicion, 'Debes elegir la columna para buscar el registro');
        if (!config.valor_condicion) errors.push('Debes escribir el valor para encontrar el registro');
      }
      break;

    case ACTION_TYPES.VALIDACION:
      if (!config.variable) errors.push('Debes indicar la variable a validar');
      if (!config.condicion) errors.push('Debes elegir la condición');
      if (!config.mensaje_error) errors.push('Debes escribir el mensaje que verá el usuario');
      break;
  }

  return errors;
};

// ============================================================
// ROW ADAPTATION FUNCTIONS
// ============================================================

/**
 * Sanitize a workflow row for a specific solution type
 */
export const sanitizeWorkflowRowForMode = (row, solutionType) => {
  if (!row.tipo) return row;

  const sanitized = { ...row };

  // Remove if type not allowed
  if (!isActionTypeAllowed(row.tipo, solutionType)) {
    sanitized.tipo = '';
  }

  // Remove validacion config in database mode
  if (
    solutionType === SOLUTION_TYPES.DATABASE &&
    row.tipo === ACTION_TYPES.VALIDACION
  ) {
    return createEmptyWorkflowRow(row.orden);
  }

  // Sanitize config for database mode
  if (solutionType === SOLUTION_TYPES.DATABASE && sanitized.config) {
    const config = { ...sanitized.config };
    // Remove enterprise-only fields
    delete config.guardar_en;
    delete config.validacion;
    sanitized.config = config;
  }

  return sanitized;
};

/**
 * Adapt all rows when solution type changes
 */
export const adaptRowsForSolutionType = (rows, solutionType) => {
  if (!Array.isArray(rows)) return [];

  const adapted = rows
    .map(row => sanitizeWorkflowRowForMode(row, solutionType))
    .filter(row => row.tipo || row.tabla);

  // Maintain at least one empty row for UX
  if (adapted.length === 0) {
    return [createEmptyWorkflowRow(1)];
  }

  return adapted.map((row, idx) => ({
    ...row,
    orden: idx + 1
  }));
};

// ============================================================
// JSON GENERATION FUNCTIONS
// ============================================================

const buildStepPayload = (row, solutionType) => {
  const stepBase = {
    tipo: row.tipo,
    descripcion: row.descripcion?.trim() || '',
    ...(row.tipo !== ACTION_TYPES.VALIDACION ? { tabla: row.tabla } : {}),
  };

  if (row.tipo === ACTION_TYPES.SELECT) {
    const selectMode = getSelectMode(row.config);
    const queryMode = selectMode === SELECT_MODES.ANALITICO
      ? 'analitico'
      : selectMode === SELECT_MODES.RELACIONAL
        ? 'compuesta'
        : 'normal';

    const relationalJoin = queryMode === 'compuesta' || queryMode === 'analitico'
      ? buildRelationalJoinFromPairs(getJoinPairs(row.config), row.tabla)
      : [];

    const filteredConditionPairs = getSelectConditionPairs(row.config)
      .filter((pair) => (
        pair.columna
        && (
          pair.compare_type === 'column'
            ? Boolean(pair.right_columna)
            : (pair.valor !== '' && pair.valor !== undefined)
        )
      ));

    const analyticWhereObject = filteredConditionPairs.reduce((acc, pair) => {
      const key = buildWhereKey(pair, row.tabla, relationalJoin.length > 0);
      acc[key] = buildWhereValue(pair);
      return acc;
    }, {});

    if (selectMode === SELECT_MODES.ANALITICO) {
      const groupBy = Array.isArray(row.config.group_by)
        ? row.config.group_by.map((entry) => String(entry || '').trim()).filter(Boolean)
        : [];
      const aggregates = Array.isArray(row.config.aggregates)
        ? row.config.aggregates
            .map((agg) => ({
              func: String(agg?.func || 'COUNT').trim(),
              column: String(agg?.column || '*').trim() || '*',
              alias: String(agg?.alias || '').trim(),
            }))
            .filter((agg) => agg.func && agg.column)
        : [];
      const orderBy = Array.isArray(row.config.order_by)
        ? row.config.order_by
            .map((entry) => ({
              column: String(entry?.column || '').trim(),
              direction: String(entry?.direction || 'ASC').toUpperCase() === 'DESC' ? 'DESC' : 'ASC',
            }))
            .filter((entry) => entry.column)
        : [];
      const having = Array.isArray(row.config.having)
        ? row.config.having
            .map((entry) => ({
              alias: String(entry?.alias || '').trim(),
              op: String(entry?.op || '=').trim() || '=',
              value: parseScriptValue(entry?.value),
            }))
            .filter((entry) => entry.alias)
        : [];

      return {
        ...stepBase,
        select_mode: SELECT_MODES.ANALITICO,
        query_mode: 'analitico',
        ...(groupBy.length > 0 ? { group_by: groupBy } : {}),
        aggregates,
        ...(orderBy.length > 0 ? { order_by: orderBy } : {}),
        ...(having.length > 0 ? { having } : {}),
        ...(Object.keys(analyticWhereObject).length > 0 ? { where: analyticWhereObject } : {}),
        ...(relationalJoin.length > 0 ? { join: relationalJoin } : {}),
        ...(solutionType === SOLUTION_TYPES.SCRIPT && row.config.guardar_en ? { guardar_en: row.config.guardar_en.trim() } : {}),
      };
    }

    const conditionPairs = getSelectConditionPairs(row.config)
      .filter((pair) => (
        pair.columna
        && (
          pair.compare_type === 'column'
            ? Boolean(pair.right_columna)
            : (pair.valor !== '' && pair.valor !== undefined)
        )
      ));

    const isJoin = queryMode === 'compuesta' && relationalJoin.length > 0;
    const selectedColumns = getSelectedColumns(row.config, row.tabla);

    const selectedByTable = selectedColumns.reduce((acc, fieldRef) => {
      const [maybeTable, maybeColumn] = String(fieldRef).split('.');
      const table = maybeColumn ? maybeTable : row.tabla;
      const column = maybeColumn || maybeTable;
      if (!table || !column) return acc;
      if (!acc[table]) acc[table] = [];
      if (!acc[table].includes(column)) acc[table].push(column);
      return acc;
    }, {});

    // Build where object: JOIN queries use "tabla.columna" keys; plain queries use bare column names
    const whereObject = conditionPairs.reduce((acc, pair) => {
      const key = buildWhereKey(pair, row.tabla, isJoin);
      acc[key] = buildWhereValue(pair);
      return acc;
    }, {});

    // Include logic (AND/OR) only when meaningful (>1 condition, or OR explicitly set)
    const logic = row.config?.where_logic || 'AND';
    const includeLogic = conditionPairs.length > 1 && logic === 'OR';

    return {
      ...stepBase,
      ...(Array.isArray(selectedByTable[row.tabla]) && selectedByTable[row.tabla].length > 0
        ? { columnas: selectedByTable[row.tabla] }
        : {}),
      select_mode: selectMode,
      where: whereObject,
      ...(includeLogic ? { logic } : {}),
      ...(queryMode === 'compuesta' ? { query_mode: 'compuesta' } : { query_mode: 'normal' }),
      ...(relationalJoin.length > 0
        ? {
            join: relationalJoin.map((joinItem) => ({
              ...joinItem,
              ...(Array.isArray(selectedByTable[joinItem.tabla]) && selectedByTable[joinItem.tabla].length > 0
                ? { columnas: selectedByTable[joinItem.tabla] }
                : {}),
            })),
          }
        : {}),
      ...(solutionType === SOLUTION_TYPES.SCRIPT && row.config.guardar_en ? { guardar_en: row.config.guardar_en.trim() } : {}),
    };
  }

  if (row.tipo === ACTION_TYPES.UPDATE) {
    const queryMode = row?.config?.query_mode === 'compuesta' ? 'compuesta' : 'normal';
    const relationalJoin = queryMode === 'compuesta'
      ? buildRelationalJoinFromPairs(getJoinPairs(row.config), row.tabla)
      : [];

    const conditionPairs = queryMode === 'compuesta'
      ? getSelectConditionPairs(row.config)
          .filter((pair) => (
            pair.columna
            && (
              pair.compare_type === 'column'
                ? Boolean(pair.right_columna)
                : (pair.valor !== '' && pair.valor !== undefined)
            )
          ))
      : [];

    const whereObject = queryMode === 'compuesta'
      ? conditionPairs.reduce((acc, pair) => {
          const key = buildWhereKey(pair, row.tabla, true);
          acc[key] = buildWhereValue(pair);
          return acc;
        }, {})
      : {
          [row.config.columna_condicion]: parseScriptValue(row.config.valor_condicion),
        };

    const logic = row.config?.where_logic || 'AND';
    const includeLogic = queryMode === 'compuesta' && conditionPairs.length > 1 && logic === 'OR';

    return {
      ...stepBase,
      set: {
        [row.config.columna_actualizar]: parseScriptValue(row.config.valor_nuevo),
      },
      where: whereObject,
      ...(includeLogic ? { logic } : {}),
      ...(queryMode === 'compuesta' ? { query_mode: 'compuesta' } : { query_mode: 'normal' }),
      ...(relationalJoin.length > 0 ? { join: relationalJoin } : {}),
      ...(solutionType === SOLUTION_TYPES.SCRIPT && row.config.guardar_en ? { guardar_en: row.config.guardar_en.trim() } : {}),
    };
  }

  if (row.tipo === ACTION_TYPES.DELETE) {
    const queryMode = row?.config?.query_mode === 'compuesta' ? 'compuesta' : 'normal';
    const relationalJoin = queryMode === 'compuesta'
      ? buildRelationalJoinFromPairs(getJoinPairs(row.config), row.tabla)
      : [];

    const conditionPairs = queryMode === 'compuesta'
      ? getSelectConditionPairs(row.config)
          .filter((pair) => (
            pair.columna
            && (
              pair.compare_type === 'column'
                ? Boolean(pair.right_columna)
                : (pair.valor !== '' && pair.valor !== undefined)
            )
          ))
      : [];

    const whereObject = queryMode === 'compuesta'
      ? conditionPairs.reduce((acc, pair) => {
          const key = buildWhereKey(pair, row.tabla, true);
          acc[key] = buildWhereValue(pair);
          return acc;
        }, {})
      : {
          [row.config.columna_condicion]: parseScriptValue(row.config.valor_condicion),
        };

    const logic = row.config?.where_logic || 'AND';
    const includeLogic = queryMode === 'compuesta' && conditionPairs.length > 1 && logic === 'OR';

    return {
      ...stepBase,
      where: whereObject,
      ...(includeLogic ? { logic } : {}),
      ...(queryMode === 'compuesta' ? { query_mode: 'compuesta' } : { query_mode: 'normal' }),
      ...(relationalJoin.length > 0 ? { join: relationalJoin } : {}),
      ...(solutionType === SOLUTION_TYPES.SCRIPT && row.config.guardar_en ? { guardar_en: row.config.guardar_en.trim() } : {}),
    };
  }

  if (row.tipo === ACTION_TYPES.INSERT) {
    const data = {};
    getInsertPairs(row.config).forEach((pair) => {
      if (pair.columna) {
        data[pair.columna] = parseScriptValue(pair.valor);
      }
    });

    return {
      ...stepBase,
      data,
      ...(solutionType === SOLUTION_TYPES.SCRIPT && row.config.guardar_en ? { guardar_en: row.config.guardar_en.trim() } : {}),
    };
  }

  return {
    tipo: ACTION_TYPES.VALIDACION,
    descripcion: row.descripcion?.trim() || '',
    variable: row.config.variable?.trim() || '',
    condicion: row.config.condicion || 'existe',
    ...(row.config.valor ? { valor: parseScriptValue(row.config.valor) } : {}),
    mensaje_error: row.config.mensaje_error?.trim() || '',
  };
};

export const generateScriptJson = (steps, mode) => {
  const nonEmptyRows = (steps || []).filter(
    (row) => row?.tipo && (row.tipo === ACTION_TYPES.VALIDACION || row.tabla) && row?.descripcion?.trim()
  );

  if (nonEmptyRows.length === 0) return null;

  const workflow = nonEmptyRows.map((row) => buildStepPayload(row, mode));

  if (mode === SOLUTION_TYPES.DATABASE) {
    return workflow.length === 1 ? workflow[0] : { workflow };
  }

  return {
    workflow,
  };
};

/**
 * Backward-compatible alias used by existing UI.
 */
export const buildWorkflowJSON = (rows, solutionType) => {
  return generateScriptJson(rows, solutionType);
};

/**
 * Parse workflow JSON back to rows (for editing)
 */
export const parseWorkflowJSONToRows = (scriptJson) => {
  if (!scriptJson) return [];

  try {
    const data = typeof scriptJson === 'string' ? JSON.parse(scriptJson) : scriptJson;

    // Handle single step object
    if (data.tipo && !data.workflow) {
      return [
        {
          orden: 1,
          descripcion: data.descripcion || '',
          tipo: data.tipo,
          tabla: data.tabla || '',
          config: data
        }
      ];
    }

    // Handle workflow array
    if (Array.isArray(data.workflow)) {
      return data.workflow.map((step, index) => {
        const joinPairs = Array.isArray(step.join)
          ? step.join
              .flatMap((joinItem) => {
                const onObject = joinItem?.on && typeof joinItem.on === 'object' ? joinItem.on : {};
                return Object.entries(onObject).map(([leftRef, rightRef]) => {
                  const leftParts = String(leftRef || '').split('.');
                  const rightParts = String(rightRef || '').split('.');

                  return {
                    tabla: String(joinItem?.tabla || '').trim().toLowerCase(),
                    base_columna: leftParts.length > 1 ? String(leftParts[1] || '').trim() : String(leftParts[0] || '').trim(),
                    join_columna: rightParts.length > 1 ? String(rightParts[1] || '').trim() : String(rightParts[0] || '').trim(),
                  };
                });
              })
              .filter((pair) => pair.tabla && pair.base_columna && pair.join_columna)
          : [];

        const wherePairs = step?.where && typeof step.where === 'object' && !Array.isArray(step.where)
          ? Object.entries(step.where).map(([key, valor]) => {
              // Handle "tabla.columna" format keys
              const dotIndex = key.indexOf('.');
              const tabla = dotIndex > -1 ? key.slice(0, dotIndex) : '';
              const columna = dotIndex > -1 ? key.slice(dotIndex + 1) : key;

              // Handle extended operator format { op, value }
              const isExtended = valor && typeof valor === 'object' && !Array.isArray(valor) && valor.op;
              const rhsField = isExtended && typeof valor.field === 'string' ? String(valor.field) : '';
              const rhsDot = rhsField.indexOf('.');
              const rhsTable = rhsDot > -1 ? rhsField.slice(0, rhsDot) : '';
              const rhsColumn = rhsDot > -1 ? rhsField.slice(rhsDot + 1) : rhsField;

              return {
                tabla,
                columna: String(columna || '').trim(),
                operador: isExtended ? String(valor.op) : '=',
                valor: String(isExtended ? (valor.value ?? '') : (valor ?? '')),
                compare_type: rhsField ? 'column' : 'value',
                right_tabla: rhsTable,
                right_columna: rhsColumn,
              };
            })
          : [];

        const selectedColumns = [];
        if (Array.isArray(step?.columnas)) {
          step.columnas.forEach((column) => {
            const col = String(column || '').trim();
            if (col) selectedColumns.push(`${step.tabla}.${col}`);
          });
        }
        if (Array.isArray(step?.join)) {
          step.join.forEach((joinItem) => {
            const joinTable = String(joinItem?.tabla || '').trim();
            if (!joinTable || !Array.isArray(joinItem?.columnas)) return;
            joinItem.columnas.forEach((column) => {
              const col = String(column || '').trim();
              if (col) selectedColumns.push(`${joinTable}.${col}`);
            });
          });
        }

        const whereLogic = step.logic || 'AND';
        const selectMode = step.query_mode === 'analitico'
          ? SELECT_MODES.ANALITICO
          : joinPairs.length > 0
            ? SELECT_MODES.RELACIONAL
            : SELECT_MODES.SIMPLE;

        return {

          orden: index + 1,
          descripcion: step.descripcion || '',
          tipo: step.tipo,
          tabla: step.tabla || '',
          config: {
            ...step,
            ...(step.tipo === ACTION_TYPES.SELECT ? { select_mode: selectMode } : {}),
            ...(wherePairs.length > 0 ? { where_pairs: wherePairs } : {}),
            ...(wherePairs.length > 0 ? { where_logic: whereLogic } : {}),
            ...(wherePairs.length > 0 ? { columna: wherePairs[0].columna } : {}),
            ...(wherePairs.length > 0 ? { valor: wherePairs[0].valor } : {}),
            ...(selectedColumns.length > 0 ? { selected_columns: selectedColumns } : {}),
            ...(step.query_mode === 'analitico'
              ? { query_mode: 'analitico' }
              : (joinPairs.length > 0 ? { query_mode: 'compuesta' } : { query_mode: 'normal' })),
            ...(joinPairs.length > 0 ? { join_pairs: joinPairs } : {}),
          },
        };
      });
    }

    return [];
  } catch (error) {
    console.error('Error parsing workflow JSON:', error);
    return [];
  }
};

// ============================================================
// EXPORT HELPER FUNCTION
// ============================================================

export const prepareWorkflowForSubmit = (rows, solutionType) => {
  const validation = validateWorkflow(rows, solutionType);

  if (!validation.isValid) {
    return {
      success: false,
      errors: validation.errors,
      scriptJson: null
    };
  }

  const scriptJson = generateScriptJson(rows, solutionType);

  if (!scriptJson) {
    return {
      success: false,
      errors: ['No hay pasos válidos para generar workflow'],
      scriptJson: null
    };
  }

  return {
    success: true,
    errors: [],
    scriptJson
  };
};
