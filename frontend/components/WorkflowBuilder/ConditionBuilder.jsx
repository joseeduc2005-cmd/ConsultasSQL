'use client';

import React, { useEffect, useMemo, useRef, useState } from 'react';

const EMPTY_CONDITION = {
  tabla: '',
  columna: '',
  operador: '=',
  valor: '',
  compare_type: 'value',
  right_tabla: '',
  right_columna: '',
};

const OPERATORS = [
  { value: '=',    label: '= igual' },
  { value: '!=',   label: '≠ distinto' },
  { value: '>',    label: '> mayor' },
  { value: '<',    label: '< menor' },
  { value: '>=',   label: '≥ mayor o igual' },
  { value: '<=',   label: '≤ menor o igual' },
  { value: 'LIKE', label: '≈ contiene (LIKE)' },
];

const LOGIC_OPTIONS = ['AND', 'OR'];

/**
 * Multi-table, multi-operator condition builder.
 *
 * Props:
 *   conditions      — array of { tabla, columna, operador, valor }
 *   baseTable       — name of the main table (always available)
 *   joinedTables    — array of table names already added via JOIN
 *   getColumnsForTable — (tableName) => string[]  (from parent schema)
 *   logic           — 'AND' | 'OR'
 *   onLogicChange   — (value) => void
 *   disabled        — boolean
 *   onChange        — (conditions) => void
 */
export default function ConditionBuilder({
  conditions = [],
  baseTable = '',
  joinedTables = [],
  relationColumnsByTable = {},
  selectedColumnsByTable = {},
  getColumnsForTable = () => [],
  logic = 'AND',
  onLogicChange = () => {},
  disabled = false,
  onChange = () => {},
}) {
  const normalizedLogic = String(logic || 'AND').toUpperCase() === 'OR' ? 'OR' : 'AND';

  const normalizedIncomingConditions = useMemo(() => {
    return Array.isArray(conditions) && conditions.length > 0
      ? conditions
      : [{ ...EMPTY_CONDITION, tabla: baseTable }];
  }, [conditions, baseTable]);

  const [draftConditions, setDraftConditions] = useState(normalizedIncomingConditions);
  // Tracks the conditions we last pushed so we can skip the parent bounce-back.
  const lastCommittedRef = useRef(null);

  useEffect(() => {
    // Only reset draft when the parent passes genuinely new external data.
    // Skips if the incoming conditions are the same reference we just pushed via onChange.
    if (conditions === lastCommittedRef.current) return;
    setDraftConditions(normalizedIncomingConditions);
  }, [normalizedIncomingConditions, conditions]);

  const safeConditions = draftConditions;

  // Active tables are already selected in previous steps.
  const availableTables = Array.from(new Set([baseTable, ...joinedTables].filter(Boolean)));
  const activeTables = availableTables.length > 0 ? availableTables : (baseTable ? [baseTable] : []);

  const commitConditions = (nextConditions) => {
    lastCommittedRef.current = nextConditions; // Track so sync effect skips the bounce-back
    setDraftConditions(nextConditions);
    onChange(nextConditions);
  };

  const getAllowedColumnsForTable = (tableName) => {
    const presetColumns = Array.isArray(relationColumnsByTable?.[tableName])
      ? relationColumnsByTable[tableName].filter(Boolean)
      : [];

    if (presetColumns.length > 0) {
      return Array.from(new Set(presetColumns));
    }

    const selectedColumns = Array.isArray(selectedColumnsByTable?.[tableName])
      ? selectedColumnsByTable[tableName].filter(Boolean)
      : [];

    if (selectedColumns.length > 0) {
      return Array.from(new Set(selectedColumns));
    }

    return getColumnsForTable(tableName);
  };

  const getUsedColumnsForTable = (tableName, ignoreIndex = -1) => (
    safeConditions
      .map((condition, index) => ({ condition, index }))
      .filter(({ condition, index }) => (condition.tabla || baseTable) === tableName && index !== ignoreIndex)
      .map(({ condition }) => condition.columna)
      .filter(Boolean)
  );

  const getSuggestedColumnForTable = (tableName, ignoreIndex = -1) => {
    const allowedColumns = getAllowedColumnsForTable(tableName);
    if (allowedColumns.length === 0) return '';

    const usedColumns = new Set(getUsedColumnsForTable(tableName, ignoreIndex));
    const nextAvailable = allowedColumns.find((columnName) => !usedColumns.has(columnName));
    return nextAvailable || allowedColumns[0] || '';
  };

  // Auto-assign columna when column availability changes (e.g. schema loads or step-4 selection changes).
  // Uses a functional setter so safeConditions is NOT a dependency (avoids a re-render loop).
  useEffect(() => {
    setDraftConditions((prev) => {
      let anyChanged = false;
      const seenByTable = new Map();
      const normalized = prev.map((condition) => {
        const tableName = condition.tabla || baseTable;
        const allowed = getAllowedColumnsForTable(tableName);
        if (allowed.length === 0) return condition;

        const seenCount = Number(seenByTable.get(tableName) || 0);
        const preferredColumn = allowed[Math.min(seenCount, allowed.length - 1)] || allowed[0];
        seenByTable.set(tableName, seenCount + 1);

        const hasUserInput = (condition.compare_type || 'value') === 'column'
          ? Boolean(condition.right_tabla || condition.right_columna)
          : String(condition.valor ?? '').trim().length > 0;

        // If the condition already has user input, preserve its current column.
        // Otherwise, keep it aligned with the mapping order from step 4.
        if (hasUserInput) return condition;

        if (condition.columna === preferredColumn) return condition;

        anyChanged = true;
        return { ...condition, tabla: tableName, columna: preferredColumn };
      });
      if (!anyChanged) return prev; // Return same reference to avoid triggering downstream effects
      // Notify parent without triggering the sync re-reset
      lastCommittedRef.current = normalized;
      onChange(normalized);
      return normalized;
    });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [baseTable, selectedColumnsByTable, relationColumnsByTable]);

  const updateCondition = (index, field, value) => {
    const next = [...safeConditions];
    const updated = { ...next[index], [field]: value };
    // Reset column when table changes
    if (field === 'tabla') {
      updated.columna = getSuggestedColumnForTable(value, index);
    }
    if (field === 'right_tabla') {
      updated.right_columna = '';
    }
    if (field === 'compare_type') {
      if (value === 'value') {
        updated.right_tabla = '';
        updated.right_columna = '';
      } else {
        updated.valor = '';
        updated.right_tabla = updated.right_tabla || baseTable;
      }
    }
    next[index] = updated;
    commitConditions(next);
  };

  const addCondition = (tableName = baseTable) => {
    const normalizedTable = tableName || baseTable;
    commitConditions([
      ...safeConditions,
      {
        ...EMPTY_CONDITION,
        tabla: normalizedTable,
        columna: getSuggestedColumnForTable(normalizedTable),
      },
    ]);
  };

  const removeCondition = (index) => {
    const next = safeConditions.filter((_, i) => i !== index);
    commitConditions(next.length > 0 ? next : [{ ...EMPTY_CONDITION, tabla: baseTable }]);
  };

  const getValidationError = (condition) => {
    if (!condition.tabla) return 'Selecciona una tabla';
    if (!condition.columna) return 'Selecciona columnas en el paso 4';
    if ((condition.compare_type || 'value') === 'column') {
      if (!condition.right_tabla) return 'Selecciona la tabla relacionada';
      if (!condition.right_columna) return 'Selecciona la columna relacionada';
      return null;
    }
    if (condition.valor === '' || condition.valor === undefined) return 'Ingresa un valor';
    return null;
  };

  const getTableConditions = (tableName) => (
    safeConditions
      .map((condition, absoluteIndex) => ({ condition, absoluteIndex }))
      .filter(({ condition }) => (condition.tabla || baseTable) === tableName)
  );

  return (
    <div className="space-y-3">
      <div className="rounded-xl border border-slate-200 bg-white p-4">
        <div className="mb-4 flex flex-wrap items-center gap-3">
          <span className="text-sm font-bold uppercase tracking-wide text-slate-700">Matriz de datos</span>
          <div className={`inline-flex rounded-xl border border-slate-300 bg-slate-100 p-1 ${disabled ? 'opacity-50' : ''}`}>
            {LOGIC_OPTIONS.map((opt) => (
              <button
                key={opt}
                type="button"
                onClick={() => onLogicChange(opt)}
                disabled={disabled}
                aria-pressed={normalizedLogic === opt}
                style={normalizedLogic === opt ? { backgroundColor: '#1d4ed8', color: '#ffffff', borderColor: '#1d4ed8' } : { backgroundColor: 'transparent' }}
                className={`rounded-lg px-4 py-1.5 text-sm font-semibold border ${
                  normalizedLogic === opt
                    ? 'shadow-sm'
                    : 'border-transparent text-slate-700 hover:bg-white'
                }`}
              >
                {opt}
              </button>
            ))}
          </div>
        </div>

        <div
          className="grid gap-3"
          style={{ gridTemplateColumns: `repeat(${Math.max(activeTables.length, 1)}, minmax(0, 1fr))` }}
        >
          {activeTables.map((tableName, tableIndex) => {
            const tableConditions = getTableConditions(tableName);
            const detectedColumns = getAllowedColumnsForTable(tableName);
            const badgeClass = tableIndex === 0
              ? 'bg-blue-600 text-white'
              : 'bg-slate-700 text-white';
            const panelClass = tableIndex === 0
              ? 'border-blue-200 bg-white'
              : 'border-slate-300 bg-white';
            const fieldBorderClass = tableIndex === 0
              ? 'border-slate-300 focus:border-blue-400 focus:ring-blue-300'
              : 'border-slate-300 focus:border-blue-400 focus:ring-blue-300';

            return (
              <div key={tableName} className={`overflow-hidden rounded-lg border ${panelClass}`}>
                <div className="flex items-center justify-between gap-2 border-b border-slate-200 bg-slate-50 px-3 py-2">
                  <div>
                    <p className={`inline-flex rounded px-2 py-1 text-xs font-bold uppercase tracking-wide ${badgeClass}`}>
                      {tableIndex === 0 ? 'Tabla A' : `Tabla ${String.fromCharCode(66 + tableIndex - 1)}`}
                    </p>
                    <p className="mt-1 font-mono text-sm font-semibold text-slate-900">{tableName}</p>
                    <p className="mt-1 text-xs text-slate-500">
                      {detectedColumns.length > 0
                        ? `Columnas detectadas: ${detectedColumns.join(', ')}`
                        : 'Sin columnas seleccionadas todavía'}
                    </p>
                  </div>
                  <button
                    type="button"
                    onClick={() => addCondition(tableName)}
                    className="rounded-md border border-slate-300 bg-white px-2.5 py-1.5 text-xs font-semibold text-slate-700 hover:bg-slate-100 disabled:opacity-40"
                    disabled={disabled}
                  >
                    + Dato
                  </button>
                </div>

                <div className="p-3">
                  {tableConditions.length > 0 ? tableConditions.map(({ condition, absoluteIndex }) => {
                    const tableColumns = getAllowedColumnsForTable(tableName);
                    const validationError = getValidationError(condition);
                    const chosenColumn = condition.columna || tableColumns[0] || '';
                    const hasPresetColumns = Array.isArray(relationColumnsByTable?.[tableName]) && relationColumnsByTable[tableName].length > 0;
                    const isColumnLocked = hasPresetColumns && tableColumns.length === 1;

                    return (
                      <div key={`${tableName}-${absoluteIndex}`} className="mb-3 overflow-hidden rounded-md border border-slate-300 bg-white last:mb-0">
                        <div className="grid grid-cols-[120px_1fr] border-b border-slate-200">
                          <div className="bg-slate-50 px-3 py-2 text-sm font-semibold text-slate-700">Tabla</div>
                          <div className="px-3 py-2 text-sm font-medium text-slate-900">{tableName}</div>
                        </div>

                        <div className="grid grid-cols-[120px_1fr] border-b border-slate-200">
                          <div className="bg-slate-50 px-3 py-2 text-sm font-semibold text-slate-700">Columna</div>
                          <div className="px-2 py-2">
                            <div className="flex min-h-10 items-center rounded-md border border-slate-200 bg-slate-50 px-3 text-sm font-semibold text-slate-900">
                              {chosenColumn || 'Selecciona columna en el paso 4'}
                            </div>
                          </div>
                        </div>

                        <div className="grid grid-cols-[120px_1fr] border-b border-slate-200">
                          <div className="bg-slate-50 px-3 py-2 text-sm font-semibold text-slate-700">Operador</div>
                          <div className="px-2 py-2">
                            <select
                              value={condition.operador || '='}
                              onChange={(e) => updateCondition(absoluteIndex, 'operador', e.target.value)}
                              className={`w-full rounded-md border bg-white px-3 py-2 text-sm focus:outline-none focus:ring-1 ${fieldBorderClass}`}
                              disabled={disabled}
                              title="Operador"
                            >
                              {OPERATORS.map(({ value, label }) => (
                                <option key={value} value={value}>{label}</option>
                              ))}
                            </select>
                          </div>
                        </div>

                        <div className="grid grid-cols-[120px_1fr] border-b border-slate-200">
                          <div className="bg-slate-50 px-3 py-2 text-sm font-semibold text-slate-700">Valor</div>
                          <div className="px-2 py-2">
                            <input
                              type="text"
                              value={condition.valor ?? ''}
                              onChange={(e) => updateCondition(absoluteIndex, 'valor', e.target.value)}
                              placeholder="Dato que necesitas..."
                              className={`w-full rounded-md border bg-white px-3 py-2 text-sm focus:outline-none focus:ring-1 ${fieldBorderClass}`}
                              disabled={disabled}
                            />
                          </div>
                        </div>

                        <div className="grid grid-cols-[120px_1fr]">
                          <div className="bg-slate-50 px-3 py-2 text-sm font-semibold text-slate-700">Acción</div>
                          <div className="px-2 py-2">
                            <button
                              type="button"
                              onClick={() => removeCondition(absoluteIndex)}
                              className="w-full rounded-md border border-red-200 bg-white px-3 py-2 text-sm font-semibold text-red-500 hover:bg-red-50 hover:text-red-700 disabled:opacity-40"
                              disabled={disabled || safeConditions.length === 1}
                              title="Quitar condición"
                            >
                              Quitar
                            </button>
                          </div>
                        </div>

                        {validationError && condition.tabla && (
                          <p className="border-t border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-700">⚠ {validationError}</p>
                        )}
                      </div>
                    );
                  }) : (
                    <div className="rounded-lg border border-dashed border-slate-300 bg-white px-3 py-4 text-sm text-slate-500">
                      Todavía no agregaste datos para {tableName}.
                    </div>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
