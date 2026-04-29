'use client';

import React, { useMemo, useState } from 'react';

const normalizeRelation = (relation, baseTable) => {
  const fromTable = String(relation?.from?.tabla || '').trim();
  const fromColumn = String(relation?.from?.columna || '').trim();
  const toTable = String(relation?.to?.tabla || '').trim();
  const toColumn = String(relation?.to?.columna || '').trim();

  if (!fromTable || !fromColumn || !toTable || !toColumn) return null;

  if (fromTable === baseTable) {
    return {
      tabla: toTable,
      base_columna: fromColumn,
      join_columna: toColumn,
      label: `${baseTable}.${fromColumn} = ${toTable}.${toColumn}`,
    };
  }

  if (toTable === baseTable) {
    return {
      tabla: fromTable,
      base_columna: toColumn,
      join_columna: fromColumn,
      label: `${baseTable}.${toColumn} = ${fromTable}.${fromColumn}`,
    };
  }

  return null;
};

export default function JoinBuilder({
  baseTable = '',
  baseColumns = [],
  availableTables = [],
  relations = [],
  mode = 'auto',
  onModeChange = () => {},
  joinPairs = [],
  onChange = () => {},
  disabled = false,
  getColumnsForTable = () => [],
}) {
  const normalizedMode = mode === 'manual' ? 'manual' : 'auto';
  const isManualMode = normalizedMode === 'manual';
  const [manualTableDraft, setManualTableDraft] = useState('');
  const activeModeButtonStyle = {
    backgroundColor: '#1d4ed8',
    color: '#ffffff',
    borderColor: '#1d4ed8',
  };

  const safeJoinPairs = Array.isArray(joinPairs)
    ? joinPairs.map((pair) => ({
        tabla: String(pair?.tabla || '').trim(),
        base_columna: String(pair?.base_columna || '').trim(),
        join_columna: String(pair?.join_columna || '').trim(),
      }))
    : [];

  const normalizedJoinPairs = safeJoinPairs.filter((pair) => pair.tabla && pair.base_columna && pair.join_columna);

  const relationOptions = useMemo(() => {
    if (!baseTable) return [];

    const available = new Set((availableTables || []).map((table) => String(table || '').trim()).filter(Boolean));

    const options = (Array.isArray(relations) ? relations : [])
      .map((relation) => normalizeRelation(relation, baseTable))
      .filter((relation) => relation && relation.tabla && relation.base_columna && relation.join_columna)
      .filter((relation) => available.size === 0 || available.has(relation.tabla));

    const unique = new Map();
    options.forEach((option) => {
      const key = `${option.tabla}|${option.base_columna}|${option.join_columna}`;
      if (!unique.has(key)) unique.set(key, option);
    });

    return Array.from(unique.values());
  }, [baseTable, availableTables, relations]);

  const relationByTable = useMemo(() => {
    const map = new Map();
    relationOptions.forEach((option) => {
      if (!map.has(option.tabla)) {
        map.set(option.tabla, option);
      }
    });
    return map;
  }, [relationOptions]);

  const relatedTables = useMemo(() => Array.from(relationByTable.keys()), [relationByTable]);

  const selectedTables = useMemo(
    () => Array.from(new Set(safeJoinPairs.map((pair) => pair.tabla).filter(Boolean))),
    [safeJoinPairs]
  );

  const toggleRelatedTable = (relatedTable, checked) => {
    if (!checked) {
      onChange(safeJoinPairs.filter((pair) => pair.tabla !== relatedTable));
      return;
    }

    const relation = relationByTable.get(relatedTable);
    if (!relation) return;

    const nextPairs = safeJoinPairs.filter((pair) => pair.tabla !== relatedTable);
    nextPairs.push({
      tabla: relation.tabla,
      base_columna: relation.base_columna,
      join_columna: relation.join_columna,
    });
    onChange(nextPairs);
  };

  const addManualTable = () => {
    const tableName = String(manualTableDraft || '').trim();
    if (!tableName) return;

    // Manual mode only selects the related table.
    // Column mapping is configured in step 4.
    const nextPairs = safeJoinPairs.filter((pair) => pair.tabla !== tableName);
    nextPairs.push({
      tabla: tableName,
      base_columna: '',
      join_columna: '',
    });

    onChange(nextPairs);
    setManualTableDraft('');
  };

  const removeManualTable = (tableName) => {
    onChange(safeJoinPairs.filter((pair) => pair.tabla !== tableName));
  };

  // Manual mode: show ALL available tables (not just FK-detected ones)
  const manualTableOptions = useMemo(() => {
    const allTables = (availableTables || [])
      .map((t) => String(t || '').trim())
      .filter((t) => t && t !== baseTable);
    return allTables.filter((tableName) => !selectedTables.includes(tableName));
  }, [availableTables, baseTable, selectedTables]);

  if (!baseTable) {
    return (
      <div className="rounded-xl border border-slate-200 bg-slate-50 p-4 text-sm text-slate-500">
        Selecciona primero una tabla base para ver relaciones automáticas.
      </div>
    );
  }

  return (
    <div className="rounded-xl border border-indigo-100 bg-indigo-50/40 p-4">
      <div className="mb-4 rounded-lg border border-indigo-200 bg-white p-3">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-xs font-semibold uppercase tracking-wide text-slate-600">Modo de relación</p>
            <p className="text-sm font-semibold text-slate-800">
              {normalizedMode === 'auto' ? 'Automático (FK)' : 'Manual'}
            </p>
          </div>

          <div
            className={`inline-flex rounded-xl border border-slate-300 bg-slate-100 p-1 ${
              disabled ? 'opacity-50' : ''
            }`}
          >
            <button
              type="button"
              onClick={() => onModeChange('auto')}
              disabled={disabled}
              aria-pressed={!isManualMode}
              style={!isManualMode ? activeModeButtonStyle : { backgroundColor: 'transparent' }}
              className={`rounded-lg px-4 py-1.5 text-xs font-semibold uppercase tracking-wide ${
                !isManualMode
                  ? 'shadow-sm border'
                  : 'text-slate-700 hover:bg-white'
              }`}
            >
              Auto
            </button>
            <button
              type="button"
              onClick={() => onModeChange('manual')}
              disabled={disabled}
              aria-pressed={isManualMode}
              style={isManualMode ? activeModeButtonStyle : { backgroundColor: 'transparent' }}
              className={`rounded-lg px-4 py-1.5 text-xs font-semibold uppercase tracking-wide ${
                isManualMode
                  ? 'shadow-sm border'
                  : 'text-slate-700 hover:bg-white'
              }`}
            >
              Manual
            </button>
          </div>
        </div>

        <div className="mt-2 text-xs text-slate-500">
          {!isManualMode
            ? 'Usa relaciones FK detectadas automáticamente.'
            : 'Define manualmente tabla y columnas del JOIN.'}
        </div>
      </div>

      <div
        className="min-h-[168px]"
      >
      {!isManualMode ? (
        <>
      <div className="mb-3">
        <p className="text-sm font-semibold text-slate-800">Relaciones automáticas detectadas</p>
        <p className="mt-1 text-xs text-slate-500">Solo selecciona tablas relacionadas. El JOIN se genera automáticamente con FK.</p>
      </div>

      {relatedTables.length === 0 ? (
        <div className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
          No hay relaciones disponibles para la tabla {baseTable}.
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-2 md:grid-cols-2 lg:grid-cols-3">
          {relatedTables.map((tableName) => {
            const relation = relationByTable.get(tableName);
            const checked = selectedTables.includes(tableName);

            return (
              <label
                key={`rel-${baseTable}-${tableName}`}
                className={`rounded-lg border px-3 py-2 ${checked ? 'border-indigo-300 bg-white' : 'border-slate-200 bg-white'}`}
              >
                <div className="flex items-start gap-2">
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={(e) => toggleRelatedTable(tableName, e.target.checked)}
                    disabled={disabled}
                    className="mt-0.5 h-4 w-4 rounded border-slate-300 text-indigo-600 focus:ring-indigo-500"
                  />
                  <div>
                    <p className="text-sm font-semibold text-slate-800">{tableName}</p>
                    <p className="text-xs text-slate-500">{relation?.label}</p>
                  </div>
                </div>
              </label>
            );
          })}
        </div>
      )}

      <div className="mt-3 rounded-lg border border-dashed border-slate-300 bg-white px-3 py-2 text-xs text-slate-600">
        {normalizedJoinPairs.length > 0
          ? normalizedJoinPairs.map((pair) => `${baseTable}.${pair.base_columna} = ${pair.tabla}.${pair.join_columna}`).join(' | ')
          : 'No has seleccionado relaciones. La consulta se ejecutará sobre una sola tabla.'}
      </div>
        </>
      ) : (
        <>
          <div className="mb-3">
            <p className="text-sm font-semibold text-slate-800">Configuración manual de JOIN</p>
            <p className="mt-1 text-xs text-slate-500">Selecciona manualmente qué tabla relacionada quieres incluir.</p>
          </div>

          {manualTableOptions.length === 0 && selectedTables.length === 0 ? (
            <div className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
              No hay tablas disponibles para {baseTable}.
            </div>
          ) : (
            <div className="space-y-3">
              <div className="grid grid-cols-1 gap-2 lg:grid-cols-[1fr_auto]">
                <select
                  value={manualTableDraft}
                  onChange={(e) => setManualTableDraft(e.target.value)}
                  disabled={disabled}
                  className="w-full rounded-lg border border-gray-300 bg-white px-3 py-2 text-sm"
                >
                  <option value="">Selecciona tabla relacionada</option>
                  {manualTableOptions.map((tableName) => (
                    <option key={`manual-table-option-${tableName}`} value={tableName}>{tableName}</option>
                  ))}
                </select>

                <button
                  type="button"
                  onClick={addManualTable}
                  disabled={disabled || !manualTableDraft}
                  className="rounded-lg border border-indigo-200 bg-white px-3 py-2 text-sm font-medium text-indigo-700 hover:bg-indigo-50 disabled:opacity-50"
                >
                  + Agregar tabla
                </button>
              </div>

              {selectedTables.length > 0 && (
                <div className="grid grid-cols-1 gap-2 md:grid-cols-2 lg:grid-cols-3">
                  {selectedTables.map((tableName) => {
                    const currentPair = safeJoinPairs.find((pair) => pair.tabla === tableName);
                    const hasColumns = Boolean(currentPair?.base_columna && currentPair?.join_columna);

                    return (
                    <div key={`manual-selected-${tableName}`} className="rounded-lg border border-slate-200 bg-white px-3 py-2">
                      <div className="flex items-center justify-between gap-2">
                        <p className="text-sm font-semibold text-slate-800">{tableName}</p>
                        <button
                          type="button"
                          onClick={() => removeManualTable(tableName)}
                          disabled={disabled}
                          className="rounded-md border border-red-200 px-2 py-1 text-xs font-medium text-red-700 hover:bg-red-50"
                        >
                          Quitar
                        </button>
                      </div>

                      <p className={`mt-2 text-xs ${hasColumns ? 'text-slate-500' : 'text-amber-700'}`}>
                        {hasColumns
                          ? `Relación: ${baseTable}.${currentPair.base_columna} = ${tableName}.${currentPair.join_columna}`
                          : 'Define el mapeo de columnas en la opción 4.'}
                      </p>
                    </div>
                    );
                  })}
                </div>
              )}
            </div>
          )}

          <div className="mt-3 rounded-lg border border-dashed border-slate-300 bg-white px-3 py-2 text-xs text-slate-600">
            {safeJoinPairs.length > 0
              ? safeJoinPairs.map((pair) => (
                pair.base_columna && pair.join_columna
                  ? `${baseTable}.${pair.base_columna} = ${pair.tabla}.${pair.join_columna}`
                  : `${pair.tabla} (falta definir columnas en opción 4)`
              )).join(' | ')
              : 'No has seleccionado relaciones. La consulta se ejecutará sobre una sola tabla.'}
          </div>
        </>
      )}
      </div>
    </div>
  );
}
