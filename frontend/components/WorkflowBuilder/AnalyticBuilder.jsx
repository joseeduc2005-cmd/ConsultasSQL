'use client';

import React, { useMemo } from 'react';

const AGG_FUNCTIONS = ['COUNT', 'COUNT DISTINCT', 'SUM', 'AVG', 'MAX', 'MIN'];
const HAVING_OPERATORS = ['=', '!=', '>', '>=', '<', '<='];

const EMPTY_AGGREGATE = { func: 'COUNT', column: '*', alias: '' };
const EMPTY_ORDER_BY = { column: '', direction: 'ASC' };
const EMPTY_HAVING = { alias: '', op: '>', value: '' };

export default function AnalyticBuilder({
  groupByCols = [],
  aggregates = [],
  orderBy = [],
  having = [],
  baseTable = '',
  joinedTables = [],
  getColumnsForTable = () => [],
  onGroupByChange = () => {},
  onAggregatesChange = () => {},
  onOrderByChange = () => {},
  onHavingChange = () => {},
}) {
  const availableFieldRefs = useMemo(() => {
    const tableNames = [baseTable, ...(Array.isArray(joinedTables) ? joinedTables : [])].filter(Boolean);
    const refs = [];

    tableNames.forEach((tableName) => {
      const columns = getColumnsForTable(tableName);
      columns.forEach((columnName) => {
        refs.push(`${tableName}.${columnName}`);
      });
    });

    return refs;
  }, [baseTable, joinedTables, getColumnsForTable]);

  const aggregateAliasOptions = aggregates
    .map((item) => String(item?.alias || '').trim())
    .filter(Boolean);

  const resolvedAggregates = aggregates.length > 0 ? aggregates : [{ ...EMPTY_AGGREGATE }];
  const resolvedOrderBy = orderBy.length > 0 ? orderBy : [{ ...EMPTY_ORDER_BY }];
  const resolvedHaving = having.length > 0 ? having : [];

  const toggleGroupBy = (fieldRef) => {
    if (groupByCols.includes(fieldRef)) {
      onGroupByChange(groupByCols.filter((entry) => entry !== fieldRef));
      return;
    }
    onGroupByChange([...groupByCols, fieldRef]);
  };

  return (
    <div className="space-y-3">
      <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
        <p className="text-sm font-semibold text-slate-800">3. GROUP BY</p>
        <p className="mt-1 text-xs text-slate-500">Selecciona las columnas por las que quieres agrupar.</p>

        <div className="mt-3 grid grid-cols-1 gap-3 lg:grid-cols-2">
          {[baseTable, ...(Array.isArray(joinedTables) ? joinedTables : [])].filter(Boolean).map((tableName) => {
            const tableColumns = getColumnsForTable(tableName);
            return (
              <div key={`analytic-group-${tableName}`} className="rounded-lg border border-slate-200 bg-white p-3">
                <p className="text-xs font-semibold uppercase tracking-wide text-slate-600">{tableName}</p>
                <div className="mt-2 max-h-36 space-y-1 overflow-y-auto pr-1">
                  {tableColumns.map((columnName) => {
                    const ref = `${tableName}.${columnName}`;
                    return (
                      <label key={ref} className="flex items-center gap-2 rounded px-2 py-1 text-sm hover:bg-slate-50">
                        <input
                          type="checkbox"
                          checked={groupByCols.includes(ref)}
                          onChange={() => toggleGroupBy(ref)}
                          className="h-4 w-4 rounded border-slate-300 text-indigo-600 focus:ring-indigo-500"
                        />
                        <span className="text-slate-700">{columnName}</span>
                      </label>
                    );
                  })}
                </div>
              </div>
            );
          })}
        </div>
      </div>

      <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
        <p className="text-sm font-semibold text-slate-800">4. Agregaciones</p>
        <p className="mt-1 text-xs text-slate-500">Define funciones como COUNT, SUM o AVG y su alias.</p>

        <div className="mt-3 space-y-2">
          {resolvedAggregates.map((item, idx) => (
            <div key={`agg-${idx}`} className="grid grid-cols-1 gap-2 lg:grid-cols-[180px_1fr_1fr_auto]">
              <select
                value={item.func || 'COUNT'}
                onChange={(e) => {
                  const next = [...resolvedAggregates];
                  next[idx] = { ...next[idx], func: e.target.value };
                  onAggregatesChange(next);
                }}
                className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
              >
                {AGG_FUNCTIONS.map((func) => (
                  <option key={func} value={func}>{func}</option>
                ))}
              </select>

              <select
                value={item.column || '*'}
                onChange={(e) => {
                  const next = [...resolvedAggregates];
                  next[idx] = { ...next[idx], column: e.target.value };
                  onAggregatesChange(next);
                }}
                className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
              >
                <option value="*">*</option>
                {availableFieldRefs.map((ref) => (
                  <option key={`agg-col-${ref}`} value={ref}>{ref}</option>
                ))}
              </select>

              <input
                type="text"
                value={item.alias || ''}
                onChange={(e) => {
                  const next = [...resolvedAggregates];
                  next[idx] = { ...next[idx], alias: e.target.value };
                  onAggregatesChange(next);
                }}
                placeholder="Alias (ej. total)"
                className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
              />

              <button
                type="button"
                onClick={() => {
                  const next = resolvedAggregates.filter((_, i) => i !== idx);
                  onAggregatesChange(next.length > 0 ? next : [{ ...EMPTY_AGGREGATE }]);
                }}
                className="rounded-lg border border-red-200 px-3 py-2 text-sm text-red-700 hover:bg-red-50"
              >
                Quitar
              </button>
            </div>
          ))}
        </div>

        <button
          type="button"
          onClick={() => onAggregatesChange([...resolvedAggregates, { ...EMPTY_AGGREGATE }])}
          className="mt-3 rounded-lg border border-blue-200 bg-white px-3 py-2 text-sm font-medium text-blue-700 hover:bg-blue-50"
        >
          + Agregar agregación
        </button>
      </div>

      <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
        <p className="text-sm font-semibold text-slate-800">5. ORDER BY</p>
        <p className="mt-1 text-xs text-slate-500">Ordena por columna o por alias agregado.</p>

        <div className="mt-3 space-y-2">
          {resolvedOrderBy.map((item, idx) => (
            <div key={`ord-${idx}`} className="grid grid-cols-1 gap-2 lg:grid-cols-[1fr_160px_auto]">
              <select
                value={item.column || ''}
                onChange={(e) => {
                  const next = [...resolvedOrderBy];
                  next[idx] = { ...next[idx], column: e.target.value };
                  onOrderByChange(next);
                }}
                className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
              >
                <option value="">Columna o alias</option>
                {availableFieldRefs.map((ref) => (
                  <option key={`ord-col-${ref}`} value={ref}>{ref}</option>
                ))}
                {aggregateAliasOptions.map((alias) => (
                  <option key={`ord-alias-${alias}`} value={alias}>{alias}</option>
                ))}
              </select>

              <select
                value={item.direction || 'ASC'}
                onChange={(e) => {
                  const next = [...resolvedOrderBy];
                  next[idx] = { ...next[idx], direction: e.target.value === 'DESC' ? 'DESC' : 'ASC' };
                  onOrderByChange(next);
                }}
                className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
              >
                <option value="ASC">ASC</option>
                <option value="DESC">DESC</option>
              </select>

              <button
                type="button"
                onClick={() => {
                  const next = resolvedOrderBy.filter((_, i) => i !== idx);
                  onOrderByChange(next.length > 0 ? next : [{ ...EMPTY_ORDER_BY }]);
                }}
                className="rounded-lg border border-red-200 px-3 py-2 text-sm text-red-700 hover:bg-red-50"
              >
                Quitar
              </button>
            </div>
          ))}
        </div>

        <button
          type="button"
          onClick={() => onOrderByChange([...resolvedOrderBy, { ...EMPTY_ORDER_BY }])}
          className="mt-3 rounded-lg border border-blue-200 bg-white px-3 py-2 text-sm font-medium text-blue-700 hover:bg-blue-50"
        >
          + Agregar orden
        </button>
      </div>

      <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
        <p className="text-sm font-semibold text-slate-800">6. HAVING</p>
        <p className="mt-1 text-xs text-slate-500">Filtra usando aliases de agregación.</p>

        {aggregateAliasOptions.length === 0 ? (
          <div className="mt-3 rounded-lg border border-dashed border-slate-300 bg-white px-3 py-2 text-xs text-slate-500">
            Define al menos una agregación con alias para habilitar HAVING.
          </div>
        ) : (
          <>
            <div className="mt-3 space-y-2">
              {resolvedHaving.map((item, idx) => (
                <div key={`having-${idx}`} className="grid grid-cols-1 gap-2 lg:grid-cols-[1fr_160px_1fr_auto]">
                  <select
                    value={item.alias || ''}
                    onChange={(e) => {
                      const next = [...resolvedHaving];
                      next[idx] = { ...next[idx], alias: e.target.value };
                      onHavingChange(next);
                    }}
                    className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
                  >
                    <option value="">Alias</option>
                    {aggregateAliasOptions.map((alias) => (
                      <option key={`having-alias-${alias}`} value={alias}>{alias}</option>
                    ))}
                  </select>

                  <select
                    value={item.op || '>'}
                    onChange={(e) => {
                      const next = [...resolvedHaving];
                      next[idx] = { ...next[idx], op: e.target.value };
                      onHavingChange(next);
                    }}
                    className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
                  >
                    {HAVING_OPERATORS.map((op) => (
                      <option key={`having-op-${op}`} value={op}>{op}</option>
                    ))}
                  </select>

                  <input
                    type="text"
                    value={item.value ?? ''}
                    onChange={(e) => {
                      const next = [...resolvedHaving];
                      next[idx] = { ...next[idx], value: e.target.value };
                      onHavingChange(next);
                    }}
                    placeholder="Valor"
                    className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
                  />

                  <button
                    type="button"
                    onClick={() => {
                      const next = resolvedHaving.filter((_, i) => i !== idx);
                      onHavingChange(next);
                    }}
                    className="rounded-lg border border-red-200 px-3 py-2 text-sm text-red-700 hover:bg-red-50"
                  >
                    Quitar
                  </button>
                </div>
              ))}
            </div>

            <button
              type="button"
              onClick={() => onHavingChange([...resolvedHaving, { ...EMPTY_HAVING }])}
              className="mt-3 rounded-lg border border-blue-200 bg-white px-3 py-2 text-sm font-medium text-blue-700 hover:bg-blue-50"
            >
              + Agregar condición HAVING
            </button>
          </>
        )}
      </div>
    </div>
  );
}
