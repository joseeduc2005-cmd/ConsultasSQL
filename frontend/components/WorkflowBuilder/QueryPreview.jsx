'use client';

import React, { useMemo } from 'react';

/**
 * Format a single WHERE entry from the generated `where` object.
 * Values can be scalar or { op, value } objects.
 */
const formatWhereEntry = (key, value) => {
  if (value && typeof value === 'object' && !Array.isArray(value) && value.op) {
    if (typeof value.field === 'string' && value.field.trim()) {
      return `${key} ${value.op} ${value.field}`;
    }
    const displayValue = typeof value.value === 'string' ? `'${value.value}'` : String(value.value ?? '');
    const op = value.op === 'LIKE' ? 'LIKE' : value.op;
    return `${key} ${op} ${displayValue}`;
  }
  const displayValue = typeof value === 'string' ? `'${value}'` : String(value ?? '');
  return `${key} = ${displayValue}`;
};

const buildSqlForStep = (step) => {
  if (!step || !step.tipo || !step.tabla) return '';

  const type = String(step.tipo).toLowerCase();
  if (type !== 'select') return '';

  const joins = Array.isArray(step.join) ? step.join : [];
  const baseColumns = Array.isArray(step.columnas) ? step.columnas : [];
  const where = step.where && typeof step.where === 'object' && !Array.isArray(step.where) ? step.where : {};
  const logic = step.logic || 'AND';

  const projection = [];
  if (baseColumns.length > 0) {
    baseColumns.forEach((column) => {
      projection.push(`${step.tabla}.${column}`);
    });
  }
  joins.forEach((joinItem) => {
    const joinColumns = Array.isArray(joinItem?.columnas) ? joinItem.columnas : [];
    joinColumns.forEach((column) => {
      projection.push(`${joinItem.tabla}.${column}`);
    });
  });

  const selectProjection = projection.length > 0 ? projection.join(', ') : '*';

  const joinSql = joins
    .map((joinItem) => {
      const onEntries = joinItem?.on && typeof joinItem.on === 'object' ? Object.entries(joinItem.on) : [];
      const first = onEntries[0];
      if (!first) return '';
      return `  JOIN ${joinItem.tabla} ON ${first[0]} = ${first[1]}`;
    })
    .filter(Boolean)
    .join('\n');

  const whereEntries = Object.entries(where);
  const whereSql = whereEntries.length > 0
    ? `WHERE ${whereEntries.map(([key, value]) => formatWhereEntry(key, value)).join(` ${logic} `)}`
    : '';

  return [`SELECT ${selectProjection}`, `FROM ${step.tabla}`, joinSql, whereSql].filter(Boolean).join('\n');
};

const buildSqlPreview = (generatedJSON) => {
  if (!generatedJSON) return '-- Completa los campos para ver SQL simulado';

  const workflow = Array.isArray(generatedJSON.workflow) ? generatedJSON.workflow : [generatedJSON];
  const sqlBlocks = workflow
    .map((step) => buildSqlForStep(step))
    .filter(Boolean);

  return sqlBlocks.length > 0 ? sqlBlocks.join('\n\n-- siguiente paso --\n\n') : '-- SQL simulado disponible para pasos SELECT';
};

export default function QueryPreview({ generatedJSON, isValid }) {
  const sqlPreview = useMemo(() => buildSqlPreview(generatedJSON), [generatedJSON]);

  return (
    <div className="border-t border-gray-200 bg-gray-50 p-4">
      <div className="mb-2 flex items-center justify-between">
        <h3 className="font-bold text-gray-900">Vista previa de la consulta</h3>
        <span className={`rounded px-2 py-1 text-xs ${isValid ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-700'}`}>
          {isValid ? 'Válido' : 'Con errores'}
        </span>
      </div>

      <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
        <div>
          <p className="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-600">JSON generado</p>
          <pre className="max-h-80 overflow-x-auto overflow-y-auto rounded border border-gray-300 bg-white p-3 font-mono text-xs text-gray-700">
            {generatedJSON ? JSON.stringify(generatedJSON, null, 2) : 'Completa los campos requeridos para generar script_json.'}
          </pre>
        </div>

        <div>
          <p className="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-600">SQL simulado (solo lectura)</p>
          <pre className="max-h-80 overflow-x-auto overflow-y-auto rounded border border-gray-300 bg-slate-900 p-3 font-mono text-xs text-slate-100">
            {sqlPreview}
          </pre>
        </div>
      </div>
    </div>
  );
}
