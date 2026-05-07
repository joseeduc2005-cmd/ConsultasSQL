// ─── Script JSON → WorkflowRow parser ────────────────────────────────────────
import { KnowledgeArticle } from '../../types';
import {
  WorkflowRow, WorkflowActionType, WorkflowTableName, WorkflowValidationCondition,
  WORKFLOW_ACTION_TYPES, WORKFLOW_TABLES, VALIDATION_CONDITIONS,
} from './adminTypes';
import { createEmptyWorkflowRow, createEmptyWorkflowConfig, normalizeWorkflowRows } from './workflowUtils';

export function getFirstEntry(record: unknown): [string, unknown] | null {
  if (!record || typeof record !== 'object' || Array.isArray(record)) return null;
  const entries = Object.entries(record as Record<string, unknown>);
  return entries[0] || null;
}

export function parseScriptSource(scriptJson: KnowledgeArticle['script_json']) {
  if (!scriptJson) return null;
  if (typeof scriptJson === 'string') {
    try { return JSON.parse(scriptJson); } catch { return null; }
  }
  return scriptJson;
}

export function parseWorkflowRowsFromScript(scriptJson: KnowledgeArticle['script_json']): WorkflowRow[] {
  const source = parseScriptSource(scriptJson);
  if (!source || typeof source !== 'object' || Array.isArray(source)) return [createEmptyWorkflowRow(1)];

  const candidate = source as Record<string, unknown>;
  const workflow = Array.isArray(candidate.workflow)
    ? candidate.workflow
    : candidate.tipo && typeof candidate.tipo === 'string' ? [candidate] : [];

  if (workflow.length === 0) return [createEmptyWorkflowRow(1)];

  return normalizeWorkflowRows(
    workflow.map((rawStep, index) => {
      const step = rawStep as Record<string, unknown>;
      const row = createEmptyWorkflowRow(index + 1);
      const type = String(step.tipo || '').trim().toLowerCase();
      row.descripcion = String(step.descripcion || `Paso ${index + 1}`);
      row.tipo = WORKFLOW_ACTION_TYPES.includes(type as WorkflowActionType) ? (type as WorkflowActionType) : '';

      if (row.tipo === 'validacion') {
        row.config.variable = String(step.variable || '');
        const cond = String(step.condicion || '').trim().toLowerCase() as WorkflowValidationCondition;
        row.config.condicion = VALIDATION_CONDITIONS.includes(cond) ? cond : 'existe';
        row.config.valorValidacion = step.valor === undefined ? '' : String(step.valor);
        row.config.mensajeError = String(step.mensaje_error || '');
        return row;
      }

      const tablaRaw = String(step.tabla || '').trim().toLowerCase() as WorkflowTableName;
      row.tabla = WORKFLOW_TABLES.includes(tablaRaw) ? tablaRaw : '';
      row.config.guardarEn = String(step.guardar_en || '');

      if (row.tipo === 'select') {
        const whereEntry = getFirstEntry(step.where);
        row.config.campo = whereEntry?.[0] || '';
        row.config.valor = whereEntry?.[1] === undefined ? '' : String(whereEntry[1]);
        if (Array.isArray(step.join)) {
          row.config.join_pairs = (step.join as Array<Record<string, unknown>>)
            .map((joinItem) => {
              const joinTable = String(joinItem?.tabla || '').trim().toLowerCase();
              const onObject = (joinItem?.on && typeof joinItem.on === 'object' && !Array.isArray(joinItem.on))
                ? (joinItem.on as Record<string, unknown>) : {};
              const firstOn = Object.entries(onObject)[0];
              if (!firstOn) return null;
              const [leftRef, rightRefRaw] = firstOn;
              const leftParts = String(leftRef || '').split('.');
              const rightParts = String(rightRefRaw || '').split('.');
              return {
                tabla: joinTable,
                base_columna: leftParts.length > 1 ? String(leftParts[1] || '').trim() : String(leftParts[0] || '').trim(),
                join_columna: rightParts.length > 1 ? String(rightParts[1] || '').trim() : String(rightParts[0] || '').trim(),
              };
            })
            .filter((pair): pair is { tabla: string; base_columna: string; join_columna: string } => Boolean(pair));
          if (row.config.join_pairs.length > 0) row.config.query_mode = 'compuesta';
        }
      }

      if (row.tipo === 'insert') {
        const dataEntry = getFirstEntry(step.data);
        row.config.campo = dataEntry?.[0] || '';
        row.config.valor = dataEntry?.[1] === undefined ? '' : String(dataEntry[1]);
      }

      if (row.tipo === 'update') {
        const setEntry = getFirstEntry(step.set);
        const whereEntry = getFirstEntry(step.where);
        row.config.campoObjetivo = setEntry?.[0] || '';
        row.config.valorNuevo = setEntry?.[1] === undefined ? '' : String(setEntry[1]);
        row.config.condicionCampo = whereEntry?.[0] || '';
        row.config.condicionValor = whereEntry?.[1] === undefined ? '' : String(whereEntry[1]);
      }

      if (row.tipo === 'delete') {
        const whereEntry = getFirstEntry(step.where);
        row.config.condicionCampo = whereEntry?.[0] || '';
        row.config.condicionValor = whereEntry?.[1] === undefined ? '' : String(whereEntry[1]);
      }

      return row;
    })
  );
}
