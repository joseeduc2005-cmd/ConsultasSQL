// ─── Pure workflow utility functions ──────────────────────────────────────────
import {
  SolutionType, WorkflowActionType, WorkflowRow, WorkflowStepConfig,
  BuilderFormState, WORKFLOW_ACTION_TYPES, DATABASE_ACTION_TYPES, SCRIPT_CREATION_MODES, ScriptCreationMode,
} from './adminTypes';

export function normalizeSolutionType(value?: string): SolutionType {
  // Los nombres viejos se leen como «dinamico»: significan lo mismo —solo lectura—
  // y asi el contenido ya creado sigue abriendose sin migrar nada.
  if (value === 'dinamico' || value === 'database' || value === 'script' || value === 'ejecutable') {
    return 'dinamico';
  }
  return 'lectura';
}

// Los tres nombres viejos se tratan como dinamico; se mantienen las funciones para
// no tocar los sitios que las llaman.
export function isDatabaseSolutionType(tipo: SolutionType) { return normalizeSolutionType(tipo) === 'dinamico'; }
export function isScriptSolutionType(tipo: SolutionType) { return normalizeSolutionType(tipo) === 'dinamico'; }
export function isExecutableSolutionType(tipo: SolutionType) {
  return normalizeSolutionType(tipo) === 'dinamico';
}

export function getAllowedActionTypes(solutionType: SolutionType): WorkflowActionType[] {
  return isScriptSolutionType(solutionType) ? WORKFLOW_ACTION_TYPES : DATABASE_ACTION_TYPES;
}

export function getCreationModesForSolutionType(tipo: SolutionType): Array<{ value: ScriptCreationMode; label: string; level: string }> {
  // Un articulo dinamico admite las tres formas de escribirlo, y el script manual es
  // la que mas se va a usar: es donde se escribe el SELECT con sus parametros
  // (... WHERE cedula = :cedula). Antes este modo solo salia para el tipo 'script', y
  // al unificar los tres nombres en 'dinamico' se habria quedado fuera.
  if (isExecutableSolutionType(tipo)) return SCRIPT_CREATION_MODES;
  return [];
}

export function createEmptyWorkflowConfig(): WorkflowStepConfig {
  return {
    campo: '', valor: '', campoObjetivo: '', valorNuevo: '',
    condicionCampo: '', condicionValor: '', variable: '',
    condicion: 'existe', valorValidacion: '', guardarEn: '', mensajeError: '',
  };
}

export function createEmptyWorkflowRow(orden = 1): WorkflowRow {
  return { orden, descripcion: '', tipo: '', tabla: '', config: createEmptyWorkflowConfig() };
}

export function normalizeWorkflowRows(rows: WorkflowRow[]): WorkflowRow[] {
  return rows.map((row, index) => ({
    ...row,
    orden: index + 1,
    descripcion: String(row.descripcion || ''),
    tipo: row.tipo || '',
    tabla: row.tipo === 'validacion' ? '' : row.tabla || '',
    config: { ...createEmptyWorkflowConfig(), ...(row.config || {}) },
  }));
}

export function sanitizeWorkflowRowForMode(row: WorkflowRow, solutionType: SolutionType): WorkflowRow | null {
  const allowedTypes = getAllowedActionTypes(solutionType);

  if (!row.tipo) {
    return { ...row, config: { ...createEmptyWorkflowConfig(), ...(row.config || {}) } };
  }

  if (!(allowedTypes as string[]).includes(row.tipo)) {
    return isDatabaseSolutionType(solutionType) && row.tipo === 'validacion' ? null : { ...row, tipo: '' };
  }

  const nextRow: WorkflowRow = {
    ...row,
    tabla: row.tipo === 'validacion' ? '' : row.tabla,
    config: { ...createEmptyWorkflowConfig(), ...(row.config || {}) },
  };

  if (isDatabaseSolutionType(solutionType)) {
    nextRow.config.guardarEn = '';
    nextRow.config.variable = '';
    nextRow.config.condicion = '';
    nextRow.config.valorValidacion = '';
    nextRow.config.mensajeError = '';
  }

  return nextRow;
}

export function adaptRowsForSolutionType(rows: WorkflowRow[], solutionType: SolutionType): WorkflowRow[] {
  const adapted = rows
    .map((row) => sanitizeWorkflowRowForMode(row, solutionType))
    .filter((row): row is WorkflowRow => Boolean(row));
  return normalizeWorkflowRows(adapted.length > 0 ? adapted : [createEmptyWorkflowRow(1)]);
}

export function updateRows(rows: WorkflowRow[], index: number, patch: Partial<WorkflowRow>): WorkflowRow[] {
  return normalizeWorkflowRows(
    rows.map((row, rowIndex) => (rowIndex === index ? { ...row, ...patch } : row))
  );
}

export function getPanelPreferenceKey(username: string, role: string) {
  return `panelPrefs:${username}:${role}`;
}

export function createInitialFormState(): BuilderFormState {
  return {
    titulo: '', categoria: '', subcategoria: '', tags: '',
    descripcion: '', contenido_md: '', target_database: '',
    visible_para: 'todos',
    tipo_solucion: 'database', archivo_md: null,
    workflow_rows: [createEmptyWorkflowRow(1)],
    script_json: null, script_creation_mode: 'visual_builder',
    unified_json_text: '', manual_json_text: '',
  };
}

export function updateRowConfig(rows: WorkflowRow[], index: number, patch: Partial<WorkflowStepConfig>): WorkflowRow[] {
  return normalizeWorkflowRows(
    rows.map((row, rowIndex) => rowIndex !== index ? row : { ...row, config: { ...row.config, ...patch } })
  );
}
