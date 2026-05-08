// ─── Admin page shared types and constants ────────────────────────────────────

export type SolutionType = 'lectura' | 'database' | 'script';
export type ScriptCreationMode = 'visual_builder' | 'json_unificado' | 'codigo_manual';
export type WorkflowActionType = 'select' | 'update' | 'insert' | 'delete' | 'validacion';
export type WorkflowTableName = 'users' | 'sessions' | 'logs' | 'employees';
export type WorkflowValidationCondition = 'existe' | 'no_existe' | 'igual';

export type WorkflowStepConfig = {
  campo: string;
  valor: string;
  campoObjetivo: string;
  valorNuevo: string;
  condicionCampo: string;
  condicionValor: string;
  variable: string;
  condicion: WorkflowValidationCondition | '';
  valorValidacion: string;
  guardarEn: string;
  mensajeError: string;
  join_pairs?: Array<{ tabla: string; base_columna: string; join_columna: string }>;
  [key: string]: unknown;
};

export type WorkflowRow = {
  orden: number;
  descripcion: string;
  tipo: WorkflowActionType | '';
  tabla: WorkflowTableName | '';
  config: WorkflowStepConfig;
};

export type BuilderFormState = {
  id?: string | number;
  titulo: string;
  categoria: string;
  subcategoria: string;
  tags: string;
  descripcion: string;
  contenido_md: string;
  tipo_solucion: SolutionType;
  target_database: string;
  archivo_md?: File | null;
  workflow_rows: WorkflowRow[];
  script_json?: unknown;
  script_creation_mode: ScriptCreationMode;
  unified_json_text: string;
  manual_json_text: string;
};

export type WorkflowValidationResult = {
  valid: boolean;
  generalError: string;
  rowErrors: Record<number, string[]>;
};

export const WORKFLOW_ACTION_TYPES: WorkflowActionType[] = ['select', 'update', 'insert', 'delete', 'validacion'];
export const DATABASE_ACTION_TYPES: WorkflowActionType[] = ['select', 'update', 'insert', 'delete'];
export const WORKFLOW_TABLES: WorkflowTableName[] = ['users', 'sessions', 'logs', 'employees'];
export const VALIDATION_CONDITIONS: WorkflowValidationCondition[] = ['existe', 'no_existe', 'igual'];
export const SCRIPT_CREATION_MODES: Array<{ value: ScriptCreationMode; label: string; level: string }> = [
  { value: 'visual_builder', label: 'Visual Builder', level: 'Recomendado' },
  { value: 'json_unificado', label: 'JSON', level: 'Avanzado' },
  { value: 'codigo_manual', label: 'Script Manual (JSON)', level: 'Solo Script' },
];
