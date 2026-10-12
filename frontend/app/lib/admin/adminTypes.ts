// ─── Admin page shared types and constants ────────────────────────────────────

// «dinamico» es el tipo de ahora: el articulo pide los datos que le falten —una
// cedula, un numero de transaccion— y consulta. Los nombres viejos (database,
// script) se siguen aceptando al leer contenido ya creado, pero no se ofrecen.
export type SolutionType = 'lectura' | 'dinamico' | 'database' | 'script';
export type ScriptCreationMode = 'visual_builder' | 'json_unificado' | 'codigo_manual';
// Lo que se puede CREAR: sin update, insert ni delete, porque el sistema ya no
// escribe. Un articulo pide los datos que le falten y consulta.
export type WorkflowActionType = 'select' | 'validacion';

// Lo que se puede LEER. Un articulo creado antes del cambio puede traer pasos de
// escritura, y hay que poder abrirlo en el editor para convertirlo o borrarlo —si el
// editor no los reconociera, se veria vacio, que es peor que verlo y que diga que ya
// no se ejecuta. Ejecutarlos sigue siendo imposible: el backend los rechaza.
export type WorkflowActionTypeLeible = WorkflowActionType | 'update' | 'insert' | 'delete';

/** Los tipos de paso retirados, para poder avisar al abrir un articulo viejo. */
export const WORKFLOW_ACTION_TYPES_RETIRADOS = ['update', 'insert', 'delete'] as const;
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
  tipo: WorkflowActionTypeLeible | '';
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
  visible_para: 'todos' | 'admin' | 'usuario';
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

export const WORKFLOW_ACTION_TYPES: WorkflowActionType[] = ['select', 'validacion'];
export const DATABASE_ACTION_TYPES: WorkflowActionType[] = ['select'];
export const WORKFLOW_TABLES: WorkflowTableName[] = ['users', 'sessions', 'logs', 'employees'];
export const VALIDATION_CONDITIONS: WorkflowValidationCondition[] = ['existe', 'no_existe', 'igual'];
export const SCRIPT_CREATION_MODES: Array<{ value: ScriptCreationMode; label: string; level: string }> = [
  { value: 'visual_builder', label: 'Visual Builder', level: 'Recomendado' },
  { value: 'json_unificado', label: 'JSON', level: 'Avanzado' },
  { value: 'codigo_manual', label: 'Script Manual (JSON)', level: 'Solo Script' },
];
