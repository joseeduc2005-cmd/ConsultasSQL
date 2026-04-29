'use client';

import { useCallback, useEffect, useMemo, useState, useTransition } from 'react';
import { useRouter } from 'next/navigation';
import { withAuth } from '../components/ProtectedRoute';
import ThemeToggle from '../components/ThemeToggle';
import { useTheme } from '../components/ThemeProvider';
import UserIdentityBadge from '../components/UserIdentityBadge';
import Sidebar from '../components/Sidebar';
import ContentPanel from '../components/ContentPanel';
import CommentsSection from '../components/CommentsSection';
import WorkflowBuilder from '@/components/WorkflowBuilder/WorkflowBuilder';
import { generateScriptJson, validateWorkflow as validateWorkflowDefinition } from '@/lib/workflowBuilder';
import { KnowledgeArticle } from '../types';

type SolutionType = 'lectura' | 'database' | 'script';
type ScriptCreationMode = 'visual_builder' | 'json_unificado' | 'codigo_manual';
type WorkflowActionType = 'select' | 'update' | 'insert' | 'delete' | 'validacion';
type WorkflowTableName = 'users' | 'sessions' | 'logs' | 'employees';
type WorkflowValidationCondition = 'existe' | 'no_existe' | 'igual';

type WorkflowStepConfig = {
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

type WorkflowRow = {
  orden: number;
  descripcion: string;
  tipo: WorkflowActionType | '';
  tabla: WorkflowTableName | '';
  config: WorkflowStepConfig;
};

type BuilderFormState = {
  id?: string | number;
  titulo: string;
  categoria: string;
  subcategoria: string;
  tags: string;
  descripcion: string;
  contenido_md: string;
  tipo_solucion: SolutionType;
  archivo_md?: File | null;
  workflow_rows: WorkflowRow[];
  script_json?: unknown;
  script_creation_mode: ScriptCreationMode;
  unified_json_text: string;
  manual_json_text: string;
};

type WorkflowValidationResult = {
  valid: boolean;
  generalError: string;
  rowErrors: Record<number, string[]>;
};

const WORKFLOW_ACTION_TYPES: WorkflowActionType[] = ['select', 'update', 'insert', 'delete', 'validacion'];
const DATABASE_ACTION_TYPES: WorkflowActionType[] = ['select', 'update', 'insert', 'delete'];
const WORKFLOW_TABLES: WorkflowTableName[] = ['users', 'sessions', 'logs', 'employees'];
const VALIDATION_CONDITIONS: WorkflowValidationCondition[] = ['existe', 'no_existe', 'igual'];
const SCRIPT_CREATION_MODES: Array<{ value: ScriptCreationMode; label: string; level: string }> = [
  { value: 'visual_builder', label: 'Visual Builder', level: 'Recomendado' },
  { value: 'json_unificado', label: 'JSON', level: 'Avanzado' },
  { value: 'codigo_manual', label: 'Script Manual (JSON)', level: 'Solo Script' },
];

function normalizeSolutionType(value?: string): SolutionType {
  if (value === 'database' || value === 'script') return value;
  return 'lectura';
}

function isDatabaseSolutionType(tipo: SolutionType) {
  return tipo === 'database';
}

function isScriptSolutionType(tipo: SolutionType) {
  return tipo === 'script';
}

function isExecutableSolutionType(tipo: SolutionType) {
  return isDatabaseSolutionType(tipo) || isScriptSolutionType(tipo);
}

function getCreationModesForSolutionType(tipo: SolutionType) {
  if (isDatabaseSolutionType(tipo)) {
    return SCRIPT_CREATION_MODES.filter((mode) => mode.value !== 'codigo_manual');
  }
  if (isScriptSolutionType(tipo)) {
    return SCRIPT_CREATION_MODES.filter((mode) => mode.value === 'codigo_manual');
  }
  return [];
}

function createEmptyWorkflowConfig(): WorkflowStepConfig {
  return {
    campo: '',
    valor: '',
    campoObjetivo: '',
    valorNuevo: '',
    condicionCampo: '',
    condicionValor: '',
    variable: '',
    condicion: 'existe',
    valorValidacion: '',
    guardarEn: '',
    mensajeError: '',
  };
}

function createEmptyWorkflowRow(orden = 1): WorkflowRow {
  return {
    orden,
    descripcion: '',
    tipo: '',
    tabla: '',
    config: createEmptyWorkflowConfig(),
  };
}

function createInitialFormState(): BuilderFormState {
  return {
    titulo: '',
    categoria: '',
    subcategoria: '',
    tags: '',
    descripcion: '',
    contenido_md: '',
    tipo_solucion: 'database',
    archivo_md: null,
    workflow_rows: [createEmptyWorkflowRow(1)],
    script_json: null,
    script_creation_mode: 'visual_builder',
    unified_json_text: '',
    manual_json_text: '',
  };
}

function getAllowedActionTypes(solutionType: SolutionType) {
  return isScriptSolutionType(solutionType) ? WORKFLOW_ACTION_TYPES : DATABASE_ACTION_TYPES;
}

function sanitizeWorkflowRowForMode(row: WorkflowRow, solutionType: SolutionType): WorkflowRow | null {
  const allowedTypes = getAllowedActionTypes(solutionType);

  if (!row.tipo) {
    return {
      ...row,
      config: {
        ...createEmptyWorkflowConfig(),
        ...(row.config || {}),
      },
    };
  }

  if (!allowedTypes.includes(row.tipo)) {
    return isDatabaseSolutionType(solutionType) && row.tipo === 'validacion' ? null : { ...row, tipo: '' };
  }

  const nextRow: WorkflowRow = {
    ...row,
    tabla: row.tipo === 'validacion' ? '' : row.tabla,
    config: {
      ...createEmptyWorkflowConfig(),
      ...(row.config || {}),
    },
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

function adaptRowsForSolutionType(rows: WorkflowRow[], solutionType: SolutionType) {
  const adapted = rows
    .map((row) => sanitizeWorkflowRowForMode(row, solutionType))
    .filter((row): row is WorkflowRow => Boolean(row));

  return normalizeWorkflowRows(adapted.length > 0 ? adapted : [createEmptyWorkflowRow(1)]);
}

function normalizeWorkflowRows(rows: WorkflowRow[]): WorkflowRow[] {
  return rows.map((row, index) => ({
    ...row,
    orden: index + 1,
    descripcion: String(row.descripcion || ''),
    tipo: row.tipo || '',
    tabla: row.tipo === 'validacion' ? '' : row.tabla || '',
    config: {
      ...createEmptyWorkflowConfig(),
      ...(row.config || {}),
    },
  }));
}

function getFirstEntry(record: unknown): [string, unknown] | null {
  if (!record || typeof record !== 'object' || Array.isArray(record)) return null;
  const entries = Object.entries(record as Record<string, unknown>);
  return entries[0] || null;
}

function parseScriptSource(scriptJson: KnowledgeArticle['script_json']) {
  if (!scriptJson) return null;
  if (typeof scriptJson === 'string') {
    try {
      return JSON.parse(scriptJson);
    } catch {
      return null;
    }
  }
  return scriptJson;
}

function parseWorkflowRowsFromScript(scriptJson: KnowledgeArticle['script_json']): WorkflowRow[] {
  const source = parseScriptSource(scriptJson);
  if (!source || typeof source !== 'object' || Array.isArray(source)) {
    return [createEmptyWorkflowRow(1)];
  }

  const candidate = source as Record<string, unknown>;
  const workflow = Array.isArray(candidate.workflow)
    ? candidate.workflow
    : candidate.tipo && typeof candidate.tipo === 'string'
      ? [candidate]
      : [];
  if (workflow.length === 0) {
    return [createEmptyWorkflowRow(1)];
  }

  return normalizeWorkflowRows(
    workflow.map((rawStep, index) => {
      const step = rawStep as Record<string, unknown>;
      const row = createEmptyWorkflowRow(index + 1);
      const type = String(step.tipo || '').trim().toLowerCase();
      row.descripcion = String(step.descripcion || `Paso ${index + 1}`);
      row.tipo = WORKFLOW_ACTION_TYPES.includes(type as WorkflowActionType) ? (type as WorkflowActionType) : '';

      if (row.tipo === 'validacion') {
        row.config.variable = String(step.variable || '');
        row.config.condicion = VALIDATION_CONDITIONS.includes(String(step.condicion || '').trim().toLowerCase() as WorkflowValidationCondition)
          ? (String(step.condicion || '').trim().toLowerCase() as WorkflowValidationCondition)
          : 'existe';
        row.config.valorValidacion = step.valor === undefined ? '' : String(step.valor);
        row.config.mensajeError = String(step.mensaje_error || '');
        return row;
      }

      row.tabla = WORKFLOW_TABLES.includes(String(step.tabla || '').trim().toLowerCase() as WorkflowTableName)
        ? (String(step.tabla || '').trim().toLowerCase() as WorkflowTableName)
        : '';
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
                ? (joinItem.on as Record<string, unknown>)
                : {};
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

          if (row.config.join_pairs.length > 0) {
            row.config.query_mode = 'compuesta';
          }
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

function parsePrimitiveValue(rawValue: string) {
  const trimmed = String(rawValue || '').trim();
  if (!trimmed) return '';
  if (/^\{\{.+\}\}$/.test(trimmed)) return trimmed;
  if (/^-?\d+(\.\d+)?$/.test(trimmed)) return Number(trimmed);
  if (/^(true|false)$/i.test(trimmed)) return trimmed.toLowerCase() === 'true';
  if (/^null$/i.test(trimmed)) return null;
  return trimmed;
}

function validateWorkflowRows(rows: WorkflowRow[], solutionType: SolutionType): WorkflowValidationResult {
  const normalizedRows = normalizeWorkflowRows(rows);
  const rowErrors: Record<number, string[]> = {};
  const allowedTypes = getAllowedActionTypes(solutionType);

  if (normalizedRows.length === 0) {
    return {
      valid: false,
      generalError: 'Debes agregar al menos un paso al workflow.',
      rowErrors: {},
    };
  }

  normalizedRows.forEach((row, index) => {
    const errors: string[] = [];
    if (!row.descripcion.trim()) errors.push('La descripción es obligatoria.');
    if (!row.tipo) errors.push('Debes seleccionar un tipo.');
    if (row.tipo && !allowedTypes.includes(row.tipo)) {
      errors.push(`El tipo ${row.tipo} no está permitido en este modo.`);
    }

    if (row.tipo && row.tipo !== 'validacion' && !row.tabla) {
      errors.push('Debes seleccionar una tabla.');
    }

    if (row.tipo === 'select' || row.tipo === 'insert') {
      if (!row.config.campo.trim()) errors.push('El campo es obligatorio.');
      if (!row.config.valor.trim()) errors.push('El valor es obligatorio.');
    }

    if (row.tipo === 'update') {
      if (!row.config.campoObjetivo.trim()) errors.push('El campo a actualizar es obligatorio.');
      if (!row.config.valorNuevo.trim()) errors.push('El valor nuevo es obligatorio.');
      if (!row.config.condicionCampo.trim()) errors.push('La condición where es obligatoria.');
      if (!row.config.condicionValor.trim()) errors.push('El valor de la condición es obligatorio.');
    }

    if (row.tipo === 'delete') {
      if (!row.config.condicionCampo.trim()) errors.push('El campo condición es obligatorio.');
      if (!row.config.condicionValor.trim()) errors.push('El valor condición es obligatorio.');
    }

    if (isScriptSolutionType(solutionType) && row.tipo === 'validacion') {
      if (!row.config.variable.trim()) errors.push('La variable a validar es obligatoria.');
      if (!row.config.condicion) errors.push('La condición es obligatoria.');
      if (row.config.condicion === 'igual' && !row.config.valorValidacion.trim()) {
        errors.push('Debes indicar el valor esperado para la condición igual.');
      }
    }

    if (errors.length > 0) {
      rowErrors[index] = errors;
    }
  });

  const valid = Object.keys(rowErrors).length === 0;
  return {
    valid,
    generalError: valid ? '' : 'Completa los campos requeridos antes de guardar.',
    rowErrors,
  };
}

function buildWorkflowScriptJson(rows: WorkflowRow[], solutionType: SolutionType) {
  const normalizedRows = adaptRowsForSolutionType(rows, solutionType);
  const workflow = normalizedRows.map((row) => {
    if (isScriptSolutionType(solutionType) && row.tipo === 'validacion') {
      return {
        tipo: 'validacion',
        descripcion: row.descripcion.trim(),
        variable: row.config.variable.trim(),
        condicion: row.config.condicion || 'existe',
        ...(row.config.condicion === 'igual' ? { valor: parsePrimitiveValue(row.config.valorValidacion) } : {}),
        ...(row.config.mensajeError.trim() ? { mensaje_error: row.config.mensajeError.trim() } : {}),
      };
    }

    const baseStep = {
      tipo: row.tipo,
      descripcion: row.descripcion.trim(),
      tabla: row.tabla,
      ...(isScriptSolutionType(solutionType) && row.config.guardarEn.trim() ? { guardar_en: row.config.guardarEn.trim() } : {}),
    };

    if (row.tipo === 'select') {
      return {
        ...baseStep,
        where: {
          [row.config.campo.trim()]: parsePrimitiveValue(row.config.valor),
        },
      };
    }

    if (row.tipo === 'insert') {
      return {
        ...baseStep,
        data: {
          [row.config.campo.trim()]: parsePrimitiveValue(row.config.valor),
        },
      };
    }

    if (row.tipo === 'update') {
      return {
        ...baseStep,
        set: {
          [row.config.campoObjetivo.trim()]: parsePrimitiveValue(row.config.valorNuevo),
        },
        where: {
          [row.config.condicionCampo.trim()]: parsePrimitiveValue(row.config.condicionValor),
        },
      };
    }

    return {
      ...baseStep,
      where: {
        [row.config.condicionCampo.trim()]: parsePrimitiveValue(row.config.condicionValor),
      },
    };
  });

  if (solutionType === 'script') {
    return {
      modo: 'script',
      origen: 'visual-workflow-builder',
      workflow,
    };
  }

  return workflow.length === 1 ? workflow[0] : { workflow };
}

function parseJsonText(value: string): { ok: true; parsed: Record<string, unknown> } | { ok: false; error: string } {
  try {
    const parsed = JSON.parse(value);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      return { ok: false, error: 'El JSON debe ser un objeto.' };
    }
    return { ok: true, parsed };
  } catch {
    return { ok: false, error: 'JSON inválido.' };
  }
}

function mapSqlOperatorToWorkflow(operator: string) {
  const normalized = String(operator || '').trim();
  if (normalized === '=') return 'eq';
  if (normalized === '!=') return 'neq';
  if (normalized === '<>') return 'neq';
  if (normalized === '>') return 'gt';
  if (normalized === '>=') return 'gte';
  if (normalized === '<') return 'lt';
  if (normalized === '<=') return 'lte';
  return 'eq';
}

function normalizeSqlFieldRef(rawFieldRef: string) {
  const trimmed = String(rawFieldRef || '').trim().replace(/["`\[\]]/g, '');
  if (!trimmed) return '';
  const match = trimmed.match(/^([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)$/);
  if (match) return `${match[1]}.${match[2]}`;
  return trimmed;
}

function parseSqlValueToken(rawToken: string) {
  const token = String(rawToken || '').trim().replace(/;\s*$/, '');
  if (!token) return '';

  const parameterMatch = token.match(/^\$(\d+)$/);
  if (parameterMatch) {
    return `{{param_${parameterMatch[1]}}}`;
  }

  const stringMatch = token.match(/^'(.*)'$/s);
  if (stringMatch) {
    return stringMatch[1].replace(/''/g, "'");
  }

  return parsePrimitiveValue(token);
}

const SQL_FORBIDDEN_KEYWORDS = /\b(drop|alter|truncate|grant|revoke|create\s+role|create\s+user|drop\s+database|drop\s+schema)\b/i;
const SQL_ALLOWED_STATEMENT_START = /^(select|with|insert|update|delete|do|begin|declare)\b/i;

// Mirrors backend PlSqlInterpreter detection
const PLSQL_MARKER_RE = /\b(DECLARE|EXCEPTION|ELSIF)\b|\bIF\b[\s\S]{0,600}\bTHEN\b|\bFOR\b\s+\w+\s+IN\b|\bBEGIN\b[\s\S]{5,}\bEND\b/i;

function isComplexSqlScript(sql: string): boolean {
  return PLSQL_MARKER_RE.test(String(sql || ''));
}

function splitManualSqlStatements(sourceText: string) {
  return String(sourceText || '')
    .split(';')
    .map((item) => item.trim())
    .filter(Boolean);
}

function stripLeadingSqlComments(sourceText: string) {
  return String(sourceText || '')
    .replace(/^(?:\s*(?:--[^\n]*(?:\r?\n|$)|\/\*[\s\S]*?\*\/))+/, '')
    .trim();
}

function validateManualSqlInput(sourceText: string): { ok: true; sql: string } | { ok: false; error: string } {
  const sql = String(sourceText || '').trim();
  if (!sql) {
    return { ok: false, error: 'Debes ingresar JSON o SQL válido.' };
  }

  // Complex PL/SQL procedural scripts are accepted as-is
  if (isComplexSqlScript(sql)) {
    return { ok: true, sql };
  }

  const statements = splitManualSqlStatements(sql);
  if (statements.length === 0) {
    return { ok: false, error: 'Debes ingresar al menos una consulta SQL.' };
  }

  for (const statement of statements) {
    const cleaned = stripLeadingSqlComments(statement.replace(/;\s*$/, '').trim());
    if (!SQL_ALLOWED_STATEMENT_START.test(cleaned)) {
      return { ok: false, error: 'En Script Manual solo se aceptan sentencias SQL de base de datos (SELECT/UPDATE/DELETE/INSERT/WITH/DO/BEGIN).' };
    }

    if (SQL_FORBIDDEN_KEYWORDS.test(cleaned)) {
      return { ok: false, error: 'SQL contiene operaciones peligrosas bloqueadas (DROP/ALTER/TRUNCATE/GRANT/REVOKE).' };
    }
  }

  const unifiedSql = statements.join(';\n\n');

  return { ok: true, sql: unifiedSql };
}

function trimOuterParentheses(input: string) {
  let value = String(input || '').trim();
  if (!value) return value;

  let changed = true;
  while (changed) {
    changed = false;
    if (!(value.startsWith('(') && value.endsWith(')'))) break;

    let depth = 0;
    let valid = true;
    for (let i = 0; i < value.length; i += 1) {
      const char = value[i];
      if (char === '(') depth += 1;
      if (char === ')') depth -= 1;
      if (depth === 0 && i < value.length - 1) {
        valid = false;
        break;
      }
      if (depth < 0) {
        valid = false;
        break;
      }
    }

    if (valid && depth === 0) {
      value = value.slice(1, -1).trim();
      changed = true;
    }
  }

  return value;
}

function splitWhereByTopLevelLogic(whereText: string) {
  const text = String(whereText || '').trim();
  if (!text) return { conditions: [], logic: 'AND', mixed: false };

  const parts: string[] = [];
  const operators: string[] = [];

  let depth = 0;
  let inSingleQuote = false;
  let current = '';

  for (let i = 0; i < text.length; i += 1) {
    const char = text[i];

    if (char === "'" && text[i - 1] !== '\\') {
      inSingleQuote = !inSingleQuote;
      current += char;
      continue;
    }

    if (!inSingleQuote) {
      if (char === '(') {
        depth += 1;
        current += char;
        continue;
      }

      if (char === ')') {
        depth = Math.max(0, depth - 1);
        current += char;
        continue;
      }

      if (depth === 0) {
        const rest = text.slice(i);
        const andMatch = rest.match(/^\s+and\s+/i);
        const orMatch = rest.match(/^\s+or\s+/i);

        if (andMatch || orMatch) {
          parts.push(current.trim());
          operators.push(andMatch ? 'AND' : 'OR');
          current = '';
          i += (andMatch ? andMatch[0].length : orMatch![0].length) - 1;
          continue;
        }
      }
    }

    current += char;
  }

  if (current.trim()) {
    parts.push(current.trim());
  }

  const uniqueOps = Array.from(new Set(operators));
  return {
    conditions: parts.map((part) => trimOuterParentheses(part)).filter(Boolean),
    logic: uniqueOps[0] || 'AND',
    mixed: uniqueOps.length > 1,
  };
}

function convertSqlSelectToWorkflow(sourceText: string): { ok: true; parsed: Record<string, unknown> } | { ok: false; error: string } {
  const validation = validateManualSqlInput(sourceText);
  if (!validation.ok) {
    return { ok: false, error: validation.error };
  }

  return {
    ok: true,
    parsed: {
      modo: 'script',
      origen: 'manual-sql',
      sql: validation.sql,
    },
  };
}

function parseFieldRef(fieldRef: string, defaultTable: string) {
  const raw = String(fieldRef || '').trim();
  if (!raw) return null;
  const parts = raw.split('.').map((part) => part.trim()).filter(Boolean);
  if (parts.length === 1) {
    return { table: defaultTable, column: parts[0] };
  }
  if (parts.length === 2) {
    return { table: parts[0], column: parts[1] };
  }
  return null;
}

function validateScriptAgainstSchema(script: Record<string, unknown>): string[] {
  const errors: string[] = [];
  const tipo = String(script.tipo || '').trim().toLowerCase();
  if (!tipo) errors.push('tipo requerido');

  // Frontend acts as passthrough for multi-DB execution.
  // Keep only structural JSON validation and leave table/column validation to backend.
  const table = String(script.tabla || '').trim();
  if (!table && !Array.isArray(script.workflow) && !Array.isArray(script.join) && !script.accion) {
    errors.push('tabla requerida');
  }

  const validateFieldMap = (obj: unknown, defaultTable: string, label: string) => {
    if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return;
    Object.keys(obj as Record<string, unknown>).forEach((fieldRef) => {
      const parsed = parseFieldRef(fieldRef, defaultTable);
      if (!parsed) {
        errors.push(`${label}: campo inválido ${fieldRef}`);
      }
    });
  };

  if (table) {
    validateFieldMap(script.where, table, 'where');
    validateFieldMap(script.set, table, 'set');
    validateFieldMap(script.data, table, 'data');
  }

  if (Array.isArray(script.join)) {
    (script.join as Array<Record<string, unknown>>).forEach((joinItem, index) => {
      const joinTable = String(joinItem?.tabla || '').trim();
      if (!joinTable) errors.push(`join[${index}]: tabla requerida`);

      const onObject = joinItem?.on;
      if (!onObject || typeof onObject !== 'object' || Array.isArray(onObject)) {
        errors.push(`join[${index}]: on inválido`);
        return;
      }

      Object.entries(onObject as Record<string, unknown>).forEach(([left, right]) => {
        const leftRef = parseFieldRef(left, table);
        const rightRef = parseFieldRef(String(right || ''), joinTable);
        if (!leftRef || !rightRef) {
          errors.push(`join[${index}]: referencia on inválida`);
        }
      });
    });
  }

  return errors;
}

function inferCreationModeFromScript(scriptSource: unknown): ScriptCreationMode {
  if (!scriptSource || typeof scriptSource !== 'object' || Array.isArray(scriptSource)) {
    return 'visual_builder';
  }

  const script = scriptSource as Record<string, unknown>;
  if (Array.isArray(script.workflow)) return 'visual_builder';
  if (script.tipo || script.workflow || script.join || script.accion) return 'json_unificado';
  return 'codigo_manual';
}

function computeScriptForMode(form: BuilderFormState) {
  const mode = form.script_creation_mode;

  if (isDatabaseSolutionType(form.tipo_solucion) && mode === 'codigo_manual') {
    return {
      valid: false,
      error: 'Código Manual solo está disponible en modo Script.',
      scriptJson: null,
    };
  }

  if (mode === 'visual_builder') {
    const validation = validateWorkflowDefinition(form.workflow_rows as any, form.tipo_solucion as any);
    if (!validation.isValid) {
      return { valid: false, error: validation.errors?.[0] || 'Workflow inválido', scriptJson: null };
    }
    const visualScript = form.script_json ?? generateScriptJson(form.workflow_rows as any, form.tipo_solucion as any);
    return { valid: Boolean(visualScript), error: visualScript ? '' : 'No se pudo generar script', scriptJson: visualScript };
  }

  const sourceText = mode === 'json_unificado'
    ? form.unified_json_text
    : form.manual_json_text;

  if (!sourceText.trim()) {
    return { valid: false, error: mode === 'codigo_manual' ? 'Debes ingresar JSON o SQL SELECT.' : 'Debes ingresar un JSON.', scriptJson: null };
  }

  const parsed = parseJsonText(sourceText);
  if (!parsed.ok) {
    if (mode === 'codigo_manual') {
      const convertedSql = convertSqlSelectToWorkflow(sourceText);
      if (!convertedSql.ok) {
        return { valid: false, error: convertedSql.error, scriptJson: null };
      }
      return { valid: true, error: '', scriptJson: convertedSql.parsed };
    }
    return { valid: false, error: parsed.error, scriptJson: null };
  }

  if (mode === 'json_unificado') {
    const unifiedErrors = validateScriptAgainstSchema(parsed.parsed);
    if (!parsed.parsed.tipo && !Array.isArray(parsed.parsed.workflow) && !parsed.parsed.accion) {
      unifiedErrors.push('El JSON debe incluir tipo, accion o workflow');
    }
    if (unifiedErrors.length > 0) {
      return { valid: false, error: unifiedErrors[0], scriptJson: null };
    }
  }

  if (mode === 'codigo_manual') {
    if (!parsed.parsed.workflow && !parsed.parsed.tipo && !parsed.parsed.accion) {
      return { valid: false, error: 'Código Manual requiere workflow, tipo o accion', scriptJson: null };
    }
  }

  return { valid: true, error: '', scriptJson: parsed.parsed };
}

function getPanelPreferenceKey(username: string, role: string) {
  return `panelPrefs:${username}:${role}`;
}

function updateRows(rows: WorkflowRow[], index: number, patch: Partial<WorkflowRow>) {
  return normalizeWorkflowRows(
    rows.map((row, rowIndex) => {
      if (rowIndex !== index) return row;
      return {
        ...row,
        ...patch,
        config: patch.config ? { ...row.config, ...patch.config } : row.config,
      };
    })
  );
}

function updateRowConfig(rows: WorkflowRow[], index: number, patch: Partial<WorkflowStepConfig>) {
  return normalizeWorkflowRows(
    rows.map((row, rowIndex) => {
      if (rowIndex !== index) return row;
      return {
        ...row,
        config: {
          ...row.config,
          ...patch,
        },
      };
    })
  );
}

function AdminPage() {
  const router = useRouter();
  const { theme } = useTheme();
  const isLightTheme = theme === 'light';

  const [articles, setArticles] = useState<KnowledgeArticle[]>([]);
  const [loading, setLoading] = useState(true);
  const [showUploadModal, setShowUploadModal] = useState(false);
  const [showEditModal, setShowEditModal] = useState(false);
  const [selectedArticle, setSelectedArticle] = useState<KnowledgeArticle | null>(null);
  const [selectedCategory, setSelectedCategory] = useState('');
  const [selectedSubcategory, setSelectedSubcategory] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [commentsCollapsed, setCommentsCollapsed] = useState(false);
  const [currentUser, setCurrentUser] = useState<{ username: string; role: string } | null>(null);
  const [categories, setCategories] = useState<Array<{ categoria: string; subcategorias: string[] }>>([]);
  const [articleCount, setArticleCount] = useState(0);
  const [uploadForm, setUploadForm] = useState<BuilderFormState>(createInitialFormState());
  const [editForm, setEditForm] = useState<BuilderFormState>({ ...createInitialFormState(), id: '' });
  const [uploadAttempted, setUploadAttempted] = useState(false);
  const [editAttempted, setEditAttempted] = useState(false);
  const [uploadSaving, setUploadSaving] = useState(false);
  const [editSaving, setEditSaving] = useState(false);
  const [uploadError, setUploadError] = useState('');
  const [editError, setEditError] = useState('');
  const [assistantQuery, setAssistantQuery] = useState<any | null>(null);
  const [, startWorkflowSyncTransition] = useTransition();

  const handleUploadWorkflowChange = useCallback((data: any) => {
    startWorkflowSyncTransition(() => {
      setUploadForm((prev) => ({
        ...prev,
        tipo_solucion: data.tipo_solucion,
        workflow_rows: data.workflow_rows,
        script_json: data.script_json ?? null,
      }));
    });
  }, []);

  const handleEditWorkflowChange = useCallback((data: any) => {
    startWorkflowSyncTransition(() => {
      setEditForm((prev) => ({
        ...prev,
        tipo_solucion: data.tipo_solucion,
        workflow_rows: data.workflow_rows,
        script_json: data.script_json ?? null,
      }));
    });
  }, []);

  const handleAssistantQuerySelected = useCallback((query: any) => {
    setAssistantQuery({ ...query, _ts: Date.now() });
  }, []);

  const clearAssistantQuery = useCallback(() => {
    setAssistantQuery(null);
  }, []);

  const buildCategoriesFromArticles = (source: KnowledgeArticle[]) => {
    const categoryMap = new Map<string, Set<string>>();

    source.forEach((article) => {
      const cat = article.categoria || 'Sin categoria';
      const sub = article.subcategoria || 'General';
      if (!categoryMap.has(cat)) categoryMap.set(cat, new Set());
      categoryMap.get(cat)?.add(sub);
    });

    return Array.from(categoryMap.entries()).map(([categoria, subcats]) => ({
      categoria,
      subcategorias: Array.from(subcats),
    }));
  };

  useEffect(() => {
    const userJson = localStorage.getItem('user');
    if (userJson) {
      try {
        setCurrentUser(JSON.parse(userJson));
      } catch (error) {
        console.error('Error parseando usuario:', error);
      }
    }

    const fetchArticles = async () => {
      setLoading(true);
      try {
        const res = await fetch('/api/articles');
        const data = await res.json();
        const fetched: KnowledgeArticle[] = data?.success && Array.isArray(data.data) ? data.data : Array.isArray(data) ? data : [];
        setArticles(fetched);
        setArticleCount(fetched.length);
        setSelectedArticle(null);
        const computedCategories = buildCategoriesFromArticles(fetched);
        setCategories(computedCategories);
        if (computedCategories.length > 0) {
          setSelectedCategory(computedCategories[0].categoria);
          setSelectedSubcategory(computedCategories[0].subcategorias[0] || '');
        }
      } catch (error) {
        console.error('Error cargando articulos:', error);
      } finally {
        setLoading(false);
      }
    };

    fetchArticles();
  }, []);

  useEffect(() => {
    if (!currentUser?.username || !currentUser?.role) return;
    const key = getPanelPreferenceKey(currentUser.username, currentUser.role);
    const raw = localStorage.getItem(key);
    if (!raw) return;

    try {
      const parsed = JSON.parse(raw);
      setSidebarCollapsed(Boolean(parsed.sidebarCollapsed));
      setCommentsCollapsed(Boolean(parsed.commentsCollapsed));
    } catch {
      // Ignore malformed localStorage values
    }
  }, [currentUser]);

  useEffect(() => {
    if (!currentUser?.username || !currentUser?.role) return;
    const key = getPanelPreferenceKey(currentUser.username, currentUser.role);
    localStorage.setItem(key, JSON.stringify({ sidebarCollapsed, commentsCollapsed }));
  }, [sidebarCollapsed, commentsCollapsed, currentUser]);

  useEffect(() => {
    setCategories(buildCategoriesFromArticles(articles));
    setArticleCount(articles.length);
  }, [articles]);

  const visibleArticles = useMemo(() => {
    if (!searchQuery.trim()) return articles;
    const query = searchQuery.toLowerCase();
    return articles.filter((article) => {
      return (
        article.titulo.toLowerCase().includes(query) ||
        (article.descripcion?.toLowerCase().includes(query) ?? false) ||
        (article.contenido_md?.toLowerCase().includes(query) ?? false) ||
        article.subcategoria.toLowerCase().includes(query) ||
        article.categoria.toLowerCase().includes(query) ||
        article.tags?.some((tag) => tag.toLowerCase().includes(query))
      );
    });
  }, [articles, searchQuery]);

  const categoryOptions = useMemo(() => {
    return Array.from(new Set(articles.map((article) => (article.categoria || '').trim()).filter(Boolean))).sort((a, b) => a.localeCompare(b));
  }, [articles]);

  const uploadWorkflowValidation = useMemo(() => validateWorkflowRows(uploadForm.workflow_rows, uploadForm.tipo_solucion), [uploadForm.workflow_rows, uploadForm.tipo_solucion]);
  const editWorkflowValidation = useMemo(() => validateWorkflowRows(editForm.workflow_rows, editForm.tipo_solucion), [editForm.workflow_rows, editForm.tipo_solucion]);

  const uploadGeneratedScript = useMemo(() => {
    if (!isExecutableSolutionType(uploadForm.tipo_solucion) || !uploadWorkflowValidation.valid) return null;
    return buildWorkflowScriptJson(uploadForm.workflow_rows, uploadForm.tipo_solucion);
  }, [uploadForm.workflow_rows, uploadForm.tipo_solucion, uploadWorkflowValidation.valid]);

  const editGeneratedScript = useMemo(() => {
    if (!isExecutableSolutionType(editForm.tipo_solucion) || !editWorkflowValidation.valid) return null;
    return buildWorkflowScriptJson(editForm.workflow_rows, editForm.tipo_solucion);
  }, [editForm.workflow_rows, editForm.tipo_solucion, editWorkflowValidation.valid]);

  const uploadEnterpriseValidation = useMemo(() => {
    if (!isExecutableSolutionType(uploadForm.tipo_solucion)) {
      return { isValid: true, errors: [] as string[] };
    }
    const result = computeScriptForMode(uploadForm);
    return { isValid: result.valid, errors: result.error ? [result.error] : [] as string[] };
  }, [uploadForm]);

  const editEnterpriseValidation = useMemo(() => {
    if (!isExecutableSolutionType(editForm.tipo_solucion)) {
      return { isValid: true, errors: [] as string[] };
    }
    const result = computeScriptForMode(editForm);
    return { isValid: result.valid, errors: result.error ? [result.error] : [] as string[] };
  }, [editForm]);

  const uploadModeResult = useMemo(() => {
    if (!isExecutableSolutionType(uploadForm.tipo_solucion)) {
      return { valid: true, error: '', scriptJson: null as unknown };
    }
    return computeScriptForMode(uploadForm);
  }, [uploadForm]);

  const editModeResult = useMemo(() => {
    if (!isExecutableSolutionType(editForm.tipo_solucion)) {
      return { valid: true, error: '', scriptJson: null as unknown };
    }
    return computeScriptForMode(editForm);
  }, [editForm]);

  const handleLogout = () => {
    localStorage.removeItem('token');
    localStorage.removeItem('user');
    router.push('/login');
  };

  const openUploadModal = () => {
    if (currentUser?.role !== 'admin') {
      alert('Solo admin puede subir soluciones.');
      return;
    }
    setUploadAttempted(false);
    setUploadSaving(false);
    setUploadError('');
    setUploadForm(createInitialFormState());
    setShowUploadModal(true);
  };

  const openEditModal = (article: KnowledgeArticle) => {
    if (currentUser?.role !== 'admin') {
      alert('Solo admin puede editar soluciones.');
      return;
    }

    const normalizedType = normalizeSolutionType(article.tipo_solucion);
    const parsedSource = parseScriptSource(article.script_json);
    const inferredCreationMode = inferCreationModeFromScript(parsedSource);
    const hasWorkflow = Boolean(parsedSource && typeof parsedSource === 'object' && !Array.isArray(parsedSource) && Array.isArray((parsedSource as Record<string, unknown>).workflow));
    const effectiveType: SolutionType = normalizedType === 'lectura' ? 'lectura' : normalizedType === 'script' || hasWorkflow ? normalizedType : 'database';
    const availableModes = getCreationModesForSolutionType(effectiveType);
    const normalizedCreationMode = availableModes.some((mode) => mode.value === inferredCreationMode)
      ? inferredCreationMode
      : (availableModes[0]?.value || 'codigo_manual');

    setSelectedArticle(article);
    setEditAttempted(false);
    setEditSaving(false);
    setEditError('');
    setEditForm({
      id: article.id,
      titulo: article.titulo || '',
      categoria: article.categoria || '',
      subcategoria: article.subcategoria || '',
      tags: (article.tags || []).join(', '),
      descripcion: article.descripcion || '',
      contenido_md: article.contenido_md || '',
      tipo_solucion: effectiveType,
      workflow_rows: adaptRowsForSolutionType(parseWorkflowRowsFromScript(article.script_json), effectiveType),
      script_json: parsedSource,
      script_creation_mode: normalizedCreationMode,
      unified_json_text: normalizedCreationMode === 'json_unificado' ? JSON.stringify(parsedSource, null, 2) : '',
      manual_json_text: normalizedCreationMode === 'codigo_manual' ? JSON.stringify(parsedSource, null, 2) : '',
    });
    setShowEditModal(true);
  };

  const handleArticleSelect = (article: KnowledgeArticle) => {
    setSelectedArticle(article);
    setSelectedCategory(article.categoria);
    setSelectedSubcategory(article.subcategoria);
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setUploadForm((prev) => ({ ...prev, archivo_md: file }));
    const reader = new FileReader();
    reader.onload = (evt) => {
      const content = evt.target?.result as string;
      setUploadForm((prev) => ({ ...prev, contenido_md: content }));
      const titleMatch = content.match(/^#\s+(.+)$/m);
      if (titleMatch && !uploadForm.titulo) {
        setUploadForm((prev) => ({ ...prev, titulo: titleMatch[1].trim() }));
      }
    };
    reader.readAsText(file);
  };

  const addWorkflowRow = (rows: WorkflowRow[]) => normalizeWorkflowRows([...rows, createEmptyWorkflowRow(rows.length + 1)]);

  const removeWorkflowRow = (rows: WorkflowRow[], index: number) => {
    const next = rows.filter((_, rowIndex) => rowIndex !== index);
    return normalizeWorkflowRows(next.length > 0 ? next : [createEmptyWorkflowRow(1)]);
  };

  const submitArticle = async (mode: 'create' | 'edit') => {
    const isEdit = mode === 'edit';
    const form = isEdit ? editForm : uploadForm;
    const validation = isEdit ? editWorkflowValidation : uploadWorkflowValidation;
    const setAttempted = isEdit ? setEditAttempted : setUploadAttempted;
    const setSaving = isEdit ? setEditSaving : setUploadSaving;
    const setError = isEdit ? setEditError : setUploadError;

    setAttempted(true);
    setError('');

    if (currentUser?.role !== 'admin') {
      alert(isEdit ? 'No tiene permisos para editar soluciones.' : 'No tiene permisos para crear soluciones.');
      return;
    }

    if (!form.titulo.trim() || !form.contenido_md.trim()) {
      alert('Titulo y contenido MD son obligatorios.');
      return;
    }

    if (!form.categoria.trim()) {
      alert('Categoria es obligatoria.');
      return;
    }

    const modeResult = isExecutableSolutionType(form.tipo_solucion)
      ? computeScriptForMode(form)
      : { valid: true, error: '', scriptJson: null };

    if (isExecutableSolutionType(form.tipo_solucion) && !modeResult.valid) {
      alert(modeResult.error || validation.generalError || 'Script inválido para el modo seleccionado.');
      return;
    }

    const scriptJson = modeResult.scriptJson;

    setSaving(true);
    try {
      const token = localStorage.getItem('token');
      const normalizedSubcategory = form.subcategoria.trim() || 'General';
      const payload = {
        titulo: form.titulo.trim(),
        categoria: form.categoria.trim(),
        subcategoria: normalizedSubcategory,
        tags: form.tags,
        descripcion: form.descripcion,
        contenido_md: form.contenido_md,
        tipo_solucion: form.tipo_solucion,
        script_json: scriptJson,
      };

      const response = await fetch(isEdit ? `/api/articles/${form.id}` : '/api/articles', {
        method: isEdit ? 'PUT' : 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-user-role': currentUser?.role || 'user',
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        const errorBody = await response.json();
        const message = errorBody.error || 'Error desconocido';
        setError(message);
        alert(`Error: ${message}`);
        return;
      }

      const data = await response.json();
      if (data.success && data.data) {
        if (isEdit) {
          setArticles((prev) => prev.map((item) => (item.id === data.data.id ? data.data : item)));
          setSelectedArticle(data.data);
          setShowEditModal(false);
        } else {
          setArticles((prev) => [data.data, ...prev]);
          setShowUploadModal(false);
          setUploadForm(createInitialFormState());
          setUploadAttempted(false);
        }
      }
    } catch (error) {
      console.error(isEdit ? 'Error editando articulo:' : 'Error subiendo articulo:', error);
      const message = isEdit ? 'Error al editar el articulo' : 'Error al subir el articulo';
      setError(message);
      alert(message);
    } finally {
      setSaving(false);
    }
  };

  const handleUpload = async (e: React.FormEvent) => {
    e.preventDefault();
    await submitArticle('create');
  };

  const handleEdit = async (e: React.FormEvent) => {
    e.preventDefault();
    await submitArticle('edit');
  };

  const handleDelete = async (articleId: string | number) => {
    if (!confirm('Estas seguro de eliminar este articulo?')) return;
    try {
      const token = localStorage.getItem('token');
      const response = await fetch(`/api/articles/${articleId}`, {
        method: 'DELETE',
        headers: {
          'x-user-role': currentUser?.role || 'user',
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
      });
      if (response.ok) setArticles((prev) => prev.filter((item) => item.id !== articleId));
      else alert('Error al eliminar el articulo');
    } catch (error) {
      console.error('Error eliminando articulo:', error);
      alert('Error al eliminar el articulo');
    }
  };

  const handleSearchChange = (value: string) => {
    setSearchQuery(value);
    if (!value.trim()) setSelectedArticle(null);
  };

  const normalizeNavValue = (value: string) => value.trim().toLowerCase();

  const handleCategorySelect = (categoria: string, subcategoria?: string) => {
    setSelectedCategory(categoria);
    setSelectedSubcategory(subcategoria || '');
    const source = searchQuery.trim() ? visibleArticles : articles;
    const article = source.find((a) => {
      if (normalizeNavValue(a.categoria || '') !== normalizeNavValue(categoria || '')) return false;
      if (!subcategoria) return true;
      return normalizeNavValue(a.subcategoria || '') === normalizeNavValue(subcategoria || '');
    }) || source.find((a) => normalizeNavValue(a.categoria || '') === normalizeNavValue(categoria || ''));
    setSelectedArticle(article || null);
  };

  const renderConfigFields = (
    row: WorkflowRow,
    rowIndex: number,
    solutionType: SolutionType,
    updateFormRows: (updater: (rows: WorkflowRow[]) => WorkflowRow[]) => void,
  ) => {
    const commonInputClass = isLightTheme
      ? 'w-full rounded-md border border-[color:var(--line)] bg-white px-3 py-2 text-sm text-[color:var(--ink-900)] placeholder:text-[color:var(--ink-500)] focus:outline-none focus:ring-2 focus:ring-[color:var(--accent-strong)]'
      : 'w-full rounded-md border border-slate-700 bg-slate-900/80 px-3 py-2 text-sm text-slate-100 placeholder:text-slate-400 focus:outline-none focus:ring-2 focus:ring-cyan-400';
    const enterpriseMode = isScriptSolutionType(solutionType);

    if (row.tipo === 'select') {
      return (
        <div className={`grid grid-cols-1 gap-2 ${enterpriseMode ? 'lg:grid-cols-3' : 'lg:grid-cols-2'}`}>
          <input value={row.config.campo} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { campo: e.target.value }))} placeholder="Campo" className={commonInputClass} />
          <input value={row.config.valor} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { valor: e.target.value }))} placeholder="Valor" className={commonInputClass} />
          {enterpriseMode && <input value={row.config.guardarEn} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { guardarEn: e.target.value }))} placeholder="guardar_en" className={commonInputClass} />}
        </div>
      );
    }

    if (row.tipo === 'insert') {
      return (
        <div className={`grid grid-cols-1 gap-2 ${enterpriseMode ? 'lg:grid-cols-3' : 'lg:grid-cols-2'}`}>
          <input value={row.config.campo} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { campo: e.target.value }))} placeholder="Campo" className={commonInputClass} />
          <input value={row.config.valor} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { valor: e.target.value }))} placeholder="Valor" className={commonInputClass} />
          {enterpriseMode && <input value={row.config.guardarEn} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { guardarEn: e.target.value }))} placeholder="guardar_en" className={commonInputClass} />}
        </div>
      );
    }

    if (row.tipo === 'update') {
      return (
        <div className={`grid grid-cols-1 gap-2 ${enterpriseMode ? 'lg:grid-cols-5' : 'lg:grid-cols-4'}`}>
          <input value={row.config.campoObjetivo} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { campoObjetivo: e.target.value }))} placeholder="Campo a actualizar" className={commonInputClass} />
          <input value={row.config.valorNuevo} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { valorNuevo: e.target.value }))} placeholder="Valor nuevo" className={commonInputClass} />
          <input value={row.config.condicionCampo} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { condicionCampo: e.target.value }))} placeholder="Where campo" className={commonInputClass} />
          <input value={row.config.condicionValor} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { condicionValor: e.target.value }))} placeholder="Where valor" className={commonInputClass} />
          {enterpriseMode && <input value={row.config.guardarEn} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { guardarEn: e.target.value }))} placeholder="guardar_en" className={commonInputClass} />}
        </div>
      );
    }

    if (row.tipo === 'delete') {
      return (
        <div className={`grid grid-cols-1 gap-2 ${enterpriseMode ? 'lg:grid-cols-3' : 'lg:grid-cols-2'}`}>
          <input value={row.config.condicionCampo} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { condicionCampo: e.target.value }))} placeholder="Campo condición" className={commonInputClass} />
          <input value={row.config.condicionValor} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { condicionValor: e.target.value }))} placeholder="Valor condición" className={commonInputClass} />
          {enterpriseMode && <input value={row.config.guardarEn} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { guardarEn: e.target.value }))} placeholder="guardar_en" className={commonInputClass} />}
        </div>
      );
    }

    if (enterpriseMode && row.tipo === 'validacion') {
      return (
        <div className="grid grid-cols-1 gap-2 lg:grid-cols-4">
          <input value={row.config.variable} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { variable: e.target.value }))} placeholder="Variable" className={commonInputClass} />
          <select value={row.config.condicion} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { condicion: e.target.value as WorkflowValidationCondition }))} className={commonInputClass}>
            {VALIDATION_CONDITIONS.map((condition) => (
              <option key={`${rowIndex}-${condition}`} value={condition}>{condition}</option>
            ))}
          </select>
          {row.config.condicion === 'igual' ? (
            <input value={row.config.valorValidacion} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { valorValidacion: e.target.value }))} placeholder="Valor esperado" className={commonInputClass} />
          ) : (
            <div className="flex items-center rounded-md border border-dashed border-[color:var(--line)] px-3 py-2 text-sm text-[color:var(--ink-600)]">Sin valor esperado</div>
          )}
          <input value={row.config.mensajeError} onChange={(e) => updateFormRows((rows) => updateRowConfig(rows, rowIndex, { mensajeError: e.target.value }))} placeholder="Mensaje error (opcional)" className={commonInputClass} />
        </div>
      );
    }

    return <div className="text-sm text-[color:var(--ink-600)]">Selecciona un tipo para configurar este paso.</div>;
  };

  const renderWorkflowBuilder = (
    form: BuilderFormState,
    setForm: React.Dispatch<React.SetStateAction<BuilderFormState>>,
    validation: WorkflowValidationResult,
    attempted: boolean,
    generatedScript: Record<string, unknown> | null,
  ) => {
    const enterpriseMode = isScriptSolutionType(form.tipo_solucion);
    const allowedTypes = getAllowedActionTypes(form.tipo_solucion);
    const updateFormRows = (updater: (rows: WorkflowRow[]) => WorkflowRow[]) => {
      setForm((prev) => ({ ...prev, workflow_rows: updater(prev.workflow_rows) }));
    };

    return (
      <div className="space-y-4 rounded-2xl border border-[color:var(--line)] bg-gradient-to-b from-white to-[color:var(--surface-2)] p-4">
        <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
          <div>
            <p className="text-sm font-semibold text-[color:var(--ink-900)]">Workflow Builder</p>
            <p className="text-xs text-[color:var(--ink-700)]">Una sola tabla visual. El modo de ejecución solo cambia reglas, campos habilitados y payload generado.</p>
          </div>
          <div className="flex items-center gap-3">
            <span className={`rounded-full px-3 py-1 text-xs font-semibold ${enterpriseMode ? 'bg-[#e7f0ff] text-[#1746a2]' : 'bg-[#e8f7ef] text-[#17633b]'}`}>
              {enterpriseMode ? 'Script' : 'Base de Datos'}
            </span>
            <button type="button" onClick={() => updateFormRows((rows) => addWorkflowRow(rows))} className="inline-flex items-center justify-center rounded-lg border border-[color:var(--line)] bg-white px-3 py-2 text-sm font-medium text-[color:var(--ink-900)] hover:bg-[color:var(--surface-2)]">
              + Agregar paso
            </button>
          </div>
        </div>

        <div className="rounded-xl border border-[color:var(--line)] bg-[color:var(--surface-2)] px-4 py-3 text-xs text-[color:var(--ink-700)]">
          {enterpriseMode
            ? 'Permite validaciones, guardar_en, contexto entre pasos y variables reutilizables.'
            : 'Permite solo operaciones select, update, insert y delete. No usa validaciones complejas ni contexto entre pasos.'}
        </div>

        {attempted && !validation.valid && <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">{validation.generalError}</div>}

        <div className="overflow-x-auto rounded-2xl border border-[color:var(--line)] bg-white">
          <table className="min-w-full border-collapse">
            <thead className="bg-[color:var(--surface-2)] text-left text-xs uppercase tracking-[0.08em] text-[color:var(--ink-700)]">
              <tr>
                <th className="px-4 py-3">Orden</th>
                <th className="px-4 py-3">Descripción</th>
                <th className="px-4 py-3">Tipo</th>
                <th className="px-4 py-3">Tabla</th>
                <th className="px-4 py-3">Configuración</th>
                <th className="px-4 py-3 text-right">Eliminar</th>
              </tr>
            </thead>
            <tbody>
              {form.workflow_rows.map((row, rowIndex) => {
                const rowErrors = attempted ? validation.rowErrors[rowIndex] || [] : [];
                const hasErrors = rowErrors.length > 0;

                return (
                  <tr key={`workflow-row-${rowIndex}`} className={hasErrors ? 'bg-red-50/40' : 'bg-white'}>
                    <td className="border-t border-[color:var(--line)] px-4 py-4 align-top">
                      <div className="flex h-10 w-10 items-center justify-center rounded-full bg-[color:var(--surface-2)] text-sm font-semibold text-[color:var(--ink-900)]">{rowIndex + 1}</div>
                    </td>
                    <td className="border-t border-[color:var(--line)] px-4 py-4 align-top">
                      <input value={row.descripcion} onChange={(e) => setForm((prev) => ({ ...prev, workflow_rows: updateRows(prev.workflow_rows, rowIndex, { descripcion: e.target.value }) }))} placeholder="Descripción del paso" className="w-full rounded-md border border-[color:var(--line)] bg-white px-3 py-2 text-sm text-[color:var(--ink-900)] placeholder:text-[color:var(--ink-500)] focus:outline-none focus:ring-2 focus:ring-[color:var(--accent-strong)]" />
                    </td>
                    <td className="border-t border-[color:var(--line)] px-4 py-4 align-top">
                      <select
                        value={row.tipo}
                        onChange={(e) => {
                          const nextType = e.target.value as WorkflowActionType | '';
                          setForm((prev) => ({
                            ...prev,
                            workflow_rows: normalizeWorkflowRows(
                              prev.workflow_rows.map((currentRow, currentIndex) => {
                                if (currentIndex !== rowIndex) return currentRow;
                                return {
                                  ...currentRow,
                                  tipo: nextType,
                                  tabla: nextType === 'validacion' ? '' : currentRow.tabla,
                                  config: {
                                    ...createEmptyWorkflowConfig(),
                                    condicion: 'existe',
                                  },
                                };
                              })
                            ),
                          }));
                        }}
                        className="w-full rounded-md border border-[color:var(--line)] bg-white px-3 py-2 text-sm text-[color:var(--ink-900)] focus:outline-none focus:ring-2 focus:ring-[color:var(--accent-strong)]"
                      >
                        <option value="">Selecciona un tipo</option>
                        {allowedTypes.map((type) => (
                          <option key={`type-${rowIndex}-${type}`} value={type}>{type}</option>
                        ))}
                      </select>
                    </td>
                    <td className="border-t border-[color:var(--line)] px-4 py-4 align-top">
                      {row.tipo === 'validacion' ? (
                        <div className="flex h-10 items-center rounded-md border border-dashed border-[color:var(--line)] px-3 text-sm text-[color:var(--ink-600)]">No aplica</div>
                      ) : (
                        <select value={row.tabla} onChange={(e) => setForm((prev) => ({ ...prev, workflow_rows: updateRows(prev.workflow_rows, rowIndex, { tabla: e.target.value as WorkflowTableName | '' }) }))} className="w-full rounded-md border border-[color:var(--line)] bg-white px-3 py-2 text-sm text-[color:var(--ink-900)] focus:outline-none focus:ring-2 focus:ring-[color:var(--accent-strong)]">
                          <option value="">Selecciona tabla</option>
                          {WORKFLOW_TABLES.map((tableName) => (
                            <option key={`table-${rowIndex}-${tableName}`} value={tableName}>{tableName}</option>
                          ))}
                        </select>
                      )}
                    </td>
                    <td className="border-t border-[color:var(--line)] px-4 py-4 align-top">
                      {renderConfigFields(row, rowIndex, form.tipo_solucion, updateFormRows)}
                      {hasErrors && (
                        <div className="mt-2 space-y-1 text-xs text-red-700">
                          {rowErrors.map((message, errorIndex) => (
                            <p key={`row-${rowIndex}-error-${errorIndex}`}>{message}</p>
                          ))}
                        </div>
                      )}
                    </td>
                    <td className="border-t border-[color:var(--line)] px-4 py-4 align-top text-right">
                      <button type="button" onClick={() => updateFormRows((rows) => removeWorkflowRow(rows, rowIndex))} className="rounded-lg border border-red-200 bg-white px-3 py-2 text-sm font-medium text-red-700 hover:bg-red-50">
                        Eliminar
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        <div className="rounded-2xl border border-[#d8def5] bg-[#0f172a] p-4">
          <div className="mb-2 flex items-center justify-between">
            <p className="text-sm font-semibold text-white">JSON generado automáticamente</p>
            <span className="rounded-full bg-white/10 px-3 py-1 text-xs text-slate-200">Solo lectura</span>
          </div>
          <pre className="overflow-x-auto whitespace-pre-wrap break-words text-xs leading-6 text-slate-200">
            {generatedScript ? JSON.stringify(generatedScript, null, 2) : 'Completa el workflow para generar el script_json.'}
          </pre>
        </div>
      </div>
    );
  };

  const modalInputClass = isLightTheme
    ? 'w-full rounded-lg border border-[color:var(--line)] bg-white p-3 text-[color:var(--ink-900)] placeholder:text-[color:var(--ink-500)]'
    : 'w-full rounded-lg border border-slate-700 bg-slate-900/80 p-3 text-slate-100 placeholder:text-slate-400';
  const modalFileClass = isLightTheme
    ? 'w-full rounded-lg border border-[color:var(--line)] bg-white p-2 text-[color:var(--ink-800)]'
    : 'w-full rounded-lg border border-slate-700 bg-slate-900/80 p-2 text-slate-100';
  const modalTopSectionClass = isLightTheme
    ? 'space-y-4 rounded-2xl border border-[color:var(--line)] bg-[color:var(--surface-2)] p-4'
    : 'space-y-4 rounded-2xl border border-slate-700 bg-slate-900/60 p-4';
  const modalExecSectionClass = isLightTheme
    ? 'space-y-4 rounded-2xl border border-[color:var(--line)] bg-white p-4'
    : 'space-y-4 rounded-2xl border border-slate-700 bg-slate-900/70 p-4';
  const modeSwitchContainerClass = isLightTheme
    ? 'inline-flex rounded-xl border border-[color:var(--line)] bg-white p-1'
    : 'inline-flex rounded-xl border border-slate-700 bg-slate-900/80 p-1';

  return (
    <div className="h-screen flex flex-col overflow-hidden px-3 py-3 sm:px-5 sm:py-5">
      <div className="glass-panel-strong sticky top-0 z-40 overflow-visible rounded-2xl border px-4 sm:px-6">
        <div className="w-full py-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-[#2363eb] to-[#1aa0c8] flex items-center justify-center text-white font-bold shadow-md">S</div>
            <div>
              <h1 className="text-xl font-bold text-[color:var(--ink-900)]">Panel de Administracion - Soluciones MD</h1>
              <p className="text-xs text-[color:var(--ink-700)]">Gestiona articulos, workflows y ejecuciones desde una sola vista.</p>
            </div>
          </div>
          <div className="ml-auto flex items-center gap-3">
            <ThemeToggle className="glass-pill" />
            <span className="glass-pill rounded-full px-3 py-1 text-sm text-[color:var(--ink-700)]">Articulos: {articleCount}</span>
            <UserIdentityBadge
              username={currentUser?.username || 'usuario'}
              role={currentUser?.role || 'user'}
              isLightTheme={isLightTheme}
              primaryActionLabel="Ir a Panel SQL Automático"
              onPrimaryAction={() => router.push('/dashboard')}
              onLogout={handleLogout}
            />
          </div>
        </div>
        <div className="w-full py-3 border-t border-[color:var(--line)]">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <input value={searchQuery} onChange={(e) => handleSearchChange(e.target.value)} placeholder="Buscar..." className="input-glass flex-1 min-w-[220px] p-2 rounded-xl" />
            {currentUser?.role === 'admin' ? (
              <div className="flex gap-2">
                <button onClick={openUploadModal} className="btn-accent px-4 py-2 rounded-xl text-sm font-semibold">Subir Nueva Solucion MD</button>
                {selectedArticle && (
                  <>
                    <button onClick={() => openEditModal(selectedArticle)} className="glass-pill px-4 py-2 rounded-xl text-[color:var(--ink-900)] transition-colors hover:bg-white/85">Editar articulo</button>
                    <button onClick={() => handleDelete(selectedArticle.id)} className="bg-[#fff1f1]/90 text-red-700 border border-red-200 hover:bg-[#ffe3e3] px-4 py-2 rounded-xl transition-colors">Eliminar articulo</button>
                  </>
                )}
              </div>
            ) : (
              <button className="hidden" />
            )}
          </div>
        </div>
      </div>

      <div className="flex-1 min-h-0 w-full py-4">
        <div className="flex gap-4 h-full">
          <aside className={`glass-panel glass-panel-hover rounded-2xl p-3 h-full min-h-0 overflow-hidden transition-[width] duration-300 ease-in-out ${sidebarCollapsed ? 'w-[68px]' : 'w-[320px]'}`}>
            <Sidebar
              categories={categories}
              articles={visibleArticles}
              onSelectCategory={handleCategorySelect}
              onSelectArticle={handleArticleSelect}
              selectedCategory={selectedCategory}
              selectedSubcategory={selectedSubcategory}
              selectedArticle={selectedArticle || undefined}
              collapsed={sidebarCollapsed}
              onToggleCollapse={() => setSidebarCollapsed((prev) => !prev)}
            />
          </aside>

          {!selectedArticle ? (
            <section className="glass-panel-strong glass-panel-hover flex-1 rounded-2xl p-4 h-full min-h-0 flex items-center justify-center">
              <p className="text-lg text-[color:var(--ink-700)]">Selecciona un articulo del menu</p>
            </section>
          ) : (
            <>
              <section className="glass-panel-strong glass-panel-hover flex-1 rounded-2xl p-4 h-full min-h-0 overflow-auto">
                <ContentPanel selectedArticle={selectedArticle} loading={loading} error="" onExecuteSolution={async () => 'OK'} />
              </section>
              <section className={`glass-panel-strong glass-panel-hover rounded-2xl p-4 h-full min-h-0 overflow-auto flex flex-col transition-[width] duration-300 ease-in-out ${commentsCollapsed ? 'w-[68px]' : 'w-[360px]'}`}>
                <CommentsSection articleId={selectedArticle.id} currentUser={currentUser} collapsed={commentsCollapsed} onToggleCollapse={() => setCommentsCollapsed((prev) => !prev)} />
              </section>
            </>
          )}
        </div>
      </div>

      {showUploadModal && (
        <div className="fixed inset-0 bg-black/45 flex items-center justify-center z-50 p-4">
          <div className="glass-panel-strong w-full max-w-7xl rounded-3xl border max-h-[92vh] overflow-hidden">
            <div className="border-b border-[#e6eaf8] px-6 py-5">
              <h2 className="text-2xl font-bold text-[color:var(--ink-900)]">Subir Solucion MD</h2>
              <p className="mt-1 text-sm text-[color:var(--ink-700)]">Configura el workflow visualmente. El sistema generará el script automáticamente.</p>
            </div>
            <form onSubmit={handleUpload} className="flex h-[calc(92vh-96px)] flex-col">
              <div className="space-y-5 overflow-y-auto px-6 py-5">
                {uploadError && <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">{uploadError}</div>}
                <div className={modalTopSectionClass}>
                  <input type="file" accept=".md" onChange={handleFileChange} required className={modalFileClass} />
                  <input type="text" value={uploadForm.titulo} onChange={(e) => setUploadForm((prev) => ({ ...prev, titulo: e.target.value }))} placeholder="Título" className={modalInputClass} required />
                    <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                      <div>
                        <input type="text" value={uploadForm.categoria} onChange={(e) => setUploadForm((prev) => ({ ...prev, categoria: e.target.value }))} placeholder="Categoría" list="upload-category-options" className={modalInputClass} required />
                        <datalist id="upload-category-options">
                          {categoryOptions.map((category) => (
                            <option key={category} value={category} />
                          ))}
                        </datalist>
                      </div>
                    </div>
                    <textarea value={uploadForm.contenido_md} onChange={(e) => setUploadForm((prev) => ({ ...prev, contenido_md: e.target.value }))} placeholder="Contenido MD" className={`min-h-[180px] ${modalInputClass}`} required />
                    <div className="space-y-2">
                      <p className="text-sm font-semibold text-[color:var(--ink-900)]">Nivel 1 · Modo de ejecución</p>
                      <div className={modeSwitchContainerClass}>
                        <button
                          type="button"
                          onClick={() => setUploadForm((prev) => {
                            const nextType: SolutionType = 'database';
                            const availableModes = getCreationModesForSolutionType(nextType);
                            const currentModeAllowed = availableModes.some((mode) => mode.value === prev.script_creation_mode);
                            return {
                              ...prev,
                              tipo_solucion: nextType,
                              workflow_rows: adaptRowsForSolutionType(prev.workflow_rows, nextType),
                              script_creation_mode: currentModeAllowed ? prev.script_creation_mode : availableModes[0].value,
                            };
                          })}
                          className={`rounded-lg px-4 py-2 text-sm font-semibold ${isDatabaseSolutionType(uploadForm.tipo_solucion) ? 'bg-[color:var(--accent-strong)] text-white shadow-sm' : isLightTheme ? 'bg-white text-[color:var(--ink-900)] hover:bg-[color:var(--surface-2)]' : 'bg-slate-900/80 text-slate-100 hover:bg-slate-800'}`}
                        >
                          Base de Datos
                        </button>
                        <button
                          type="button"
                          onClick={() => setUploadForm((prev) => {
                            const nextType: SolutionType = 'script';
                            const availableModes = getCreationModesForSolutionType(nextType);
                            const currentModeAllowed = availableModes.some((mode) => mode.value === prev.script_creation_mode);
                            return {
                              ...prev,
                              tipo_solucion: nextType,
                              workflow_rows: adaptRowsForSolutionType(prev.workflow_rows, nextType),
                              script_creation_mode: currentModeAllowed ? prev.script_creation_mode : availableModes[0].value,
                            };
                          })}
                          className={`rounded-lg px-4 py-2 text-sm font-semibold ${isScriptSolutionType(uploadForm.tipo_solucion) ? 'bg-[color:var(--accent-strong)] text-white shadow-sm' : isLightTheme ? 'bg-white text-[color:var(--ink-900)] hover:bg-[color:var(--surface-2)]' : 'bg-slate-900/80 text-slate-100 hover:bg-slate-800'}`}
                        >
                          Script
                        </button>
                      </div>
                    </div>
                </div>

                {isExecutableSolutionType(uploadForm.tipo_solucion) && (
                  <div className={modalExecSectionClass}>
                    <div className="space-y-2">
                      <p className="text-sm font-semibold text-[color:var(--ink-900)]">Nivel 2 · Forma de creación</p>
                      <div className="flex flex-wrap gap-2">
                        {getCreationModesForSolutionType(uploadForm.tipo_solucion).map((mode) => (
                          <button
                            key={`upload-mode-${mode.value}`}
                            type="button"
                            onClick={() => setUploadForm((prev) => ({ ...prev, script_creation_mode: mode.value }))}
                            className={`rounded-lg border px-3 py-2 text-sm font-semibold ${uploadForm.script_creation_mode === mode.value ? 'border-[color:var(--accent-strong)] bg-[color:var(--accent-strong)] text-white shadow-sm' : isLightTheme ? 'border-[color:var(--line)] bg-white text-[color:var(--ink-900)] hover:bg-[color:var(--surface-2)]' : 'border-slate-700 bg-slate-900/80 text-slate-100 hover:bg-slate-800'}`}
                          >
                            {mode.label} · {mode.level}
                          </button>
                        ))}
                      </div>
                      <p className="text-xs text-[color:var(--ink-700)]">
                        {isDatabaseSolutionType(uploadForm.tipo_solucion)
                          ? 'Base de Datos: Visual Builder y JSON. Código Manual está deshabilitado.'
                          : 'Script: solo Código Manual (JSON workflow).'}
                      </p>
                    </div>

                    {uploadForm.script_creation_mode === 'visual_builder' && (
                      <div>
                        <WorkflowBuilder
                          value={{
                            tipo_solucion: uploadForm.tipo_solucion,
                            workflow_rows: uploadForm.workflow_rows
                          } as any}
                          onChange={handleUploadWorkflowChange as any}
                          assistantQuery={assistantQuery}
                          onAssistantQueryConsumed={clearAssistantQuery}
                          showHistoryPanel={false}
                        />
                      </div>
                    )}

                    {uploadForm.script_creation_mode === 'json_unificado' && (
                      <textarea
                        value={uploadForm.unified_json_text}
                        onChange={(e) => setUploadForm((prev) => ({ ...prev, unified_json_text: e.target.value }))}
                        placeholder={JSON.stringify({ workflow: [{ tipo: 'select', tabla: 'users', columnas: ['id', 'username'], join: [{ tabla: 'employees', on: { 'users.id': 'employees.user_id' }, columnas: ['name'] }], where: { 'users.id': '{{usuario_id}}' }, guardar_en: 'usuario_info' }] }, null, 2)}
                        className="min-h-[260px] w-full rounded-lg border border-[color:var(--line)] bg-[color:var(--surface-2)] p-3 font-mono text-sm text-[color:var(--ink-900)]"
                      />
                    )}

                    {uploadForm.script_creation_mode === 'codigo_manual' && (
                      <div className="space-y-2">
                        <div className="rounded-lg border border-amber-300 bg-amber-50 px-3 py-2 text-xs text-amber-800">
                          Modo avanzado. Puedes escribir JSON de workflow, SQL SELECT multi-paso, o scripts PL/SQL procedurales (DECLARE / BEGIN…END, FOR LOOP, IF/THEN).
                        </div>
                        <textarea
                          value={uploadForm.manual_json_text}
                          onChange={(e) => setUploadForm((prev) => ({ ...prev, manual_json_text: e.target.value }))}
                          placeholder={JSON.stringify({ workflow: [{ tipo: 'select', tabla: 'users', columnas: ['id', 'username'], join: [{ tabla: 'employees', on: { 'users.id': 'employees.user_id' }, columnas: ['cargo'] }], where: { 'users.username': '{{usuario}}' }, guardar_en: 'user_data' }] }, null, 2)}
                          className="min-h-[260px] w-full rounded-lg border border-[color:var(--line)] bg-[#fff9f0] p-3 font-mono text-sm text-[color:var(--ink-900)]"
                        />
                      </div>
                    )}

                    <div className="rounded-xl border border-[color:var(--line)] bg-[#0f172a] p-4">
                      <div className="mb-2 flex items-center justify-between">
                        <p className="text-sm font-semibold text-white">Preview JSON unificado</p>
                        <span className={`rounded px-2 py-1 text-xs ${uploadModeResult.valid ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-700'}`}>
                          {uploadModeResult.valid ? 'Válido' : 'Error'}
                        </span>
                      </div>
                      {!uploadModeResult.valid && (
                        <p className="mb-2 text-xs text-red-300">{uploadModeResult.error}</p>
                      )}
                      <pre className="overflow-x-auto whitespace-pre-wrap break-words text-xs leading-6 text-slate-200">
                        {uploadModeResult.scriptJson ? JSON.stringify(uploadModeResult.scriptJson, null, 2) : 'Completa el contenido según el modo seleccionado.'}
                      </pre>
                    </div>
                  </div>
                )}
              </div>
              <div className="flex justify-end gap-3 border-t border-[color:var(--line)] px-6 py-4">
                <button type="button" onClick={() => setShowUploadModal(false)} className="rounded-lg border border-[color:var(--line)] px-4 py-2 text-[color:var(--ink-800)]">Cancelar</button>
                <button type="submit" disabled={uploadSaving || !uploadEnterpriseValidation.isValid} className="rounded-lg bg-[#25295c] px-5 py-2 text-white disabled:cursor-not-allowed disabled:opacity-60">{uploadSaving ? 'Guardando...' : 'Subir'}</button>
              </div>
            </form>
          </div>
        </div>
      )}

      {showEditModal && selectedArticle && (
        <div className="fixed inset-0 bg-black/45 flex items-center justify-center z-50 p-4">
          <div className="glass-panel-strong w-full max-w-7xl rounded-3xl border max-h-[92vh] overflow-hidden">
            <div className="border-b border-[color:var(--line)] px-6 py-5">
              <h2 className="text-2xl font-bold text-[color:var(--ink-900)]">Editar articulo</h2>
              <p className="mt-1 text-sm text-[color:var(--ink-700)]">Actualiza el workflow visual y el sistema regenerará el script automáticamente.</p>
            </div>
            <form onSubmit={handleEdit} className="flex h-[calc(92vh-96px)] flex-col">
              <div className="space-y-5 overflow-y-auto px-6 py-5">
                {editError && <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">{editError}</div>}
                <div className={modalTopSectionClass}>
                  <input type="text" value={editForm.titulo} onChange={(e) => setEditForm((prev) => ({ ...prev, titulo: e.target.value }))} placeholder="Título" className={modalInputClass} required />
                    <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                      <div>
                        <input type="text" value={editForm.categoria} onChange={(e) => setEditForm((prev) => ({ ...prev, categoria: e.target.value }))} placeholder="Categoría" list="edit-category-options" className={modalInputClass} required />
                        <datalist id="edit-category-options">
                          {categoryOptions.map((category) => (
                            <option key={category} value={category} />
                          ))}
                        </datalist>
                      </div>
                    </div>
                    <textarea value={editForm.contenido_md} onChange={(e) => setEditForm((prev) => ({ ...prev, contenido_md: e.target.value }))} placeholder="Contenido MD" className={`min-h-[180px] ${modalInputClass}`} required />
                    <div className="space-y-2">
                      <p className="text-sm font-semibold text-[color:var(--ink-900)]">Nivel 1 · Modo de ejecución</p>
                      <div className={modeSwitchContainerClass}>
                        <button
                          type="button"
                          onClick={() => setEditForm((prev) => {
                            const nextType: SolutionType = 'database';
                            const availableModes = getCreationModesForSolutionType(nextType);
                            const currentModeAllowed = availableModes.some((mode) => mode.value === prev.script_creation_mode);
                            return {
                              ...prev,
                              tipo_solucion: nextType,
                              workflow_rows: adaptRowsForSolutionType(prev.workflow_rows, nextType),
                              script_creation_mode: currentModeAllowed ? prev.script_creation_mode : availableModes[0].value,
                            };
                          })}
                          className={`rounded-lg px-4 py-2 text-sm font-semibold ${isDatabaseSolutionType(editForm.tipo_solucion) ? 'bg-[color:var(--accent-strong)] text-white shadow-sm' : isLightTheme ? 'bg-white text-[color:var(--ink-900)] hover:bg-[color:var(--surface-2)]' : 'bg-slate-900/80 text-slate-100 hover:bg-slate-800'}`}
                        >
                          Base de Datos
                        </button>
                        <button
                          type="button"
                          onClick={() => setEditForm((prev) => {
                            const nextType: SolutionType = 'script';
                            const availableModes = getCreationModesForSolutionType(nextType);
                            const currentModeAllowed = availableModes.some((mode) => mode.value === prev.script_creation_mode);
                            return {
                              ...prev,
                              tipo_solucion: nextType,
                              workflow_rows: adaptRowsForSolutionType(prev.workflow_rows, nextType),
                              script_creation_mode: currentModeAllowed ? prev.script_creation_mode : availableModes[0].value,
                            };
                          })}
                          className={`rounded-lg px-4 py-2 text-sm font-semibold ${isScriptSolutionType(editForm.tipo_solucion) ? 'bg-[color:var(--accent-strong)] text-white shadow-sm' : isLightTheme ? 'bg-white text-[color:var(--ink-900)] hover:bg-[color:var(--surface-2)]' : 'bg-slate-900/80 text-slate-100 hover:bg-slate-800'}`}
                        >
                          Script
                        </button>
                      </div>
                    </div>
                </div>

                {isExecutableSolutionType(editForm.tipo_solucion) && (
                  <div className={modalExecSectionClass}>
                    <div className="space-y-2">
                      <p className="text-sm font-semibold text-[color:var(--ink-900)]">Nivel 2 · Forma de creación</p>
                      <div className="flex flex-wrap gap-2">
                        {getCreationModesForSolutionType(editForm.tipo_solucion).map((mode) => (
                          <button
                            key={`edit-mode-${mode.value}`}
                            type="button"
                            onClick={() => setEditForm((prev) => ({ ...prev, script_creation_mode: mode.value }))}
                            className={`rounded-lg border px-3 py-2 text-sm font-semibold ${editForm.script_creation_mode === mode.value ? 'border-[color:var(--accent-strong)] bg-[color:var(--accent-strong)] text-white shadow-sm' : isLightTheme ? 'border-[color:var(--line)] bg-white text-[color:var(--ink-900)] hover:bg-[color:var(--surface-2)]' : 'border-slate-700 bg-slate-900/80 text-slate-100 hover:bg-slate-800'}`}
                          >
                            {mode.label} · {mode.level}
                          </button>
                        ))}
                      </div>
                      <p className="text-xs text-[color:var(--ink-700)]">
                        {isDatabaseSolutionType(editForm.tipo_solucion)
                          ? 'Base de Datos: Visual Builder y JSON. Código Manual está deshabilitado.'
                          : 'Script: solo Código Manual (JSON workflow).'}
                      </p>
                    </div>

                    {editForm.script_creation_mode === 'visual_builder' && (
                      <div>
                        <WorkflowBuilder
                          value={{
                            tipo_solucion: editForm.tipo_solucion,
                            workflow_rows: editForm.workflow_rows
                          } as any}
                          onChange={handleEditWorkflowChange as any}
                          assistantQuery={assistantQuery}
                          onAssistantQueryConsumed={clearAssistantQuery}
                          showHistoryPanel={false}
                        />
                      </div>
                    )}

                    {editForm.script_creation_mode === 'json_unificado' && (
                      <textarea
                        value={editForm.unified_json_text}
                        onChange={(e) => setEditForm((prev) => ({ ...prev, unified_json_text: e.target.value }))}
                        placeholder={JSON.stringify({ workflow: [{ tipo: 'select', tabla: 'users', columnas: ['id', 'username'], join: [{ tabla: 'employees', on: { 'users.id': 'employees.user_id' }, columnas: ['name'] }], where: { 'users.id': '{{usuario_id}}' }, guardar_en: 'usuario_info' }] }, null, 2)}
                        className="min-h-[260px] w-full rounded-lg border border-[color:var(--line)] bg-[color:var(--surface-2)] p-3 font-mono text-sm text-[color:var(--ink-900)]"
                      />
                    )}

                    {editForm.script_creation_mode === 'codigo_manual' && (
                      <div className="space-y-2">
                        <div className="rounded-lg border border-amber-300 bg-amber-50 px-3 py-2 text-xs text-amber-800">
                          Modo avanzado. Puedes escribir JSON de workflow, SQL SELECT multi-paso, o scripts PL/SQL procedurales (DECLARE / BEGIN…END, FOR LOOP, IF/THEN).
                        </div>
                        <textarea
                          value={editForm.manual_json_text}
                          onChange={(e) => setEditForm((prev) => ({ ...prev, manual_json_text: e.target.value }))}
                          placeholder={JSON.stringify({ workflow: [{ tipo: 'select', tabla: 'users', columnas: ['id', 'username'], join: [{ tabla: 'employees', on: { 'users.id': 'employees.user_id' }, columnas: ['cargo'] }], where: { 'users.username': '{{usuario}}' }, guardar_en: 'user_data' }] }, null, 2)}
                          className="min-h-[260px] w-full rounded-lg border border-[color:var(--line)] bg-[#fff9f0] p-3 font-mono text-sm text-[color:var(--ink-900)]"
                        />
                      </div>
                    )}

                    <div className="rounded-xl border border-[color:var(--line)] bg-[#0f172a] p-4">
                      <div className="mb-2 flex items-center justify-between">
                        <p className="text-sm font-semibold text-white">Preview JSON unificado</p>
                        <span className={`rounded px-2 py-1 text-xs ${editModeResult.valid ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-700'}`}>
                          {editModeResult.valid ? 'Válido' : 'Error'}
                        </span>
                      </div>
                      {!editModeResult.valid && (
                        <p className="mb-2 text-xs text-red-300">{editModeResult.error}</p>
                      )}
                      <pre className="overflow-x-auto whitespace-pre-wrap break-words text-xs leading-6 text-slate-200">
                        {editModeResult.scriptJson ? JSON.stringify(editModeResult.scriptJson, null, 2) : 'Completa el contenido según el modo seleccionado.'}
                      </pre>
                    </div>
                  </div>
                )}
              </div>
              <div className="flex justify-end gap-3 border-t border-[color:var(--line)] px-6 py-4">
                <button type="button" onClick={() => setShowEditModal(false)} className="rounded-lg border border-[color:var(--line)] px-4 py-2 text-[color:var(--ink-800)]">Cancelar</button>
                <button type="submit" disabled={editSaving || !editEnterpriseValidation.isValid} className="rounded-lg bg-[#25295c] px-5 py-2 text-white disabled:cursor-not-allowed disabled:opacity-60">{editSaving ? 'Guardando...' : 'Guardar'}</button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}

export default withAuth(AdminPage);