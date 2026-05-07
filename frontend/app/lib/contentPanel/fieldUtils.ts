// ─── Execution field validation and normalization ─────────────────────────────
import { ExecutionField } from './types';

function inferParamSemantic(name: string) {
  const n = String(name || '').trim().toLowerCase();
  if (n.includes('date') || n.includes('fecha')) return 'date';
  if (n.includes('role') || n.includes('rol')) return 'role';
  if (n.includes('codigo') || n.includes('code')) return 'code';
  if (n.includes('log')) return 'log';
  if (n.includes('id')) return 'id';
  if (n.includes('name') || n.includes('user') || n.includes('usuario')) return 'user_text';
  return 'text';
}

export function validateExecutionFieldValue(field: ExecutionField, rawValue: string | undefined): string | null {
  const value = String(rawValue ?? '');
  const trimmed = value.trim();

  if (field.required && field.type !== 'checkbox' && !trimmed) return `El campo "${field.label}" es obligatorio`;
  if (!trimmed) return null;

  const semantic = inferParamSemantic(field.name);
  const normalizedName = String(field.name || '').trim().toLowerCase();

  if (semantic === 'id') {
    if (/\s/.test(trimmed)) return `El campo "${field.label}" no puede contener espacios`;
    const requiresUuid = normalizedName === 'user_id' || normalizedName.endsWith('_uuid') || normalizedName.includes('uuid');
    if (requiresUuid && !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(trimmed)) {
      return 'El campo ID debe ser un UUID válido';
    }
  }
  if (semantic === 'role' && !/^[\w\s-]{2,40}$/i.test(trimmed)) return `El campo "${field.label}" contiene un formato inválido`;
  if (semantic === 'code' && !/^[\w-]{1,60}$/i.test(trimmed)) return `El campo "${field.label}" contiene un formato inválido`;
  if (semantic === 'date' && Number.isNaN(new Date(trimmed).getTime())) return `El campo "${field.label}" debe tener una fecha válida`;
  if (field.type === 'number' && !Number.isFinite(Number(trimmed))) return `El campo "${field.label}" debe ser numérico`;
  return null;
}

export function normalizeExecutionFieldValue(field: ExecutionField, rawValue: string | undefined) {
  if (field.type === 'checkbox') return rawValue === 'true';
  const trimmed = String(rawValue ?? '').replace(/[​-‍﻿]/g, '').trim();
  if (!trimmed) return '';
  if (field.type === 'number') return Number(trimmed);
  return trimmed;
}

export function buildNormalizedParamsPayload(fields: ExecutionField[], inputs: Record<string, string>) {
  return fields.reduce<Record<string, unknown>>((acc, field) => {
    const val = normalizeExecutionFieldValue(field, inputs[field.name]);
    if (val !== '' && val !== undefined && val !== null) acc[field.name] = val;
    return acc;
  }, {});
}

export function buildPreExecutionSummary(fields: ExecutionField[], inputs: Record<string, string>) {
  const details = fields
    .map((field) => {
      const raw = inputs[field.name];
      if (raw === undefined || raw === null || String(raw).trim() === '') return null;
      if (field.type === 'password') return `${field.name} = ******`;
      return `${field.name} = ${String(raw).trim()}`;
    })
    .filter(Boolean);
  return details.length === 0 ? 'Se ejecutará la consulta sin parámetros de entrada.' : `Se ejecutará búsqueda con ${details.join(', ')}`;
}
