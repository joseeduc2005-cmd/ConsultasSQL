// app/components/ContentPanel.tsx

'use client';

import { useEffect, useRef, useState } from 'react';
import { KnowledgeArticle } from '../types';
import ReactMarkdown from 'react-markdown';
import { useTheme } from './ThemeProvider';

const BACKEND_URL = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:3002';
const SCRIPT_VARIABLE_REGEX = /{{\s*([a-zA-Z0-9_.]+)\s*}}/g;
const SQL_NAMED_PARAM_REGEX = /(^|[^:]):([a-zA-Z_][a-zA-Z0-9_]*)/g;

interface ContentPanelProps {
  selectedArticle: KnowledgeArticle | null;
  loading: boolean;
  error: string;
  onExecuteSolution: (articleId: number | string, formData: Record<string, any>) => Promise<string>;
}

interface ProgressStep {
  message: string;
  type: 'progress' | 'error';
}

interface ExecutionResultPayload {
  success?: boolean;
  message?: string;
  affectedRows?: number;
  data?: unknown;
  resultado_final?: unknown;
  resultado?: unknown[];
  resumenHumano?: string;
  steps?: string[];
  explicacion?: string;
  workflow?: {
    steps?: Array<{ step?: string; title?: string; executedQuery?: string; executionMs?: number }>;
    context?: Record<string, unknown>;
    final?: unknown;
  };
  workflowContext?: Record<string, unknown>;
  executedQueries?: Array<{ step?: string; title?: string; executedQuery?: string; executionMs?: number }>;
}

type TimelineTone = 'select' | 'validation' | 'update' | 'error' | 'info';

interface ParsedTimelineStep {
  title: string;
  detail: string;
  tone: TimelineTone;
  icon: string;
}

interface ExecutionField {
  name: string;
  label: string;
  type: 'text' | 'number' | 'checkbox' | 'password' | 'email' | 'date';
  required: boolean;
}

interface HumanExecutionStep {
  key: string;
  startLabel: string;
  completeLabel: string;
  tone: TimelineTone;
  icon: string;
}


function parseStructuredScript(scriptJson: KnowledgeArticle['script_json']) {
  if (!scriptJson) return null;
  if (typeof scriptJson === 'object') return scriptJson;

  try {
    return JSON.parse(scriptJson);
  } catch {
    return null;
  }
}

function collectScriptVariables(node: unknown, variables = new Set<string>()) {
  if (typeof node === 'string') {
    let match;
    while ((match = SCRIPT_VARIABLE_REGEX.exec(node)) !== null) {
      variables.add(match[1]);
    }
    SCRIPT_VARIABLE_REGEX.lastIndex = 0;

    let sqlMatch;
    while ((sqlMatch = SQL_NAMED_PARAM_REGEX.exec(node)) !== null) {
      variables.add(sqlMatch[2]);
    }
    SQL_NAMED_PARAM_REGEX.lastIndex = 0;

    return variables;
  }

  if (Array.isArray(node)) {
    node.forEach((item) => collectScriptVariables(item, variables));
    return variables;
  }

  if (node && typeof node === 'object') {
    Object.values(node).forEach((value) => collectScriptVariables(value, variables));
  }

  return variables;
}

function extractVariables(scriptJson: unknown): string[] {
  const parsedScript = typeof scriptJson === 'object' ? scriptJson : parseStructuredScript(scriptJson as KnowledgeArticle['script_json']);
  if (!parsedScript || typeof parsedScript !== 'object') {
    return [];
  }

  return Array.from(collectScriptVariables(parsedScript));
}

function inferScriptFieldType(name: string): ExecutionField['type'] {
  const normalized = name.toLowerCase();
  if (normalized.includes('date') || normalized.includes('fecha')) return 'date';
  if (normalized.includes('password') || normalized.includes('contrasena') || normalized.includes('clave')) return 'password';
  if (normalized.includes('email') || normalized.includes('correo')) return 'email';
  if (/^(es_|is_|has_|incluir_|activo|enabled|flag)/.test(normalized)) return 'checkbox';
  if (/(codigo|code|horas|dias|minutos|monto|amount|cantidad|numero|count|edad)/.test(normalized) && !normalized.includes('id')) return 'number';
  return 'text';
}

function formatScriptFieldLabel(name: string) {
  const normalized = String(name || '').trim().toLowerCase();
  if (normalized === 'user_id' || normalized === 'usuario_id') return 'Ingrese ID de usuario';
  if (normalized.includes('identificacion')) return 'Ingrese identificación';
  if (normalized.includes('codigo') || normalized === 'code') return 'Ingrese código';
  if (normalized === 'username' || normalized === 'usuario' || normalized === 'user') return 'Ingrese nombre de usuario';
  if (normalized === 'role' || normalized === 'rol') return 'Ingrese rol';
  if (normalized.includes('date') || normalized.includes('fecha')) return 'Ingrese fecha';
  if (normalized.includes('id')) return `Ingrese ${normalized.replace(/_/g, ' ')}`;
  return `Ingrese ${normalized.replace(/_/g, ' ')}`;
}

function inferParamSemantic(name: string) {
  const normalized = String(name || '').trim().toLowerCase();

  if (normalized.includes('date') || normalized.includes('fecha')) return 'date';
  if (normalized.includes('role') || normalized.includes('rol')) return 'role';
  if (normalized.includes('codigo') || normalized.includes('code')) return 'code';
  if (normalized.includes('log')) return 'log';
  if (normalized.includes('id')) return 'id';
  if (normalized.includes('name') || normalized.includes('user') || normalized.includes('usuario')) return 'user_text';
  return 'text';
}

function validateExecutionFieldValue(field: ExecutionField, rawValue: string | undefined): string | null {
  const value = String(rawValue ?? '');
  const trimmed = value.trim();

  if (field.required && field.type !== 'checkbox' && !trimmed) {
    return `El campo "${field.label}" es obligatorio`;
  }

  if (!trimmed) {
    return null;
  }

  const semantic = inferParamSemantic(field.name);
  const normalizedName = String(field.name || '').trim().toLowerCase();
  if (semantic === 'id') {
    // IDs should preserve exact string semantics; disallow internal spaces.
    if (/\s/.test(trimmed)) {
      return `El campo "${field.label}" no puede contener espacios`;
    }

    const requiresUuid = normalizedName === 'user_id' || normalizedName.endsWith('_uuid') || normalizedName.includes('uuid');
    if (requiresUuid) {
      const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
      if (!uuidRegex.test(trimmed)) {
        return 'El campo ID debe ser un UUID válido';
      }
    }
  }

  if (semantic === 'role') {
    if (!/^[\w\s-]{2,40}$/i.test(trimmed)) {
      return `El campo "${field.label}" contiene un formato inválido`;
    }
  }

  if (semantic === 'code') {
    if (!/^[\w-]{1,60}$/i.test(trimmed)) {
      return `El campo "${field.label}" contiene un formato inválido`;
    }
  }

  if (semantic === 'date') {
    const parsed = new Date(trimmed);
    if (Number.isNaN(parsed.getTime())) {
      return `El campo "${field.label}" debe tener una fecha válida`;
    }
  }

  if (field.type === 'number') {
    if (!Number.isFinite(Number(trimmed))) {
      return `El campo "${field.label}" debe ser numérico`;
    }
  }

  return null;
}

function normalizeExecutionFieldValue(field: ExecutionField, rawValue: string | undefined) {
  if (field.type === 'checkbox') {
    return rawValue === 'true';
  }

  const trimmed = String(rawValue ?? '')
    .replace(/[\u200B-\u200D\uFEFF]/g, '')
    .trim();
  if (!trimmed) {
    return '';
  }

  if (field.type === 'number') {
    return Number(trimmed);
  }

  if (field.type === 'date') {
    return trimmed;
  }

  return trimmed;
}

function buildNormalizedParamsPayload(fields: ExecutionField[], inputs: Record<string, string>) {
  return fields.reduce<Record<string, unknown>>((acc, field) => {
    const normalizedValue = normalizeExecutionFieldValue(field, inputs[field.name]);
    if (normalizedValue !== '' && normalizedValue !== undefined && normalizedValue !== null) {
      acc[field.name] = normalizedValue;
    }
    return acc;
  }, {});
}

function buildPreExecutionSummary(fields: ExecutionField[], inputs: Record<string, string>) {
  const details = fields
    .map((field) => {
      const raw = inputs[field.name];
      if (raw === undefined || raw === null || String(raw).trim() === '') return null;
      if (field.type === 'password') return `${field.name} = ******`;
      return `${field.name} = ${String(raw).trim()}`;
    })
    .filter(Boolean);

  if (details.length === 0) {
    return 'Se ejecutará la consulta sin parámetros de entrada.';
  }

  return `Se ejecutará búsqueda con ${details.join(', ')}`;
}

function scriptHasMutation(node: unknown): boolean {
  if (!node || typeof node !== 'object') return false;

  if (Array.isArray(node)) {
    return node.some((item) => scriptHasMutation(item));
  }

  const current = node as Record<string, unknown>;
  const tipo = String(current.tipo || '').trim().toLowerCase();
  if (tipo === 'update' || tipo === 'delete' || tipo === 'insert') {
    return true;
  }

  const modo = String(current.modo || '').trim().toLowerCase();
  const origen = String(current.origen || '').trim().toLowerCase();
  const sql = String(current.sql || '').trim().toLowerCase();
  if (modo === 'script' && origen === 'manual-sql' && /^(update|delete|insert)\b/.test(sql)) {
    return true;
  }

  return Object.values(current).some((value) => scriptHasMutation(value));
}

const PLSQL_MARKER_RE = /\b(DECLARE|EXCEPTION|ELSIF)\b|\bIF\b[\s\S]{0,600}\bTHEN\b|\bFOR\b\s+\w+\s+IN\b|\bBEGIN\b[\s\S]{5,}\bEND\b/i;

function isComplexSqlScript(sql: string): boolean {
  return PLSQL_MARKER_RE.test(String(sql || ''));
}

/** Extract {{variable}} names from PL/SQL text, excluding step/loopRow internal references. */
function extractInputVarsFromPlSql(sql: string): string[] {
  const vars = new Set<string>();
  for (const m of String(sql || '').matchAll(/{{\s*([a-zA-Z0-9_.]+)\s*}}/g)) {
    const name = m[1];
    if (!/^(step\d+\.|loopRow\.)/i.test(name)) vars.add(name);
  }
  // Also detect :named_param style (not ::cast)
  const namedRe = /(?<![:])[:]([a-zA-Z_][a-zA-Z0-9_]*)/g;
  for (const m of String(sql || '').matchAll(namedRe)) {
    vars.add(m[1]);
  }
  return Array.from(vars);
}

function scriptRequiresWriteConfirmation(article: KnowledgeArticle | null) {
  const parsed = parseStructuredScript(article?.script_json);
  return scriptHasMutation(parsed);
}

function getScriptExecutionFields(article: KnowledgeArticle | null): ExecutionField[] {
  const parsed = parseStructuredScript(article?.script_json);
  if (!parsed || typeof parsed !== 'object') return [];

  const scriptObject = parsed as Record<string, unknown>;

  // For complex PL/SQL scripts, parse input vars from the raw SQL text directly
  const sqlText = String(scriptObject.sql || '').trim();
  if (
    String(scriptObject.modo || '').trim().toLowerCase() === 'script' &&
    String(scriptObject.origen || '').trim().toLowerCase() === 'manual-sql' &&
    isComplexSqlScript(sqlText)
  ) {
    const fromSql = extractInputVarsFromPlSql(sqlText);
    const requiredParams = Array.isArray(scriptObject.parametros_requeridos)
      ? scriptObject.parametros_requeridos.map((item) => String(item || '').trim()).filter(Boolean)
      : [];
    const mergedParams = Array.from(new Set([...requiredParams, ...fromSql]));
    return mergedParams.map((name) => ({
      name,
      label: formatScriptFieldLabel(name),
      type: inferScriptFieldType(name),
      required: false,
    }));
  }

  const requiredParams = Array.isArray(scriptObject.parametros_requeridos)
    ? scriptObject.parametros_requeridos
        .map((item) => String(item || '').trim())
        .filter(Boolean)
    : [];

  const autoDetected = extractVariables(parsed)
    .map((item) => String(item || '').trim())
    .filter((item) => Boolean(item) && !item.includes('.'));

  const mergedParams = Array.from(new Set([...requiredParams, ...autoDetected]));

  return mergedParams.map((name) => ({
    name,
    label: formatScriptFieldLabel(name),
    type: inferScriptFieldType(name),
    required: false,
  }));
}

function getScriptPreviewText(article: KnowledgeArticle | null): string {
  const parsed = parseStructuredScript(article?.script_json);
  if (!parsed || typeof parsed !== 'object') return '';

  const script = parsed as Record<string, unknown>;
  const modo = String(script.modo || '').trim().toLowerCase();

  // Manual SQL script: extract sql field
  if (modo === 'script') {
    const origen = String(script.origen || '').trim().toLowerCase();
    if (origen === 'manual-sql') {
      const sql = String(script.sql || '').trim();
      if (sql) {
        if (isComplexSqlScript(sql)) {
          return `-- [Motor PL/SQL híbrido]\n-- Este script será interpretado y ejecutado paso a paso.\n\n${sql}`;
        }
        return sql;
      }
    }
    // Fallback: dump readable JSON
    return JSON.stringify(parsed, null, 2);
  }

  // Any other structured script: pretty JSON
  return JSON.stringify(parsed, null, 2);
}

function getBusinessScriptDetails(article: KnowledgeArticle | null) {
  const parsed = parseStructuredScript(article?.script_json);
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return null;

  const script = parsed as Record<string, unknown>;
  if (String(script.modo || '').trim().toLowerCase() !== 'script') return null;

  const pasos = Array.isArray(script.pasos)
    ? script.pasos
        .map((step, index) => {
          const item = step as Record<string, unknown>;
          return {
            orden: Number(item.orden) > 0 ? Number(item.orden) : index + 1,
            descripcion: String(item.descripcion || ''),
            accion: String(item.accion || ''),
          };
        })
        .sort((a, b) => a.orden - b.orden)
    : [];

  return {
    script: String(script.script || ''),
    descripcion: String(script.descripcion || ''),
    pasos,
  };
}

function inferArticleExecutionTarget(article: KnowledgeArticle | null): { databaseId?: string } {
  const parsed = parseStructuredScript(article?.script_json);
  const parsedRecord = parsed as Record<string, unknown> | null;
  const explicitDatabaseId = String(
    parsedRecord?.databaseId
      || parsedRecord?.database_id
      || parsedRecord?.targetDatabaseId
      || parsedRecord?.target_database_id
      || ''
  ).trim();
  if (explicitDatabaseId) {
    return { databaseId: explicitDatabaseId };
  }

  return {};
}

function parseTimelineStep(step: ProgressStep): ParsedTimelineStep {
  const raw = (step.message || '').trim();
  const match = raw.match(/^\[STEP\s+(\d+)\]\s*(.+)$/i);
  const content = (match?.[2] || raw).trim();
  const upper = content.toUpperCase();

  if (step.type === 'error' || upper.includes('ERROR') || upper.includes('❌')) {
    return {
      title: content || 'Error de ejecución',
      detail: raw,
      tone: 'error',
      icon: '❌',
    };
  }

  if (upper.includes('VALIDACION')) {
    return {
      title: content,
      detail: raw,
      tone: 'validation',
      icon: '✔',
    };
  }

  if (upper.includes('SELECT')) {
    return {
      title: content,
      detail: raw,
      tone: 'select',
      icon: '🔍',
    };
  }

  if (upper.includes('UPDATE') || upper.includes('INSERT') || upper.includes('DELETE')) {
    return {
      title: content,
      detail: raw,
      tone: 'update',
      icon: '⚙',
    };
  }

  return {
    title: content || 'Paso de ejecución',
    detail: raw,
    tone: 'info',
    icon: '•',
  };
}

function getTimelineToneClasses(tone: TimelineTone) {
  if (tone === 'select') {
    return {
      row: 'border-blue-200 bg-blue-50/60',
      icon: 'bg-blue-600 text-white',
      title: 'text-blue-800',
    };
  }
  if (tone === 'validation') {
    return {
      row: 'border-emerald-200 bg-emerald-50/60',
      icon: 'bg-emerald-600 text-white',
      title: 'text-emerald-800',
    };
  }
  if (tone === 'update') {
    return {
      row: 'border-violet-200 bg-violet-50/60',
      icon: 'bg-violet-600 text-white',
      title: 'text-violet-800',
    };
  }
  if (tone === 'error') {
    return {
      row: 'border-red-200 bg-red-50/60',
      icon: 'bg-red-600 text-white',
      title: 'text-red-800',
    };
  }
  return {
    row: 'border-slate-200 bg-slate-50/70',
    icon: 'bg-slate-500 text-white',
    title: 'text-slate-700',
  };
}

function sanitizeExecutionData(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => sanitizeExecutionData(item));
  }

  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .filter(([key]) => !['password', 'token', 'secret', 'authorization'].includes(key.toLowerCase()))
        .map(([key, nestedValue]) => [key, sanitizeExecutionData(nestedValue)])
    );
  }

  return value;
}

function getFinalExecutionPayload(result: ExecutionResultPayload | null): ExecutionResultPayload | null {
  if (!result) return null;

  const finalResult = result.resultado_final;
  if (finalResult && typeof finalResult === 'object' && !Array.isArray(finalResult)) {
    return finalResult as ExecutionResultPayload;
  }

  return result;
}

function getResultRows(result: ExecutionResultPayload | null): Record<string, unknown>[] {
  const finalPayload = getFinalExecutionPayload(result);
  const rows = finalPayload?.data;
  if (!Array.isArray(rows)) {
    return [];
  }

  return rows.filter((item): item is Record<string, unknown> => Boolean(item && typeof item === 'object'));
}

function describeRecord(record: Record<string, unknown>): string[] {
  const details: string[] = [];

  for (const [key, value] of Object.entries(record)) {
    const lower = key.toLowerCase();
    if (/(^id$|_id$|token|password|secret)/.test(lower)) continue;
    if (value === null || value === undefined || String(value).trim() === '') continue;

    const label = key.replace(/_/g, ' ').toLowerCase();
    if (typeof value === 'boolean') {
      details.push(`${label}: ${value ? 'sí' : 'no'}`);
    } else {
      details.push(`${label}: ${String(value)}`);
    }

    if (details.length >= 3) break;
  }

  return details;
}

function buildExecutionFindings(result: ExecutionResultPayload | null): string[] {
  const rows = getResultRows(result);
  if (rows.length === 0) {
    return [];
  }

  const record = rows[0];
  const findings: string[] = [];

  for (const [key, value] of Object.entries(record)) {
    const lower = key.toLowerCase();
    if (/(^id$|_id$|token|password|secret)/.test(lower)) continue;
    if (value === null || value === undefined || String(value).trim() === '') continue;

    const label = key.replace(/_/g, ' ');
    if (typeof value === 'boolean') {
      findings.push(`${label}: ${value ? 'Sí' : 'No'}`);
    } else if (/(date|fecha|time|created|updated)/i.test(key) && typeof value === 'string') {
      const parsedDate = new Date(value);
      const formatted = Number.isNaN(parsedDate.getTime())
        ? value
        : parsedDate.toLocaleString('es-ES');
      findings.push(`${label}: ${formatted}`);
    } else {
      findings.push(`${label}: ${String(value)}`);
    }

    if (findings.length >= 5) break;
  }

  return findings;
}

function buildExecutionSummary(
  progressSteps: ProgressStep[],
  execSuccess: boolean,
  executionResult: ExecutionResultPayload | null,
): string {
  const combined = progressSteps.map((step) => step.message.toLowerCase()).join(' | ');
  const finalPayload = getFinalExecutionPayload(executionResult);
  const rows = getResultRows(executionResult);

  if (execSuccess && rows.length > 0) {
    const details = describeRecord(rows[0]);
    const base = rows.length === 1
      ? 'El proceso finalizó correctamente y encontró 1 resultado.'
      : `El proceso finalizó correctamente y encontró ${rows.length} resultados.`;

    if (details.length > 0) {
      return `${base} Resultado principal: ${details.join(', ')}.`;
    }

    return base;
  }

  if (execSuccess && typeof finalPayload?.affectedRows === 'number') {
    const count = finalPayload.affectedRows;
    return `El proceso finalizó correctamente y aplicó cambios en ${count} ${count === 1 ? 'fila' : 'filas'}.`;
  }

  if (execSuccess) {
    if (combined.includes('usuario no existe') || combined.includes('no se encontraron')) {
      return 'La ejecución terminó correctamente, pero no se encontraron datos del usuario para aplicar cambios.';
    }
    if (combined.includes('update ejecutado') || combined.includes('filas afectadas')) {
      return 'El usuario fue validado y se aplicaron los cambios esperados sin errores.';
    }
    if (combined.includes('validacion ok')) {
      return 'Todas las validaciones del flujo se cumplieron y el proceso finalizó de forma correcta.';
    }

    return 'El proceso finalizó correctamente y el resultado está listo.';
  }

  const backendMessage = String(executionResult?.message || '').trim();
  if (backendMessage) {
    return backendMessage;
  }

  if (combined.includes('falta parámetro')) {
    return 'La ejecución falló porque faltan datos obligatorios en el formulario.';
  }
  if (combined.includes('usuario no existe')) {
    return 'La ejecución se detuvo porque el usuario indicado no existe en el sistema.';
  }
  if (combined.includes('clave') || combined.includes('password') || combined.includes('contrase')) {
    return 'Se detectó un problema relacionado con credenciales. Revisa usuario y contraseña.';
  }
  if (combined.includes('login') || combined.includes('sesion')) {
    return 'La ejecución reportó un problema en autenticación o sesión.';
  }

  return 'No fue posible completar el proceso. Revisa los datos ingresados o intenta nuevamente.';
}

function buildHumanExecutionExplanation(result: ExecutionResultPayload | null, fallbackSummary: string) {
  if (!result) return fallbackSummary;
  if (typeof result.resumenHumano === 'string' && result.resumenHumano.trim()) {
    return result.resumenHumano.trim();
  }
  if (typeof result.explicacion === 'string' && result.explicacion.trim()) {
    return result.explicacion.trim();
  }

  const rows = getResultRows(result);
  if (rows.length === 0) {
    return fallbackSummary;
  }

  const first = rows[0];
  const parts: string[] = [];

  const entries = Object.entries(first)
    .filter(([key, value]) => {
      const lower = key.toLowerCase();
      if (/(^id$|_id$|token|password|secret)/.test(lower)) return false;
      return value !== null && value !== undefined && String(value).trim() !== '';
    })
    .slice(0, 4);

  for (const [key, value] of entries) {
    const label = key.replace(/_/g, ' ').toLowerCase();
    if (typeof value === 'number' && /(count|total|cantidad|monto|importe|amount|price|precio|valor)/i.test(key)) {
      parts.push(`El total de ${label} es ${value}.`);
      continue;
    }
    if (typeof value === 'boolean') {
      parts.push(`${label}: ${value ? 'sí' : 'no'}.`);
      continue;
    }
    parts.push(`${label}: ${String(value)}.`);
  }

  if (parts.length === 0) return fallbackSummary;
  return parts.join(' ').replace(/\s+/g, ' ').trim();
}

function inferQueryTitle(sql: string): string {
  const upper = (sql || '').trim().toUpperCase();
  const lower = (sql || '').toLowerCase();
  if (/\bCOUNT\b/.test(upper)) return 'Análisis de actividad';
  if (/\bjoin\b.*\broles?\b|\broles?\b.*\bjoin\b/i.test(lower)) return 'Obtener rol';
  if (/\bselect\b.*\busers?\b|\bfrom\b.*\busers?\b/i.test(lower)) return 'Buscar usuario';
  if (/\bUPDATE\b/.test(upper)) return 'Actualizar registro';
  if (/\bINSERT\b/.test(upper)) return 'Insertar registro';
  if (/\bDELETE\b/.test(upper)) return 'Eliminar registro';
  if (/\bSELECT\b/.test(upper)) return 'Consultar datos';
  return 'Ejecutar query';
}

function isTechnicalProgressMessage(message: string): boolean {
  const normalized = String(message || '').trim();
  const upper = normalized.toUpperCase();

  return (
    upper.includes('SQL OPERATION')
    || upper.includes('SQL PARAMS')
    || upper.includes('SQL PLACEHOLDERS')
    || upper.startsWith('[SQL]')
    || upper.startsWith('[PARAMS]')
    || upper.startsWith('[PARAM ')
    || upper.includes('PLACEHOLDER')
    || upper.includes('CANONICAL')
    || upper.includes('PREPARED')
    || upper.includes('MOTOR SQL')
    || upper.includes('WORKFLOW COMPLETADO')
    || upper.includes('SCRIPT EMPRESARIAL DETECTADO')
  );
}

function inferHumanExecutionStep(rawText: string, index: number, total: number): HumanExecutionStep {
  const text = String(rawText || '').trim();
  const lower = text.toLowerCase();

  if (/\bjoin\b.*\brol|\brol\b.*\bjoin\b|\broles?\b/.test(lower)) {
    return {
      key: 'role',
      startLabel: 'Obteniendo rol del usuario...',
      completeLabel: 'Rol obtenido',
      tone: 'select',
      icon: '🔍',
    };
  }

  if (/\bcount\b|\blogs?\b|\bm[eé]tric|\btotal\b|\ban[aá]lis|\bgroup by\b|\bsum\b|\bavg\b/.test(lower)) {
    return {
      key: 'analysis',
      startLabel: 'Analizando datos...',
      completeLabel: 'Datos procesados',
      tone: 'validation',
      icon: '📊',
    };
  }

  if (/\bupdate\b|\binsert\b|\bdelete\b|\bproces|\bactualiz|\bmodific/.test(lower)) {
    return {
      key: 'processing',
      startLabel: 'Procesando registros...',
      completeLabel: 'Cambios aplicados',
      tone: 'update',
      icon: '⚙️',
    };
  }

  if (/\bif\b|\bthen\b|\bvalid|\bverific|\bcondici/.test(lower)) {
    return {
      key: 'validation',
      startLabel: 'Validando información...',
      completeLabel: 'Validación completada',
      tone: 'validation',
      icon: '🛡️',
    };
  }

  if (index === total - 1 || /\bresultado\b|\bfinal\b|\bgenerando\b/.test(lower)) {
    return {
      key: 'result',
      startLabel: 'Generando resultado...',
      completeLabel: 'Resultado listo',
      tone: 'info',
      icon: '📈',
    };
  }

  if (/\busers?\b|\busername\b|\busuario\b|\bbuscando usuario\b/.test(lower)) {
    return {
      key: 'user',
      startLabel: 'Buscando usuario...',
      completeLabel: 'Usuario encontrado',
      tone: 'select',
      icon: '🔍',
    };
  }

  if (/\bselect\b|\bfrom\b/.test(lower)) {
    return {
      key: `query-${index}`,
      startLabel: 'Consultando información...',
      completeLabel: 'Información obtenida',
      tone: 'select',
      icon: '🔍',
    };
  }

  return {
    key: `generic-${index}`,
    startLabel: 'Procesando solicitud...',
    completeLabel: 'Proceso completado',
    tone: 'info',
    icon: '📌',
  };
}

function limitHumanExecutionSteps(steps: HumanExecutionStep[]): HumanExecutionStep[] {
  if (steps.length <= 5) return steps;
  const lastStep = steps[steps.length - 1];
  const head = steps.slice(0, 4);
  if (head.some((step) => step.key === lastStep.key)) {
    return head;
  }
  return [...head, lastStep];
}

function buildHumanExecutionSteps(
  progressSteps: ProgressStep[],
  result: ExecutionResultPayload | null,
): HumanExecutionStep[] {
  const executedQueries = Array.isArray(result?.executedQueries) ? result.executedQueries : [];

  const rawItems = executedQueries.length > 0
    ? executedQueries
        .map((item) => String(item?.title || item?.executedQuery || '').trim())
        .filter(Boolean)
    : progressSteps
        .filter((step) => !isTechnicalProgressMessage(step.message) && step.type !== 'error')
        .map((step) => String(step.message || '').replace(/^\[STEP\s+\d+\]\s*/i, '').trim())
        .filter(Boolean);

  const uniqueSteps: HumanExecutionStep[] = [];
  const seen = new Set<string>();

  rawItems.forEach((item, index) => {
    const step = inferHumanExecutionStep(item, index, rawItems.length);
    if (seen.has(step.key)) return;
    seen.add(step.key);
    uniqueSteps.push(step);
  });

  return limitHumanExecutionSteps(uniqueSteps);
}

function buildAnnotatedSqlScript(
  executedQueries: Array<{ step?: string; title?: string; executedQuery?: string; executionMs?: number }>,
): string {
  if (!executedQueries || executedQueries.length === 0) return '';
  return executedQueries
    .map((item, index) => {
      const sql = String(item?.executedQuery || '').trim();
      const title = inferQueryTitle(sql);
      return `-- Paso ${index + 1}: ${title}\n${sql}`;
    })
    .join('\n\n');
}

export default function ContentPanel({ selectedArticle, loading }: ContentPanelProps) {
  const { theme } = useTheme();
  const isLightTheme = theme === 'light';
  const [dynamicInputs, setDynamicInputs] = useState<{ [key: string]: string }>({});
  const [executing, setExecuting] = useState(false);
  const [progressSteps, setProgressSteps] = useState<ProgressStep[]>([]);
  const [execDone, setExecDone] = useState(false);
  const [execSuccess, setExecSuccess] = useState(false);
  const [executionResult, setExecutionResult] = useState<ExecutionResultPayload | null>(null);
  const [writeConfirmed, setWriteConfirmed] = useState(false);
  const progressEndRef = useRef<HTMLDivElement | null>(null);
  const hasStructuredScript = Boolean(parseStructuredScript(selectedArticle?.script_json));
  const businessScriptDetails = getBusinessScriptDetails(selectedArticle);
  const solutionType = (selectedArticle?.tipo_solucion || 'lectura').toLowerCase();
  const isExecutableArticle = solutionType === 'ejecutable' || solutionType === 'database' || solutionType === 'script';
  const scriptExecutionFields = getScriptExecutionFields(selectedArticle);
  const legacyExecutionFields = ((selectedArticle?.camposFormulario || []) as any[]).filter((field: any) => field.mode !== 'output');
  const executionFields = hasStructuredScript ? scriptExecutionFields : legacyExecutionFields;
  const preExecutionSummary = buildPreExecutionSummary(executionFields, dynamicInputs);
  const requiresWriteConfirmation = scriptRequiresWriteConfirmation(selectedArticle);
  const executionSummary = buildExecutionSummary(progressSteps, execSuccess, executionResult);
  const humanExecutionExplanation = buildHumanExecutionExplanation(executionResult, executionSummary);
  const sanitizedExecutionResult = sanitizeExecutionData(getFinalExecutionPayload(executionResult));
  const executionFindings = buildExecutionFindings(executionResult);
  const humanWorkflowSteps = buildHumanExecutionSteps(progressSteps, executionResult);
  const executedQueriesList = Array.isArray(executionResult?.executedQueries) ? executionResult.executedQueries : [];
  const annotatedSqlScript = buildAnnotatedSqlScript(executedQueriesList);
  const scriptPreviewText = getScriptPreviewText(selectedArticle);
  const [execView, setExecView] = useState<'script' | 'execute'>('script');
  const latestVisibleError = [...progressSteps]
    .reverse()
    .find((step) => step.type === 'error' && !isTechnicalProgressMessage(step.message));
  const executionTarget = inferArticleExecutionTarget(selectedArticle);

  useEffect(() => {
    setDynamicInputs({});
    setProgressSteps([]);
    setExecDone(false);
    setExecSuccess(false);
    setExecuting(false);
    setExecutionResult(null);
    setWriteConfirmed(false);
    setExecView('script');
  }, [selectedArticle?.id]);

  useEffect(() => {
    progressEndRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
  }, [progressSteps.length]);

  const handleInputChange = (name: string, value: string) => {
    setDynamicInputs((prev) => ({
      ...prev,
      [name]: value,
    }));
  };

  const resetExec = () => {
    setDynamicInputs({});
    setProgressSteps([]);
    setExecDone(false);
    setExecSuccess(false);
    setExecuting(false);
    setExecutionResult(null);
    setWriteConfirmed(false);
  };

  const handleExecute = () => {
    for (const field of executionFields) {
      const value = dynamicInputs[field.name];
      const validationError = validateExecutionFieldValue(field, value);
      if (validationError) {
        alert(validationError);
        return;
      }
    }

    if (requiresWriteConfirmation && !writeConfirmed) {
      alert('Esta acción modificará datos. Debes confirmar para continuar.');
      return;
    }

    setExecuting(true);
    setProgressSteps([]);
    setExecDone(false);
    setExecSuccess(false);
    setExecutionResult(null);

    const paramsPayload = {
      ...buildNormalizedParamsPayload(executionFields, dynamicInputs),
      __write_confirmed: requiresWriteConfirmation ? (writeConfirmed ? 'true' : 'false') : 'false',
    };
    const paramsStr = encodeURIComponent(JSON.stringify(paramsPayload));
    const extraQuery = new URLSearchParams();
    if (executionTarget.databaseId) {
      extraQuery.set('databaseId', executionTarget.databaseId);
    }

    const url = `${BACKEND_URL}/api/execute/stream/${selectedArticle!.id}?params=${paramsStr}${extraQuery.toString() ? `&${extraQuery.toString()}` : ''}`;

    const parsedScript = parseStructuredScript(selectedArticle?.script_json);
    const sqlPreview = String((parsedScript as Record<string, unknown> | null)?.sql || '').trim();
    console.info('[ContentPanel][execute] dispatch', {
      articleId: selectedArticle?.id,
      databaseId: executionTarget.databaseId || '',
      sql: sqlPreview.slice(0, 500),
      url,
    });

    const es = new EventSource(url);
    let closed = false;

    es.addEventListener('progress', (e: MessageEvent) => {
      const data = JSON.parse(e.data);
      setProgressSteps((prev) => [...prev, { message: data.message, type: 'progress' }]);
    });

    es.addEventListener('execution-error', (e: MessageEvent) => {
      if (e.data) {
        try {
          const data = JSON.parse(e.data);
          setProgressSteps((prev) => [...prev, { message: data.message, type: 'error' }]);
        } catch {}
      }
    });

    es.addEventListener('done', (e: MessageEvent) => {
      const data = JSON.parse(e.data);
      closed = true;
      setExecuting(false);
      setExecDone(true);
      setExecSuccess(data.success);
      setExecutionResult(data.result || null);
      es.close();
    });

    es.onerror = () => {
      if (closed) return;
      closed = true;
      setExecuting(false);
      setExecDone(true);
      setExecSuccess(false);
      setExecutionResult(null);
      setProgressSteps((prev) => [
        ...prev,
        { message: '❌ Error de conexión con el servidor', type: 'error' },
      ]);
      es.close();
    };
  };

  if (loading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <div className="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-[#25295c] mb-3"></div>
          <p className="text-[color:var(--ink-700)]">Cargando...</p>
        </div>
      </div>
    );
  }

  if (!selectedArticle) {
    return (
      <div className="flex-1 flex items-center justify-center p-6">
        <div className="text-center">
          <svg className="w-16 h-16 text-[color:var(--ink-500)] mx-auto mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
          </svg>
          <h2 className="text-lg font-semibold text-[color:var(--ink-900)]">Selecciona una categoría</h2>
          <p className="text-sm text-[color:var(--ink-700)] mt-1">Elige un problema del menú para ver los detalles</p>
        </div>
      </div>
    );
  }

  const mdPanelClass = isLightTheme
    ? 'dashboard-panel dashboard-elevate rounded-2xl border border-sky-200 bg-white/90 p-6 mb-6 overflow-hidden'
    : 'dashboard-panel dashboard-elevate rounded-2xl border border-slate-700/70 bg-slate-950/55 p-6 mb-6 overflow-hidden';

  const mdPanelSubtitleClass = isLightTheme
    ? 'text-[11px] text-slate-500'
    : 'text-[11px] text-slate-400';

  const mdInlineCodeClass = isLightTheme
    ? 'inline-flex items-center rounded-md border border-sky-200 bg-sky-50 px-2 py-1 text-sm font-mono text-sky-800'
    : 'inline-flex items-center rounded-md border border-cyan-500/30 bg-cyan-500/10 px-2 py-1 text-sm font-mono text-cyan-200';

  const mdBlockquoteClass = isLightTheme
    ? 'my-4 border-l-4 border-sky-500 bg-sky-50/70 pl-4 italic text-slate-700'
    : 'my-4 border-l-4 border-cyan-400 bg-slate-900/70 pl-4 italic text-slate-300';

  return (
    <div className="flex-1 overflow-y-auto">
      <div className="p-6 max-w-4xl">
        {/* Article header */}
        <div className="mb-6">
          <h1 className="text-3xl font-bold text-[color:var(--ink-900)] mb-2">{selectedArticle.titulo}</h1>
          {selectedArticle.tags && selectedArticle.tags.length > 0 && (
            <div className="mt-2 flex flex-wrap gap-2">
              {selectedArticle.tags.map((tag, idx) => (
                <span key={idx} className="glass-pill inline-flex items-center px-3 py-1 rounded-full text-xs font-medium text-[color:var(--ink-800)]">
                  #{tag}
                </span>
              ))}
            </div>
          )}
        </div>

        {/* Markdown solution */}
        {(selectedArticle.contenido_md || selectedArticle.contenido) && (
          <div className={mdPanelClass}>
            <div className="mb-4 flex flex-wrap items-start justify-between gap-3">
              <div>
                <p className={`text-xs font-semibold uppercase tracking-[0.18em] ${isLightTheme ? 'text-sky-700' : 'text-cyan-300'}`}>
                  Soluciones MD
                </p>
                <h2 className="dashboard-gradient-text mt-1 text-lg font-semibold">Solución</h2>
                <p className={`mt-1 ${mdPanelSubtitleClass}`}>Mismo estilo visual del panel SQL automático.</p>
              </div>
              <span className={`rounded-full border px-2.5 py-1 text-[10px] font-semibold ${isLightTheme ? 'border-sky-200 bg-sky-50 text-sky-700' : 'border-cyan-500/30 bg-cyan-500/10 text-cyan-200'}`}>
                Markdown
              </span>
            </div>
            <div className="prose prose-sm max-w-none text-[color:var(--ink-800)]">
              <ReactMarkdown
                components={{
                  h1: ({ children }) => <h1 className="text-2xl font-bold text-[color:var(--ink-900)] mb-4 mt-6">{children}</h1>,
                  h2: ({ children }) => <h2 className="text-xl font-semibold text-[color:var(--ink-900)] mb-3 mt-5">{children}</h2>,
                  h3: ({ children }) => <h3 className="text-lg font-medium text-[color:var(--ink-900)] mb-2 mt-4">{children}</h3>,
                  p: ({ children }) => <p className="text-[color:var(--ink-800)] mb-3 leading-relaxed">{children}</p>,
                  ul: ({ children }) => <ul className="list-disc list-inside space-y-2 mb-4 text-[color:var(--ink-800)]">{children}</ul>,
                  ol: ({ children }) => <ol className="list-decimal list-inside space-y-2 mb-4 text-[color:var(--ink-800)]">{children}</ol>,
                  li: ({ children }) => <li className="ml-2">{children}</li>,
                  code: ({ className, children }) => {
                    const text = String(children);
                    const isBlockCode = Boolean(className) || text.includes('\n');
                    if (!isBlockCode) {
                      return <code className={mdInlineCodeClass}>{children}</code>;
                    }
                    return <code className={className}>{children}</code>;
                  },
                  pre: ({ children }) => (
                    <pre className="bg-[#16294e] p-4 rounded-lg overflow-x-auto text-sm font-mono text-[#e9f0ff] mb-4">{children}</pre>
                  ),
                  blockquote: ({ children }) => (
                    <blockquote className={mdBlockquoteClass}>{children}</blockquote>
                  ),
                }}
              >
                {selectedArticle.contenido_md || selectedArticle.contenido}
              </ReactMarkdown>
            </div>
          </div>
        )}

        {/* Execute section */}
        {isExecutableArticle && (
          <div className="glass-panel glass-panel-hover rounded-xl p-6">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-sm font-bold text-[color:var(--ink-900)] uppercase tracking-wide">🔧 Ejecutar Solución</h2>
              {execDone && (
                <button
                  onClick={resetExec}
                  className="text-xs text-[color:var(--accent-strong)] hover:text-[color:var(--ink-900)] font-medium glass-pill px-3 py-1 rounded-lg transition-colors"
                >
                  🔄 Reiniciar
                </button>
              )}
            </div>

            {/* Tab toggle: Script / Ejecución */}
            {scriptPreviewText && !execDone && (
              <div className="flex gap-1 mb-5 glass-pill rounded-lg p-1 w-fit">
                <button
                  onClick={() => setExecView('script')}
                  className={`px-4 py-1.5 rounded-md text-xs font-semibold transition-colors ${
                    execView === 'script'
                      ? 'bg-[color:var(--accent)] text-white'
                      : 'text-[color:var(--ink-700)] hover:text-[color:var(--ink-900)]'
                  }`}
                >
                  📋 Ver Script
                </button>
                <button
                  onClick={() => setExecView('execute')}
                  className={`px-4 py-1.5 rounded-md text-xs font-semibold transition-colors ${
                    execView === 'execute'
                      ? 'bg-[color:var(--accent)] text-white'
                      : 'text-[color:var(--ink-700)] hover:text-[color:var(--ink-900)]'
                  }`}
                >
                  ▶ Ejecutar
                </button>
              </div>
            )}

            {/* Script preview panel */}
            {execView === 'script' && scriptPreviewText && !execDone && (
              <div className="mb-5 rounded-lg border border-slate-700 bg-[#1e1e2e] overflow-hidden">
                <div className="px-3 py-2 bg-[#2a2a3d] border-b border-slate-700 text-xs font-bold uppercase tracking-wide text-slate-300 flex items-center justify-between">
                  <span>📋 Script configurado</span>
                  <button
                    onClick={() => setExecView('execute')}
                    className="text-[10px] text-blue-400 hover:text-blue-200 font-medium transition-colors"
                  >
                    ▶ Ir a ejecutar →
                  </button>
                </div>
                <pre className="p-4 text-xs leading-relaxed font-mono text-[#cdd6f4] overflow-x-auto whitespace-pre">
                  {scriptPreviewText}
                </pre>
              </div>
            )}

            {/* Execution panel (form + progress + results) */}
            {(execView === 'execute' || execDone) && (
            <div>

            {businessScriptDetails && (
              <div className="mb-5 rounded-lg glass-panel p-3">
                <p className="text-xs font-bold uppercase tracking-wide text-[color:var(--ink-900)] mb-1">Script</p>
                {businessScriptDetails.script && (
                  <p className="text-sm text-[color:var(--ink-800)] mb-1">
                    <span className="font-semibold">Proceso:</span> {businessScriptDetails.script}
                  </p>
                )}
                {businessScriptDetails.descripcion && (
                  <p className="text-sm text-[color:var(--ink-700)] mb-2">{businessScriptDetails.descripcion}</p>
                )}
                {businessScriptDetails.pasos.length > 0 && (
                  <ul className="space-y-2">
                    {businessScriptDetails.pasos.map((step) => (
                      <li key={`business-step-${step.orden}`} className="rounded glass-panel p-2">
                        <p className="text-sm text-[color:var(--ink-900)] font-medium">Paso {step.orden}: {step.descripcion}</p>
                        <p className="text-xs text-[color:var(--ink-700)]">Acción: {step.accion}</p>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            )}

            {/* Dynamic form fields */}
            {!execDone && executionFields.length > 0 && (
              <div className="space-y-3 mb-5">
                {hasStructuredScript && (
                  <div className="rounded-lg glass-panel px-3 py-2 text-xs text-[color:var(--ink-700)]">
                    Los campos se generaron automáticamente desde el script_json del artículo.
                  </div>
                )}
                {executionFields.map((field: any) => (
                  <div key={field.name}>
                    <label className="block text-sm font-medium text-[color:var(--ink-900)] mb-1">
                      {field.label}
                      {field.required && <span className="text-red-500 ml-1">*</span>}
                    </label>
                    {field.type === 'textarea' ? (
                      <textarea
                        value={dynamicInputs[field.name] || ''}
                        onChange={(e) => handleInputChange(field.name, e.target.value)}
                        placeholder={`Ingresa ${field.label.toLowerCase()}`}
                        disabled={executing}
                        rows={3}
                        className="input-glass w-full px-3 py-2 rounded-lg text-sm disabled:opacity-70 resize-none"
                      />
                    ) : field.type === 'checkbox' ? (
                      <label className="flex items-center gap-2 cursor-pointer">
                        <input
                          type="checkbox"
                          checked={dynamicInputs[field.name] === 'true' || false}
                          onChange={(e) => handleInputChange(field.name, e.target.checked ? 'true' : 'false')}
                          disabled={executing}
                          className="w-4 h-4 text-[#2363eb] rounded border-[#cfe0ff] focus:ring-2 focus:ring-[#2363eb] disabled:opacity-50"
                        />
                        <span className="text-sm text-[color:var(--ink-800)]">{field.label}</span>
                      </label>
                    ) : (
                      <input
                        type={field.type || 'text'}
                        value={dynamicInputs[field.name] || ''}
                        onChange={(e) => handleInputChange(field.name, e.target.value)}
                        placeholder={`Ingresa ${field.label.toLowerCase()}`}
                        disabled={executing}
                        className="input-glass w-full px-3 py-2 rounded-lg text-sm disabled:opacity-70"
                      />
                    )}
                  </div>
                ))}
              </div>
            )}

            {!execDone && executionFields.length === 0 && (
              <div className="mb-5 text-sm text-[color:var(--ink-700)] glass-panel rounded-lg p-3">
                Esta acción no requiere parámetros de entrada.
              </div>
            )}

            {!execDone && requiresWriteConfirmation && (
              <div className="mb-5 rounded-lg border border-amber-200 bg-amber-50 px-3 py-3 text-sm text-amber-900">
                <p className="font-semibold">Esta acción modificará datos en la base de datos.</p>
                <label className="mt-2 flex items-center gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={writeConfirmed}
                    onChange={(e) => setWriteConfirmed(e.target.checked)}
                    disabled={executing}
                    className="h-4 w-4 rounded border-amber-300"
                  />
                  <span>Confirmo que deseo continuar con UPDATE/DELETE/INSERT</span>
                </label>
              </div>
            )}

            {!execDone && (
              <div className="mb-5 rounded-lg glass-panel px-3 py-2 text-xs text-[color:var(--ink-700)]">
                {preExecutionSummary}
              </div>
            )}

            {/* Real-time progress log */}
            {(humanWorkflowSteps.length > 0 || Boolean(latestVisibleError)) && (
              <div className="mb-4 glass-panel rounded-lg p-4">
                <p className="text-xs font-bold text-[color:var(--ink-900)] uppercase tracking-wide mb-2">
                  Progreso del proceso
                </p>
                <div className="max-h-64 overflow-y-auto pr-1">
                  <div className="space-y-3">
                    {humanWorkflowSteps.map((step, index) => {
                      const tone = getTimelineToneClasses(step.tone);
                      const isDone = execDone && execSuccess;
                      const isCurrent = !execDone && index === humanWorkflowSteps.length - 1;
                      const isCompleted = isDone || (!execDone && index < humanWorkflowSteps.length - 1);

                      return (
                        <div key={`${step.key}-${index}`} className={`rounded-lg border ${tone.row} px-4 py-3`}>
                          <p className={`text-sm font-semibold ${tone.title}`}>
                            {step.icon} Paso {index + 1}: {step.startLabel}
                          </p>
                          <p className={`mt-1 text-sm ${isCompleted || isDone ? 'text-emerald-700' : 'text-[color:var(--ink-700)]'}`}>
                            {isCompleted || isDone ? `✅ ${step.completeLabel}` : isCurrent ? '⏳ En proceso' : '⏳ Pendiente'}
                          </p>
                        </div>
                      );
                    })}

                    {latestVisibleError && (
                      <div className="rounded-lg border border-red-200 bg-red-50/80 px-4 py-3">
                        <p className="text-sm font-semibold text-red-800">❌ No se pudo completar el proceso</p>
                        <p className="mt-1 text-sm text-red-700">{latestVisibleError.message}</p>
                      </div>
                    )}
                  </div>
                  <div ref={progressEndRef} />
                </div>

                {executing && (
                  <div className="mt-2 text-sm text-[color:var(--ink-700)] flex items-center gap-2">
                    <span className="inline-block animate-spin">⏳</span>
                    <span>Procesando...</span>
                  </div>
                )}
              </div>
            )}

            {/* Final result */}
            {execDone && (
              <div
                className={`mb-2 p-3 rounded-lg text-sm font-semibold ${
                  execSuccess
                    ? 'border border-emerald-200 bg-emerald-50 text-emerald-800 dark:border-emerald-700 dark:bg-emerald-950/40 dark:text-emerald-200'
                    : 'bg-red-50 text-red-800 border border-red-200'
                }`}
              >
                {execSuccess ? '✅ Ejecución completada exitosamente' : '❌ La ejecución encontró errores'}
              </div>
            )}

            {execDone && (() => {
              const lines = humanExecutionExplanation.split('\n').map((l: string) => l.trim()).filter(Boolean);
              const isNoticeLog = lines.length > 1;
              if (isNoticeLog) {
                return (
                  <div className="mb-4 rounded-lg border border-slate-700 overflow-hidden" style={{ background: 'rgba(15,23,42,0.85)' }}>
                    <div className="px-3 py-2 border-b border-slate-700 text-[10px] font-bold uppercase tracking-wide text-slate-400" style={{ background: 'rgba(255,255,255,0.03)' }}>
                      📋 Resumen del resultado
                    </div>
                    <div style={{ padding: '10px 14px', fontFamily: 'monospace', fontSize: '11px', lineHeight: '1.8', overflowX: 'auto' }}>
                      {lines.map((line: string, i: number) => {
                        const isSep = /^={3,}/.test(line);
                        const isWarn = /^[⚠✖]/.test(line);
                        const isOk = /^[✔✅]/.test(line);
                        const color = isSep ? 'rgba(148,163,184,0.4)' : isWarn ? '#fbbf24' : isOk ? '#4ade80' : '#7dd3fc';
                        return (
                          <div key={i} style={{ display: 'flex', gap: '8px', color }}>
                            {!isSep && <span style={{ color: 'rgba(100,116,139,0.7)', flexShrink: 0 }}>NOTICE:</span>}
                            <span>{line}</span>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                );
              }
              return (
                <div className="mb-4 p-3 rounded-lg text-sm bg-white border border-slate-200 text-slate-700 dark:bg-slate-900/80 dark:border-slate-700 dark:text-slate-200">
                  <span className="font-semibold text-slate-900 dark:text-slate-100">Resumen del resultado:</span>{' '}
                  {humanExecutionExplanation}
                </div>
              );
            })()}

            {execDone && execSuccess && executionFindings.length > 0 && (
              <div className="mb-4 rounded-lg border border-slate-200 bg-white overflow-hidden dark:bg-slate-900/80 dark:border-slate-700">
                <div className="px-3 py-2 bg-slate-100 border-b border-slate-200 text-xs font-bold uppercase tracking-wide text-slate-800 dark:bg-slate-800 dark:border-slate-700 dark:text-slate-100">
                  Información encontrada
                </div>
                <ul className="p-3 space-y-2 text-sm text-slate-700 dark:text-slate-200">
                  {executionFindings.map((finding, index) => (
                    <li key={index} className="flex items-start gap-2">
                      <span className="text-emerald-600 mt-0.5">•</span>
                      <span>{finding}</span>
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {execDone && annotatedSqlScript && (
              <details className="mb-4 rounded-lg border border-slate-700 bg-[#1e1e2e] overflow-hidden">
                <summary className="px-3 py-2 bg-[#2a2a3d] border-b border-slate-700 text-xs font-bold uppercase tracking-wide text-slate-300 cursor-pointer select-none">
                  📋 Ver SQL ejecutado
                </summary>
                <pre className="p-4 text-xs leading-relaxed font-mono text-[#cdd6f4] overflow-x-auto whitespace-pre">
                  {annotatedSqlScript}
                </pre>
              </details>
            )}

            {execDone && execSuccess && (() => {
              const res = executionResult;
              const rows = Array.isArray(res?.resultado) ? res.resultado as Record<string, unknown>[] : [];
              const hasRows = rows.length > 0;
              // If no rows and resumenHumano is multi-line (already shown as notice log above), skip this section
              const resumen = String(res?.resumenHumano || '').trim();
              const isNoticeOutput = !hasRows && resumen.includes('\n');
              if (isNoticeOutput) return null;
              if (!hasRows) return null;

              // Oracle DBMS_OUTPUT pattern: all rows have a single "output" key
              const cols = Object.keys(rows[0] || {});
              const isDbmsOutput = cols.length === 1 && cols[0] === 'output';
              if (isDbmsOutput) {
                const lines = rows.map((r) => String(r.output ?? '')).filter(Boolean);
                return (
                  <div className="mb-4 rounded-lg border border-orange-900/40 overflow-hidden" style={{ background: 'rgba(15,10,5,0.9)' }}>
                    <div className="px-3 py-2 border-b border-orange-900/30 text-[10px] font-bold uppercase tracking-wide text-orange-400/80" style={{ background: 'rgba(255,255,255,0.02)' }}>
                      🔶 DBMS_OUTPUT
                    </div>
                    <div style={{ padding: '10px 14px', fontFamily: 'monospace', fontSize: '11px', lineHeight: '1.8', overflowX: 'auto' }}>
                      {lines.map((line, i) => {
                        const isSep = /^={3,}|-{3,}/.test(line);
                        const isWarn = /error|warning|fail|✖|⚠/i.test(line);
                        const isOk = /success|ok|✔|✅|completed/i.test(line);
                        const color = isSep ? 'rgba(180,140,100,0.4)' : isWarn ? '#fbbf24' : isOk ? '#4ade80' : '#fcd9a0';
                        return (
                          <div key={i} style={{ display: 'flex', gap: '8px', color }}>
                            {!isSep && <span style={{ color: 'rgba(180,100,30,0.7)', flexShrink: 0 }}>DBMS_OUTPUT:</span>}
                            <span>{line}</span>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                );
              }

              return (
                <div className="mb-4 rounded-lg border border-slate-200 bg-white overflow-hidden dark:bg-slate-900/80 dark:border-slate-700">
                  <div className="px-3 py-2 bg-slate-100 border-b border-slate-200 text-xs font-bold uppercase tracking-wide text-slate-800 dark:bg-slate-800 dark:border-slate-700 dark:text-slate-100">
                    Resultado final
                  </div>
                  <div className="overflow-x-auto">
                    <table className="min-w-full text-xs">
                      <thead>
                        <tr className="bg-slate-50 dark:bg-slate-800">
                          {cols.map((col) => (
                            <th key={col} className="px-3 py-2 text-left text-[10px] font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400 whitespace-nowrap">
                              {col}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {rows.slice(0, 50).map((row, i) => (
                          <tr key={i} className="border-t border-slate-100 dark:border-slate-700">
                            {cols.map((col) => (
                              <td key={col} className="px-3 py-2 text-slate-700 dark:text-slate-200 whitespace-nowrap">
                                {String(row[col] ?? '')}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              );
            })()}

            {/* Execute button */}
            {!execDone && (
              <button
                onClick={handleExecute}
                disabled={executing}
                className="btn-accent w-full disabled:opacity-50 disabled:cursor-not-allowed text-white font-medium py-3 px-4 rounded-lg transition-colors duration-200 flex items-center justify-center gap-2"
              >
                {executing ? (
                  <>
                    <span className="animate-spin">⏳</span>
                    Ejecutando...
                  </>
                ) : (
                  <>
                    <span>🔧</span>
                    Ejecutar Solución
                  </>
                )}
              </button>
            )}

            </div>
            )}
          </div>
        )}

        {/* Footer */}
        {selectedArticle.creado_en && (
          <div className="mt-6 text-xs text-[color:var(--ink-700)] text-center">
            Última actualización: {new Date(selectedArticle.creado_en).toLocaleDateString('es-ES')}
          </div>
        )}
      </div>
    </div>
  );
}


