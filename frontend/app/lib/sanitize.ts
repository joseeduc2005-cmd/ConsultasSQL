import DOMPurify from 'isomorphic-dompurify';

const BLOCKED_PATTERNS = /\b(drop|delete|alter|truncate)\b/gi;

export function sanitizeUserText(value: unknown, maxLength = 500): string {
  const raw = String(value || '');
  // Sanitizar HTML pero PRESERVAR espacios originales (no colapsar)
  const cleaned = DOMPurify.sanitize(raw, { ALLOWED_TAGS: [], ALLOWED_ATTR: [] })
    .replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/g, '') // solo control chars
    .slice(0, Math.max(1, maxLength));

  return cleaned;
}

export function sanitizeApiPayload<T>(payload: T): T {
  if (Array.isArray(payload)) {
    return payload.map((item) => sanitizeApiPayload(item)) as T;
  }

  if (!payload || typeof payload !== 'object') {
    if (typeof payload === 'string') {
      return sanitizeUserText(payload, 2000) as T;
    }
    return payload;
  }

  const output: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(payload as Record<string, unknown>)) {
    output[key] = sanitizeApiPayload(value);
  }
  return output as T;
}

/**
 * Remover palabras clave SQL peligrosas pero PRESERVAR espacios normales
 * Esta función se aplica ANTES de enviar al servidor, no en onChange
 */
export function stripDangerousSqlTerms(value: unknown): string {
  const text = String(value || '').trim();
  // Remover solo keywords, no colapsar espacios
  return text.replace(BLOCKED_PATTERNS, '').trim();
}
