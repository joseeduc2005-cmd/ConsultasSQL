/**
 * Qué conexiones de salida se permiten, y de qué orígenes se aceptan peticiones.
 *
 * La política es «solo local»: nada de este sistema debería llamar a un servicio de
 * fuera. Estaba escrita en `app.js` entre otras seis mil líneas; aquí se lee de un
 * vistazo y, sobre todo, se puede probar que de verdad bloquea lo que dice bloquear.
 */

export const ALLOWED_OUTBOUND_HOSTS = new Set(['localhost', '127.0.0.1', '::1']);

export const BLOCKED_OUTBOUND_PATTERNS = ['openai', 'anthropic', 'googleapis', 'huggingface', 'ai'];

export const CORS_ALLOWED_ORIGINS = (process.env.CORS_ALLOWED_ORIGINS || 'http://localhost:3000,http://localhost:3001,http://localhost:3002')
  .split(',')
  .map((origin) => String(origin || '').trim())
  .filter(Boolean);

export function sanitizeOrigin(origin) {
  return String(origin || '').trim();
}

export function isOriginAllowed(origin) {
  if (!origin) return true;
  const normalizedOrigin = sanitizeOrigin(origin);
  return CORS_ALLOWED_ORIGINS.includes(normalizedOrigin);
}

export function extractHostFromRequestOptions(options, maybeUrl) {
  if (typeof options === 'string') {
    try {
      return new URL(options).hostname || '';
    } catch {
      return '';
    }
  }

  if (options instanceof URL) {
    return options.hostname || '';
  }

  if (options && typeof options === 'object') {
    return String(options.hostname || options.host || '').toLowerCase();
  }

  if (typeof maybeUrl === 'string') {
    try {
      return new URL(maybeUrl).hostname || '';
    } catch {
      return '';
    }
  }

  return '';
}

export function isOutboundBlocked(hostnameRaw) {
  const hostname = String(hostnameRaw || '').toLowerCase().trim();
  if (!hostname) return false;

  if (ALLOWED_OUTBOUND_HOSTS.has(hostname)) return false;

  if (BLOCKED_OUTBOUND_PATTERNS.some((pattern) => hostname.includes(pattern))) {
    return true;
  }

  return true;
}
