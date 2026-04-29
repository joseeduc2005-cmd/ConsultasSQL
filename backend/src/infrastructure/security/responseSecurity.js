const DEFAULT_SENSITIVE_FIELDS = [
  'password',
  'users_password',
  'token',
  'secret',
];

function normalizeKey(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '_');
}

function shouldRedactKey(key, sensitiveFieldsSet) {
  const normalized = normalizeKey(key);
  if (!normalized) return false;
  if (sensitiveFieldsSet.has(normalized)) return true;

  // Harden against common variants (access_token, refresh_token, api_secret, etc)
  return /(password|passwd|token|secret|api_key|key_secret)/i.test(normalized);
}

function shouldSkipRedactionForRequest(req, key, excludeTokenPaths = []) {
  const normalizedKey = normalizeKey(key);
  if (normalizedKey !== 'token') return false;

  const requestPath = String(req?.path || req?.originalUrl || '').split('?')[0];
  return excludeTokenPaths.some((prefix) => requestPath.startsWith(prefix));
}

function sanitizeValue(value, sensitiveFieldsSet, req, excludeTokenPaths = []) {
  if (Array.isArray(value)) {
    return value.map((item) => sanitizeValue(item, sensitiveFieldsSet, req, excludeTokenPaths));
  }

  if (!value || typeof value !== 'object') {
    return value;
  }

  const output = {};
  for (const [key, nestedValue] of Object.entries(value)) {
    if (shouldRedactKey(key, sensitiveFieldsSet) && !shouldSkipRedactionForRequest(req, key, excludeTokenPaths)) {
      output[key] = '[REDACTED]';
      continue;
    }
    output[key] = sanitizeValue(nestedValue, sensitiveFieldsSet, req, excludeTokenPaths);
  }

  return output;
}

function capRows(payload, maxRows) {
  if (!payload || typeof payload !== 'object') return payload;

  const output = { ...payload };
  if (Array.isArray(output.data) && output.data.length > maxRows) {
    output.data = output.data.slice(0, maxRows);
    output.rowCount = maxRows;

    const nextWarnings = Array.isArray(output.warnings) ? [...output.warnings] : [];
    nextWarnings.push(`Resultado truncado por seguridad a ${maxRows} filas`);
    output.warnings = [...new Set(nextWarnings)];
  }

  return output;
}

export function createResponseSecurityMiddleware(options = {}) {
  const sensitiveFields = options.sensitiveFields || DEFAULT_SENSITIVE_FIELDS;
  const maxRows = Math.max(1, Math.min(Number(options.maxRows) || 100, 1000));
  const sensitiveFieldsSet = new Set(sensitiveFields.map((field) => normalizeKey(field)));
  const excludeTokenPaths = Array.isArray(options.excludeTokenPaths)
    ? options.excludeTokenPaths
    : ['/api/auth/login', '/api/auth/register'];

  return function responseSecurityMiddleware(req, res, next) {
    const originalJson = res.json.bind(res);

    res.json = (body) => {
      const redacted = sanitizeValue(body, sensitiveFieldsSet, req, excludeTokenPaths);
      const bounded = capRows(redacted, maxRows);
      return originalJson(bounded);
    };

    next();
  };
}

export default createResponseSecurityMiddleware;
