export class UniversalDbError extends Error {
  constructor(code, message, details = {}) {
    super(message);
    this.name = 'UniversalDbError';
    this.code = String(code || 'UNIVERSAL_DB_ERROR');
    this.details = details && typeof details === 'object' ? details : {};
  }

  toJSON() {
    return {
      name: this.name,
      code: this.code,
      message: this.message,
      details: this.details,
    };
  }
}

export function sanitizeErrorMessage(message = '', secretValues = []) {
  let safeMessage = String(message || '');

  for (const secretValue of secretValues) {
    const token = String(secretValue || '').trim();
    if (!token) continue;
    safeMessage = safeMessage.split(token).join('***');
  }

  return safeMessage;
}

export function buildSafeError(error, options = {}) {
  const secretValues = Array.isArray(options.secretValues) ? options.secretValues : [];
  const defaultCode = options.defaultCode || 'UNIVERSAL_DB_ERROR';
  const defaultMessage = options.defaultMessage || 'Unexpected database connector error';

  if (error instanceof UniversalDbError) {
    return error;
  }

  const safeMessage = sanitizeErrorMessage(error?.message || defaultMessage, secretValues);
  return new UniversalDbError(defaultCode, safeMessage || defaultMessage);
}
