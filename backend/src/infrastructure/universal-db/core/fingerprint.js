import crypto from 'crypto';

export function normalizeIdentifier(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_.:@-]+/g, '_');
}

export function createDatabaseId(config = {}) {
  if (config.dbId) return normalizeIdentifier(config.dbId);
  if (config.id) return normalizeIdentifier(config.id);

  const seed = [
    normalizeIdentifier(config.type),
    normalizeIdentifier(config.host),
    String(config.port || '').trim(),
    normalizeIdentifier(config.database),
    normalizeIdentifier(config.user),
  ].join('|');

  const digest = crypto.createHash('sha256').update(seed).digest('hex').slice(0, 16);
  return `db_${digest}`;
}

export function buildDatabaseFingerprint({ type, database, tables = [] }) {
  const sortedTables = [...new Set((tables || []).map((tableName) => normalizeIdentifier(tableName)).filter(Boolean))]
    .sort((left, right) => left.localeCompare(right));

  const fingerprintPayload = JSON.stringify({
    type: normalizeIdentifier(type),
    database: normalizeIdentifier(database),
    tables: sortedTables,
  });

  return crypto.createHash('sha256').update(fingerprintPayload).digest('hex');
}
