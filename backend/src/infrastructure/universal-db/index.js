import UniversalDatabaseConnector from './core/UniversalDatabaseConnector.js';

const defaultConnector = new UniversalDatabaseConnector({
  schemaTtlMs: 60 * 1000,
});

export function connectDatabase(config) {
  return defaultConnector.connectDatabase(config);
}

export function getSchema(dbId) {
  return defaultConnector.getSchema(dbId);
}

export function getConnection(dbId) {
  return defaultConnector.getConnection(dbId);
}

export function refreshSchema(dbId) {
  return defaultConnector.refreshSchema(dbId);
}

export function closeConnection(dbId) {
  return defaultConnector.closeConnection(dbId);
}

export function listDatabaseContexts() {
  return defaultConnector.listContexts();
}

export function getDatabaseContext(dbId) {
  return defaultConnector.getContext(dbId);
}

export { UniversalDatabaseConnector };

export default defaultConnector;
