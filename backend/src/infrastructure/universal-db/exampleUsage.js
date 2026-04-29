import {
  closeConnection,
  connectDatabase,
  getConnection,
  getSchema,
  refreshSchema,
} from './index.js';

async function runExample() {
  const config = {
    type: 'postgres',
    host: 'localhost',
    port: 5432,
    database: 'my_company_db',
    user: 'app_user',
    password: 'replace_me',
  };

  const { dbId, fingerprint } = await connectDatabase(config);
  console.log('[UniversalConnector] Connected:', { dbId, fingerprint });

  const schema = await getSchema(dbId);
  console.log('[UniversalConnector] Tables detected:', Object.keys(schema.tables || {}));

  const pool = getConnection(dbId);
  console.log('[UniversalConnector] Pool ready:', Boolean(pool));

  const refreshed = await refreshSchema(dbId);
  console.log('[UniversalConnector] Refreshed fingerprint:', refreshed.fingerprint);

  await closeConnection(dbId);
  console.log('[UniversalConnector] Closed:', dbId);
}

runExample().catch((error) => {
  console.error('[UniversalConnector] Error:', error?.code || 'UNKNOWN', error?.message || error);
});
