const { Pool } = require('pg');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../../backend/.env') });

function resolvePrimaryDatabaseUrl(baseDir) {
  const configFile = String(
    process.env.MULTI_DB_CONFIG_FILE
    || process.env.DATABASES_CONFIG_FILE
    || './config/multidb.databases.json'
    || ''
  ).trim();

  if (configFile) {
    try {
      const resolvedPath = path.isAbsolute(configFile)
        ? configFile
        : path.resolve(baseDir, configFile);

      if (require('fs').existsSync(resolvedPath)) {
        const parsed = JSON.parse(require('fs').readFileSync(resolvedPath, 'utf8') || '{}');
        const databases = Array.isArray(parsed?.databases) ? parsed.databases : [];
        const primary = databases.find((entry) => entry?.enabled !== false && (entry?.primary === true || entry?.isPrimary === true || String(entry?.role || '').toLowerCase() === 'primary'))
          || (databases.length === 1 ? databases[0] : null);

        if (primary) {
          const direct = String(primary.connectionString || primary.url || '').trim();
          if (direct) {
            return direct;
          }

          const host = String(primary.host || '').trim();
          const database = String(primary.database || '').trim();
          const user = String(primary.user || primary.username || '').trim();
          const password = String(primary.password || '').trim();
          const port = Number(primary.port) || 5432;

          if (host && database && user) {
            const auth = password
              ? `${encodeURIComponent(user)}:${encodeURIComponent(password)}`
              : encodeURIComponent(user);
            return `postgresql://${auth}@${host}:${port}/${database}`;
          }
        }
      }
    } catch (error) {
      console.error('RESOLVE_PRIMARY_DB_ERROR', error.message);
    }
  }

  return process.env.DATABASE_URL;
}

const pool = new Pool({ connectionString: resolvePrimaryDatabaseUrl(path.join(__dirname, '../../backend')) });

pool.query(`
  CREATE TABLE IF NOT EXISTS comments (
    id SERIAL PRIMARY KEY,
    article_id VARCHAR(255) NOT NULL,
    parent_id INTEGER REFERENCES comments(id) ON DELETE CASCADE,
    author_username VARCHAR(255) NOT NULL,
    author_role VARCHAR(50) NOT NULL DEFAULT 'user',
    content TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
  );
  CREATE INDEX IF NOT EXISTS idx_comments_article_id ON comments(article_id);
  CREATE INDEX IF NOT EXISTS idx_comments_parent_id ON comments(parent_id);
`).then(() => {
  console.log('TABLE CREATED OK');
  pool.end();
}).catch(e => {
  console.error('ERROR:', e.message);
  pool.end();
});
