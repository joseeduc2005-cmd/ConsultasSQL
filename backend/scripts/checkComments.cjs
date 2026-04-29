const path = require('path');
const dotenv = require('dotenv');
const { Pool } = require('pg');

dotenv.config({ path: path.join(__dirname, '../.env') });

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

async function run() {
  const pool = new Pool({ connectionString: resolvePrimaryDatabaseUrl(path.join(__dirname, '..')) });
  try {
    const table = await pool.query(
      "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name='comments'"
    );
    const count = await pool.query('SELECT COUNT(*)::int AS total FROM comments');
    const last = await pool.query(
      'SELECT id, article_id, parent_id, author_username, author_role, content, created_at FROM comments ORDER BY id DESC LIMIT 5'
    );

    console.log(`TABLE_EXISTS=${table.rowCount > 0}`);
    console.log(`TOTAL_COMMENTS=${count.rows[0].total}`);
    console.log(`LAST_COMMENTS=${JSON.stringify(last.rows)}`);
  } catch (error) {
    console.error('CHECK_ERROR', error.message);
    process.exitCode = 1;
  } finally {
    await pool.end();
  }
}

run();
