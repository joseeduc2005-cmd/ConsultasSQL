/**
 * SqlEngineDetector.js
 *
 * Universal SQL engine detection and query classification.
 * Determines which database engine should execute a given SQL statement
 * without requiring manual configuration from the user.
 *
 * Detection priority:
 *   1. Explicit "-- database: <id>" directive in SQL comments
 *   2. Engine-specific syntax markers (deterministic)
 *   3. null → caller falls back to primary DB
 *
 * Supported engines: oracle | postgres | mysql | mssql
 */

// ─── Oracle markers ──────────────────────────────────────────────────────────
// These identifiers and constructs only exist in Oracle / PL/SQL.
const ORACLE_SYNTAX_RE = new RegExp(
  [
    // Oracle-only data types
    '\\bVARCHAR2\\b',
    '\\bNVARCHAR2\\b',
    '\\bNUMBER\\s*\\(',        // NUMBER(p,s) — Oracle numeric type
    '\\bNUMBER\\s*;',          // variable NUMBER;
    '\\bCLOB\\b',
    '\\bBLOB\\b',
    '\\bNCLOB\\b',
    '\\bRAW\\s*\\(',
    '\\bLONG\\s+RAW\\b',
    '\\bBINARY_FLOAT\\b',
    '\\bBINARY_DOUBLE\\b',
    // Oracle pseudo-columns and functions
    '\\bROWNUM\\b',
    '\\bROWID\\b',
    '\\bDUAL\\b',
    '\\bSYSDATE\\b',
    '\\bSYSTIMESTAMP\\b',
    '\\bCURRENT_DATE\\b(?!.*\\bCURRENT_DATE\\b.*::)',  // without PG cast
    // Oracle built-in packages
    '\\bDBMS_[A-Z_]+\\b',
    '\\bUTL_[A-Z_]+\\b',
    '\\bCTXSYS\\.\\w+',
    '\\bAPEX_[A-Z_]+\\b',
    // Oracle hierarchical / analytical
    '\\bCONNECT\\s+BY\\b',
    '\\bSTART\\s+WITH\\b',
    '\\bLEVEL\\b(?!\\s+\\d)',   // LEVEL pseudo-column (not "LEVEL 3" in text)
    '\\bPRIOR\\b',
    // Oracle functions
    '\\bNVL\\s*\\(',
    '\\bNVL2\\s*\\(',
    '\\bDECODE\\s*\\(',
    '\\bSYS_CONTEXT\\s*\\(',
    '\\bTO_DATE\\s*\\(',
    '\\bTO_CHAR\\s*\\(',
    '\\bTO_NUMBER\\s*\\(',
    '\\bTRUNC\\s*\\([^)]*DATE',
    '\\bWM_CONCAT\\s*\\(',
    '\\bLISTAGG\\s*\\(',
    '\\bREGEXP_LIKE\\s*\\(',
    '\\bREGEXP_SUBSTR\\s*\\(',
    '\\bREGEXP_REPLACE\\s*\\(',
    // Oracle PL/SQL constructs (NOTE: EXCEPTION WHEN removed — shared with PostgreSQL)
    '\\bRAISE_APPLICATION_ERROR\\b',
    '%TYPE\\b',
    '%ROWTYPE\\b',
    '\\bPRAGMA\\s+\\w+',
    '\\bIS\\s+TABLE\\b',
    // Oracle schema prefixes (most distinctive)
    'SYSTEM\\.',
    'SCOTT\\.',
    '\\bSYS\\.\\w+',
  ].join('|'),
  'i',
);

// ─── PostgreSQL markers ───────────────────────────────────────────────────────
const POSTGRES_SYNTAX_RE = new RegExp(
  [
    '::[a-zA-Z]',              // type cast ::text, ::int, etc.
    '\\bILIKE\\b',
    '\\bRETURNING\\b',
    '\\bNOW\\(\\)',
    '\\bpg_catalog\\b',
    '\\bpg_class\\b',
    '\\bpg_tables\\b',
    '\\bpg_indexes\\b',
    '\\binformation_schema\\b',
    '\\bGEN_RANDOM_UUID\\(\\)',
    '\\buuid_generate_v4\\(\\)',
    '\\barray_agg\\b',
    '\\bstring_agg\\b',
    '\\bjsonb?\\b',
    '\\bhstore\\b',
    '\\$\\d+\\b',              // positional parameter $1, $2
    '\\bSERIAL\\b',
    '\\bBIGSERIAL\\b',
    '\\bSMALLSERIAL\\b',
    '\\bBOOLEAN\\b',
    '\\bDO\\s+\\$\\$',         // PostgreSQL anonymous blocks DO $$ ... $$
    '\\bpublic\\.',
    '\\bLIMIT\\s+\\d+',        // LIMIT n (MySQL also has this, but checked after Oracle)
    '\\bOFFSET\\s+\\d+',
  ].join('|'),
  'i',
);

// ─── MySQL markers ────────────────────────────────────────────────────────────
const MYSQL_SYNTAX_RE = new RegExp(
  [
    '\\bAUTO_INCREMENT\\b',
    '\\bENGINE\\s*=\\s*\\w+',
    '\\bCHARSET\\s*=\\s*\\w+',
    '\\bCOLLATE\\s+\\w+',
    '\\bSHOW\\s+TABLES\\b',
    '\\bSHOW\\s+DATABASES\\b',
    '\\bDESCRIBE\\s+\\w+',
    '\\bTINYINT\\b',
    '\\bMEDIUMINT\\b',
    '\\bDATETIME\\b(?!\\s+WITHOUT)',
    '\\bGROUP_CONCAT\\s*\\(',
    '\\bIFNULL\\s*\\(',
    '\\bFIELD\\s*\\(',
    '\\bINSERT_ID\\s*\\(',
    '`[a-zA-Z_\\u0080-\\uFFFF]',  // backtick identifiers
    '\\bUSE\\s+\\w+\\s*;',
  ].join('|'),
  'i',
);

// ─── SQL Server markers ───────────────────────────────────────────────────────
const MSSQL_SYNTAX_RE = new RegExp(
  [
    '\\bTOP\\s+\\d+\\b',       // TOP n (before SELECT list)
    '\\bNVARCHAR\\b',
    '\\bNTEXT\\b',
    '\\bGETDATE\\(\\)',
    '\\bGETUTCDATE\\(\\)',
    '\\bDATEADD\\s*\\(',
    '\\bDATEDIFF\\s*\\(',
    '\\bDATENAME\\s*\\(',
    '\\bDATEPART\\s*\\(',
    '\\bCONVERT\\s*\\([^,]+,',  // CONVERT(type, expr) - SQL Server style
    '@@[A-Z_]+',               // @@IDENTITY, @@ROWCOUNT, @@VERSION
    '\\bNOLOCK\\b',
    '\\bWITH\\s*\\(NOLOCK\\)',
    '\\bIDENTITY\\s*\\(',
    '\\bNEWID\\(\\)',
    '\\bSELECT\\s+TOP\\b',
    '\\bROW_NUMBER\\s*\\(\\)',
    '\\bSYSUTCDATETIME\\(\\)',
    '\\[\\w+\\]',              // [column] bracket identifiers
    '\\bPRINT\\s+',
    '\\bEXEC\\s+\\w+',         // EXEC stored_proc
    '\\bSET\\s+NOCOUNT\\b',
  ].join('|'),
  'i',
);

// ─── Oracle PL/SQL package calls (native execution required) ─────────────────
const ORACLE_PLSQL_PACKAGES_RE = /\bDBMS_[A-Z_]+\b|\bUTL_[A-Z_]+\b|\bRAISE_APPLICATION_ERROR\b|\bAPEX_[A-Z_]+\b/i;

// ─── Public API ──────────────────────────────────────────────────────────────

/**
 * Parses a "-- database: <id>" directive from the first 5 lines of SQL.
 * Returns the database ID or null.
 *
 * Example:
 *   -- database: oracle_test
 *   SELECT * FROM system.comments WHERE ROWNUM <= 10
 */
export function extractDatabaseDirective(sql) {
  const lines = String(sql || '').slice(0, 800).split('\n');
  for (let i = 0; i < Math.min(5, lines.length); i++) {
    const m = lines[i].match(/^--\s*database\s*:\s*(\S+)/i);
    if (m) return m[1].trim();
  }
  return null;
}

/**
 * Detects the SQL engine from syntax markers.
 * Returns 'oracle' | 'postgres' | 'mysql' | 'mssql' | null.
 *
 * Priority: Oracle → SQL Server → MySQL → PostgreSQL
 * Oracle is first because its markers are most distinctive and failures
 * are most costly (type system incompatibilities cause hard errors).
 * SQL Server before MySQL because TOP/NVARCHAR are unambiguous.
 * Postgres last because LIMIT and OFFSET appear in MySQL too.
 */
export function detectSqlSyntaxEngine(sql) {
  const src = String(sql || '');
  if (!src.trim()) return null;
  // DO $$ is unambiguous PostgreSQL — must win over any Oracle marker match
  if (/\bDO\s+\$\$/i.test(src)) return 'postgres';
  if (ORACLE_SYNTAX_RE.test(src)) return 'oracle';
  if (MSSQL_SYNTAX_RE.test(src)) return 'mssql';
  if (MYSQL_SYNTAX_RE.test(src)) return 'mysql';
  if (POSTGRES_SYNTAX_RE.test(src)) return 'postgres';
  return null;
}

/**
 * Classifies the SQL statement type.
 * Returns 'plsql' | 'select' | 'dml' | 'ddl' | 'unknown'.
 *
 * Used for logging, security checks, and model selection.
 */
export function detectQueryType(sql) {
  const src = String(sql || '').trim();
  if (!src) return 'unknown';

  // PL/SQL / procedural blocks
  if (/^\s*(?:DECLARE\b|BEGIN\b)/i.test(src)) return 'plsql';
  if (/\bDO\s+\$\$/i.test(src)) return 'plsql';       // PostgreSQL anonymous block

  const firstToken = src.match(/^\s*([A-Z_]+)/i)?.[1]?.toUpperCase() || '';

  if (firstToken === 'SELECT' || firstToken === 'WITH') return 'select';
  if (['INSERT', 'UPDATE', 'DELETE', 'MERGE', 'UPSERT'].includes(firstToken)) return 'dml';
  if (['CREATE', 'DROP', 'ALTER', 'TRUNCATE', 'GRANT', 'REVOKE'].includes(firstToken)) return 'ddl';
  return 'unknown';
}

/**
 * Returns true if the SQL is an Oracle PL/SQL block that requires native
 * Oracle execution (BEGIN...END with Oracle package calls like DBMS_OUTPUT).
 */
export function isNativePlSql(sql) {
  const src = String(sql || '').trim();
  if (!/\bBEGIN\b/i.test(src)) return false;
  // Must have Oracle package calls OR Oracle-specific syntax
  return ORACLE_PLSQL_PACKAGES_RE.test(src) || detectSqlSyntaxEngine(src) === 'oracle';
}

/**
 * Detects query complexity for AI model selection.
 * Returns 'plsql' | 'complex' | 'simple'.
 */
export function detectQueryComplexity(sql) {
  const qType = detectQueryType(sql);
  if (qType === 'plsql') return 'plsql';

  const src = String(sql || '').toUpperCase();
  if (
    /\bJOIN\b[\s\S]{0,300}\bJOIN\b/.test(src)   // multiple JOINs
    || /\bGROUP\s+BY\b/.test(src)
    || /\bHAVING\b/.test(src)
    || /\bUNION\b/.test(src)
    || /\bWITH\b[\s\S]{0,200}\bSELECT\b/.test(src)
  ) return 'complex';

  return 'simple';
}

/**
 * Maps query complexity to the recommended Claude model ID.
 *
 * simple  → Haiku  (single-table SELECTs, fast + cheap)
 * complex → Sonnet (JOINs, GROUP BY, aggregations)
 * plsql   → Opus   (PL/SQL, multi-base distributed logic)
 */
export function pickModelForComplexity(complexity) {
  if (complexity === 'plsql') return 'claude-opus-4-7';
  if (complexity === 'complex') return 'claude-sonnet-4-6';
  return 'claude-haiku-4-5-20251001';
}

/**
 * Produces a structured log object for engine routing decisions.
 * All routing points should call this for consistent observability.
 *
 * @param {object} opts
 * @param {string|null} opts.engine    - Detected engine type
 * @param {string}      opts.queryType - Output of detectQueryType()
 * @param {string}      opts.databaseId - Resolved database ID
 * @param {string}      opts.reason    - Human-readable routing reason
 * @param {string}      [opts.sql]     - First 80 chars of the SQL (optional)
 */
export function buildEngineLog({ engine, queryType, databaseId, reason, sql = '' }) {
  const preview = String(sql || '').replace(/\s+/g, ' ').slice(0, 80);
  return [
    `[ENGINE DETECTED] ${engine || 'unknown'}`,
    `[QUERY TYPE] ${queryType || 'unknown'}`,
    `[ROUTING] ${databaseId || 'primary (fallback)'}`,
    `[REASON] ${reason || 'no specific markers'}`,
    preview ? `[SQL] ${preview}${sql.length > 80 ? '...' : ''}` : '',
  ].filter(Boolean).join(' | ');
}
