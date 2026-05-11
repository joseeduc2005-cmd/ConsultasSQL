// Schemas del sistema que nunca deben incluirse en la introspección
const PG_SYSTEM_SCHEMAS = `(
  'information_schema', 'pg_catalog', 'pg_toast',
  'pg_temp_1', 'pg_toast_temp_1'
)`;

export const POSTGRES_SCHEMA_QUERIES = {
  tables: `
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema NOT IN ${PG_SYSTEM_SCHEMAS}
      AND table_schema NOT LIKE 'pg_%'
      AND table_type = 'BASE TABLE'
    ORDER BY table_schema, table_name
  `,
  columns: `
    SELECT table_schema, table_name, column_name, data_type, is_nullable, column_default
    FROM information_schema.columns
    WHERE table_schema NOT IN ${PG_SYSTEM_SCHEMAS}
      AND table_schema NOT LIKE 'pg_%'
    ORDER BY table_schema, table_name, ordinal_position
  `,
  primaryKeys: `
    SELECT tc.table_schema, tc.table_name, kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
      AND tc.table_name = kcu.table_name
    WHERE tc.table_schema NOT IN ${PG_SYSTEM_SCHEMAS}
      AND tc.table_schema NOT LIKE 'pg_%'
      AND tc.constraint_type = 'PRIMARY KEY'
    ORDER BY tc.table_schema, tc.table_name, kcu.ordinal_position
  `,
  foreignKeys: `
    SELECT
      tc.table_schema,
      tc.table_name,
      kcu.column_name,
      tc.constraint_name,
      ccu.table_name AS referenced_table,
      ccu.column_name AS referenced_column
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage ccu
      ON ccu.constraint_name = tc.constraint_name
      AND ccu.table_schema = tc.table_schema
    WHERE tc.table_schema NOT IN ${PG_SYSTEM_SCHEMAS}
      AND tc.table_schema NOT LIKE 'pg_%'
      AND tc.constraint_type = 'FOREIGN KEY'
    ORDER BY tc.table_schema, tc.table_name, kcu.ordinal_position
  `,
};

export const MYSQL_SCHEMA_QUERIES = {
  tables: `
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = ?
      AND table_type = 'BASE TABLE'
    ORDER BY table_name
  `,
  columns: `
    SELECT table_name, column_name, data_type, is_nullable, column_default
    FROM information_schema.columns
    WHERE table_schema = ?
    ORDER BY table_name, ordinal_position
  `,
  primaryKeys: `
    SELECT tc.table_name, kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
      AND tc.table_name = kcu.table_name
    WHERE tc.table_schema = ?
      AND tc.constraint_type = 'PRIMARY KEY'
    ORDER BY tc.table_name, kcu.ordinal_position
  `,
  foreignKeys: `
    SELECT
      table_name,
      column_name,
      constraint_name,
      referenced_table_name AS referenced_table,
      referenced_column_name AS referenced_column
    FROM information_schema.key_column_usage
    WHERE table_schema = ?
      AND referenced_table_name IS NOT NULL
    ORDER BY table_name, ordinal_position
  `,
};

// Todos los owners de sistema estándar de Oracle (11g, 12c, 19c, 21c)
// Excluirlos evita que la introspección traiga miles de tablas internas de Oracle
const ORACLE_SYSTEM_OWNERS = `(
  'SYS','SYSTEM','XDB','MDSYS','CTXSYS','ORDSYS','ORDDATA','ORDPLUGINS',
  'OLAPSYS','WMSYS','EXFSYS','DMSYS','OUTLN','DBSNMP','APPQOSSYS',
  'OJVMSYS','LBACSYS','OWBSYS','OWBSYS_AUDIT','APEX_030200','APEX_040000',
  'APEX_040200','APEX_050000','APEX_180200','APEX_190200','APEX_200200',
  'APEX_210200','FLOWS_FILES','ANONYMOUS','DIP','ORACLE_OCM','SPATIAL_CSW_ADMIN_USR',
  'SPATIAL_WFS_ADMIN_USR','MDDATA','AUDSYS','DVF','DVSYS','GGSYS',
  'GSMADMIN_INTERNAL','GSMCATUSER','GSMROOTUSER','GSMUSER','SYSBACKUP',
  'SYSDG','SYSKM','SYSRAC','REMOTE_SCHEDULER_AGENT','PDBADMIN',
  'ORDS_METADATA','ORDS_PUBLIC_USER','C##ORDS_METADATA','C##ORDS_PUBLIC_USER'
)`;

export const ORACLE_SCHEMA_QUERIES = {
  tables: `
    SELECT owner, table_name
    FROM all_tables
    WHERE owner NOT IN ${ORACLE_SYSTEM_OWNERS}
    ORDER BY owner, table_name
  `,
  columns: `
    SELECT owner, table_name, column_name, data_type, nullable, data_default
    FROM all_tab_columns
    WHERE owner NOT IN ${ORACLE_SYSTEM_OWNERS}
    ORDER BY owner, table_name, column_id
  `,
  primaryKeys: `
    SELECT acc.owner, acc.table_name, acc.column_name
    FROM all_constraints ac
    JOIN all_cons_columns acc
      ON ac.owner = acc.owner
      AND ac.constraint_name = acc.constraint_name
    WHERE ac.constraint_type = 'P'
      AND acc.owner NOT IN ${ORACLE_SYSTEM_OWNERS}
    ORDER BY acc.owner, acc.table_name, acc.position
  `,
  foreignKeys: `
    SELECT
      a.owner,
      a.table_name,
      a.column_name,
      a.constraint_name,
      c_pk.table_name AS referenced_table,
      b.column_name AS referenced_column
    FROM all_cons_columns a
    JOIN all_constraints c
      ON a.owner = c.owner
      AND a.constraint_name = c.constraint_name
    JOIN all_constraints c_pk
      ON c.r_owner = c_pk.owner
      AND c.r_constraint_name = c_pk.constraint_name
    JOIN all_cons_columns b
      ON c_pk.owner = b.owner
      AND c_pk.constraint_name = b.constraint_name
      AND a.position = b.position
    WHERE c.constraint_type = 'R'
      AND a.owner NOT IN ${ORACLE_SYSTEM_OWNERS}
    ORDER BY a.owner, a.table_name, a.position
  `,
};
