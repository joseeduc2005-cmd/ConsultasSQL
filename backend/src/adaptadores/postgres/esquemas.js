/**
 * El esquema que el sistema se asegura de tener antes de arrancar.
 *
 * Seis funciones de DDL idempotente: crean la tabla si falta, anaden la columna si
 * falta, arreglan la restriccion si quedo vieja. No deciden nada del negocio, solo
 * dejan la base en la forma que el resto del codigo da por supuesta.
 *
 * Como la auditoria, reciben el pool por getter y no por valor: al recargar la
 * configuracion multi-base el pool se reasigna, y quedarse con el viejo significaria
 * crear las tablas en la base equivocada sin que nadie viera un error.
 */

let obtenerPool = () => {
  throw new Error('Los esquemas no tienen pool: falta llamar a configurarEsquemas al arrancar.');
};

let obtenerHuellaConexion = () => '';

export function configurarEsquemas({ pool, huellaConexion }) {
  if (pool) obtenerPool = pool;
  if (huellaConexion) obtenerHuellaConexion = huellaConexion;
}

export async function ensureKnowledgeBaseSchema() {
  try {
    await obtenerPool().query(`ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS descripcion TEXT`);
    await obtenerPool().query(`ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS contenido_md TEXT`);
    await obtenerPool().query(`ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS tipo_solucion VARCHAR(20) DEFAULT 'lectura'`);
    await obtenerPool().query(`ALTER TABLE knowledge_base DROP CONSTRAINT IF EXISTS knowledge_base_tipo_solucion_check`);
    await obtenerPool().query(`
      DO $$
      DECLARE c record;
      BEGIN
        FOR c IN
          SELECT con.conname
          FROM pg_constraint con
          JOIN pg_class rel ON rel.oid = con.conrelid
          JOIN pg_namespace nsp ON nsp.oid = con.connamespace
          WHERE rel.relname = 'knowledge_base'
            AND nsp.nspname = 'public'
            AND con.contype = 'c'
            AND pg_get_constraintdef(con.oid) ILIKE '%tipo_solucion%'
        LOOP
          EXECUTE format('ALTER TABLE knowledge_base DROP CONSTRAINT %I', c.conname);
        END LOOP;
      END $$;
    `);
    await obtenerPool().query(`
      ALTER TABLE knowledge_base
      ADD CONSTRAINT knowledge_base_tipo_solucion_check
      CHECK (tipo_solucion IN ('lectura', 'ejecutable', 'database', 'script'))
    `);
    await obtenerPool().query(`ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS pasos JSONB`);
    await obtenerPool().query(`ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS campos_formulario JSONB`);
    await obtenerPool().query(`ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS script TEXT`);
    await obtenerPool().query(`ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS script_json JSONB`);
    // ── Auditoría y control de visibilidad ────────────────────────────────
    await obtenerPool().query(`ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS visible_para VARCHAR(20) DEFAULT 'todos'`);
    await obtenerPool().query(`ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS creado_por_username VARCHAR(100)`);
    await obtenerPool().query(`ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS actualizado_por VARCHAR(100)`);
    await obtenerPool().query(`ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS actualizado TIMESTAMP`);
    // Tabla de auditoría de ejecuciones (article_id = TEXT para soportar UUIDs)
    await obtenerPool().query(`
      CREATE TABLE IF NOT EXISTS article_executions (
        id SERIAL PRIMARY KEY,
        article_id TEXT,
        article_titulo TEXT,
        ejecutado_por VARCHAR(100),
        ejecutado_rol VARCHAR(20),
        parametros_usados JSONB,
        filas_resultado INTEGER DEFAULT 0,
        duracion_ms INTEGER DEFAULT 0,
        estado VARCHAR(10) DEFAULT 'ok',
        mensaje_error TEXT,
        ejecutado_en TIMESTAMP DEFAULT NOW()
      )
    `);
    // Migración: si la columna article_id ya existe como INTEGER, convertirla a TEXT
    await obtenerPool().query(`
      DO $$
      BEGIN
        IF EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_name='article_executions' AND column_name='article_id' AND data_type='integer'
        ) THEN
          ALTER TABLE article_executions ALTER COLUMN article_id TYPE TEXT USING article_id::TEXT;
          ALTER TABLE article_executions ALTER COLUMN article_id DROP NOT NULL;
        END IF;
        IF EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_name='article_executions' AND column_name='ejecutado_por' AND is_nullable='NO'
        ) THEN
          ALTER TABLE article_executions ALTER COLUMN ejecutado_por DROP NOT NULL;
        END IF;
      END$$
    `);
    // Tabla de auditoría de cambios a artículos (article_id = TEXT para soportar UUIDs)
    await obtenerPool().query(`
      CREATE TABLE IF NOT EXISTS article_changes (
        id SERIAL PRIMARY KEY,
        article_id TEXT,
        article_titulo TEXT,
        accion VARCHAR(50) NOT NULL,
        realizado_por VARCHAR(100),
        realizado_rol VARCHAR(20),
        detalle JSONB,
        realizado_en TIMESTAMP DEFAULT NOW()
      )
    `);
    // Migración: ampliar accion de VARCHAR(10) a VARCHAR(50) para soportar 'eliminar_comentario' etc.
    await obtenerPool().query(`
      DO $$ BEGIN
        IF EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_name='article_changes' AND column_name='accion'
            AND character_maximum_length <= 10
        ) THEN
          ALTER TABLE article_changes ALTER COLUMN accion TYPE VARCHAR(50);
        END IF;
      END $$;
    `);
    // Migración: si article_id en article_changes es INTEGER, convertir a TEXT
    await obtenerPool().query(`
      DO $$
      BEGIN
        IF EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_name='article_changes' AND column_name='article_id' AND data_type='integer'
        ) THEN
          ALTER TABLE article_changes ALTER COLUMN article_id TYPE TEXT USING article_id::TEXT;
        END IF;
        IF EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_name='article_changes' AND column_name='realizado_por' AND is_nullable='NO'
        ) THEN
          ALTER TABLE article_changes ALTER COLUMN realizado_por DROP NOT NULL;
        END IF;
      END$$
    `);
    // ── Auditoría de logins ───────────────────────────────────────────────────
    await obtenerPool().query(`
      CREATE TABLE IF NOT EXISTS login_audit (
        id        SERIAL PRIMARY KEY,
        username  VARCHAR(100) NOT NULL,
        rol       VARCHAR(20),
        ip        VARCHAR(60),
        user_agent TEXT,
        estado    VARCHAR(10) DEFAULT 'ok',
        detalle   TEXT,
        fecha     TIMESTAMP DEFAULT NOW()
      )
    `);
    // ── Auditoría de eventos del sistema (descargas, exportaciones, etc.) ─────
    await obtenerPool().query(`
      CREATE TABLE IF NOT EXISTS system_events (
        id       SERIAL PRIMARY KEY,
        evento   VARCHAR(60) NOT NULL,
        usuario  VARCHAR(100),
        rol      VARCHAR(20),
        detalle  JSONB,
        ip       VARCHAR(60),
        fecha    TIMESTAMP DEFAULT NOW()
      )
    `);
    console.log('✓ Esquema knowledge_base: article_executions, article_changes, login_audit, system_events presentes');
  } catch (err) {
    console.error('✗ Error asegurando esquema knowledge_base:', err.message || err);
  }
}

export async function ensureCommentsSchema() {
  try {
    await obtenerPool().query(`
      CREATE TABLE IF NOT EXISTS comments (
        id SERIAL PRIMARY KEY,
        article_id VARCHAR(255) NOT NULL,
        article_title VARCHAR(500),
        parent_id INTEGER REFERENCES comments(id) ON DELETE CASCADE,
        author_username VARCHAR(255) NOT NULL,
        author_role VARCHAR(50) NOT NULL DEFAULT 'user',
        content TEXT NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
      )
    `);
    // Migración: añadir article_title si ya existe la tabla sin esa columna
    await obtenerPool().query(`
      DO $$ BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_name = 'comments' AND column_name = 'article_title'
        ) THEN
          ALTER TABLE comments ADD COLUMN article_title VARCHAR(500);
        END IF;
      END $$;
    `);
    await obtenerPool().query(`CREATE INDEX IF NOT EXISTS idx_comments_article_id ON comments(article_id)`);
    await obtenerPool().query(`CREATE INDEX IF NOT EXISTS idx_comments_parent_id ON comments(parent_id)`);
    console.log('✓ Esquema comments asegurado: tabla comments lista para uso');
  } catch (err) {
    console.error('✗ Error asegurando esquema comments:', err.message || err);
  }
}

export async function ensureBusinessLogsSchema() {
  try {
    await obtenerPool().query(`
      CREATE TABLE IF NOT EXISTS logs (
        id SERIAL PRIMARY KEY,
        accion VARCHAR(120) NOT NULL,
        usuario VARCHAR(255) NOT NULL,
        detalle TEXT,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
      )
    `);
    await obtenerPool().query(`CREATE INDEX IF NOT EXISTS idx_logs_usuario ON logs(usuario)`);
    await obtenerPool().query(`CREATE INDEX IF NOT EXISTS idx_logs_created_at ON logs(created_at)`);
    console.log('✓ Esquema logs asegurado: tabla logs lista para auditoría empresarial');
  } catch (err) {
    console.error('✗ Error asegurando esquema logs:', err.message || err);
  }
}

export async function ensureQueryHistorySchema() {
  try {
    await obtenerPool().query(`
      CREATE TABLE IF NOT EXISTS query_history (
        id SERIAL PRIMARY KEY,
        username VARCHAR(255) NOT NULL,
        user_role VARCHAR(50) NOT NULL,
        query_text TEXT,
        generated_sql TEXT NOT NULL,
        query_params JSONB,
        placeholder_order JSONB,
        execution_ms INTEGER NOT NULL DEFAULT 0,
        row_count INTEGER NOT NULL DEFAULT 0,
        was_cached BOOLEAN NOT NULL DEFAULT FALSE,
        status VARCHAR(20) NOT NULL DEFAULT 'ok',
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
      )
    `);
    await obtenerPool().query('ALTER TABLE query_history ADD COLUMN IF NOT EXISTS query_params JSONB');
    await obtenerPool().query('ALTER TABLE query_history ADD COLUMN IF NOT EXISTS placeholder_order JSONB');
    await obtenerPool().query('CREATE INDEX IF NOT EXISTS idx_query_history_created_at ON query_history(created_at DESC)');
    await obtenerPool().query('CREATE INDEX IF NOT EXISTS idx_query_history_username ON query_history(username)');
    console.log('✓ Esquema query_history asegurado');
  } catch (err) {
    console.error('✗ Error asegurando esquema query_history:', err.message || err);
  }
}

export async function ensureErrorLogsSchema() {
  try {
    await obtenerPool().query(`
      CREATE TABLE IF NOT EXISTS error_logs (
        id SERIAL PRIMARY KEY,
        mensaje TEXT NOT NULL,
        stack TEXT,
        modulo VARCHAR(255),
        capa VARCHAR(120),
        contexto VARCHAR(255),
        archivo VARCHAR(255),
        linea INTEGER,
        metadata JSONB,
        fecha TIMESTAMP WITH TIME ZONE DEFAULT NOW()
      )
    `);
    await obtenerPool().query('CREATE INDEX IF NOT EXISTS idx_error_logs_fecha ON error_logs(fecha DESC)');
    await obtenerPool().query('CREATE INDEX IF NOT EXISTS idx_error_logs_modulo ON error_logs(modulo)');
    console.log('✓ Esquema error_logs asegurado');
  } catch (err) {
    console.error('✗ Error asegurando esquema error_logs:', err.message || err);
  }
}

export async function ensureSemanticLearningSchema() {
  try {
    const connectionFingerprint = obtenerHuellaConexion();
    await obtenerPool().query(`
      CREATE TABLE IF NOT EXISTS semantic_learning (
        id SERIAL PRIMARY KEY,
        db_fingerprint TEXT NOT NULL DEFAULT '',
        normalized_term VARCHAR(120) NOT NULL,
        original_term VARCHAR(120) NOT NULL,
        target_kind VARCHAR(20) NOT NULL,
        target_name VARCHAR(120) NOT NULL,
        table_name VARCHAR(120) NOT NULL DEFAULT '',
        confidence NUMERIC(4,3) NOT NULL DEFAULT 0.650,
        hit_count INTEGER NOT NULL DEFAULT 1,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        last_seen TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        CONSTRAINT semantic_learning_target_kind_check CHECK (target_kind IN ('table', 'column'))
      )
    `);
    await obtenerPool().query('ALTER TABLE semantic_learning ADD COLUMN IF NOT EXISTS db_fingerprint TEXT NOT NULL DEFAULT \'\'');
    await obtenerPool().query('UPDATE semantic_learning SET db_fingerprint = $1 WHERE COALESCE(db_fingerprint, \'\') = \'\'', [connectionFingerprint]);
    await obtenerPool().query('ALTER TABLE semantic_learning DROP CONSTRAINT IF EXISTS semantic_learning_unique');
    await obtenerPool().query('CREATE INDEX IF NOT EXISTS idx_semantic_learning_term ON semantic_learning(normalized_term)');
    await obtenerPool().query('CREATE INDEX IF NOT EXISTS idx_semantic_learning_last_seen ON semantic_learning(last_seen DESC)');
    await obtenerPool().query('CREATE INDEX IF NOT EXISTS idx_semantic_learning_fingerprint ON semantic_learning(db_fingerprint)');
    await obtenerPool().query(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_semantic_learning_unique_scope
      ON semantic_learning (db_fingerprint, normalized_term, target_kind, target_name, table_name)
    `);
    console.log('✓ Esquema semantic_learning asegurado');
  } catch (err) {
    console.error('✗ Error asegurando esquema semantic_learning:', err.message || err);
  }
}
