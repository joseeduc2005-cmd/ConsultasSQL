-- Base de conocimiento del módulo de soporte (tickets, diagnósticos, vigilancia).
--
-- Migrado desde el SQLite de ticket v2.0. Vive en su propio esquema para no
-- mezclarse con las tablas de knowledge_base (users, roles, logs, query_history…).
--
-- Este archivo lo ejecuta el backend al arrancar, así que TODO debe ser idempotente.

CREATE SCHEMA IF NOT EXISTS soporte;

-- Quita tildes sin depender de la extensión unaccent (que exige superusuario).
-- IMMUTABLE es obligatorio para poder usarla en una columna generada.
CREATE OR REPLACE FUNCTION soporte.sin_tildes(texto text)
  RETURNS text LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
  SELECT translate($1,
    'áàäâãéèëêíìïîóòöôõúùüûñçÁÀÄÂÃÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÑÇ',
    'aaaaaeeeeiiiiooooouuuuncAAAAAEEEEIIIIOOOOOUUUUNC')
$$;

CREATE TABLE IF NOT EXISTS soporte.tickets (
  numero         text PRIMARY KEY,
  titulo         text NOT NULL,
  descripcion    text,
  autor          text,
  sistema        text,
  modulo         text,
  categoria      text,
  prioridad      text,
  estado         text NOT NULL DEFAULT 'abierto',
  resultado      text,
  url_sir        text,
  causa_raiz     text,
  solucion       text,
  observaciones  text,
  -- Fechas de día, sin hora ni zona: se muestran tal cual llegan del SIR.
  creado_en      text,
  resuelto_en    text,
  actualizado_en timestamptz NOT NULL DEFAULT now(),
  -- Reemplaza a la tabla virtual tickets_fts (FTS5) del SQLite original.
  -- Al ser generada se mantiene sola: ya no hace falta reindexar al guardar.
  busqueda tsvector GENERATED ALWAYS AS (
    to_tsvector('spanish', soporte.sin_tildes(
      coalesce(titulo, '') || ' ' || coalesce(descripcion, '') || ' ' ||
      coalesce(causa_raiz, '') || ' ' || coalesce(modulo, '')))
  ) STORED
);

-- Base de datos registrada contra la que se investiga el caso (id de
-- multidb.databases.json). Es contexto de investigación: el ticket siempre se
-- guarda aquí, en PostgreSQL; esto dice de qué base habla.
-- Se añade aparte porque la tabla ya existe en instalaciones en marcha.
ALTER TABLE soporte.tickets ADD COLUMN IF NOT EXISTS base_datos text;

-- Quien pulso «Revisar error». Sin esto el diagnostico quedaba sin dueno, y en un
-- piloto lo que importa es poder rastrear que propuso el sistema y quien lo acepto.
ALTER TABLE soporte.diagnosticos ADD COLUMN IF NOT EXISTS ejecutado_por text;

-- Columnas de knowledge_base que este modulo escribe al publicar una solucion.
--
-- El arranque del backend ya garantiza tipo_solucion, script_json, campos_formulario
-- y visible_para, pero no estas cuatro. Una instalacion antigua —como la del otro
-- servidor— puede no tenerlas, y entonces publicar una solucion falla en el INSERT.
-- Que el modulo declare lo que necesita sale mas barato que descubrirlo en produccion.
ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS categoria varchar(120);
ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS subcategoria varchar(120);
ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS tags text[];
ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS actualizado timestamp;

-- Busqueda de texto sobre los articulos, para encontrar precedentes de un ticket.
--
-- `soporte.tickets` ya tenia la suya; los articulos no, y son justo donde vive lo que
-- el equipo ya resolvio. Se usa la misma funcion sin tildes para que «bloqueo» y
-- «bloqueó» encuentren lo mismo, y GIN porque es lo que sirve para tsvector.
-- Busqueda de texto sobre los articulos, con el titulo pesando mas que el cuerpo.
--
-- Sin pesos, un articulo largo que mencionaba «clave» de pasada ganaba a otro que la
-- lleva en el titulo: el cuerpo tiene cien veces mas palabras y arrastra el ranking.
-- A = titulo, B = descripcion y categoria, C = contenido.
--
-- La columna se recrea si venia sin pesos, porque una columna generada no se altera.
DO $peso$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_name = 'knowledge_base' AND column_name = 'busqueda'
  ) AND NOT EXISTS (
    SELECT 1 FROM pg_attrdef d
      JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
     WHERE a.attrelid = 'knowledge_base'::regclass
       AND a.attname = 'busqueda'
       AND pg_get_expr(d.adbin, d.adrelid) LIKE '%setweight%'
  ) THEN
    EXECUTE 'ALTER TABLE knowledge_base DROP COLUMN busqueda';
  END IF;
END
$peso$;

ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS busqueda tsvector
  GENERATED ALWAYS AS (
    setweight(to_tsvector('spanish', soporte.sin_tildes(coalesce(titulo, ''))), 'A') ||
    setweight(to_tsvector('spanish', soporte.sin_tildes(
      coalesce(descripcion, '') || ' ' || coalesce(categoria, '') || ' ' || coalesce(subcategoria, ''))), 'B') ||
    setweight(to_tsvector('spanish', soporte.sin_tildes(coalesce(contenido, ''))), 'C')
  ) STORED;

CREATE INDEX IF NOT EXISTS knowledge_base_busqueda_idx ON knowledge_base USING GIN (busqueda);

-- Los articulos que este modulo publico antes llevan solo { sql, databaseId }, que
-- basta para ejecutarlos pero no pasa la validacion del editor del panel: exige
-- `modo` y `origen`. Quedaban ejecutables pero imposibles de volver a guardar desde
-- la pantalla. Se completan una vez; los que ya los traen no se tocan.
UPDATE knowledge_base
   SET script_json = script_json || '{"modo":"script","origen":"manual-sql"}'::jsonb
 WHERE script_json IS NOT NULL
   AND script_json ? 'sql'
   AND NOT (script_json ? 'modo');

CREATE TABLE IF NOT EXISTS soporte.ticket_pasos (
  id             integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  ticket         text NOT NULL REFERENCES soporte.tickets (numero) ON DELETE CASCADE,
  numero         integer NOT NULL,
  fecha          text,
  titulo         text NOT NULL,
  sql            text,
  resultado      text,
  evidencia      text,
  conclusion     text,
  origen         text NOT NULL DEFAULT 'manual',
  diagnostico_id integer,
  UNIQUE (ticket, numero)
);

CREATE TABLE IF NOT EXISTS soporte.consultas (
  codigo     text PRIMARY KEY,
  nombre     text NOT NULL,
  categoria  text,
  tipo       text,
  riesgo     text NOT NULL DEFAULT 'lectura',
  sql        text NOT NULL,
  parametros text,
  uso        text,
  sistema    text,
  tickets    text
);

CREATE TABLE IF NOT EXISTS soporte.diagnosticos (
  id           integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  playbook     text NOT NULL,
  entrada      jsonb NOT NULL,
  veredicto    text NOT NULL,
  resumen      text,
  resultado    jsonb NOT NULL,
  ticket       text,
  origen_datos text,
  duracion_ms  integer,
  creado_en    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS soporte.vigilancia_ejecuciones (
  id          integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  monitor     text NOT NULL,
  estado      text NOT NULL,
  hallazgos   integer NOT NULL DEFAULT 0,
  detalle     text,
  duracion_ms integer,
  creado_en   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS soporte.alertas (
  id          integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  monitor     text NOT NULL,
  clave       text NOT NULL,
  titulo      text NOT NULL,
  detalle     text,
  severidad   text NOT NULL,
  datos       jsonb,
  estado      text NOT NULL DEFAULT 'abierta',
  primera_vez timestamptz NOT NULL DEFAULT now(),
  ultima_vez  timestamptz NOT NULL DEFAULT now(),
  veces       integer NOT NULL DEFAULT 1,
  UNIQUE (monitor, clave)
);

CREATE INDEX IF NOT EXISTS idx_soporte_tickets_busqueda  ON soporte.tickets USING gin (busqueda);
CREATE INDEX IF NOT EXISTS idx_soporte_tickets_estado    ON soporte.tickets (estado);
CREATE INDEX IF NOT EXISTS idx_soporte_pasos_ticket      ON soporte.ticket_pasos (ticket, numero);
CREATE INDEX IF NOT EXISTS idx_soporte_diag_fecha        ON soporte.diagnosticos (creado_en DESC);
CREATE INDEX IF NOT EXISTS idx_soporte_diag_ticket       ON soporte.diagnosticos (ticket);
CREATE INDEX IF NOT EXISTS idx_soporte_alertas_estado    ON soporte.alertas (estado, severidad);
CREATE INDEX IF NOT EXISTS idx_soporte_ejec_monitor      ON soporte.vigilancia_ejecuciones (monitor, id DESC);

-- Soluciones que propone quien reporta.
--
-- No entran directo a la base de conocimiento: alguien tiene que leerlas antes. Aquí
-- esperan, y al aceptarlas se crea el artículo y queda anotado cuál salió de cuál.
CREATE TABLE IF NOT EXISTS soporte.aportes (
  id           integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  autor        text NOT NULL,
  titulo       text NOT NULL,
  problema     text,
  pasos        text NOT NULL,
  sql_sugerido text,
  base         text,
  caso         text,
  estado       text NOT NULL DEFAULT 'pendiente',
  nota         text,
  revisado_por text,
  revisado_en  timestamptz,
  articulo     text,
  creado_en    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_soporte_aportes_estado ON soporte.aportes (estado, id DESC);
CREATE INDEX IF NOT EXISTS idx_soporte_aportes_autor  ON soporte.aportes (autor, id DESC);

-- Los aportes pasaron de ser un texto corrido a una lista de pasos, cada uno con su
-- descripción y, si lo hubo, el script que se usó y si ese script solo consultaba o
-- cambiaba algo. `pasos` se conserva con la versión legible: es lo que indexa la
-- búsqueda y lo que ya tenían los aportes anteriores.
ALTER TABLE soporte.aportes ADD COLUMN IF NOT EXISTS pasos_json jsonb;

-- Lo que la gente abre después de buscar.
--
-- Cuando alguien escribe su problema y entra en un tema, está respondiendo a una
-- pregunta que ningún algoritmo contesta bien: «esta forma de decirlo se resuelve con
-- este tema». Guardarlo hace que el sistema mejore con el uso en vez de depender de
-- que alguien afine pesos a mano, y desempata justo donde la búsqueda por palabras se
-- queda corta: entre dos temas que se parecen.
CREATE TABLE IF NOT EXISTS soporte.aprendizaje (
  termino    text NOT NULL,
  articulo   text NOT NULL,
  veces      integer NOT NULL DEFAULT 1,
  ultima_vez timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (termino, articulo)
);

CREATE INDEX IF NOT EXISTS idx_soporte_aprendizaje_termino ON soporte.aprendizaje (termino);
