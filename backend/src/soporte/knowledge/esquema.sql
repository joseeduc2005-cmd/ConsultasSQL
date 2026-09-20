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
