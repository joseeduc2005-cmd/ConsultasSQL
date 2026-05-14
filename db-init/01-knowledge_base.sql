--
-- PostgreSQL database dump
--

\restrict 0If1jujhwoiSFqQ6e3tPfFJ6YacESqjG8dhH5SGbH6eev1yazQegMfNpgeBGUc7

-- Dumped from database version 16.10
-- Dumped by pg_dump version 16.10

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;


--
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: article_changes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.article_changes (
    id integer NOT NULL,
    article_id text,
    article_titulo text,
    accion character varying(50) NOT NULL,
    realizado_por character varying(100),
    realizado_rol character varying(20),
    detalle jsonb,
    realizado_en timestamp without time zone DEFAULT now()
);


--
-- Name: article_changes_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.article_changes_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: article_changes_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.article_changes_id_seq OWNED BY public.article_changes.id;


--
-- Name: article_executions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.article_executions (
    id integer NOT NULL,
    article_id text,
    article_titulo text,
    ejecutado_por character varying(100),
    ejecutado_rol character varying(20),
    parametros_usados jsonb,
    filas_resultado integer DEFAULT 0,
    duracion_ms integer DEFAULT 0,
    estado character varying(10) DEFAULT 'ok'::character varying,
    mensaje_error text,
    ejecutado_en timestamp without time zone DEFAULT now()
);


--
-- Name: article_executions_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.article_executions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: article_executions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.article_executions_id_seq OWNED BY public.article_executions.id;


--
-- Name: comments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.comments (
    id integer NOT NULL,
    article_id character varying(255) NOT NULL,
    parent_id integer,
    author_username character varying(255) NOT NULL,
    author_role character varying(50) DEFAULT 'user'::character varying NOT NULL,
    content text NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    article_title character varying(500)
);


--
-- Name: comments_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.comments_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: comments_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.comments_id_seq OWNED BY public.comments.id;


--
-- Name: error_logs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.error_logs (
    id integer NOT NULL,
    mensaje text NOT NULL,
    stack text,
    modulo character varying(255),
    capa character varying(120),
    contexto character varying(255),
    archivo character varying(255),
    linea integer,
    metadata jsonb,
    fecha timestamp with time zone DEFAULT now()
);


--
-- Name: error_logs_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.error_logs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: error_logs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.error_logs_id_seq OWNED BY public.error_logs.id;


--
-- Name: knowledge_base; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.knowledge_base (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    titulo character varying(200) NOT NULL,
    tags text[] DEFAULT '{}'::text[] NOT NULL,
    contenido text NOT NULL,
    categoria character varying(100),
    subcategoria character varying(100),
    pasos jsonb,
    script text,
    creado_por uuid NOT NULL,
    fecha timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    actualizado timestamp without time zone,
    contenido_md text,
    tipo_solucion character varying(20) DEFAULT 'lectura'::character varying,
    descripcion text,
    campos_formulario jsonb,
    script_json jsonb,
    visible_para character varying(20) DEFAULT 'todos'::character varying,
    creado_por_username character varying(100),
    actualizado_por character varying(100),
    CONSTRAINT knowledge_base_tipo_solucion_check CHECK (((tipo_solucion)::text = ANY ((ARRAY['lectura'::character varying, 'ejecutable'::character varying, 'database'::character varying, 'script'::character varying])::text[])))
);


--
-- Name: login_audit; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.login_audit (
    id integer NOT NULL,
    username character varying(100) NOT NULL,
    rol character varying(20),
    ip character varying(60),
    user_agent text,
    estado character varying(10) DEFAULT 'ok'::character varying,
    detalle text,
    fecha timestamp without time zone DEFAULT now()
);


--
-- Name: login_audit_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.login_audit_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: login_audit_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.login_audit_id_seq OWNED BY public.login_audit.id;


--
-- Name: logs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.logs (
    id integer NOT NULL,
    accion character varying(120) NOT NULL,
    usuario character varying(255) NOT NULL,
    detalle text,
    created_at timestamp with time zone DEFAULT now(),
    user_id uuid
);


--
-- Name: logs_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.logs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: logs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.logs_id_seq OWNED BY public.logs.id;


--
-- Name: query_history; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.query_history (
    id integer NOT NULL,
    username character varying(255) NOT NULL,
    user_role character varying(50) NOT NULL,
    query_text text,
    generated_sql text NOT NULL,
    execution_ms integer DEFAULT 0 NOT NULL,
    row_count integer DEFAULT 0 NOT NULL,
    was_cached boolean DEFAULT false NOT NULL,
    status character varying(20) DEFAULT 'ok'::character varying NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    query_params jsonb,
    placeholder_order jsonb
);


--
-- Name: query_history_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.query_history_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: query_history_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.query_history_id_seq OWNED BY public.query_history.id;


--
-- Name: roles; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.roles (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    nombre character varying(50),
    descripcion text
);


--
-- Name: semantic_learning; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.semantic_learning (
    id integer NOT NULL,
    db_fingerprint text DEFAULT ''::text NOT NULL,
    normalized_term character varying(120) NOT NULL,
    original_term character varying(120) NOT NULL,
    target_kind character varying(20) NOT NULL,
    target_name character varying(120) NOT NULL,
    table_name character varying(120) DEFAULT ''::character varying NOT NULL,
    confidence numeric(4,3) DEFAULT 0.650 NOT NULL,
    hit_count integer DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    last_seen timestamp with time zone DEFAULT now(),
    CONSTRAINT semantic_learning_target_kind_check CHECK (((target_kind)::text = ANY ((ARRAY['table'::character varying, 'column'::character varying])::text[])))
);


--
-- Name: semantic_learning_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.semantic_learning_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: semantic_learning_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.semantic_learning_id_seq OWNED BY public.semantic_learning.id;


--
-- Name: sessions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.sessions (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid,
    token character varying(500),
    activo boolean DEFAULT true,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: system_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.system_events (
    id integer NOT NULL,
    evento character varying(60) NOT NULL,
    usuario character varying(100),
    rol character varying(20),
    detalle jsonb,
    ip character varying(60),
    fecha timestamp without time zone DEFAULT now()
);


--
-- Name: system_events_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.system_events_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: system_events_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.system_events_id_seq OWNED BY public.system_events.id;


--
-- Name: users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.users (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    username character varying(50) NOT NULL,
    password character varying(255) NOT NULL,
    role character varying(20) DEFAULT 'user'::character varying NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    bloqueado boolean DEFAULT false,
    role_id uuid,
    activo boolean DEFAULT true
);


--
-- Name: article_changes id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.article_changes ALTER COLUMN id SET DEFAULT nextval('public.article_changes_id_seq'::regclass);


--
-- Name: article_executions id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.article_executions ALTER COLUMN id SET DEFAULT nextval('public.article_executions_id_seq'::regclass);


--
-- Name: comments id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.comments ALTER COLUMN id SET DEFAULT nextval('public.comments_id_seq'::regclass);


--
-- Name: error_logs id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.error_logs ALTER COLUMN id SET DEFAULT nextval('public.error_logs_id_seq'::regclass);


--
-- Name: login_audit id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.login_audit ALTER COLUMN id SET DEFAULT nextval('public.login_audit_id_seq'::regclass);


--
-- Name: logs id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.logs ALTER COLUMN id SET DEFAULT nextval('public.logs_id_seq'::regclass);


--
-- Name: query_history id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.query_history ALTER COLUMN id SET DEFAULT nextval('public.query_history_id_seq'::regclass);


--
-- Name: semantic_learning id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.semantic_learning ALTER COLUMN id SET DEFAULT nextval('public.semantic_learning_id_seq'::regclass);


--
-- Name: system_events id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.system_events ALTER COLUMN id SET DEFAULT nextval('public.system_events_id_seq'::regclass);


--
-- Data for Name: article_changes; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.article_changes (id, article_id, article_titulo, accion, realizado_por, realizado_rol, detalle, realizado_en) FROM stdin;
1	\N	Limpieza de Sesiones	eliminar	admin	admin	{"titulo": "Limpieza de Sesiones", "article_id": "50566ce3-b6ad-474e-bb73-4eaa42c97c55"}	2026-05-12 22:52:10.20486
2	\N	Problemas con login	eliminar	admin	admin	{"titulo": "Problemas con login", "article_id": "9b63c9d5-07ee-466c-a4c5-57d7ddeea8b8"}	2026-05-12 22:55:59.11883
3	b946ef9b-f6e1-4ce2-a9dd-b3f33c5d991e	Error en transferencia bancaria.	editar	admin	admin	{"categoria": "TEST", "titulo_nuevo": "Error en transferencia bancaria.", "visible_para": "todos", "tipo_solucion": "script", "titulo_anterior": "Error en transferencia bancaria."}	2026-05-12 23:16:05.347243
5	b946ef9b-f6e1-4ce2-a9dd-b3f33c5d991e	Error en transferencia bancaria.	eliminar_comentario	admin	admin	{"contenido": "hola hola", "creado_en": "2026-05-13T15:14:20.219Z", "rol_autor": "admin", "comentario_id": 17, "autor_comentario": "admin"}	2026-05-13 10:14:21.595987
6	bed57404-2a43-47dd-8bbc-aa8d555e99f9	MySQL — Alta de empleado (INSERT dinámico)	editar	admin	admin	{"categoria": "RRHH", "titulo_nuevo": "MySQL — Alta de empleado (INSERT dinámico)", "visible_para": "admin", "tipo_solucion": "script", "titulo_anterior": "MySQL — Alta de empleado (INSERT dinámico)"}	2026-05-13 11:20:15.450865
7	8e598568-5963-4e03-965a-6cfaae251630	MySQL — Ranking salarial con CTE y Window Functions	editar	admin	admin	{"categoria": "RRHH", "titulo_nuevo": "MySQL — Ranking salarial con CTE y Window Functions", "visible_para": "todos", "tipo_solucion": "script", "titulo_anterior": "MySQL — Ranking salarial con CTE y Window Functions"}	2026-05-13 11:33:12.91192
8	8e598568-5963-4e03-965a-6cfaae251630	MySQL — Ranking salarial con CTE y Window Functions	editar	admin	admin	{"categoria": "RRHH", "titulo_nuevo": "MySQL — Ranking salarial con CTE y Window Functions", "visible_para": "todos", "tipo_solucion": "script", "titulo_anterior": "MySQL — Ranking salarial con CTE y Window Functions"}	2026-05-13 11:33:21.667094
9	8e598568-5963-4e03-965a-6cfaae251630	MySQL — Ranking salarial con CTE y Window Functions	editar	admin	admin	{"categoria": "RRHH", "titulo_nuevo": "MySQL — Ranking salarial con CTE y Window Functions", "visible_para": "todos", "tipo_solucion": "script", "titulo_anterior": "MySQL — Ranking salarial con CTE y Window Functions"}	2026-05-13 11:36:05.395595
10	8e598568-5963-4e03-965a-6cfaae251630	MySQL — Ranking salarial con CTE y Window Functions	editar	admin	admin	{"categoria": "RRHH", "titulo_nuevo": "MySQL — Ranking salarial con CTE y Window Functions", "visible_para": "todos", "tipo_solucion": "script", "titulo_anterior": "MySQL — Ranking salarial con CTE y Window Functions"}	2026-05-13 11:48:30.107393
11	8e598568-5963-4e03-965a-6cfaae251630	MySQL — Ranking salarial con CTE y Window Functions	editar	admin	admin	{"categoria": "RRHH", "titulo_nuevo": "MySQL — Ranking salarial con CTE y Window Functions", "visible_para": "todos", "tipo_solucion": "script", "titulo_anterior": "MySQL — Ranking salarial con CTE y Window Functions"}	2026-05-13 11:48:53.978486
12	8e598568-5963-4e03-965a-6cfaae251630	MySQL — Ranking salarial con CTE y Window Functions	editar	admin	admin	{"categoria": "RRHH", "titulo_nuevo": "MySQL — Ranking salarial con CTE y Window Functions", "visible_para": "todos", "tipo_solucion": "script", "titulo_anterior": "MySQL — Ranking salarial con CTE y Window Functions"}	2026-05-13 11:52:28.250769
13	8e598568-5963-4e03-965a-6cfaae251630	MySQL — Ranking salarial con CTE y Window Functions	editar	admin	admin	{"categoria": "RRHH", "titulo_nuevo": "MySQL — Ranking salarial con CTE y Window Functions", "visible_para": "todos", "tipo_solucion": "script", "titulo_anterior": "MySQL — Ranking salarial con CTE y Window Functions"}	2026-05-13 11:56:12.198747
14	8e598568-5963-4e03-965a-6cfaae251630	MySQL — Ranking salarial con CTE y Window Functions	editar	admin	admin	{"categoria": "RRHH", "titulo_nuevo": "MySQL — Ranking salarial con CTE y Window Functions", "visible_para": "todos", "tipo_solucion": "script", "titulo_anterior": "MySQL — Ranking salarial con CTE y Window Functions"}	2026-05-13 11:56:25.12572
15	8e598568-5963-4e03-965a-6cfaae251630	MySQL — Ranking salarial con CTE y Window Functions	editar	admin	admin	{"categoria": "RRHH", "titulo_nuevo": "MySQL — Ranking salarial con CTE y Window Functions", "visible_para": "todos", "tipo_solucion": "script", "titulo_anterior": "MySQL — Ranking salarial con CTE y Window Functions"}	2026-05-13 12:01:57.470584
16	8e598568-5963-4e03-965a-6cfaae251630	MySQL — Ranking salarial con CTE y Window Functions	editar	admin	admin	{"categoria": "RRHH", "titulo_nuevo": "MySQL — Ranking salarial con CTE y Window Functions", "visible_para": "todos", "tipo_solucion": "script", "titulo_anterior": "MySQL — Ranking salarial con CTE y Window Functions"}	2026-05-13 12:02:03.531156
17	8e598568-5963-4e03-965a-6cfaae251630	MySQL — Ranking salarial con CTE y Window Functions	editar	admin	admin	{"categoria": "RRHH", "titulo_nuevo": "MySQL — Ranking salarial con CTE y Window Functions", "visible_para": "todos", "tipo_solucion": "script", "titulo_anterior": "MySQL — Ranking salarial con CTE y Window Functions"}	2026-05-13 12:02:13.437901
18	8e598568-5963-4e03-965a-6cfaae251630	MySQL — Ranking salarial con CTE y Window Functions	editar	admin	admin	{"categoria": "RRHH", "titulo_nuevo": "MySQL — Ranking salarial con CTE y Window Functions", "visible_para": "todos", "tipo_solucion": "script", "titulo_anterior": "MySQL — Ranking salarial con CTE y Window Functions"}	2026-05-13 12:02:22.649312
19	25f28d88-312c-4877-abb9-bdeb19383296	Oracle — Empleados por rango salarial con RANK (dinámico)	editar	admin	admin	{"categoria": "RRHH", "titulo_nuevo": "Oracle — Empleados por rango salarial con RANK (dinámico)", "visible_para": "todos", "tipo_solucion": "script", "titulo_anterior": "Oracle — Empleados por rango salarial con RANK (dinámico)"}	2026-05-13 12:15:50.500652
20	25f28d88-312c-4877-abb9-bdeb19383296	Oracle — Empleados por rango salarial con RANK (dinámico)	editar	admin	admin	{"categoria": "RRHH", "titulo_nuevo": "Oracle — Empleados por rango salarial con RANK (dinámico)", "visible_para": "todos", "tipo_solucion": "script", "titulo_anterior": "Oracle — Empleados por rango salarial con RANK (dinámico)"}	2026-05-13 12:16:05.59006
21	bed57404-2a43-47dd-8bbc-aa8d555e99f9	MySQL — Alta de empleado (INSERT dinámico)	editar	admin	admin	{"categoria": "RRHH", "titulo_nuevo": "MySQL — Alta de empleado (INSERT dinámico)", "visible_para": "todos", "tipo_solucion": "script", "titulo_anterior": "MySQL — Alta de empleado (INSERT dinámico)"}	2026-05-13 12:28:48.072632
22	b946ef9b-f6e1-4ce2-a9dd-b3f33c5d991e	Error en transferencia bancaria.	editar	admin	admin	{"categoria": "TEST", "titulo_nuevo": "Error en transferencia bancaria.", "visible_para": "todos", "tipo_solucion": "script", "titulo_anterior": "Error en transferencia bancaria."}	2026-05-13 12:54:58.619575
23	b946ef9b-f6e1-4ce2-a9dd-b3f33c5d991e	Error en transferencia bancaria.	editar	admin	admin	{"categoria": "TEST", "titulo_nuevo": "Error en transferencia bancaria.", "visible_para": "todos", "tipo_solucion": "script", "titulo_anterior": "Error en transferencia bancaria."}	2026-05-13 12:59:34.739362
24	fa177af1-189e-4017-986a-4ebeb41469a8	Problemas con login	crear	admin	admin	{"titulo": "Problemas con login", "categoria": "TEST", "visible_para": "todos", "tipo_solucion": "script"}	2026-05-13 20:23:14.70604
25	b946ef9b-f6e1-4ce2-a9dd-b3f33c5d991e	Error en transferencia bancaria.	editar	admin	admin	{"categoria": "TEST", "titulo_nuevo": "Error en transferencia bancaria.", "visible_para": "todos", "tipo_solucion": "script", "titulo_anterior": "Error en transferencia bancaria."}	2026-05-13 20:23:48.144304
26	7c4df9e4-6dfc-420d-8c56-53e16f42fd4a	Reparación de login	editar	admin	admin	{"categoria": "GENERAL", "titulo_nuevo": "Reparación de login", "visible_para": "todos", "tipo_solucion": "script", "titulo_anterior": "Reparación de login"}	2026-05-13 20:24:06.858319
27	b946ef9b-f6e1-4ce2-a9dd-b3f33c5d991e	Error en transferencia bancaria.	editar	admin	admin	{"categoria": "TEST", "titulo_nuevo": "Error en transferencia bancaria.", "visible_para": "todos", "tipo_solucion": "script", "titulo_anterior": "Error en transferencia bancaria."}	2026-05-13 20:36:04.863986
28	b946ef9b-f6e1-4ce2-a9dd-b3f33c5d991e	Error en transferencia bancaria.	editar	admin	admin	{"categoria": "TEST", "titulo_nuevo": "Error en transferencia bancaria.", "visible_para": "todos", "tipo_solucion": "script", "titulo_anterior": "Error en transferencia bancaria."}	2026-05-14 08:36:05.806614
29	013dc51f-ef9d-4c8e-84a5-1e13339a3ae3	PROBLEMA AUTORIZACION USUARIOS	crear	admin	admin	{"titulo": "PROBLEMA AUTORIZACION USUARIOS", "categoria": "TEST", "visible_para": "todos", "tipo_solucion": "script"}	2026-05-14 08:45:19.691744
30	013dc51f-ef9d-4c8e-84a5-1e13339a3ae3	PROBLEMA AUTORIZACION USUARIOS	eliminar	admin	admin	{"titulo": "PROBLEMA AUTORIZACION USUARIOS", "article_id": "013dc51f-ef9d-4c8e-84a5-1e13339a3ae3"}	2026-05-14 08:48:30.956662
31	b19f69c3-0913-4a55-9b28-b32eecd3df04	Error en transferencia bancaria	crear	admin	admin	{"titulo": "Error en transferencia bancaria", "categoria": "TEST", "visible_para": "todos", "tipo_solucion": "script"}	2026-05-14 08:50:22.021764
32	b19f69c3-0913-4a55-9b28-b32eecd3df04	Error en transferencia bancaria	editar	admin	admin	{"categoria": "TEST", "titulo_nuevo": "Error en transferencia bancaria", "visible_para": "todos", "tipo_solucion": "script", "titulo_anterior": "Error en transferencia bancaria"}	2026-05-14 09:02:17.067266
33	b946ef9b-f6e1-4ce2-a9dd-b3f33c5d991e	Error en transferencia bancaria.	editar	admin	admin	{"categoria": "TEST", "titulo_nuevo": "Error en transferencia bancaria.", "visible_para": "todos", "tipo_solucion": "script", "titulo_anterior": "Error en transferencia bancaria."}	2026-05-14 09:02:32.179186
34	b19f69c3-0913-4a55-9b28-b32eecd3df04	Error en transferencia bancaria	eliminar	admin	admin	{"titulo": "Error en transferencia bancaria", "article_id": "b19f69c3-0913-4a55-9b28-b32eecd3df04"}	2026-05-14 09:02:57.673209
35	b946ef9b-f6e1-4ce2-a9dd-b3f33c5d991e	Error en transferencia bancaria.	eliminar	admin	admin	{"titulo": "Error en transferencia bancaria.", "article_id": "b946ef9b-f6e1-4ce2-a9dd-b3f33c5d991e"}	2026-05-14 09:03:04.641104
36	690d2f59-018c-4278-9925-d70e4ce9f8ad	PRUEBA VENTANA ERROR	crear	admin	admin	{"titulo": "PRUEBA VENTANA ERROR", "categoria": "TEST", "visible_para": "todos", "tipo_solucion": "script"}	2026-05-14 09:04:18.485344
37	9076fbc7-85c4-44bb-ae9a-c2cb2aed6e8c	Problema de Login - Solución Ejecutable	crear	admin	admin	{"titulo": "Problema de Login - Solución Ejecutable", "categoria": "TEST", "visible_para": "todos", "tipo_solucion": "script"}	2026-05-14 09:10:18.083113
38	3f18a2f0-9b0d-4629-8f47-ebaa1aaa69ab	🔍 Consulta: Análisis de actividad de un usuario específico	crear	admin	admin	{"titulo": "🔍 Consulta: Análisis de actividad de un usuario específico", "categoria": "NUEVONUEVO", "visible_para": "todos", "tipo_solucion": "script"}	2026-05-14 11:31:00.411259
\.


--
-- Data for Name: article_executions; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.article_executions (id, article_id, article_titulo, ejecutado_por, ejecutado_rol, parametros_usados, filas_resultado, duracion_ms, estado, mensaje_error, ejecutado_en) FROM stdin;
3	b946ef9b-f6e1-4ce2-a9dd-b3f33c5d991e		anonymous	user	{"__write_confirmed": "false"}	0	50	ok	\N	2026-05-12 23:06:16.881506
4	5f966350-d379-410c-8349-355309e5cf2c		admin	admin	{"__write_confirmed": "false"}	0	41	ok	\N	2026-05-12 23:14:44.287374
5	b946ef9b-f6e1-4ce2-a9dd-b3f33c5d991e		admin	admin	{"__write_confirmed": "false"}	0	34	ok	\N	2026-05-12 23:16:13.353169
6	b946ef9b-f6e1-4ce2-a9dd-b3f33c5d991e		admin	admin	{"__write_confirmed": "false"}	0	33	ok	\N	2026-05-12 23:24:16.674368
7	b946ef9b-f6e1-4ce2-a9dd-b3f33c5d991e		user	user	{"__write_confirmed": "false"}	0	28	ok	\N	2026-05-12 23:25:23.129152
8	b946ef9b-f6e1-4ce2-a9dd-b3f33c5d991e		admin	admin	{"__write_confirmed": "false"}	0	57	ok	\N	2026-05-12 23:39:34.100245
9	b946ef9b-f6e1-4ce2-a9dd-b3f33c5d991e		user	user	{"__write_confirmed": "false"}	0	27	ok	\N	2026-05-12 23:41:41.266701
10	21961a02-af0e-4237-abb1-dd0e89eccc7f		admin	admin	{"__write_confirmed": "false"}	0	50	error	SQL manual vacío	2026-05-13 11:09:16.234978
11	3c2bca46-6a46-46c5-952f-a16b5b26dfbe		admin	admin	{"__write_confirmed": "false"}	0	579	ok	\N	2026-05-13 11:09:30.193745
12	41428192-6c8f-47c6-a3d9-45a5903d8a42		admin	admin	{"dias": 30, "estado": "ok", "min_ms": "0", "__write_confirmed": "false"}	0	76	error	no existe la columna qh.role	2026-05-13 11:11:08.76296
13	19a1e879-7450-473c-9582-23022ec10344		admin	admin	{"monto_min": 0, "fecha_desde": "2025-01-01", "fecha_hasta": "2026-12-31", "__write_confirmed": "false"}	0	114	error	Incorrect arguments to COM_STMT_EXECUTE	2026-05-13 11:13:25.006848
14	bed57404-2a43-47dd-8bbc-aa8d555e99f9		admin	admin	{"cargo": "Analista", "email": "juan@empresa.com", "nombre": "Juan", "salario": "2500", "apellido": "Pérez", "fecha_ingreso": "2026-01-15", "departamento_id": "1", "__write_confirmed": "true"}	0	79	error	error de sintaxis en o cerca de «{»	2026-05-13 11:14:33.684679
15	8e598568-5963-4e03-965a-6cfaae251630		admin	admin	{"__write_confirmed": "false"}	0	48	ok	\N	2026-05-13 11:33:15.028701
16	8e598568-5963-4e03-965a-6cfaae251630		admin	admin	{"__write_confirmed": "false"}	0	4	ok	\N	2026-05-13 11:33:25.638272
17	8e598568-5963-4e03-965a-6cfaae251630		admin	admin	{"__write_confirmed": "false"}	0	4	ok	\N	2026-05-13 11:36:14.129891
18	8e598568-5963-4e03-965a-6cfaae251630		anonymous	admin	{}	0	149	error	ORA-02000: missing , keyword\nHelp: https://docs.oracle.com/error-help/db/ora-02000/	2026-05-13 11:47:03.302083
19	8e598568-5963-4e03-965a-6cfaae251630		admin	admin	{"__write_confirmed": "false"}	0	71	error	ORA-02000: missing , keyword\nHelp: https://docs.oracle.com/error-help/db/ora-02000/	2026-05-13 11:48:21.410157
20	8e598568-5963-4e03-965a-6cfaae251630		admin	admin	{"__write_confirmed": "false"}	0	7	error	ORA-02000: missing , keyword\nHelp: https://docs.oracle.com/error-help/db/ora-02000/	2026-05-13 11:48:33.477348
21	8e598568-5963-4e03-965a-6cfaae251630		admin	admin	{"__write_confirmed": "false"}	0	8	error	ORA-02000: missing , keyword\nHelp: https://docs.oracle.com/error-help/db/ora-02000/	2026-05-13 11:48:59.30714
22	8e598568-5963-4e03-965a-6cfaae251630		admin	admin	{"__write_confirmed": "false"}	0	18	error	ORA-02000: missing , keyword\nHelp: https://docs.oracle.com/error-help/db/ora-02000/	2026-05-13 11:49:31.934141
23	8e598568-5963-4e03-965a-6cfaae251630		admin	admin	{"__write_confirmed": "false"}	0	48	error	ORA-02000: missing , keyword\nHelp: https://docs.oracle.com/error-help/db/ora-02000/	2026-05-13 11:52:30.906675
24	8e598568-5963-4e03-965a-6cfaae251630		admin	admin	{}	0	25	ok	\N	2026-05-13 11:52:55.680562
25	21961a02-af0e-4237-abb1-dd0e89eccc7f		admin	admin	{}	0	4	error	SQL manual vacío	2026-05-13 11:53:01.631462
26	8e598568-5963-4e03-995a-6cfaae251630		anonymous	admin	{}	0	65	error	Artículo no encontrado	2026-05-13 11:55:20.970569
27	21961a02-af0e-4237-abb1-dd0e89eccc7f		anonymous	admin	{}	0	92	ok	\N	2026-05-13 11:55:21.158226
28	8e598568-5963-4e03-965a-6cfaae251630		anonymous	admin	{}	0	20	ok	\N	2026-05-13 11:55:35.027556
29	8e598568-5963-4e03-965a-6cfaae251630		admin	admin	{"__write_confirmed": "false"}	0	9	ok	\N	2026-05-13 11:56:15.000816
30	8e598568-5963-4e03-965a-6cfaae251630		admin	admin	{"__write_confirmed": "false"}	0	6	ok	\N	2026-05-13 11:56:26.032374
31	8e598568-5963-4e03-965a-6cfaae251630		anonymous	admin	{}	0	124	error	ORA-02000: missing , keyword\nHelp: https://docs.oracle.com/error-help/db/ora-02000/	2026-05-13 12:01:13.969921
32	8e598568-5963-4e03-965a-6cfaae251630		anonymous	admin	{}	0	38	ok	\N	2026-05-13 12:01:29.86699
33	8e598568-5963-4e03-965a-6cfaae251630		admin	admin	{"__write_confirmed": "false"}	0	10	error	ORA-02000: missing , keyword\nHelp: https://docs.oracle.com/error-help/db/ora-02000/	2026-05-13 12:01:58.966183
34	8e598568-5963-4e03-965a-6cfaae251630		admin	admin	{"__write_confirmed": "false"}	0	10	ok	\N	2026-05-13 12:02:06.804406
35	8e598568-5963-4e03-965a-6cfaae251630		admin	admin	{"__write_confirmed": "false"}	0	51	error	MongoDB: el campo SQL debe ser un JSON válido.\nEjemplos:\n  {"collection":"orders","find":{"status":"active"},"limit":100}\n  {"collection":"orders","aggregate":[{"$group":{"_id":"$status","total":{"$sum":1}}}]}\n  {"collection":"orders","updateMany":{"filter":{"status":"old"},"update":{"$set":{"status":"archived"}}}}	2026-05-13 12:02:14.676277
36	25f28d88-312c-4877-abb9-bdeb19383296		admin	admin	{"departamento": "TI", "solo_activos": "1", "__write_confirmed": "false"}	0	120	error	NJS-098: 6 bind placeholders were used in the SQL statement but 4 bind values were provided	2026-05-13 12:06:43.166466
37	41428192-6c8f-47c6-a3d9-45a5903d8a42		anonymous	admin	{"usuario": "", "fecha_fin": "2026-12-31", "fecha_inicio": "2025-01-01"}	0	73	error	no existe la función round(double precision, integer)	2026-05-13 12:10:33.604506
38	abbad29e-ea6e-4401-803a-4fd3457b6ec9		anonymous	admin	{}	0	10	error	no existe la columna «user_role»	2026-05-13 12:10:33.684128
39	8e598568-5963-4e03-965a-6cfaae251630		anonymous	admin	{}	0	19	ok	\N	2026-05-13 12:10:33.71414
40	bed57404-2a43-47dd-8bbc-aa8d555e99f9		anonymous	admin	{"cargo": "Dev", "nombre": "Test", "salario": "2000", "apellido": "Test", "fecha_ingreso": "2025-01-01", "departamento_id": "1"}	0	14	error	Field 'cedula' doesn't have a default value	2026-05-13 12:10:33.744739
41	25f28d88-312c-4877-abb9-bdeb19383296		anonymous	admin	{"salario_max": "", "salario_min": "", "departamento": "TI", "solo_activos": "1"}	0	177	ok	\N	2026-05-13 12:10:33.927798
42	05ce43a2-2ab0-4de2-84af-7a11b4821d47		anonymous	admin	{}	0	2	error	SQL manual vacío	2026-05-13 12:10:33.936629
43	21961a02-af0e-4237-abb1-dd0e89eccc7f		anonymous	admin	{}	0	29	ok	\N	2026-05-13 12:10:33.972161
44	bed57404-2a43-47dd-8bbc-aa8d555e99f9		anonymous	admin	{}	0	42	error	Column 'nombre' cannot be null	2026-05-13 12:11:21.064097
45	41428192-6c8f-47c6-a3d9-45a5903d8a42		anonymous	admin	{"dias": "7", "estado": "", "min_ms": "0", "usuario": ""}	0	72	error	no se pudo determinar el tipo del parámetro $1	2026-05-13 12:14:05.143407
46	abbad29e-ea6e-4401-803a-4fd3457b6ec9		anonymous	admin	{}	0	10	ok	\N	2026-05-13 12:14:05.180554
47	8e598568-5963-4e03-965a-6cfaae251630		anonymous	admin	{}	0	17	ok	\N	2026-05-13 12:14:05.208825
49	25f28d88-312c-4877-abb9-bdeb19383296		anonymous	admin	{"salario_max": "", "salario_min": "", "departamento": "TI", "solo_activos": "1"}	0	48	ok	\N	2026-05-13 12:14:05.310391
51	3c2bca46-6a46-46c5-952f-a16b5b26dfbe		anonymous	admin	{}	0	555	ok	\N	2026-05-13 12:14:05.877053
52	05ce43a2-2ab0-4de2-84af-7a11b4821d47		anonymous	admin	{"salario_min": "2000", "departamento": "TI"}	0	18	ok	\N	2026-05-13 12:14:05.899973
48	bed57404-2a43-47dd-8bbc-aa8d555e99f9		anonymous	admin	{"cargo": "Dev", "email": "test@test.com", "cedula": "12345678", "nombre": "Test", "salario": "2000", "apellido": "Usuario", "fecha_ingreso": "2025-01-01", "departamento_id": "1"}	0	46	ok	\N	2026-05-13 12:14:05.258926
50	1ff6addb-b02c-47eb-af86-43825bb2dbba		anonymous	admin	{"departamento": "TI", "incremento_pct": "5"}	0	2	error	Esta acción modificará datos, confirma para continuar.	2026-05-13 12:14:05.316573
53	21961a02-af0e-4237-abb1-dd0e89eccc7f		anonymous	admin	{}	0	8	ok	\N	2026-05-13 12:14:05.911401
54	41428192-6c8f-47c6-a3d9-45a5903d8a42		anonymous	admin	{"dias": "7", "estado": "", "min_ms": "0", "usuario": ""}	0	77	ok	\N	2026-05-13 12:15:06.509501
55	abbad29e-ea6e-4401-803a-4fd3457b6ec9		anonymous	admin	{}	0	9	ok	\N	2026-05-13 12:15:06.538401
56	8e598568-5963-4e03-965a-6cfaae251630		anonymous	admin	{}	0	39	ok	\N	2026-05-13 12:15:06.58179
57	bed57404-2a43-47dd-8bbc-aa8d555e99f9		anonymous	admin	{"cargo": "Dev", "email": "t@t.com", "cedula": "TEST-001", "nombre": "Test", "salario": "2000", "apellido": "Usuario", "fecha_ingreso": "2025-01-01", "departamento_id": "1"}	0	19	ok	\N	2026-05-13 12:15:06.604634
58	dc84bf6d-0c42-4802-87ee-e03791db0f14		anonymous	admin	{}	0	17	ok	\N	2026-05-13 12:15:06.625878
59	19a1e879-7450-473c-9582-23022ec10344		anonymous	admin	{}	0	6	ok	\N	2026-05-13 12:15:06.637104
60	25f28d88-312c-4877-abb9-bdeb19383296		anonymous	admin	{"salario_max": "", "salario_min": "", "departamento": "TI", "solo_activos": "1"}	0	6	ok	\N	2026-05-13 12:15:06.647239
61	1ff6addb-b02c-47eb-af86-43825bb2dbba		anonymous	admin	{"departamento": "TI", "incremento_pct": "5", "__write_confirmed": "true"}	0	61	ok	\N	2026-05-13 12:15:06.712428
62	3c2bca46-6a46-46c5-952f-a16b5b26dfbe		anonymous	admin	{}	0	6	ok	\N	2026-05-13 12:15:06.722406
63	05ce43a2-2ab0-4de2-84af-7a11b4821d47		anonymous	admin	{"salario_min": "2000", "departamento": "TI"}	0	6	ok	\N	2026-05-13 12:15:06.731189
64	21961a02-af0e-4237-abb1-dd0e89eccc7f		anonymous	admin	{}	0	5	ok	\N	2026-05-13 12:15:06.740188
65	25f28d88-312c-4877-abb9-bdeb19383296		admin	admin	{"departamento": "TI", "solo_activos": "1", "__write_confirmed": "false"}	0	8	ok	\N	2026-05-13 12:15:41.510478
66	25f28d88-312c-4877-abb9-bdeb19383296		admin	admin	{"departamento": "TI", "solo_activos": "1", "__write_confirmed": "false"}	0	10	error	Every derived table must have its own alias	2026-05-13 12:15:57.873062
67	25f28d88-312c-4877-abb9-bdeb19383296		admin	admin	{"departamento": "TI", "solo_activos": "1", "__write_confirmed": "false"}	0	28	ok	\N	2026-05-13 12:16:15.896132
68	25f28d88-312c-4877-abb9-bdeb19383296		admin	admin	{"salario_max": "3200", "salario_min": "3000", "departamento": "TI", "solo_activos": "1", "__write_confirmed": "false"}	0	8	ok	\N	2026-05-13 12:16:32.901824
69	21961a02-af0e-4237-abb1-dd0e89eccc7f		admin	admin	{"__write_confirmed": "false"}	0	8	ok	\N	2026-05-13 12:17:00.984959
70	dc84bf6d-0c42-4802-87ee-e03791db0f14		admin	admin	{"__write_confirmed": "false"}	0	21	ok	\N	2026-05-13 12:17:18.751828
71	19a1e879-7450-473c-9582-23022ec10344		admin	admin	{"fecha_desde": "2026-01-01", "fecha_hasta": "2026-12-31", "__write_confirmed": "false"}	0	64	ok	\N	2026-05-13 12:19:45.738739
72	19a1e879-7450-473c-9582-23022ec10344		admin	admin	{"monto_min": 1000, "fecha_desde": "2026-01-01", "fecha_hasta": "2026-12-31", "institucion": "1500", "__write_confirmed": "false"}	0	5	ok	\N	2026-05-13 12:20:15.37166
73	19a1e879-7450-473c-9582-23022ec10344		admin	admin	{"monto_min": 500, "fecha_desde": "2026-01-01", "fecha_hasta": "2026-12-31", "__write_confirmed": "false"}	0	6	ok	\N	2026-05-13 12:20:43.886369
74	19a1e879-7450-473c-9582-23022ec10344		admin	admin	{"monto_min": 1000, "fecha_desde": "2026-01-01", "fecha_hasta": "2026-12-31", "institucion": "Banco del PacÃ­fico", "__write_confirmed": "false"}	0	16	ok	\N	2026-05-13 12:21:09.737432
75	bed57404-2a43-47dd-8bbc-aa8d555e99f9		admin	admin	{"cargo": "Desarrollador", "email": "carlos.mendoza@empresa.com", "cedula": "0912345678", "nombre": "Carlos", "salario": "2500", "apellido": "Mendoza", "fecha_ingreso": "2026-01-15", "departamento_id": "1", "__write_confirmed": "true"}	0	89	ok	\N	2026-05-13 12:25:34.772736
76	8e598568-5963-4e03-965a-6cfaae251630		admin	admin	{"__write_confirmed": "false"}	0	215	ok	\N	2026-05-13 12:36:01.255714
77	21961a02-af0e-4237-abb1-dd0e89eccc7f		admin	admin	{"__write_confirmed": "false"}	0	61	ok	\N	2026-05-13 12:36:10.059909
78	b946ef9b-f6e1-4ce2-a9dd-b3f33c5d991e		admin	admin	{"__write_confirmed": "false"}	0	31	ok	\N	2026-05-13 12:55:05.235087
79	b946ef9b-f6e1-4ce2-a9dd-b3f33c5d991e		admin	admin	{"__write_confirmed": "false"}	0	21	ok	\N	2026-05-13 12:59:37.469052
80	b946ef9b-f6e1-4ce2-a9dd-b3f33c5d991e		admin	admin	{"__write_confirmed": "false"}	0	102	ok	\N	2026-05-13 20:23:37.010429
81	7c4df9e4-6dfc-420d-8c56-53e16f42fd4a		admin	admin	{"__write_confirmed": "false"}	0	39	ok	\N	2026-05-13 20:24:09.836415
82	fa177af1-189e-4017-986a-4ebeb41469a8		admin	admin	{"__write_confirmed": "false"}	0	38	ok	\N	2026-05-13 20:34:57.02585
83	b946ef9b-f6e1-4ce2-a9dd-b3f33c5d991e		admin	admin	{"__write_confirmed": "false"}	0	11	ok	\N	2026-05-13 20:35:34.108179
84	b946ef9b-f6e1-4ce2-a9dd-b3f33c5d991e		admin	admin	{"__write_confirmed": "false"}	0	5134	error	connect ECONNREFUSED ::1:27017, connect ECONNREFUSED 127.0.0.1:27017	2026-05-14 08:31:29.321672
85	b946ef9b-f6e1-4ce2-a9dd-b3f33c5d991e		admin	admin	{"__write_confirmed": "false"}	0	21	ok	\N	2026-05-14 08:36:06.41757
86	013dc51f-ef9d-4c8e-84a5-1e13339a3ae3		admin	admin	{"__write_confirmed": "false"}	0	19	error	Paso 1 (Paso 1: Procesando) falló: This command is not supported in the prepared statement protocol yet	2026-05-14 08:45:24.984105
87	8e598568-5963-4e03-965a-6cfaae251630		admin	admin	{"__write_confirmed": "false"}	0	62	ok	\N	2026-05-14 09:09:55.65075
88	9076fbc7-85c4-44bb-ae9a-c2cb2aed6e8c		admin	admin	{"__write_confirmed": "false"}	0	34	ok	\N	2026-05-14 09:54:39.881747
89	690d2f59-018c-4278-9925-d70e4ce9f8ad		admin	admin	{"__write_confirmed": "true"}	0	129	ok	\N	2026-05-14 09:54:52.227302
90	fa177af1-189e-4017-986a-4ebeb41469a8		admin	admin	{"__write_confirmed": "true"}	0	54	ok	\N	2026-05-14 11:21:28.203853
91	9076fbc7-85c4-44bb-ae9a-c2cb2aed6e8c		admin	admin	{"__write_confirmed": "false"}	0	34	ok	\N	2026-05-14 11:25:38.619086
92	fa177af1-189e-4017-986a-4ebeb41469a8		admin	admin	{"__write_confirmed": "true"}	0	11	ok	\N	2026-05-14 11:25:45.254159
93	aaaa1111-2222-3333-4444-555566667777		anonymous	admin	{}	0	3	error	Artículo no encontrado	2026-05-14 11:27:35.564347
94	aaaa1111-2222-3333-4444-555566667777		anonymous	admin	{}	0	2	error	Esta acción modificará datos, confirma para continuar.	2026-05-14 11:28:35.296715
95	aaaa1111-2222-3333-4444-555566667777		anonymous	admin	{"__write_confirmed": true}	0	47	ok	\N	2026-05-14 11:28:59.503758
96	3f18a2f0-9b0d-4629-8f47-ebaa1aaa69ab		admin	admin	{"__write_confirmed": "true"}	0	57	ok	\N	2026-05-14 11:31:08.704701
\.


--
-- Data for Name: comments; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.comments (id, article_id, parent_id, author_username, author_role, content, created_at, article_title) FROM stdin;
8	add0778f-3a24-4a9f-b982-9e50f4054523	\N	admin	admin	hola mundo	2026-04-28 17:06:05.654025-05	\N
13	5f966350-d379-410c-8349-355309e5cf2c	\N	admin	admin	prueba	2026-05-13 09:59:48.800716-05	🔍 Consulta: Análisis de actividad de un usuario específico
\.


--
-- Data for Name: error_logs; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.error_logs (id, mensaje, stack, modulo, capa, contexto, archivo, linea, metadata, fecha) FROM stdin;
\.


--
-- Data for Name: knowledge_base; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.knowledge_base (id, titulo, tags, contenido, categoria, subcategoria, pasos, script, creado_por, fecha, actualizado, contenido_md, tipo_solucion, descripcion, campos_formulario, script_json, visible_para, creado_por_username, actualizado_por) FROM stdin;
2939eb64-5ff4-4e35-8937-003dc88b725c	MongoDB — Conciliación: insertar nuevo registro de conciliación	{}	Inserta un nuevo registro de conciliación para la institución 9, fecha de corte 2025-05-10. Útil para probar la escritura en MongoDB desde la app.	mongodb	General	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-05-13 20:14:33.636261	2026-05-13 20:14:33.636261	\N	script	\N	\N	{"sql": "{\\"collection\\":\\"conciliacion\\",\\"insertOne\\":{\\"institutionCode\\":10,\\"cutOffDate\\":\\"2025-05-10\\",\\"conciliationDate\\":\\"2025-05-10T14:22:00.000Z\\",\\"transactionCount\\":3,\\"transactionError\\":1,\\"transactionOk\\":2,\\"pending\\":1,\\"reconciled\\":2,\\"debitTotal\\":875.25,\\"creditTotal\\":30,\\"netAmount\\":-845.25,\\"creationDate\\":\\"2025-05-10T14:22:00.000Z\\",\\"creationUser\\":\\"\\",\\"editDate\\":\\"2025-05-10T14:22:00.000Z\\",\\"editUser\\":\\"\\",\\"statusConciliation\\":1,\\"details\\":[{\\"id\\":1,\\"state\\":1,\\"observation\\":\\"OK\\",\\"reconciled\\":1,\\"description\\":\\"\\",\\"uuid\\":\\"a2ef34bc-c412-55f0-91b4-01c3e0fd67a1\\",\\"cutOffNumber\\":\\"\\",\\"cutOffDate\\":\\"0001-01-01\\",\\"transactionDateSwitch\\":\\"2025-05-10T09:15:00.000Z\\",\\"accountTypeIdSwitch\\":\\"Y9K5kRnyEjLdQpRXS1iKaw==\\",\\"accountNumberSwitch\\":\\"cg2zp/8oKDg8mkr8qnS6oA==\\",\\"transactionValueSwitch\\":15,\\"movementCodeSwitch\\":\\"10000001\\",\\"transactionTypeSwitch\\":\\"C\\",\\"institutionCodeSwitch\\":10,\\"serviceCodeSwitch\\":1,\\"transactionStatusSwitch\\":1,\\"reverseMovementCodeSwitch\\":\\"\\",\\"transactionDateInstitution\\":\\"2025-05-10T09:15:00.000Z\\",\\"accountTypeIdInstitution\\":\\"Y9K5kRnyEjLdQpRXS1iKaw==\\",\\"accountNumberInstitution\\":\\"cg2zp/8oKDg8mkr8qnS6oA==\\",\\"transactionValueInstitution\\":15,\\"movementCodeInstitution\\":\\"10000001\\",\\"transactionTypeInstitution\\":\\"C\\",\\"institutionCodeInstitution\\":10,\\"serviceCodInstitution\\":\\"QR\\",\\"transactionStatusInstitution\\":1,\\"reverseMovementCodeInstitution\\":\\"\\",\\"editDate\\":\\"2025-05-10T14:22:00.000Z\\"},{\\"id\\":2,\\"state\\":1,\\"observation\\":\\"OK\\",\\"reconciled\\":1,\\"description\\":\\"\\",\\"uuid\\":\\"b3fc45cd-d523-66g1-02c5-12d4f1ge78b2\\",\\"cutOffNumber\\":\\"\\",\\"cutOffDate\\":\\"0001-01-01\\",\\"transactionDateSwitch\\":\\"2025-05-10T10:30:00.000Z\\",\\"accountTypeIdSwitch\\":\\"dKog62Hg2Rb8bs+JYuy0zQ==\\",\\"accountNumberSwitch\\":\\"3qHuYYMpx18lHSr3Ktsbpw==\\",\\"transactionValueSwitch\\":15,\\"movementCodeSwitch\\":\\"10000002\\",\\"transactionTypeSwitch\\":\\"C\\",\\"institutionCodeSwitch\\":10,\\"serviceCodeSwitch\\":1,\\"transactionStatusSwitch\\":1,\\"reverseMovementCodeSwitch\\":\\"\\",\\"transactionDateInstitution\\":\\"2025-05-10T10:30:00.000Z\\",\\"accountTypeIdInstitution\\":\\"dKog62Hg2Rb8bs+JYuy0zQ==\\",\\"accountNumberInstitution\\":\\"3qHuYYMpx18lHSr3Ktsbpw==\\",\\"transactionValueInstitution\\":15,\\"movementCodeInstitution\\":\\"10000002\\",\\"transactionTypeInstitution\\":\\"C\\",\\"institutionCodeInstitution\\":10,\\"serviceCodInstitution\\":\\"QR\\",\\"transactionStatusInstitution\\":1,\\"reverseMovementCodeInstitution\\":\\"\\",\\"editDate\\":\\"2025-05-10T14:22:00.000Z\\"},{\\"id\\":3,\\"state\\":0,\\"observation\\":\\"Sin coincidencia en SWITCH\\",\\"reconciled\\":0,\\"description\\":\\"\\",\\"uuid\\":\\"c4gd56de-e634-77h2-13d6-23e5g2hf89c3\\",\\"cutOffNumber\\":\\"\\",\\"cutOffDate\\":\\"0001-01-01\\",\\"transactionDateSwitch\\":null,\\"accountTypeIdSwitch\\":\\"\\",\\"accountNumberSwitch\\":\\"\\",\\"transactionValueSwitch\\":0,\\"movementCodeSwitch\\":\\"\\",\\"transactionTypeSwitch\\":\\"\\",\\"institutionCodeSwitch\\":0,\\"serviceCodeSwitch\\":\\"\\",\\"transactionStatusSwitch\\":0,\\"reverseMovementCodeSwitch\\":\\"\\",\\"transactionDateInstitution\\":\\"2025-05-10T11:45:00.000Z\\",\\"accountTypeIdInstitution\\":\\"dKog62Hg2Rb8bs+JYuy0zQ==\\",\\"accountNumberInstitution\\":\\"3qHuYYMpx18lHSr3Ktsbpw==\\",\\"transactionValueInstitution\\":875.25,\\"movementCodeInstitution\\":\\"10000003\\",\\"transactionTypeInstitution\\":\\"D\\",\\"institutionCodeInstitution\\":10,\\"serviceCodInstitution\\":\\"QR\\",\\"transactionStatusInstitution\\":1,\\"reverseMovementCodeInstitution\\":\\"\\",\\"editDate\\":\\"2025-05-10T14:22:00.000Z\\"}]}}", "modo": "script", "origen": "manual-sql", "databaseId": "mongo_local", "target_database": "mongo_local"}	admin	admin	\N
1ff6addb-b02c-47eb-af86-43825bb2dbba	Oracle — Ajuste salarial masivo por departamento (UPDATE)	{oracle,update,dinamico,salarios,escritura}	UPDATE Oracle salarios	RRHH	Gestión Salarial	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-05-13 11:03:38.352124	\N	## Ajuste masivo de salarios en Oracle	script	Aplica % de aumento a todos los empleados activos de un departamento. Escribe en Oracle.	[{"tipo": "text", "nombre": "departamento", "etiqueta": "Departamento", "requerido": true, "placeholder": "TI"}, {"tipo": "number", "nombre": "pct_aumento", "etiqueta": "% de aumento", "requerido": true, "placeholder": "5"}]	{"sql": "UPDATE empleados\\nSET salario = ROUND(salario * (1 + {{pct_aumento}} / 100), 2)\\nWHERE departamento = {{departamento}}\\n  AND activo = 1", "modo": "script", "origen": "manual-sql", "databaseId": "oracle_local", "database_id": "oracle_local", "target_database": "oracle_local"}	admin	admin	\N
8cd91481-64a8-4af1-b598-c75302515e62	Recuperar contraseña	{password,recuperacion,seguridad}	# Recuperación de Contraseña\n\n## Descripción\nProceso para recuperar contraseña olvidada.\n\n## Información General\nEste es un proceso de solo lectura que explica cómo recuperar la contraseña.\n\n## Pasos a Seguir\n1. Hacer clic en "Olvidé mi contraseña"\n2. Ingresar email registrado\n3. Seguir instrucciones del email\n\n## Nota\nEste proceso no requiere ejecución automática.	Usuarios	otros temas	[{"paso": 1, "descripcion": "Hacer clic en \\"Olvidé mi contraseña\\""}, {"paso": 2, "descripcion": "Ingresar email registrado"}, {"paso": 3, "descripcion": "Seguir instrucciones del email"}]	\N	747bd085-6444-49a4-9919-64660808316c	2026-03-31 18:43:59.554272	2026-04-10 09:04:45.518487	# Recuperación de Contraseña\n\n## Descripción\nProceso para recuperar contraseña olvidada.\n\n## Información General\nEste es un proceso de solo lectura que explica cómo recuperar la contraseña.\n\n## Pasos a Seguir\n1. Hacer clic en "Olvidé mi contraseña"\n2. Ingresar email registrado\n3. Seguir instrucciones del email\n\n## Nota\nEste proceso no requiere ejecución automática.	lectura		\N	\N	todos	\N	\N
956b447f-4d1c-425f-9458-7c2107eb20b7	Error en transferencia bancaria	{}	# Errrror en transferencia bancaria\n\n## Descripción\nEl usuario no puede completar una transferencia. El sistema muestra error o la operación queda pendiente.\n\n## Posibles causas\n- Saldo insuficiente\n- Datos incorrectos del destinatario\n- Problemas de conexión con el banco\n\n## Solución paso a paso\n1. Verificar saldo disponible\n2. Confirmar datos del destinatario\n3. Reintentar la operación\n4. Validar conexión con el servicio bancario\n\n## ejecutar\nverificar_saldo\nvalidar_destinatario\nprocesar_transferencia\n\n## parametros\ncuenta_origen\ncuenta_destino\nmonto\n\n## resultado_esperado\nTransferencia completada correctamente con número de referencia	TRANSFERENCIAS	Otros temas	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-04-02 10:04:20.041182	2026-04-02 12:54:06.760015	# Errrror en transferencia bancaria\n\n## Descripción\nEl usuario no puede completar una transferencia. El sistema muestra error o la operación queda pendiente.\n\n## Posibles causas\n- Saldo insuficiente\n- Datos incorrectos del destinatario\n- Problemas de conexión con el banco\n\n## Solución paso a paso\n1. Verificar saldo disponible\n2. Confirmar datos del destinatario\n3. Reintentar la operación\n4. Validar conexión con el servicio bancario\n\n## ejecutar\nverificar_saldo\nvalidar_destinatario\nprocesar_transferencia\n\n## parametros\ncuenta_origen\ncuenta_destino\nmonto\n\n## resultado_esperado\nTransferencia completada correctamente con número de referencia	lectura		\N	\N	todos	\N	\N
ffcaab9f-ffc1-40ed-9808-1240ca344c83	App no abre	{app,movil,error}	# Aplicación No Abre\n\n## Descripción\nLa aplicación móvil se cierra al intentar iniciar.\n\n## Solución Ejecutable\n```yaml\ninstrucciones:\n  - reiniciar_app\n  - limpiar_cache_app\n  - verificar_conectividad\n  - actualizar_app\n```\n\n## Pasos de Diagnóstico\n1. Reiniciar el dispositivo\n2. Limpiar caché de la aplicación\n3. Verificar conexión a internet\n4. Actualizar la aplicación	Banca móvil	otros temas	[{"paso": 1, "descripcion": "Reiniciar el dispositivo"}, {"paso": 2, "descripcion": "Actualizar la app"}, {"paso": 3, "descripcion": "Limpiar datos de la app"}]	\N	747bd085-6444-49a4-9919-64660808316c	2026-03-31 18:43:59.55665	2026-04-09 12:19:26.996264	# Aplicación No Abre\n\n## Descripción\nLa aplicación móvil se cierra al intentar iniciar.\n\n## Solución Ejecutable\n```yaml\ninstrucciones:\n  - reiniciar_app\n  - limpiar_cache_app\n  - verificar_conectividad\n  - actualizar_app\n```\n\n## Pasos de Diagnóstico\n1. Reiniciar el dispositivo\n2. Limpiar caché de la aplicación\n3. Verificar conexión a internet\n4. Actualizar la aplicación	lectura		\N	\N	todos	\N	\N
7c4df9e4-6dfc-420d-8c56-53e16f42fd4a	Reparación de login	{}	# Reparación de login\n\nPaso 1:\nVerificar si el usuario existe\n\nPaso 2:\nValidar contraseña\n\nPaso 3:\nReiniciar sesión\n\nResultado esperado:\nAcceso correcto al sistema\n	GENERAL	Otros temas	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-04-02 10:19:28.919174	2026-05-13 20:24:06.857078	# Reparación de login\n\nPaso 1:\nVerificar si el usuario existe\n\nPaso 2:\nValidar contraseña\n\nPaso 3:\nReiniciar sesión\n\nResultado esperado:\nAcceso correcto al sistema\n	script		\N	{"modo": "script", "filter": "{\\n  \\"institutionCode\\": 9,\\n  \\"cutOffDate\\": \\"2025-05-10\\",\\n  \\"conciliationDate\\": \\"2025-05-10T14:22:00.000Z\\",\\n  \\"transactionCount\\": 3,\\n  \\"transactionError\\": 1,\\n  \\"transactionOk\\": 2,\\n  \\"pending\\": 1,\\n  \\"reconciled\\": 2,\\n  \\"debitTotal\\": 875.25,\\n  \\"creditTotal\\": 30,\\n  \\"netAmount\\": -845.25,\\n  \\"creationDate\\": \\"2025-05-10T14:22:00.000Z\\",\\n  \\"creationUser\\": \\"\\",\\n  \\"editDate\\": \\"2025-05-10T14:22:00.000Z\\",\\n  \\"editUser\\": \\"\\",\\n  \\"statusConciliation\\": 1,\\n  \\"details\\": [\\n    {\\n      \\"id\\": 1,\\n      \\"state\\": 1,\\n      \\"observation\\": \\"OK\\",\\n      \\"reconciled\\": 1,\\n      \\"description\\": \\"\\",\\n      \\"uuid\\": \\"a2ef34bc-c412-55f0-91b4-01c3e0fd67a1\\",\\n      \\"cutOffNumber\\": \\"\\",\\n      \\"cutOffDate\\": \\"0001-01-01\\",\\n      \\"transactionDateSwitch\\": \\"2025-05-10T09:15:00.000Z\\",\\n      \\"accountTypeIdSwitch\\": \\"Y9K5kRnyEjLdQpRXS1iKaw==\\",\\n      \\"accountNumberSwitch\\": \\"cg2zp/8oKDg8mkr8qnS6oA==\\",\\n      \\"transactionValueSwitch\\": 15,\\n      \\"movementCodeSwitch\\": \\"91000001\\",\\n      \\"transactionTypeSwitch\\": \\"C\\",\\n      \\"institutionCodeSwitch\\": 9,\\n      \\"serviceCodeSwitch\\": 1,\\n      \\"transactionStatusSwitch\\": 1,\\n      \\"reverseMovementCodeSwitch\\": \\"\\",\\n      \\"transactionDateInstitution\\": \\"2025-05-10T09:15:00.000Z\\",\\n      \\"accountTypeIdInstitution\\": \\"Y9K5kRnyEjLdQpRXS1iKaw==\\",\\n      \\"accountNumberInstitution\\": \\"cg2zp/8oKDg8mkr8qnS6oA==\\",\\n      \\"transactionValueInstitution\\": 15,\\n      \\"movementCodeInstitution\\": \\"91000001\\",\\n      \\"transactionTypeInstitution\\": \\"C\\",\\n      \\"institutionCodeInstitution\\": 9,\\n      \\"serviceCodInstitution\\": \\"QR\\",\\n      \\"transactionStatusInstitution\\": 1,\\n      \\"reverseMovementCodeInstitution\\": \\"\\",\\n      \\"editDate\\": \\"2025-05-10T14:22:00.000Z\\"\\n    },\\n    {\\n      \\"id\\": 2,\\n      \\"state\\": 1,\\n      \\"observation\\": \\"OK\\",\\n      \\"reconciled\\": 1,\\n      \\"description\\": \\"\\",\\n      \\"uuid\\": \\"b3fc45cd-d523-66g1-02c5-12d4f1ge78b2\\",\\n      \\"cutOffNumber\\": \\"\\",\\n      \\"cutOffDate\\": \\"0001-01-01\\",\\n      \\"transactionDateSwitch\\": \\"2025-05-10T10:30:00.000Z\\",\\n      \\"accountTypeIdSwitch\\": \\"dKog62Hg2Rb8bs+JYuy0zQ==\\",\\n      \\"accountNumberSwitch\\": \\"3qHuYYMpx18lHSr3Ktsbpw==\\",\\n      \\"transactionValueSwitch\\": 15,\\n      \\"movementCodeSwitch\\": \\"91000002\\",\\n      \\"transactionTypeSwitch\\": \\"C\\",\\n      \\"institutionCodeSwitch\\": 9,\\n      \\"serviceCodeSwitch\\": 1,\\n      \\"transactionStatusSwitch\\": 1,\\n      \\"reverseMovementCodeSwitch\\": \\"\\",\\n      \\"transactionDateInstitution\\": \\"2025-05-10T10:30:00.000Z\\",\\n      \\"accountTypeIdInstitution\\": \\"dKog62Hg2Rb8bs+JYuy0zQ==\\",\\n      \\"accountNumberInstitution\\": \\"3qHuYYMpx18lHSr3Ktsbpw==\\",\\n      \\"transactionValueInstitution\\": 15,\\n      \\"movementCodeInstitution\\": \\"91000002\\",\\n      \\"transactionTypeInstitution\\": \\"C\\",\\n      \\"institutionCodeInstitution\\": 9,\\n      \\"serviceCodInstitution\\": \\"QR\\",\\n      \\"transactionStatusInstitution\\": 1,\\n      \\"reverseMovementCodeInstitution\\": \\"\\",\\n      \\"editDate\\": \\"2025-05-10T14:22:00.000Z\\"\\n    },\\n    {\\n      \\"id\\": 3,\\n      \\"state\\": 0,\\n      \\"observation\\": \\"Sin coincidencia, no existe registro en el SWITCH\\",\\n      \\"reconciled\\": 0,\\n      \\"description\\": \\"\\",\\n      \\"uuid\\": \\"c4gd56de-e634-77h2-13d6-23e5g2hf89c3\\",\\n      \\"cutOffNumber\\": \\"\\",\\n      \\"cutOffDate\\": \\"0001-01-01\\",\\n      \\"transactionDateSwitch\\": null,\\n      \\"accountTypeIdSwitch\\": \\"\\",\\n      \\"accountNumberSwitch\\": \\"\\",\\n      \\"transactionValueSwitch\\": 0,\\n      \\"movementCodeSwitch\\": \\"\\",\\n      \\"transactionTypeSwitch\\": \\"\\",\\n      \\"institutionCodeSwitch\\": 0,\\n      \\"serviceCodeSwitch\\": \\"\\",\\n      \\"transactionStatusSwitch\\": 0,\\n      \\"reverseMovementCodeSwitch\\": \\"\\",\\n      \\"transactionDateInstitution\\": \\"2025-05-10T11:45:00.000Z\\",\\n      \\"accountTypeIdInstitution\\": \\"dKog62Hg2Rb8bs+JYuy0zQ==\\",\\n      \\"accountNumberInstitution\\": \\"3qHuYYMpx18lHSr3Ktsbpw==\\",\\n      \\"transactionValueInstitution\\": 875.25,\\n      \\"movementCodeInstitution\\": \\"91000003\\",\\n      \\"transactionTypeInstitution\\": \\"D\\",\\n      \\"institutionCodeInstitution\\": 9,\\n      \\"serviceCodInstitution\\": \\"QR\\",\\n      \\"transactionStatusInstitution\\": 1,\\n      \\"reverseMovementCodeInstitution\\": \\"\\",\\n      \\"editDate\\": \\"2025-05-10T14:22:00.000Z\\"\\n    }\\n  ]\\n}", "origen": "manual-sql", "operation": "insertone", "collection": "conciliacion", "databaseId": "mongo_local", "target_database": "mongo_local"}	todos	\N	admin
3f18a2f0-9b0d-4629-8f47-ebaa1aaa69ab	🔍 Consulta: Análisis de actividad de un usuario específico	{}	# 🔍 Consulta: Análisis de actividad de un usuario específico\r\n\r\n## 📌 Descripción\r\nEsta consulta permite analizar la actividad de un usuario en el sistema, mostrando:\r\n\r\n- Su nombre de usuario\r\n- Su rol dentro del sistema\r\n- Número total de sesiones\r\n- Número total de logs generados\r\n\r\n## ⚠️ Parámetro requerido\r\nEl usuario debe ingresar:\r\n\r\n👉 `username` (nombre de usuario a buscar)\r\n\r\n## 🧠 Lógica\r\n1. Se busca el usuario por su username\r\n2. Se unen las tablas:\r\n   - users\r\n   - roles\r\n   - sessions\r\n   - logs\r\n3. Se cuentan sesiones y logs\r\n4. Se agrupa por usuario\r\n\r\n## 📊 Resultado esperado\r\nSe mostrará un resumen del usuario con su nivel de actividad.\r\n\r\n## 🧾 Interpretación automática\r\n- Si `total_logs > 10` → Usuario muy activo\r\n- Si `total_sessions = 0` → Usuario inactivo\r\n- Si `role = admin` → Usuario con privilegios altos\r\n\r\n## 🧩 Entrada requerida del usuario\r\nIngrese el username en el campo antes de ejecutar la consulta.	NUEVONUEVO	General	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-05-14 11:31:00.405114	\N	# 🔍 Consulta: Análisis de actividad de un usuario específico\r\n\r\n## 📌 Descripción\r\nEsta consulta permite analizar la actividad de un usuario en el sistema, mostrando:\r\n\r\n- Su nombre de usuario\r\n- Su rol dentro del sistema\r\n- Número total de sesiones\r\n- Número total de logs generados\r\n\r\n## ⚠️ Parámetro requerido\r\nEl usuario debe ingresar:\r\n\r\n👉 `username` (nombre de usuario a buscar)\r\n\r\n## 🧠 Lógica\r\n1. Se busca el usuario por su username\r\n2. Se unen las tablas:\r\n   - users\r\n   - roles\r\n   - sessions\r\n   - logs\r\n3. Se cuentan sesiones y logs\r\n4. Se agrupa por usuario\r\n\r\n## 📊 Resultado esperado\r\nSe mostrará un resumen del usuario con su nivel de actividad.\r\n\r\n## 🧾 Interpretación automática\r\n- Si `total_logs > 10` → Usuario muy activo\r\n- Si `total_sessions = 0` → Usuario inactivo\r\n- Si `role = admin` → Usuario con privilegios altos\r\n\r\n## 🧩 Entrada requerida del usuario\r\nIngrese el username en el campo antes de ejecutar la consulta.	script		\N	{"sql": "-- PASO 1: Promover empleado y subirle el sueldo\\nUPDATE empleados SET cargo = 'Líder de Equipo', salario = salario + 500 WHERE id = 3;\\n\\n-- PASO 2: Reasignar a otro departamento\\nUPDATE empleados SET departamento_id = 1 WHERE id = 4", "modo": "script", "origen": "manual-sql", "databaseId": "mysql_test", "target_database": "mysql_test"}	todos	admin	\N
db84ece4-3394-4031-b8e5-6cde79a312ba	Verificación Completa de Usuario	{}	# Verificación Completa de Usuario\r\n\r\n## Descripción\r\nEsta solución permite verificar el estado completo de un usuario en el sistema.\r\n\r\n## ¿Qué valida?\r\n- Existencia del usuario\r\n- Estado (bloqueado o activo)\r\n- Rol asignado\r\n\r\n## Instrucciones\r\nIngresa el nombre de usuario para validar su estado.\r\n\r\n## Resultado esperado\r\nEl sistema indicará si el usuario está activo, bloqueado o no existe.	Usuarios	otros temas	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-04-07 09:32:13.319744	2026-04-10 09:04:59.320092	# Verificación Completa de Usuario\r\n\r\n## Descripción\r\nEsta solución permite verificar el estado completo de un usuario en el sistema.\r\n\r\n## ¿Qué valida?\r\n- Existencia del usuario\r\n- Estado (bloqueado o activo)\r\n- Rol asignado\r\n\r\n## Instrucciones\r\nIngresa el nombre de usuario para validar su estado.\r\n\r\n## Resultado esperado\r\nEl sistema indicará si el usuario está activo, bloqueado o no existe.	lectura		\N	\N	todos	\N	\N
19a1e879-7450-473c-9582-23022ec10344	MySQL — Transacciones por institución (filtros dinámicos)	{mysql,dinamico,filtros,transacciones,between}	Transacciones dinámico MySQL	Finanzas	Consultas Dinámicas	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-05-13 11:03:38.348567	\N	## Transacciones filtradas — parámetros opcionales	script	Parámetros: institución, fecha_desde, fecha_hasta, monto_min. COALESCE para valores opcionales.	[{"tipo": "text", "nombre": "institucion", "etiqueta": "Institución", "requerido": false, "placeholder": "Ej: Banco del Pichincha"}, {"tipo": "date", "nombre": "fecha_desde", "etiqueta": "Fecha desde", "requerido": true}, {"tipo": "date", "nombre": "fecha_hasta", "etiqueta": "Fecha hasta", "requerido": true}, {"tipo": "number", "nombre": "monto_min", "etiqueta": "Monto mínimo", "requerido": false, "placeholder": "0"}]	{"sql": "SELECT\\n    op.id,\\n    i.nombre                            AS institucion,\\n    sps.nombre                          AS servicio,\\n    op.monto,\\n    op.estado,\\n    op.fecha_pago,\\n    DATEDIFF(NOW(), op.fecha_pago)      AS dias_atras,\\n    CASE op.estado WHEN 1 THEN 'Procesada' ELSE 'Pendiente' END AS estado_texto\\nFROM ordenes_pago op\\nJOIN servicios_punto_venta sps ON op.sale_point_service_id = sps.id\\nJOIN instituciones i           ON sps.institucion_id = i.id\\nWHERE op.fecha_pago BETWEEN {{fecha_desde}} AND {{fecha_hasta}}\\n  AND op.monto >= COALESCE({{monto_min}}, 0)\\n  AND ({{institucion}} IS NULL OR i.nombre LIKE CONCAT('%', {{institucion}}, '%'))\\nORDER BY op.monto DESC", "modo": "script", "origen": "manual-sql", "databaseId": "mysql_test", "database_id": "mysql_test", "target_database": "mysql_test"}	todos	admin	\N
5f966350-d379-410c-8349-355309e5cf2c	🔍 Consulta: Análisis de actividad de un usuario específico	{}	# 🔍 Consulta: Análisis de actividad de un usuario específico\r\n\r\n## 📌 Descripción\r\nEsta consulta permite analizar la actividad de un usuario en el sistema, mostrando:\r\n\r\n- Su nombre de usuario\r\n- Su rol dentro del sistema\r\n- Número total de sesiones\r\n- Número total de logs generados\r\n\r\n## ⚠️ Parámetro requerido\r\nEl usuario debe ingresar:\r\n\r\n👉 `username` (nombre de usuario a buscar)\r\n\r\n## 🧠 Lógica\r\n1. Se busca el usuario por su username\r\n2. Se unen las tablas:\r\n   - users\r\n   - roles\r\n   - sessions\r\n   - logs\r\n3. Se cuentan sesiones y logs\r\n4. Se agrupa por usuario\r\n\r\n## 📊 Resultado esperado\r\nSe mostrará un resumen del usuario con su nivel de actividad.\r\n\r\n## 🧾 Interpretación automática\r\n- Si `total_logs > 10` → Usuario muy activo\r\n- Si `total_sessions = 0` → Usuario inactivo\r\n- Si `role = admin` → Usuario con privilegios altos\r\n\r\n## 🧩 Entrada requerida del usuario\r\nIngrese el username en el campo antes de ejecutar la consulta.	Usuarios	otro tema	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-04-15 11:30:40.106793	2026-04-29 00:37:39.660435	# 🔍 Consulta: Análisis de actividad de un usuario específico\r\n\r\n## 📌 Descripción\r\nEsta consulta permite analizar la actividad de un usuario en el sistema, mostrando:\r\n\r\n- Su nombre de usuario\r\n- Su rol dentro del sistema\r\n- Número total de sesiones\r\n- Número total de logs generados\r\n\r\n## ⚠️ Parámetro requerido\r\nEl usuario debe ingresar:\r\n\r\n👉 `username` (nombre de usuario a buscar)\r\n\r\n## 🧠 Lógica\r\n1. Se busca el usuario por su username\r\n2. Se unen las tablas:\r\n   - users\r\n   - roles\r\n   - sessions\r\n   - logs\r\n3. Se cuentan sesiones y logs\r\n4. Se agrupa por usuario\r\n\r\n## 📊 Resultado esperado\r\nSe mostrará un resumen del usuario con su nivel de actividad.\r\n\r\n## 🧾 Interpretación automática\r\n- Si `total_logs > 10` → Usuario muy activo\r\n- Si `total_sessions = 0` → Usuario inactivo\r\n- Si `role = admin` → Usuario con privilegios altos\r\n\r\n## 🧩 Entrada requerida del usuario\r\nIngrese el username en el campo antes de ejecutar la consulta.	script		\N	{"sql": "-- [Motor PL/SQL híbrido]\\n-- Este script será interpretado y ejecutado paso a paso.\\n\\nDO $$\\nDECLARE\\n    v_user_id UUID;\\n    v_username TEXT := 'admin';\\n\\n    v_total_logs INTEGER := 0;\\n    v_total_sessions INTEGER := 0;\\n    v_sessions_active INTEGER := 0;\\n\\n    v_last_log TIMESTAMP;\\n    v_created_at TIMESTAMP;\\n\\n    v_error TEXT;\\n\\n    v_control INTEGER := 0;\\n    v_threshold_logs INTEGER := 5;\\n\\n    rec_user RECORD;\\n\\nBEGIN\\n\\n    -- 🔥 INPUT COMPLETO (ESTILO EMPRESA)\\n    RAISE NOTICE 'INPUT => usuario=%, threshold_logs=%',\\n        v_username,\\n        v_threshold_logs;\\n\\n    -- =========================================\\n    -- VALIDAR EXISTENCIA DEL USUARIO\\n    -- =========================================\\n    SELECT id, created_at\\n    INTO v_user_id, v_created_at\\n    FROM users\\n    WHERE username = v_username;\\n\\n    IF v_user_id IS NULL THEN\\n        v_error := 'ERROR: usuario no existe';\\n        RAISE NOTICE '%', v_error;\\n        RETURN;\\n    END IF;\\n\\n    RAISE NOTICE 'Usuario encontrado: % | creado_en=%',\\n        v_user_id,\\n        v_created_at;\\n\\n    -- =========================================\\n    -- OBTENER LOGS DEL USUARIO\\n    -- =========================================\\n    SELECT COUNT(*), MAX(created_at)\\n    INTO v_total_logs, v_last_log\\n    FROM logs\\n    WHERE user_id = v_user_id;\\n\\n    RAISE NOTICE 'Logs => total=% | ultimo_log=%',\\n        v_total_logs,\\n        v_last_log;\\n\\n    -- =========================================\\n    -- OBTENER SESIONES\\n    -- =========================================\\n    SELECT COUNT(*)\\n    INTO v_total_sessions\\n    FROM sessions\\n    WHERE user_id = v_user_id;\\n\\n    SELECT COUNT(*)\\n    INTO v_sessions_active\\n    FROM sessions\\n    WHERE user_id = v_user_id\\n    AND activo = true;\\n\\n    RAISE NOTICE 'Sesiones => total=% | activas=%',\\n        v_total_sessions,\\n        v_sessions_active;\\n\\n    -- =========================================\\n    -- VALIDACIONES\\n    -- =========================================\\n\\n    IF v_total_logs = 0 THEN\\n        RAISE NOTICE '⚠ Usuario sin actividad';\\n    END IF;\\n\\n    IF v_sessions_active = 0 THEN\\n        RAISE NOTICE '⚠ Usuario sin sesiones activas';\\n    END IF;\\n\\n    -- =========================================\\n    -- LOGICA DE CONTROL\\n    -- =========================================\\n\\n    IF v_total_logs >= v_threshold_logs THEN\\n\\n        v_control := 1;\\n\\n        RAISE NOTICE '✔ Usuario supera umbral (% >= %)',\\n            v_total_logs,\\n            v_threshold_logs;\\n\\n        INSERT INTO query_history (\\n            username,\\n            status,\\n            created_at,\\n            user_role\\n        )\\n        SELECT \\n            u.username,\\n            'ok',\\n            NOW(),\\n            r.name\\n        FROM users u\\n        LEFT JOIN roles r ON u.role_id = r.id\\n        WHERE u.id = v_user_id;\\n\\n        RAISE NOTICE '✔ Insert en query_history realizado';\\n\\n    ELSE\\n        v_control := 0;\\n\\n        RAISE NOTICE '✖ Usuario no supera umbral (% < %)',\\n            v_total_logs,\\n            v_threshold_logs;\\n    END IF;\\n\\n    -- =========================================\\n    -- ANALISIS DE COMENTARIOS\\n    -- =========================================\\n\\n    PERFORM 1\\n    FROM comments\\n    WHERE author_username = v_username;\\n\\n    IF NOT FOUND THEN\\n        RAISE NOTICE 'Usuario sin comentarios';\\n    ELSE\\n        RAISE NOTICE 'Usuario tiene comentarios';\\n    END IF;\\n\\n    -- =========================================\\n    -- RESUMEN FINAL\\n    -- =========================================\\n\\n    RAISE NOTICE '=== RESUMEN FINAL ===';\\n    RAISE NOTICE 'Usuario: %', v_username;\\n    RAISE NOTICE 'Logs: %', v_total_logs;\\n    RAISE NOTICE 'Sesiones: %', v_total_sessions;\\n    RAISE NOTICE 'Sesiones activas: %', v_sessions_active;\\n    RAISE NOTICE 'Control: %', v_control;\\n\\nEXCEPTION\\n    WHEN OTHERS THEN\\n        RAISE NOTICE 'ERROR GENERAL: %', SQLERRM;\\nEND $$;", "modo": "script", "origen": "manual-sql"}	todos	\N	\N
add0778f-3a24-4a9f-b982-9e50f4054523	Problemas con loginn ORACLE	{}	# Problemas con login\r\n\r\n## Descripción\r\nEl usuario no puede iniciar sesión.\r\n\r\n## Tipo de error\r\nlogin, autenticación\r\n\r\n## Pasos a seguir\r\n1. Verificar usuario\r\n2. Resetear contraseña\r\n3. Limpiar caché del navegador\r\n\r\n## Solución automática\r\n```bash\r\necho "Reiniciando sesión..."\r\necho "Verificando credenciales..."\r\necho "Login exitoso"	Usuarios	otro tema	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-04-10 09:29:36.752971	2026-04-29 00:35:02.401495	# Problemas con login\r\n\r\n## Descripción\r\nEl usuario no puede iniciar sesión.\r\n\r\n## Tipo de error\r\nlogin, autenticación\r\n\r\n## Pasos a seguir\r\n1. Verificar usuario\r\n2. Resetear contraseña\r\n3. Limpiar caché del navegador\r\n\r\n## Solución automática\r\n```bash\r\necho "Reiniciando sesión..."\r\necho "Verificando credenciales..."\r\necho "Login exitoso"	script		\N	{"sql": "DECLARE\\n\\n    v_username        VARCHAR2(100) := 'admin';\\n    v_user_id         VARCHAR2(50);\\n    v_total_logs      NUMBER := 0;\\n    v_total_sessions  NUMBER := 0;\\n    v_active_sessions NUMBER := 0;\\n    v_last_log        TIMESTAMP;\\n    v_control         NUMBER := 0;\\n\\nBEGIN\\n\\n    DBMS_OUTPUT.PUT_LINE('=== INICIO ===');\\n\\n    SELECT id INTO v_user_id\\n    FROM users\\n    WHERE username = v_username;\\n\\n    DBMS_OUTPUT.PUT_LINE('Usuario: ' || v_user_id);\\n\\n    SELECT COUNT(*), MAX(created_at)\\n    INTO v_total_logs, v_last_log\\n    FROM logs\\n    WHERE user_id = v_user_id;\\n\\n    DBMS_OUTPUT.PUT_LINE('Logs: ' || v_total_logs);\\n    DBMS_OUTPUT.PUT_LINE('Último log: ' || v_last_log);\\n\\n    SELECT COUNT(*) INTO v_total_sessions\\n    FROM sessions\\n    WHERE user_id = v_user_id;\\n\\n    SELECT COUNT(*) INTO v_active_sessions\\n    FROM sessions\\n    WHERE user_id = v_user_id\\n    AND activo = 1;\\n\\n    DBMS_OUTPUT.PUT_LINE('Sesiones: ' || v_total_sessions);\\n    DBMS_OUTPUT.PUT_LINE('Activas: ' || v_active_sessions);\\n\\n    IF v_total_logs > 2 THEN\\n        v_control := 1;\\n\\n        INSERT INTO query_history(username, status, created_at)\\n        VALUES (v_username, 'OK', SYSTIMESTAMP);\\n\\n        DBMS_OUTPUT.PUT_LINE('Usuario supera umbral');\\n    ELSE\\n        v_control := 0;\\n        DBMS_OUTPUT.PUT_LINE('Usuario no supera umbral');\\n    END IF;\\n\\n    FOR rec IN (\\n        SELECT author_username, COUNT(*) total\\n        FROM comments\\n        GROUP BY author_username\\n    )\\n    LOOP\\n        DBMS_OUTPUT.PUT_LINE(\\n            'Usuario: ' || rec.author_username ||\\n            ' | Comentarios: ' || rec.total\\n        );\\n    END LOOP;\\n\\n    DBMS_OUTPUT.PUT_LINE('Control final: ' || v_control);\\n\\nEXCEPTION\\n    WHEN NO_DATA_FOUND THEN\\n        DBMS_OUTPUT.PUT_LINE('Usuario no existe');\\n    WHEN OTHERS THEN\\n        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);\\n\\nEND;\\n/", "modo": "script", "origen": "manual-sql"}	todos	\N	\N
8e598568-5963-4e03-965a-6cfaae251630	MySQL — Ranking salarial con CTE y Window Functions	{mysql,cte,window-functions,rank,salarios,estatico}	## Ranking salarial por departamento con CTE	RRHH	Análisis Salarial	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-05-13 11:03:38.347187	2026-05-13 12:02:22.64792	## Ranking salarial por departamento con CTE	script	WITH anidado: stats por depto + RANK() por salario + diferencia vs promedio + % uso presupuesto	\N	{"sql": "WITH stats_depto AS (\\n    SELECT\\n        d.id                                        AS depto_id,\\n        d.nombre                                    AS departamento,\\n        COUNT(e.id)                                 AS total_empleados,\\n        AVG(e.salario)                              AS salario_promedio,\\n        MAX(e.salario)                              AS salario_max,\\n        MIN(e.salario)                              AS salario_min,\\n        SUM(e.salario)                              AS masa_salarial,\\n        d.presupuesto,\\n        ROUND(SUM(e.salario) / d.presupuesto * 100, 2) AS uso_presupuesto_pct\\n    FROM empleados e\\n    JOIN departamentos d ON e.departamento_id = d.id\\n    WHERE e.activo = 1\\n    GROUP BY d.id, d.nombre, d.presupuesto\\n),\\nranking_empleados AS (\\n    SELECT\\n        e.id,\\n        CONCAT(e.nombre, ' ', e.apellido)           AS nombre_completo,\\n        e.cargo,\\n        e.salario,\\n        e.fecha_ingreso,\\n        d.nombre                                    AS departamento,\\n        DATEDIFF(NOW(), e.fecha_ingreso) / 365      AS anos_empresa,\\n        RANK() OVER (\\n            PARTITION BY e.departamento_id\\n            ORDER BY e.salario DESC\\n        )                                           AS ranking_depto\\n    FROM empleados e\\n    JOIN departamentos d ON e.departamento_id = d.id\\n    WHERE e.activo = 1\\n)\\nSELECT\\n    r.nombre_completo,\\n    r.cargo,\\n    r.departamento,\\n    r.salario,\\n    ROUND(r.anos_empresa, 1)                        AS anos_empresa,\\n    r.ranking_depto,\\n    ROUND(s.salario_promedio, 2)                    AS promedio_depto,\\n    ROUND(r.salario - s.salario_promedio, 2)        AS diferencia_vs_promedio,\\n    ROUND(r.salario / s.salario_promedio * 100, 1)  AS pct_vs_promedio,\\n    s.uso_presupuesto_pct\\nFROM ranking_empleados r\\nJOIN stats_depto s ON r.departamento = s.departamento\\nORDER BY r.departamento, r.ranking_depto", "modo": "script", "origen": "manual-sql", "databaseId": "mysql_test", "database_id": "mysql_test", "target_database": "mysql_test"}	todos	admin	admin
3c2bca46-6a46-46c5-952f-a16b5b26dfbe	Oracle — Ventas con ROLLUP y RATIO_TO_REPORT	{oracle,rollup,ratio-to-report,analiticas,ventas,estatico}	Analíticas Oracle	Ventas	Reportes Oracle	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-05-13 11:03:38.350094	\N	## Ventas con ROLLUP y funciones analíticas Oracle	script	ROLLUP para subtotales automáticos, RATIO_TO_REPORT para % participación por categoría y global	\N	{"sql": "SELECT\\n    NVL(p.categoria, 'TOTAL GENERAL')           AS categoria,\\n    NVL(p.nombre,    '-- Subtotal --')          AS producto,\\n    COUNT(v.id)                                 AS num_ventas,\\n    SUM(v.total)                                AS total_ventas,\\n    AVG(v.total)                                AS ticket_promedio,\\n    MAX(v.total)                                AS venta_max,\\n    ROUND(\\n        RATIO_TO_REPORT(SUM(v.total))\\n        OVER (PARTITION BY p.categoria) * 100, 2\\n    )                                           AS participacion_categoria_pct,\\n    ROUND(\\n        RATIO_TO_REPORT(SUM(v.total))\\n        OVER () * 100, 2\\n    )                                           AS participacion_total_pct,\\n    COUNT(CASE WHEN v.pagado = 1 THEN 1 END)    AS pagadas,\\n    COUNT(CASE WHEN v.pagado = 0 THEN 1 END)    AS pendientes,\\n    SUM(CASE WHEN v.pagado = 0 THEN v.total ELSE 0 END) AS monto_pendiente\\nFROM ventas v\\nJOIN productos p ON v.producto_id = p.id\\nGROUP BY ROLLUP(p.categoria, p.nombre)\\nORDER BY p.categoria NULLS LAST, total_ventas DESC NULLS LAST", "modo": "script", "origen": "manual-sql", "databaseId": "oracle_local", "database_id": "oracle_local", "target_database": "oracle_local"}	admin	admin	\N
abbad29e-ea6e-4401-803a-4fd3457b6ec9	PostgreSQL — Auditoría de sesiones con LAG y LEAD	{postgres,auditoria,cte,lag,lead,window-functions,seguridad,estatico}	Auditoría PostgreSQL	Seguridad	Auditoría	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-05-13 11:03:38.352947	\N	## Análisis de actividad de usuarios — window functions avanzadas	script	CTE doble: sesiones individuales con LAG/LEAD + métricas por usuario. Detección de patrones sospechosos.	\N	{"sql": "WITH sesiones AS (\\n    SELECT\\n        username,\\n        rol                                               AS rol,\\n        ip,\\n        estado,\\n        fecha,\\n        LAG(fecha)  OVER (PARTITION BY username ORDER BY fecha) AS login_anterior,\\n        LEAD(fecha) OVER (PARTITION BY username ORDER BY fecha) AS siguiente_evento,\\n        ROW_NUMBER() OVER (PARTITION BY username ORDER BY fecha DESC) AS nro_evento\\n    FROM login_audit\\n    WHERE fecha >= NOW() - INTERVAL '30 days'\\n),\\nmetricas AS (\\n    SELECT\\n        username,\\n        COUNT(*)                                            AS total_eventos,\\n        COUNT(CASE WHEN estado = 'ok'     THEN 1 END)      AS logins_ok,\\n        COUNT(CASE WHEN estado = 'fail'   THEN 1 END)      AS logins_fail,\\n        COUNT(CASE WHEN estado = 'logout' THEN 1 END)      AS logouts,\\n        MIN(fecha)                                          AS primer_evento,\\n        MAX(fecha)                                          AS ultimo_evento,\\n        COUNT(DISTINCT ip)                                  AS ips_distintas,\\n        ROUND(\\n            COUNT(CASE WHEN estado = 'fail' THEN 1 END)::numeric\\n            / NULLIF(COUNT(*), 0) * 100, 1\\n        )                                                   AS pct_fallos\\n    FROM login_audit\\n    WHERE fecha >= NOW() - INTERVAL '30 days'\\n    GROUP BY username\\n)\\nSELECT\\n    m.username,\\n    m.total_eventos,\\n    m.logins_ok,\\n    m.logins_fail,\\n    m.logouts,\\n    m.ips_distintas,\\n    m.pct_fallos,\\n    ROUND(EXTRACT(EPOCH FROM (m.ultimo_evento - m.primer_evento)) / 3600, 1) AS horas_activo,\\n    m.primer_evento,\\n    m.ultimo_evento,\\n    CASE\\n        WHEN m.pct_fallos > 50   THEN 'ALERTA: muchos fallos'\\n        WHEN m.ips_distintas > 3 THEN 'AVISO: multiples IPs'\\n        ELSE 'Normal'\\n    END                                                     AS alerta\\nFROM metricas m\\nORDER BY m.pct_fallos DESC, m.total_eventos DESC", "modo": "script", "origen": "manual-sql", "databaseId": "pg_main", "database_id": "pg_main", "target_database": "pg_main"}	admin	admin	\N
bed57404-2a43-47dd-8bbc-aa8d555e99f9	MySQL — Alta de empleado (INSERT dinámico)	{mysql,insert,dinamico,empleados,escritura}	## Registrar nuevo empleado en MySQL	RRHH	Gestión	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-05-13 11:03:38.349388	2026-05-13 12:28:48.065894	## Registrar nuevo empleado en MySQL	script	Registra nuevo empleado en MySQL. Todos los campos son variables del formulario.	\N	{"sql": "INSERT INTO empleados (cedula, nombre, apellido, email, departamento_id, cargo, salario, fecha_ingreso, activo) VALUES ({{cedula}}, {{nombre}}, {{apellido}}, {{email}}, {{departamento_id}}, {{cargo}}, {{salario}}, {{fecha_ingreso}}, 1)", "modo": "script", "origen": "manual-sql", "databaseId": "mysql_test", "database_id": "mysql_test", "target_database": "mysql_test"}	todos	admin	admin
dc84bf6d-0c42-4802-87ee-e03791db0f14	MySQL — Análisis de transacciones por institución	{mysql,finanzas,group-by,having,joins,estatico}	Análisis financiero MySQL	Finanzas	Transacciones	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-05-13 11:03:38.340445	\N	## Análisis completo de transacciones por institución y servicio	script	GROUP BY multitable, HAVING, DATEDIFF, SUM condicional, comisiones. Query de 100+ líneas lógicas.	\N	{"sql": "SELECT\\n    i.nombre                                    AS institucion,\\n    i.tipo                                      AS tipo_institucion,\\n    sps.nombre                                  AS servicio,\\n    COUNT(op.id)                                AS total_transacciones,\\n    SUM(op.monto)                               AS monto_total,\\n    AVG(op.monto)                               AS monto_promedio,\\n    MAX(op.monto)                               AS transaccion_mayor,\\n    MIN(op.monto)                               AS transaccion_menor,\\n    SUM(op.monto * sps.comision)                AS comision_generada,\\n    COUNT(CASE WHEN op.estado = 1 THEN 1 END)   AS procesadas,\\n    COUNT(CASE WHEN op.estado = 0 THEN 1 END)   AS pendientes,\\n    MAX(op.fecha_pago)                          AS ultima_transaccion,\\n    DATEDIFF(NOW(), MAX(op.fecha_pago))         AS dias_desde_ultima\\nFROM ordenes_pago op\\nJOIN servicios_punto_venta sps ON op.sale_point_service_id = sps.id\\nJOIN instituciones i           ON sps.institucion_id = i.id\\nWHERE op.fecha_pago >= DATE_SUB(NOW(), INTERVAL 100 DAY)\\n  AND i.activo = 1\\n  AND sps.activo = 1\\nGROUP BY i.id, i.nombre, i.tipo, sps.id, sps.nombre, sps.comision\\nHAVING total_transacciones > 0\\n   AND monto_total > 100\\nORDER BY monto_total DESC, total_transacciones DESC", "modo": "script", "origen": "manual-sql", "databaseId": "mysql_test", "database_id": "mysql_test", "target_database": "mysql_test"}	admin	admin	\N
05ce43a2-2ab0-4de2-84af-7a11b4821d47	MongoDB — Empleados por departamento y salario (dinámico)	{mongodb,dinamico,find,empleados,gte}	find dinamico MongoDB	RRHH	Consultas Dinámicas	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-05-13 11:03:38.355589	\N	## Búsqueda de empleados en MongoDB con filtros	script	find() con $gte en salario y filtro por departamento. Parámetros opcionales dinámicos.	[{"tipo": "text", "nombre": "departamento", "etiqueta": "Departamento", "requerido": false, "placeholder": "TI, Finanzas, RRHH"}, {"tipo": "number", "nombre": "salario_min", "etiqueta": "Salario mínimo", "requerido": false, "placeholder": "0"}]	{"modo": "script", "sort": "{\\"salario\\":-1}", "limit": 50, "filter": "{\\"departamento\\":{{departamento}},\\"salario\\":{\\"$gte\\":{{salario_min}}}}", "origen": "manual-sql", "operation": "find", "collection": "empleados", "databaseId": "mongo_local", "database_id": "mongo_local", "target_database": "mongo_local"}	todos	admin	\N
21961a02-af0e-4237-abb1-dd0e89eccc7f	MongoDB — Pipeline ventas: doble $group + $addFields	{mongodb,aggregate,pipeline,group,addfields,estatico}	Pipeline MongoDB ventas	Ventas	Análisis MongoDB	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-05-13 11:03:38.354791	\N	## Pipeline de agregación: rendimiento por vendedor	script	Aggregation pipeline: $addFields fecha, doble $group (por vendedor+producto → por vendedor), % cobrado, sort	\N	{"modo": "script", "origen": "manual-sql", "pipeline": "[{\\"$addFields\\":{\\"mes\\":{\\"$month\\":\\"$fecha\\"},\\"anio\\":{\\"$year\\":\\"$fecha\\"}}},{\\"$group\\":{\\"_id\\":{\\"vendedor\\":\\"$vendedor\\",\\"producto\\":\\"$producto_codigo\\",\\"pagado\\":\\"$pagado\\"},\\"total_ventas\\":{\\"$sum\\":\\"$total\\"},\\"total_unidades\\":{\\"$sum\\":\\"$cantidad\\"},\\"num_operaciones\\":{\\"$sum\\":1},\\"ticket_promedio\\":{\\"$avg\\":\\"$total\\"},\\"venta_max\\":{\\"$max\\":\\"$total\\"}}},{\\"$group\\":{\\"_id\\":\\"$_id.vendedor\\",\\"total_facturado\\":{\\"$sum\\":\\"$total_ventas\\"},\\"total_operaciones\\":{\\"$sum\\":\\"$num_operaciones\\"},\\"monto_cobrado\\":{\\"$sum\\":{\\"$cond\\":[{\\"$eq\\":[\\"$_id.pagado\\",true]},\\"$total_ventas\\",0]}},\\"monto_pendiente\\":{\\"$sum\\":{\\"$cond\\":[{\\"$eq\\":[\\"$_id.pagado\\",false]},\\"$total_ventas\\",0]}},\\"productos\\":{\\"$addToSet\\":\\"$_id.producto\\"}}},{\\"$addFields\\":{\\"pct_cobrado\\":{\\"$round\\":[{\\"$multiply\\":[{\\"$divide\\":[\\"$monto_cobrado\\",{\\"$max\\":[\\"$total_facturado\\",1]}]},100]},1]},\\"num_productos\\":{\\"$size\\":\\"$productos\\"}}},{\\"$sort\\":{\\"total_facturado\\":-1}},{\\"$project\\":{\\"_id\\":0,\\"vendedor\\":\\"$_id\\",\\"total_facturado\\":1,\\"total_operaciones\\":1,\\"monto_cobrado\\":1,\\"monto_pendiente\\":1,\\"pct_cobrado\\":1,\\"num_productos\\":1}}]", "operation": "aggregate", "collection": "ventas", "databaseId": "mongo_local", "database_id": "mongo_local", "target_database": "mongo_local"}	admin	admin	\N
0b35464c-882c-441f-a78d-170b822e9e56	MongoDB — Conciliación: detalle de transacciones por fecha de corte	{}	Consulta el detalle de cada transacción en una conciliación específica. Muestra el estado de cada movimiento: OK, sin coincidencia o diferencia de monto.	mongodb	\N	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-05-13 19:44:58.606781	2026-05-13 19:44:58.606781	\N	script	\N	\N	{"modo": "script", "origen": "manual-sql", "pipeline": "[{\\"$match\\":{\\"institutionCode\\":{\\"$eq\\":\\"{{institutionCode}}\\"},\\"cutOffDate\\":\\"{{cutOffDate}}\\"}},{\\"$unwind\\":\\"$details\\"},{\\"$project\\":{\\"_id\\":0,\\"cutOffDate\\":1,\\"detalleId\\":\\"$details.id\\",\\"estado\\":{\\"$cond\\":[{\\"$eq\\":[\\"$details.state\\",1]},\\"OK\\",\\"ERROR\\"]},\\"observacion\\":\\"$details.observation\\",\\"conciliado\\":\\"$details.reconciled\\",\\"uuid\\":\\"$details.uuid\\",\\"valorSwitch\\":\\"$details.transactionValueSwitch\\",\\"tipoSwitch\\":\\"$details.transactionTypeSwitch\\",\\"codigoMovSwitch\\":\\"$details.movementCodeSwitch\\",\\"valorInstitucion\\":\\"$details.transactionValueInstitution\\",\\"tipoInstitucion\\":\\"$details.transactionTypeInstitution\\",\\"codigoMovInstitucion\\":\\"$details.movementCodeInstitution\\"}},{\\"$sort\\":{\\"detalleId\\":1}}]", "operation": "aggregate", "collection": "conciliacion", "databaseId": "mongo_local", "target_database": "mongo_local"}	todos	admin	\N
25f28d88-312c-4877-abb9-bdeb19383296	Oracle — Empleados por rango salarial con RANK (dinámico)	{oracle,dinamico,rownum,rank,empleados,paginacion}	## Empleados filtrados con paginación Oracle	RRHH	Consultas Dinámicas	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-05-13 11:03:38.351067	2026-05-13 12:16:05.588139	## Empleados filtrados con paginación Oracle	script	Subquery con ROWNUM para paginación Oracle. RANK() por departamento. Filtros NVL para valores opcionales.	\N	{"sql": "SELECT *\\nFROM (\\n    SELECT\\n        e.id,\\n        e.nombre || ' ' || e.apellido              AS nombre_completo,\\n        e.cargo,\\n        e.departamento,\\n        e.salario,\\n        CASE e.activo WHEN 1 THEN 'Activo' ELSE 'Inactivo' END  AS estado,\\n        ROUND(MONTHS_BETWEEN(SYSDATE, e.fecha_ingreso) / 12, 1) AS anos_servicio,\\n        RANK() OVER (\\n            PARTITION BY e.departamento\\n            ORDER BY e.salario DESC\\n        )                                           AS ranking_depto\\n    FROM empleados e\\n    WHERE ({{departamento}} IS NULL OR e.departamento = {{departamento}})\\n      AND e.salario >= NVL({{salario_min}}, 0)\\n      AND e.salario <= NVL({{salario_max}}, 999999)\\n      AND ({{solo_activos}} IS NULL OR e.activo = {{solo_activos}})\\n    ORDER BY e.salario DESC\\n)\\nWHERE ROWNUM <= 50", "modo": "script", "origen": "manual-sql", "databaseId": "oracle_local", "database_id": "oracle_local", "target_database": "oracle_local"}	todos	admin	admin
931fca6b-1477-416f-b42f-90c62d1b80a3	MongoDB — Conciliación: errores pendientes por institución	{}	Lista todas las conciliaciones con transacciones pendientes (sin conciliar). Útil para detectar inconsistencias entre el SWITCH y la institución financiera.	mongodb	\N	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-05-13 19:44:58.608173	2026-05-13 19:44:58.608173	\N	script	\N	\N	{"modo": "script", "origen": "manual-sql", "pipeline": "[{\\"$match\\":{\\"pending\\":{\\"$gt\\":0}}},{\\"$project\\":{\\"_id\\":0,\\"institutionCode\\":1,\\"cutOffDate\\":1,\\"transactionCount\\":1,\\"pending\\":1,\\"transactionError\\":1,\\"netAmount\\":1,\\"statusConciliation\\":1}},{\\"$sort\\":{\\"pending\\":-1,\\"institutionCode\\":1}}]", "operation": "aggregate", "collection": "conciliacion", "databaseId": "mongo_local", "target_database": "mongo_local"}	todos	admin	\N
41428192-6c8f-47c6-a3d9-45a5903d8a42	PostgreSQL — Historial de ejecuciones del sistema (dinámico)	{postgres,dinamico,query-history,percent-rank,monitoreo}	Historial dinámico PostgreSQL	Administración	Monitoreo	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-05-13 11:03:38.353597	\N	## Historial de ejecuciones con filtros y percentil de velocidad	script	query_history filtrado por usuario, estado, días y duración. PERCENT_RANK para clasificar velocidad.	[{"tipo": "text", "nombre": "usuario", "etiqueta": "Usuario", "requerido": false, "placeholder": "Todos"}, {"tipo": "text", "nombre": "estado", "etiqueta": "Estado", "requerido": false, "placeholder": "ok o error"}, {"tipo": "number", "nombre": "dias", "etiqueta": "Últimos N días", "requerido": false, "placeholder": "7"}, {"tipo": "number", "nombre": "min_ms", "etiqueta": "Duración mínima (ms)", "requerido": false, "placeholder": "0"}]	{"sql": "SELECT\\n    qh.id,\\n    qh.username,\\n    qh.user_role                                        AS rol,\\n    LEFT(qh.query_text, 100)                            AS consulta,\\n    qh.execution_ms,\\n    qh.row_count,\\n    qh.status,\\n    qh.was_cached,\\n    qh.created_at,\\n    CASE\\n        WHEN qh.execution_ms > 5000 THEN 'LENTA'\\n        WHEN qh.execution_ms > 1000 THEN 'MEDIA'\\n        ELSE 'RAPIDA'\\n    END                                                 AS velocidad,\\n    ROUND((PERCENT_RANK() OVER (ORDER BY qh.execution_ms) * 100)::numeric, 1) AS percentil\\nFROM query_history qh\\nWHERE (NULLIF({{usuario}}, '') IS NULL OR qh.username = {{usuario}})\\n  AND (NULLIF({{estado}}, '') IS NULL OR qh.status = {{estado}})\\n  AND qh.created_at >= NOW() - (COALESCE({{dias}}, 7) || ' days')::INTERVAL\\n  AND qh.execution_ms >= COALESCE({{min_ms}}, 0)\\nORDER BY qh.created_at DESC\\nLIMIT 100", "modo": "script", "origen": "manual-sql", "databaseId": "pg_main", "database_id": "pg_main", "target_database": "pg_main"}	todos	admin	\N
a9671769-3ad1-49e4-8345-c9472c5d1830	MongoDB — Auditoría: logs de actividad por usuario	{}	Consulta el historial de acciones de un usuario en la colección auditoria. Agrupa por tipo de acción, muestra conteos, fechas y IPs únicas.	mongodb	\N	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-05-13 12:45:07.844607	2026-05-13 12:45:07.844607	\N	script	\N	\N	{"modo": "script", "origen": "manual-sql", "pipeline": "[{\\"$match\\":{\\"usuario\\":\\"{{usuario}}\\"}},{\\"$group\\":{\\"_id\\":\\"$accion\\",\\"total_eventos\\":{\\"$sum\\":1},\\"primera_vez\\":{\\"$min\\":\\"$fecha\\"},\\"ultima_vez\\":{\\"$max\\":\\"$fecha\\"},\\"ips\\":{\\"$addToSet\\":\\"$ip\\"}}},{\\"$sort\\":{\\"total_eventos\\":-1}},{\\"$project\\":{\\"_id\\":0,\\"accion\\":\\"$_id\\",\\"total_eventos\\":1,\\"primera_vez\\":1,\\"ultima_vez\\":1,\\"ips\\":1}}]", "operation": "aggregate", "collection": "auditoria", "databaseId": "mongo_local", "target_database": "mongo_local"}	todos	admin	\N
3215d312-bad0-4252-9a06-f3e76765724c	MongoDB — Conciliación: resumen por institución y fecha	{}	Consulta el resumen de conciliaciones por institución. Muestra totales de transacciones, errores, monto neto y estado de conciliación.	mongodb	\N	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-05-13 19:44:58.574585	2026-05-13 19:44:58.574585	\N	script	\N	\N	{"modo": "script", "origen": "manual-sql", "pipeline": "[{\\"$match\\":{\\"institutionCode\\":{\\"$eq\\":\\"{{institutionCode}}\\"}}},{\\"$project\\":{\\"_id\\":0,\\"cutOffDate\\":1,\\"conciliationDate\\":1,\\"transactionCount\\":1,\\"transactionOk\\":1,\\"transactionError\\":1,\\"pending\\":1,\\"reconciled\\":1,\\"debitTotal\\":1,\\"creditTotal\\":1,\\"netAmount\\":1,\\"statusConciliation\\":1,\\"creationUser\\":1}},{\\"$sort\\":{\\"cutOffDate\\":-1}}]", "operation": "aggregate", "collection": "conciliacion", "databaseId": "mongo_local", "target_database": "mongo_local"}	todos	admin	\N
fa177af1-189e-4017-986a-4ebeb41469a8	Problemas con login	{}	# Problemas con login\r\n\r\n## Descripción\r\nEl usuario no puede iniciar sesión.\r\n\r\n## Tipo de error\r\nlogin, autenticación\r\n\r\n## Pasos a seguir\r\n1. Verificar usuario\r\n2. Resetear contraseña\r\n3. Limpiar caché del navegador\r\n\r\n## Solución automática\r\n```bash\r\necho "Reiniciando sesión..."\r\necho "Verificando credenciales..."\r\necho "Login exitoso"	TEST	General	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-05-13 20:23:14.697804	\N	# Problemas con login\r\n\r\n## Descripción\r\nEl usuario no puede iniciar sesión.\r\n\r\n## Tipo de error\r\nlogin, autenticación\r\n\r\n## Pasos a seguir\r\n1. Verificar usuario\r\n2. Resetear contraseña\r\n3. Limpiar caché del navegador\r\n\r\n## Solución automática\r\n```bash\r\necho "Reiniciando sesión..."\r\necho "Verificando credenciales..."\r\necho "Login exitoso"	script		\N	{"modo": "script", "filter": "{\\n  \\"institutionCode\\": 9,\\n  \\"cutOffDate\\": \\"2025-05-10\\",\\n  \\"conciliationDate\\": \\"2025-05-10T14:22:00.000Z\\",\\n  \\"transactionCount\\": 3,\\n  \\"transactionError\\": 1,\\n  \\"transactionOk\\": 2,\\n  \\"pending\\": 1,\\n  \\"reconciled\\": 2,\\n  \\"debitTotal\\": 875.25,\\n  \\"creditTotal\\": 30,\\n  \\"netAmount\\": -845.25,\\n  \\"creationDate\\": \\"2025-05-10T14:22:00.000Z\\",\\n  \\"creationUser\\": \\"\\",\\n  \\"editDate\\": \\"2025-05-10T14:22:00.000Z\\",\\n  \\"editUser\\": \\"\\",\\n  \\"statusConciliation\\": 1,\\n  \\"details\\": [\\n    {\\n      \\"id\\": 1,\\n      \\"state\\": 1,\\n      \\"observation\\": \\"OK\\",\\n      \\"reconciled\\": 1,\\n      \\"description\\": \\"\\",\\n      \\"uuid\\": \\"a2ef34bc-c412-55f0-91b4-01c3e0fd67a1\\",\\n      \\"cutOffNumber\\": \\"\\",\\n      \\"cutOffDate\\": \\"0001-01-01\\",\\n      \\"transactionDateSwitch\\": \\"2025-05-10T09:15:00.000Z\\",\\n      \\"accountTypeIdSwitch\\": \\"Y9K5kRnyEjLdQpRXS1iKaw==\\",\\n      \\"accountNumberSwitch\\": \\"cg2zp/8oKDg8mkr8qnS6oA==\\",\\n      \\"transactionValueSwitch\\": 15,\\n      \\"movementCodeSwitch\\": \\"91000001\\",\\n      \\"transactionTypeSwitch\\": \\"C\\",\\n      \\"institutionCodeSwitch\\": 9,\\n      \\"serviceCodeSwitch\\": 1,\\n      \\"transactionStatusSwitch\\": 1,\\n      \\"reverseMovementCodeSwitch\\": \\"\\",\\n      \\"transactionDateInstitution\\": \\"2025-05-10T09:15:00.000Z\\",\\n      \\"accountTypeIdInstitution\\": \\"Y9K5kRnyEjLdQpRXS1iKaw==\\",\\n      \\"accountNumberInstitution\\": \\"cg2zp/8oKDg8mkr8qnS6oA==\\",\\n      \\"transactionValueInstitution\\": 15,\\n      \\"movementCodeInstitution\\": \\"91000001\\",\\n      \\"transactionTypeInstitution\\": \\"C\\",\\n      \\"institutionCodeInstitution\\": 9,\\n      \\"serviceCodInstitution\\": \\"QR\\",\\n      \\"transactionStatusInstitution\\": 1,\\n      \\"reverseMovementCodeInstitution\\": \\"\\",\\n      \\"editDate\\": \\"2025-05-10T14:22:00.000Z\\"\\n    },\\n    {\\n      \\"id\\": 2,\\n      \\"state\\": 1,\\n      \\"observation\\": \\"OK\\",\\n      \\"reconciled\\": 1,\\n      \\"description\\": \\"\\",\\n      \\"uuid\\": \\"b3fc45cd-d523-66g1-02c5-12d4f1ge78b2\\",\\n      \\"cutOffNumber\\": \\"\\",\\n      \\"cutOffDate\\": \\"0001-01-01\\",\\n      \\"transactionDateSwitch\\": \\"2025-05-10T10:30:00.000Z\\",\\n      \\"accountTypeIdSwitch\\": \\"dKog62Hg2Rb8bs+JYuy0zQ==\\",\\n      \\"accountNumberSwitch\\": \\"3qHuYYMpx18lHSr3Ktsbpw==\\",\\n      \\"transactionValueSwitch\\": 15,\\n      \\"movementCodeSwitch\\": \\"91000002\\",\\n      \\"transactionTypeSwitch\\": \\"C\\",\\n      \\"institutionCodeSwitch\\": 9,\\n      \\"serviceCodeSwitch\\": 1,\\n      \\"transactionStatusSwitch\\": 1,\\n      \\"reverseMovementCodeSwitch\\": \\"\\",\\n      \\"transactionDateInstitution\\": \\"2025-05-10T10:30:00.000Z\\",\\n      \\"accountTypeIdInstitution\\": \\"dKog62Hg2Rb8bs+JYuy0zQ==\\",\\n      \\"accountNumberInstitution\\": \\"3qHuYYMpx18lHSr3Ktsbpw==\\",\\n      \\"transactionValueInstitution\\": 15,\\n      \\"movementCodeInstitution\\": \\"91000002\\",\\n      \\"transactionTypeInstitution\\": \\"C\\",\\n      \\"institutionCodeInstitution\\": 9,\\n      \\"serviceCodInstitution\\": \\"QR\\",\\n      \\"transactionStatusInstitution\\": 1,\\n      \\"reverseMovementCodeInstitution\\": \\"\\",\\n      \\"editDate\\": \\"2025-05-10T14:22:00.000Z\\"\\n    },\\n    {\\n      \\"id\\": 3,\\n      \\"state\\": 0,\\n      \\"observation\\": \\"Sin coincidencia, no existe registro en el SWITCH\\",\\n      \\"reconciled\\": 0,\\n      \\"description\\": \\"\\",\\n      \\"uuid\\": \\"c4gd56de-e634-77h2-13d6-23e5g2hf89c3\\",\\n      \\"cutOffNumber\\": \\"\\",\\n      \\"cutOffDate\\": \\"0001-01-01\\",\\n      \\"transactionDateSwitch\\": null,\\n      \\"accountTypeIdSwitch\\": \\"\\",\\n      \\"accountNumberSwitch\\": \\"\\",\\n      \\"transactionValueSwitch\\": 0,\\n      \\"movementCodeSwitch\\": \\"\\",\\n      \\"transactionTypeSwitch\\": \\"\\",\\n      \\"institutionCodeSwitch\\": 0,\\n      \\"serviceCodeSwitch\\": \\"\\",\\n      \\"transactionStatusSwitch\\": 0,\\n      \\"reverseMovementCodeSwitch\\": \\"\\",\\n      \\"transactionDateInstitution\\": \\"2025-05-10T11:45:00.000Z\\",\\n      \\"accountTypeIdInstitution\\": \\"dKog62Hg2Rb8bs+JYuy0zQ==\\",\\n      \\"accountNumberInstitution\\": \\"3qHuYYMpx18lHSr3Ktsbpw==\\",\\n      \\"transactionValueInstitution\\": 875.25,\\n      \\"movementCodeInstitution\\": \\"91000003\\",\\n      \\"transactionTypeInstitution\\": \\"D\\",\\n      \\"institutionCodeInstitution\\": 9,\\n      \\"serviceCodInstitution\\": \\"QR\\",\\n      \\"transactionStatusInstitution\\": 1,\\n      \\"reverseMovementCodeInstitution\\": \\"\\",\\n      \\"editDate\\": \\"2025-05-10T14:22:00.000Z\\"\\n    }\\n  ]\\n}", "origen": "manual-sql", "operation": "insertone", "collection": "conciliacion", "databaseId": "mongo_local", "target_database": "mongo_local"}	todos	admin	\N
690d2f59-018c-4278-9925-d70e4ce9f8ad	PRUEBA VENTANA ERROR	{}	# Guía de Configuración de Perfil\r\n\r\nEsta es una guía informativa sobre cómo configurar el perfil de usuario.\r\n\r\n## Información General\r\nLos usuarios pueden personalizar su perfil con la siguiente información:\r\n- Nombre completo\r\n- Correo electrónico\r\n- Preferencias de notificación\r\n- Foto de perfil\r\n\r\n## Pasos de Configuración\r\n\r\n### 1. Acceder al Perfil\r\nVe a Configuración > Perfil de Usuario\r\n\r\n### 2. Editar Información\r\n- Completa todos los campos requeridos\r\n- Sube una foto de perfil (opcional)\r\n- Configura tus preferencias\r\n\r\n### 3. Guardar Cambios\r\nHaz clic en "Guardar" para aplicar los cambios.\r\n\r\n## Notas Importantes\r\n- El correo electrónico debe ser único\r\n- La foto debe ser menor a 2MB\r\n- Los cambios se aplican inmediatamente\r\n\r\n---\r\n*Esta es una guía de solo lectura. No contiene instrucciones ejecutables.*	TEST	General	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-05-14 09:04:18.473002	\N	# Guía de Configuración de Perfil\r\n\r\nEsta es una guía informativa sobre cómo configurar el perfil de usuario.\r\n\r\n## Información General\r\nLos usuarios pueden personalizar su perfil con la siguiente información:\r\n- Nombre completo\r\n- Correo electrónico\r\n- Preferencias de notificación\r\n- Foto de perfil\r\n\r\n## Pasos de Configuración\r\n\r\n### 1. Acceder al Perfil\r\nVe a Configuración > Perfil de Usuario\r\n\r\n### 2. Editar Información\r\n- Completa todos los campos requeridos\r\n- Sube una foto de perfil (opcional)\r\n- Configura tus preferencias\r\n\r\n### 3. Guardar Cambios\r\nHaz clic en "Guardar" para aplicar los cambios.\r\n\r\n## Notas Importantes\r\n- El correo electrónico debe ser único\r\n- La foto debe ser menor a 2MB\r\n- Los cambios se aplican inmediatamente\r\n\r\n---\r\n*Esta es una guía de solo lectura. No contiene instrucciones ejecutables.*	script		\N	{"sql": "-- PASO 1: Aumento de 8% a empleados activos del departamento de Tecnología (id=1)\\nUPDATE empleados\\nSET salario = ROUND(salario * 1.08, 2)\\nWHERE departamento_id = 1\\n  AND activo = 1;\\n\\n-- PASO 2: Registrar pedidos de laptops para los nuevos empleados de Ventas\\nINSERT INTO pedidos (empleado_id, producto_id, cantidad, precio_total, estado)\\nSELECT\\n  e.id,\\n  1,               -- Laptop Dell XPS 15 (TECH-001)\\n  1,\\n  1299.99,\\n  'pendiente'\\nFROM empleados e\\nWHERE e.departamento_id = 2   -- Ventas\\n  AND e.activo = 1\\n  AND e.cargo = 'Ejecutiva de Ventas';\\n\\n-- PASO 3: Desactivar productos sin stock\\nUPDATE productos\\nSET activo = 0\\nWHERE stock = 0\\n  AND activo = 1;\\n\\n-- PASO 4: Actualizar presupuesto del depto de Tecnología por los nuevos salarios\\nUPDATE departamentos\\nSET presupuesto = (\\n  SELECT SUM(salario) * 12\\n  FROM empleados\\n  WHERE departamento_id = 1 AND activo = 1\\n)\\nWHERE id = 1", "modo": "script", "origen": "manual-sql", "databaseId": "mysql_test", "target_database": "mysql_test"}	todos	admin	\N
9076fbc7-85c4-44bb-ae9a-c2cb2aed6e8c	Problema de Login - Solución Ejecutable	{}	# Problema de Login - Solución Ejecutable\r\n\r\nEste artículo demuestra cómo resolver problemas de login usando instrucciones dinámicas.\r\n\r\n## Descripción del Problema\r\nLos usuarios no pueden iniciar sesión en la aplicación.\r\n\r\n## Solución\r\n\r\n### Pasos a seguir:\r\n1. Verificar credenciales del usuario\r\n2. Resetear contraseña si es necesario\r\n3. Validar estado de la cuenta\r\n\r\n### Instrucciones Ejecutables\r\n\r\n```yaml\r\nacciones:\r\n  - comando: validar_usuario\r\n    parametros:\r\n      username: "{{username}}"\r\n      email: "{{email}}"\r\n  - comando: reset_password\r\n    parametros:\r\n      username: "{{username}}"\r\n      nueva_password: "temp123"\r\n  - comando: enviar_notificacion\r\n    parametros:\r\n      tipo: "email"\r\n      destinatario: "{{email}}"\r\n      mensaje: "Tu contraseña ha sido reseteada. Nueva contraseña temporal: temp123"\r\n```\r\n\r\n## Información Adicional\r\n- Categoría: Seguridad\r\n- Tags: login, password, reset\r\n- Tipo: Ejecutable	TEST	General	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-05-14 09:10:18.073763	\N	# Problema de Login - Solución Ejecutable\r\n\r\nEste artículo demuestra cómo resolver problemas de login usando instrucciones dinámicas.\r\n\r\n## Descripción del Problema\r\nLos usuarios no pueden iniciar sesión en la aplicación.\r\n\r\n## Solución\r\n\r\n### Pasos a seguir:\r\n1. Verificar credenciales del usuario\r\n2. Resetear contraseña si es necesario\r\n3. Validar estado de la cuenta\r\n\r\n### Instrucciones Ejecutables\r\n\r\n```yaml\r\nacciones:\r\n  - comando: validar_usuario\r\n    parametros:\r\n      username: "{{username}}"\r\n      email: "{{email}}"\r\n  - comando: reset_password\r\n    parametros:\r\n      username: "{{username}}"\r\n      nueva_password: "temp123"\r\n  - comando: enviar_notificacion\r\n    parametros:\r\n      tipo: "email"\r\n      destinatario: "{{email}}"\r\n      mensaje: "Tu contraseña ha sido reseteada. Nueva contraseña temporal: temp123"\r\n```\r\n\r\n## Información Adicional\r\n- Categoría: Seguridad\r\n- Tags: login, password, reset\r\n- Tipo: Ejecutable	script		\N	{"sql": "WITH stats_depto AS (\\n    SELECT\\n        d.id                                        AS depto_id,\\n        d.nombre                                    AS departamento,\\n        COUNT(e.id)                                 AS total_empleados,\\n        AVG(e.salario)                              AS salario_promedio,\\n        MAX(e.salario)                              AS salario_max,\\n        MIN(e.salario)                              AS salario_min,\\n        SUM(e.salario)                              AS masa_salarial,\\n        d.presupuesto,\\n        ROUND(SUM(e.salario) / d.presupuesto * 100, 2) AS uso_presupuesto_pct\\n    FROM empleados e\\n    JOIN departamentos d ON e.departamento_id = d.id\\n    WHERE e.activo = 1\\n    GROUP BY d.id, d.nombre, d.presupuesto\\n),\\nranking_empleados AS (\\n    SELECT\\n        e.id,\\n        CONCAT(e.nombre, ' ', e.apellido)           AS nombre_completo,\\n        e.cargo,\\n        e.salario,\\n        e.fecha_ingreso,\\n        d.nombre                                    AS departamento,\\n        DATEDIFF(NOW(), e.fecha_ingreso) / 365      AS anos_empresa,\\n        RANK() OVER (\\n            PARTITION BY e.departamento_id\\n            ORDER BY e.salario DESC\\n        )                                           AS ranking_depto\\n    FROM empleados e\\n    JOIN departamentos d ON e.departamento_id = d.id\\n    WHERE e.activo = 1\\n)\\nSELECT\\n    r.nombre_completo,\\n    r.cargo,\\n    r.departamento,\\n    r.salario,\\n    ROUND(r.anos_empresa, 1)                        AS anos_empresa,\\n    r.ranking_depto,\\n    ROUND(s.salario_promedio, 2)                    AS promedio_depto,\\n    ROUND(r.salario - s.salario_promedio, 2)        AS diferencia_vs_promedio,\\n    ROUND(r.salario / s.salario_promedio * 100, 1)  AS pct_vs_promedio,\\n    s.uso_presupuesto_pct\\nFROM ranking_empleados r\\nJOIN stats_depto s ON r.departamento = s.departamento\\nORDER BY r.departamento, r.ranking_depto", "modo": "script", "origen": "manual-sql", "databaseId": "mysql_test", "target_database": "mysql_test"}	todos	admin	\N
9515ae4e-df33-42ef-bf7d-58f4f287a34e	Problemas con login	{}	# Problemas con login\r\n\r\n## Descripción\r\nEl usuario no puede iniciar sesión.\r\n\r\n## Tipo de error\r\nlogin, autenticación\r\n\r\n## Pasos a seguir\r\n1. Verificar usuario\r\n2. Resetear contraseña\r\n3. Limpiar caché del navegador\r\n\r\n## Solución automática\r\n```bash\r\necho "Reiniciando sesión..."\r\necho "Verificando credenciales..."\r\necho "Login exitoso"	TRANSFERENCIAS	Otros temas	\N	\N	747bd085-6444-49a4-9919-64660808316c	2026-04-02 10:14:20.518856	2026-04-02 12:54:42.932817	# Problemas con login\r\n\r\n## Descripción\r\nEl usuario no puede iniciar sesión.\r\n\r\n## Tipo de error\r\nlogin, autenticación\r\n\r\n## Pasos a seguir\r\n1. Verificar usuario\r\n2. Resetear contraseña\r\n3. Limpiar caché del navegador\r\n\r\n## Solución automática\r\n```bash\r\necho "Reiniciando sesión..."\r\necho "Verificando credenciales..."\r\necho "Login exitoso"	lectura		\N	\N	todos	\N	\N
\.


--
-- Data for Name: login_audit; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.login_audit (id, username, rol, ip, user_agent, estado, detalle, fecha) FROM stdin;
1	admin	admin	::1	node	ok	\N	2026-05-12 23:35:56.417173
2	user	user	::1	node	ok	\N	2026-05-12 23:36:57.412012
3	213	\N	::1	node	fail	Usuario no encontrado	2026-05-12 23:37:11.660443
4	admin 	\N	::1	node	fail	Usuario no encontrado	2026-05-12 23:39:17.427227
5	admin	admin	::1	node	ok	\N	2026-05-12 23:39:25.281623
6	user	user	::1	node	ok	\N	2026-05-12 23:41:36.659438
7	admin	admin	::1	node	ok	\N	2026-05-12 23:56:46.386428
8	admin	admin	::1	node	ok	\N	2026-05-13 09:49:13.870763
9	admin	admin	::1	node	ok	\N	2026-05-13 10:03:02.811463
10	admin	admin	::1	node	logout	\N	2026-05-13 10:21:36.767384
11	admin	admin	::1	node	ok	\N	2026-05-13 10:37:25.668217
12	admin	admin	::1	Mozilla/5.0 (Windows NT; Windows NT 10.0; es-EC) WindowsPowerShell/5.1.26100.8115	fail	Contraseña incorrecta	2026-05-13 10:39:05.151232
13	admin	admin	::1	Mozilla/5.0 (Windows NT; Windows NT 10.0; es-EC) WindowsPowerShell/5.1.26100.8115	fail	Contraseña incorrecta	2026-05-13 10:39:41.090329
14	admin	admin	::1	Mozilla/5.0 (Windows NT; Windows NT 10.0; es-EC) WindowsPowerShell/5.1.26100.8115	fail	Contraseña incorrecta	2026-05-13 10:40:09.794157
15	admin	admin	::1	node	logout	\N	2026-05-13 10:45:09.306939
16	admin	admin	::1	node	ok	\N	2026-05-13 10:45:10.297142
17	admin	admin	::1	curl/8.16.0	fail	Contraseña incorrecta	2026-05-13 11:29:43.986765
18	admin	admin	::1	curl/8.16.0	fail	Contraseña incorrecta	2026-05-13 11:29:59.736939
19	admin	admin	::1	node	logout	\N	2026-05-13 12:28:09.943639
20	user	user	::1	node	ok	\N	2026-05-13 12:28:14.165083
21	user	user	::1	node	logout	\N	2026-05-13 12:28:33.971812
22	admin	admin	::1	node	ok	\N	2026-05-13 12:28:35.498761
23	admin	admin	::1	node	logout	\N	2026-05-13 12:28:50.781959
24	user	user	::1	node	ok	\N	2026-05-13 12:28:54.909027
25	user	user	::1	node	logout	\N	2026-05-13 12:32:26.434823
26	admin	admin	::1	node	ok	\N	2026-05-13 12:32:32.580656
27	admin	admin	::1	node	ok	\N	2026-05-13 19:38:53.7972
28	admin	admin	::1	node	ok	\N	2026-05-14 08:24:54.222208
29	admin	admin	::1	node	ok	\N	2026-05-14 11:25:17.806167
\.


--
-- Data for Name: logs; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.logs (id, accion, usuario, detalle, created_at, user_id) FROM stdin;
1	smoke_test	user	registry smoke	2026-04-08 09:45:25.803231-05	\N
3	LOGIN	test_user	Usuario inició sesión [procesado] [procesado]	2026-04-09 10:40:36.53255-05	f074b61c-16e6-4a8e-a614-30946b4b056b
4	LOGIN	admin	Usuario inició sesión [procesado] [procesado] [procesado] [procesado]	2026-04-16 11:48:53.756854-05	747bd085-6444-49a4-9919-64660808316c
\.


--
-- Data for Name: query_history; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.query_history (id, username, user_role, query_text, generated_sql, execution_ms, row_count, was_cached, status, created_at, query_params, placeholder_order) FROM stdin;
294	anonymous	admin	SELECT NOW() AS ahora, '2024-01-15'::date AS fecha, 42 AS n	SELECT NOW() AS ahora, '2024-01-15'::date AS fecha, 42 AS n	2	1	f	ok	2026-05-14 09:37:18.042335-05	[]	[]
319	anonymous	admin	SELECT nombre, salario, departamento,\n      LAG(salario) OVER (PARTITION BY departamento ORDER BY salario) AS sal_anterior,\n      LEAD(salario) OVER (PARTITION BY departamento ORDER BY salario) AS sal_siguiente,\n      ROUND(RATIO_TO_REPORT(salario) OVER (PARTITION BY departamento) * 100, 2) AS pct_del_depto,\n      SUM(salario) OVER (PARTITION BY departamento ORDER BY salario ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS acumulado\n    FROM empleados ORDER BY departamento, salario	SELECT nombre, salario, departamento,\n      LAG(salario) OVER (PARTITION BY departamento ORDER BY salario) AS sal_anterior,\n      LEAD(salario) OVER (PARTITION BY departamento ORDER BY salario) AS sal_siguiente,\n      ROUND(RATIO_TO_REPORT(salario) OVER (PARTITION BY departamento) * 100, 2) AS pct_del_depto,\n      SUM(salario) OVER (PARTITION BY departamento ORDER BY salario ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS acumulado\n    FROM empleados ORDER BY departamento, salario	17	7	f	ok	2026-05-14 09:44:47.945189-05	[]	[]
320	anonymous	admin	SELECT * FROM (\n      SELECT departamento, salario FROM empleados\n    ) PIVOT (\n      COUNT(*) AS cnt, AVG(salario) AS prom\n      FOR departamento IN ('TI' AS ti, 'Ventas' AS ventas, 'RRHH' AS rrhh, 'Gerencia' AS gerencia)\n    )	SELECT * FROM (\n      SELECT departamento, salario FROM empleados\n    ) PIVOT (\n      COUNT(*) AS cnt, AVG(salario) AS prom\n      FOR departamento IN ('TI' AS ti, 'Ventas' AS ventas, 'RRHH' AS rrhh, 'Gerencia' AS gerencia)\n    )	0	0	f	ok	2026-05-14 09:44:47.969654-05	[]	[]
321	anonymous	admin	WITH ventas_agg AS (\n      SELECT producto_id, COUNT(*) AS num_ventas, SUM(total) AS ingresos, SUM(cantidad) AS unidades\n      FROM ventas GROUP BY producto_id\n    ), con_ranking AS (\n      SELECT va.*, p.nombre, p.categoria, p.precio,\n        RANK() OVER (ORDER BY va.ingresos DESC) AS rank_global,\n        ROUND(va.ingresos / NULLIF(va.unidades,0), 2) AS precio_promedio_venta\n      FROM ventas_agg va JOIN productos p ON p.id = va.producto_id\n    )\n    SELECT rank_global, nombre, categoria, num_ventas, unidades, ingresos, precio_promedio_venta,\n      CASE WHEN precio_promedio_venta > precio THEN 'sobre lista' ELSE 'bajo lista' END AS comparacion\n    FROM con_ranking ORDER BY rank_global	WITH ventas_agg AS (\n      SELECT producto_id, COUNT(*) AS num_ventas, SUM(total) AS ingresos, SUM(cantidad) AS unidades\n      FROM ventas GROUP BY producto_id\n    ), con_ranking AS (\n      SELECT va.*, p.nombre, p.categoria, p.precio,\n        RANK() OVER (ORDER BY va.ingresos DESC) AS rank_global,\n        ROUND(va.ingresos / NULLIF(va.unidades,0), 2) AS precio_promedio_venta\n      FROM ventas_agg va JOIN productos p ON p.id = va.producto_id\n    )\n    SELECT rank_global, nombre, categoria, num_ventas, unidades, ingresos, precio_promedio_venta,\n      CASE WHEN precio_promedio_venta > precio THEN 'sobre lista' ELSE 'bajo lista' END AS comparacion\n    FROM con_ranking ORDER BY rank_global	24	4	f	ok	2026-05-14 09:44:47.997397-05	[]	[]
322	anonymous	admin	{"collection":"empleados","aggregate":[{"$facet":{"por_departamento":[{"$group":{"_id":"$departamento","total":{"$sum":1},"salario_prom":{"$avg":"$salario"}}}],"por_estado":[{"$group":{"_id":"$activo","total":{"$sum":1}}}],"rango_salarial":[{"$bucket":{"groupBy":"$salario","boundaries":[0,3000,4000,5000,10000],"default":"otros","output":{"count":{"$sum":1},"empleados":{"$push":"$nombre"}}}}]}}]}	{"collection":"empleados","aggregate":[{"$facet":{"por_departamento":[{"$group":{"_id":"$departamento","total":{"$sum":1},"salario_prom":{"$avg":"$salario"}}}],"por_estado":[{"$group":{"_id":"$activo","total":{"$sum":1}}}],"rango_salarial":[{"$bucket":{"groupBy":"$salario","boundaries":[0,3000,4000,5000,10000],"default":"otros","output":{"count":{"$sum":1},"empleados":{"$push":"$nombre"}}}}]}}]}	13	1	f	ok	2026-05-14 09:44:48.013752-05	[]	[]
323	anonymous	admin	{"collection":"ventas","aggregate":[{"$lookup":{"from":"productos","localField":"producto_codigo","foreignField":"codigo","as":"producto"}},{"$unwind":"$producto"},{"$group":{"_id":{"categoria":"$producto.categoria","vendedor":"$vendedor"},"ingresos":{"$sum":"$total"},"unidades":{"$sum":"$cantidad"},"ventas":{"$sum":1}}},{"$sort":{"ingresos":-1}},{"$project":{"_id":0,"categoria":"$_id.categoria","vendedor":"$_id.vendedor","ingresos":1,"unidades":1,"ventas":1,"ticket_promedio":{"$round":[{"$divide":["$ingresos","$ventas"]},2]}}}]}	{"collection":"ventas","aggregate":[{"$lookup":{"from":"productos","localField":"producto_codigo","foreignField":"codigo","as":"producto"}},{"$unwind":"$producto"},{"$group":{"_id":{"categoria":"$producto.categoria","vendedor":"$vendedor"},"ingresos":{"$sum":"$total"},"unidades":{"$sum":"$cantidad"},"ventas":{"$sum":1}}},{"$sort":{"ingresos":-1}},{"$project":{"_id":0,"categoria":"$_id.categoria","vendedor":"$_id.vendedor","ingresos":1,"unidades":1,"ventas":1,"ticket_promedio":{"$round":[{"$divide":["$ingresos","$ventas"]},2]}}}]}	15	5	f	ok	2026-05-14 09:44:48.032767-05	[]	[]
324	anonymous	admin	{"collection":"conciliacion","aggregate":[{"$unwind":"$details"},{"$group":{"_id":"$details.transactionTypeSwitch","total_transacciones":{"$sum":1},"valor_total":{"$sum":"$details.transactionValueSwitch"},"reconciliados":{"$sum":"$details.reconciled"}}},{"$sort":{"valor_total":-1}}]}	{"collection":"conciliacion","aggregate":[{"$unwind":"$details"},{"$group":{"_id":"$details.transactionTypeSwitch","total_transacciones":{"$sum":1},"valor_total":{"$sum":"$details.transactionValueSwitch"},"reconciliados":{"$sum":"$details.reconciled"}}},{"$sort":{"valor_total":-1}}]}	4	3	f	ok	2026-05-14 09:44:48.040473-05	[]	[]
325	anonymous	admin	{"collection":"empleados","aggregate":[{"$addFields":{"categoria_salarial":{"$cond":[{"$gte":["$salario",4000]},"alto",{"$cond":[{"$gte":["$salario",3000]},"medio","bajo"]}]}}},{"$group":{"_id":{"depto":"$departamento","categoria":"$categoria_salarial"},"count":{"$sum":1},"nombres":{"$push":"$nombre"}}},{"$sort":{"count":-1}}]}	{"collection":"empleados","aggregate":[{"$addFields":{"categoria_salarial":{"$cond":[{"$gte":["$salario",4000]},"alto",{"$cond":[{"$gte":["$salario",3000]},"medio","bajo"]}]}}},{"$group":{"_id":{"depto":"$departamento","categoria":"$categoria_salarial"},"count":{"$sum":1},"nombres":{"$push":"$nombre"}}},{"$sort":{"count":-1}}]}	4	4	f	ok	2026-05-14 09:44:48.048213-05	[]	[]
337	anonymous	admin	{"collection":"empleados","aggregate":[{"$facet":{"por_departamento":[{"$group":{"_id":"$departamento","total":{"$sum":1},"salario_prom":{"$avg":"$salario"}}}],"por_estado":[{"$group":{"_id":"$activo","total":{"$sum":1}}}],"rango_salarial":[{"$bucket":{"groupBy":"$salario","boundaries":[0,3000,4000,5000,10000],"default":"otros","output":{"count":{"$sum":1},"empleados":{"$push":"$nombre"}}}}]}}]}	{"collection":"empleados","aggregate":[{"$facet":{"por_departamento":[{"$group":{"_id":"$departamento","total":{"$sum":1},"salario_prom":{"$avg":"$salario"}}}],"por_estado":[{"$group":{"_id":"$activo","total":{"$sum":1}}}],"rango_salarial":[{"$bucket":{"groupBy":"$salario","boundaries":[0,3000,4000,5000,10000],"default":"otros","output":{"count":{"$sum":1},"empleados":{"$push":"$nombre"}}}}]}}]}	6	1	f	ok	2026-05-14 09:47:43.811802-05	[]	[]
341	anonymous	admin	WITH RECURSIVE serie(n) AS (\n      SELECT 1 UNION ALL SELECT n+1 FROM serie WHERE n < 10\n    ) SELECT array_agg(n) AS numeros, SUM(n) AS suma, AVG(n)::numeric(10,2) AS promedio FROM serie	WITH RECURSIVE serie(n) AS (\n      SELECT 1 UNION ALL SELECT n+1 FROM serie WHERE n < 10\n    ) SELECT array_agg(n) AS numeros, SUM(n) AS suma, AVG(n)::numeric(10,2) AS promedio FROM serie	2	1	f	ok	2026-05-14 09:47:49.896695-05	[]	[]
202	anonymous	user	usuarios con logs "admin"	-- db_users\nSELECT "t1"."id" AS "users_id", "t1"."username" AS "users_username", "t1"."role" AS "users_role" FROM "users" "t1" WHERE LOWER(CAST("t1"."username" AS TEXT)) LIKE LOWER($1) LIMIT 20\n\n-- db_logs\nSELECT "t1"."id" AS "merge_key", "t1"."id" AS "logs_id", "t1"."created_at" AS "logs_created_at", "t1"."usuario" AS "logs_usuario", "t1"."accion" AS "logs_accion" FROM "logs" "t1" LIMIT 500	1	0	f	ok	2026-04-27 11:34:07.016692-05	\N	\N
205	anonymous	admin	SELECT author_username, COUNT(*) AS total_comments FROM SYSTEM.COMMENTS GROUP BY author_username ORDER BY total_comments DESC	SELECT author_username, COUNT(*) AS total_comments FROM SYSTEM.COMMENTS GROUP BY author_username ORDER BY total_comments DESC	29	0	f	error	2026-04-28 16:57:32.509029-05	[]	\N
207	anonymous	admin	SELECT author_username, COUNT(*) AS total_comments FROM SYSTEM.COMMENTS GROUP BY author_username ORDER BY total_comments DESC	SELECT author_username, COUNT(*) AS total_comments FROM SYSTEM.COMMENTS GROUP BY author_username ORDER BY total_comments DESC	116	1	f	ok	2026-04-28 17:03:59.578546-05	[]	[]
208	anonymous	admin	\N	SELECT author_username, COUNT(*) AS total_comments FROM SYSTEM.COMMENTS GROUP BY author_username ORDER BY total_comments DESC	3	1	f	ok	2026-04-28 17:04:01.241175-05	\N	\N
209	anonymous	admin	SELECT author_username, COUNT(*) AS total_comments FROM SYSTEM.COMMENTS GROUP BY author_username ORDER BY total_comments DESC	SELECT author_username, COUNT(*) AS total_comments FROM SYSTEM.COMMENTS GROUP BY author_username ORDER BY total_comments DESC	2	1	f	ok	2026-04-28 17:04:03.02533-05	[]	[]
210	anonymous	admin	\N	SELECT author_username, COUNT(*) AS total_comments FROM SYSTEM.COMMENTS GROUP BY author_username ORDER BY total_comments DESC	3	1	f	ok	2026-04-28 17:04:04.663692-05	\N	\N
214	admin	admin	\N	SELECT "t1"."id" AS "comments_id", "t1"."created_at" AS "comments_created_at", "t1"."article_id" AS "comments_article_id", "t1"."parent_id" AS "comments_parent_id", "t1"."author_username" AS "comments_author_username" FROM "comments" "t1" WHERE "t1"."author_username"::text ILIKE '%oracle%' ORDER BY "t1"."id" DESC LIMIT 80 OFFSET 0	0	0	f	error	2026-04-29 06:20:29.963827-05	\N	\N
216	admin	admin	\N	SELECT "t1"."id" AS "query_history_id", "t1"."username" AS "query_history_username", "t1"."created_at" AS "query_history_created_at", "t1"."user_role" AS "query_history_user_role", "t1"."query_text" AS "query_history_query_text" FROM "query_history" "t1" WHERE ("t1"."status"::text ILIKE '%oracle%') OR ("t1"."username"::text ILIKE '%oracle%') ORDER BY "t1"."id" DESC LIMIT 80 OFFSET 0	0	0	f	error	2026-04-29 07:10:11.109671-05	\N	\N
218	admin	admin	\N	SELECT * FROM query_history ORDER BY id	27	3	f	ok	2026-04-29 07:15:30.944373-05	\N	\N
220	admin	admin	\N	SELECT * FROM query_history ORDER BY created_at DESC	15	3	f	ok	2026-04-29 08:21:08.70945-05	\N	\N
222	admin	admin	execute article 8e598568-5963-4e03-965a-6cfaae251630	WITH stats_depto AS (\n    SELECT\n        d.id                                        AS depto_id,\n        d.nombre                                    AS departamento,\n        COUNT(e.id)                                 AS total_empleados,\n        AVG(e.salario)                              AS salario_promedio,\n        MAX(e.salario)                              AS salario_max,\n        MIN(e.salario)                              AS salario_min,\n        SUM(e.salario)                              AS masa_salarial,\n        d.presupuesto,\n        ROUND(SUM(e.salario) / d.presupuesto * 100, 2) AS uso_presupuesto_pct\n    FROM empleados e\n    JOIN departamentos d ON e.departamento_id = d.id\n    WHERE e.activo = 1\n    GROUP BY d.id, d.nombre, d.presupuesto\n),\nranking_empleados AS (\n    SELECT\n        e.id,\n        CONCAT(e.nombre, ' ', e.apellido)           AS nombre_completo,\n        e.cargo,\n        e.salario,\n        e.fecha_ingreso,\n        d.nombre                                    AS departamento,\n        DATEDIFF(NOW(), e.fecha_ingreso) / 365      AS anos_empresa,\n        RANK() OVER (\n            PARTITION BY e.departamento_id\n            ORDER BY e.salario DESC\n        )                                           AS ranking_depto\n    FROM empleados e\n    JOIN departamentos d ON e.departamento_id = d.id\n    WHERE e.activo = 1\n)\nSELECT\n    r.nombre_completo,\n    r.cargo,\n    r.departamento,\n    r.salario,\n    ROUND(r.anos_empresa, 1)                        AS anos_empresa,\n    r.ranking_depto,\n    ROUND(s.salario_promedio, 2)                    AS promedio_depto,\n    ROUND(r.salario - s.salario_promedio, 2)        AS diferencia_vs_promedio,\n    ROUND(r.salario / s.salario_promedio * 100, 1)  AS pct_vs_promedio,\n    s.uso_presupuesto_pct\nFROM ranking_empleados r\nJOIN stats_depto s ON r.departamento = s.departamento\nORDER BY r.departamento, r.ranking_depto	25	0	f	ok	2026-05-13 11:52:55.647342-05	[]	[]
224	anonymous	admin	execute article 8e598568-5963-4e03-995a-6cfaae251630	workflow execution	65	0	f	error	2026-05-13 11:55:20.935506-05	\N	\N
226	anonymous	admin	execute article 8e598568-5963-4e03-965a-6cfaae251630	WITH stats_depto AS (\n    SELECT\n        d.id                                        AS depto_id,\n        d.nombre                                    AS departamento,\n        COUNT(e.id)                                 AS total_empleados,\n        AVG(e.salario)                              AS salario_promedio,\n        MAX(e.salario)                              AS salario_max,\n        MIN(e.salario)                              AS salario_min,\n        SUM(e.salario)                              AS masa_salarial,\n        d.presupuesto,\n        ROUND(SUM(e.salario) / d.presupuesto * 100, 2) AS uso_presupuesto_pct\n    FROM empleados e\n    JOIN departamentos d ON e.departamento_id = d.id\n    WHERE e.activo = 1\n    GROUP BY d.id, d.nombre, d.presupuesto\n),\nranking_empleados AS (\n    SELECT\n        e.id,\n        CONCAT(e.nombre, ' ', e.apellido)           AS nombre_completo,\n        e.cargo,\n        e.salario,\n        e.fecha_ingreso,\n        d.nombre                                    AS departamento,\n        DATEDIFF(NOW(), e.fecha_ingreso) / 365      AS anos_empresa,\n        RANK() OVER (\n            PARTITION BY e.departamento_id\n            ORDER BY e.salario DESC\n        )                                           AS ranking_depto\n    FROM empleados e\n    JOIN departamentos d ON e.departamento_id = d.id\n    WHERE e.activo = 1\n)\nSELECT\n    r.nombre_completo,\n    r.cargo,\n    r.departamento,\n    r.salario,\n    ROUND(r.anos_empresa, 1)                        AS anos_empresa,\n    r.ranking_depto,\n    ROUND(s.salario_promedio, 2)                    AS promedio_depto,\n    ROUND(r.salario - s.salario_promedio, 2)        AS diferencia_vs_promedio,\n    ROUND(r.salario / s.salario_promedio * 100, 1)  AS pct_vs_promedio,\n    s.uso_presupuesto_pct\nFROM ranking_empleados r\nJOIN stats_depto s ON r.departamento = s.departamento\nORDER BY r.departamento, r.ranking_depto	20	0	f	ok	2026-05-13 11:55:35.027486-05	[]	[]
227	anonymous	admin	execute article 8e598568-5963-4e03-965a-6cfaae251630	workflow execution	124	0	f	error	2026-05-13 12:01:13.938081-05	\N	\N
229	anonymous	admin	execute article 41428192-6c8f-47c6-a3d9-45a5903d8a42	workflow execution	73	0	f	error	2026-05-13 12:10:33.573129-05	\N	\N
236	anonymous	admin	execute article bed57404-2a43-47dd-8bbc-aa8d555e99f9	workflow execution	42	0	f	error	2026-05-13 12:11:21.029091-05	\N	\N
242	anonymous	admin	execute article 1ff6addb-b02c-47eb-af86-43825bb2dbba	workflow execution	2	0	f	error	2026-05-13 12:14:05.316493-05	\N	\N
201	anonymous	admin	SELECT id, username, role FROM users LIMIT 1	SELECT id, username, role FROM users LIMIT 1	4	1	f	ok	2026-04-27 11:32:41.812306-05	[]	[]
203	anonymous	user	usuarios con logs	-- db_users\nSELECT "t1"."id" AS "users_id", "t1"."username" AS "users_username", "t1"."role" AS "users_role" FROM "users" "t1" LIMIT 20\n\n-- db_logs\nSELECT "t1"."id" AS "merge_key", "t1"."id" AS "logs_id", "t1"."created_at" AS "logs_created_at", "t1"."usuario" AS "logs_usuario", "t1"."accion" AS "logs_accion" FROM "logs" "t1" LIMIT 500	2	0	f	ok	2026-04-27 11:38:25.249967-05	\N	\N
204	anonymous	admin	SELECT author_username, COUNT(*) AS total_comments FROM SYSTEM.COMMENTS GROUP BY author_username ORDER BY total_comments DESC	SELECT author_username, COUNT(*) AS total_comments FROM SYSTEM.COMMENTS GROUP BY author_username ORDER BY total_comments DESC	228	0	f	error	2026-04-28 16:56:11.486781-05	[]	\N
206	anonymous	admin	SELECT author_username, COUNT(*) AS total_comments FROM SYSTEM.COMMENTS GROUP BY author_username ORDER BY total_comments DESC	SELECT author_username, COUNT(*) AS total_comments FROM SYSTEM.COMMENTS GROUP BY author_username ORDER BY total_comments DESC	201	0	f	error	2026-04-28 16:59:24.796928-05	[]	\N
211	admin	admin	\N	SELECT "t1"."id" AS "query_history_id", "t1"."username" AS "query_history_username", "t1"."created_at" AS "query_history_created_at", "t1"."user_role" AS "query_history_user_role", "t1"."query_text" AS "query_history_query_text" FROM "query_history" "t1" WHERE ("t1"."status"::text ILIKE '%oracle%') OR ("t1"."username"::text ILIKE '%oracle%') ORDER BY "t1"."id" DESC LIMIT 80 OFFSET 0	0	0	f	error	2026-04-29 00:59:17.94013-05	\N	\N
212	admin	admin	\N	SELECT "t1"."id" AS "query_history_id", "t1"."username" AS "query_history_username", "t1"."created_at" AS "query_history_created_at", "t1"."user_role" AS "query_history_user_role", "t1"."query_text" AS "query_history_query_text" FROM "query_history" "t1" WHERE ("t1"."status"::text ILIKE '%oracle%') OR ("t1"."username"::text ILIKE '%oracle%') ORDER BY "t1"."id" DESC LIMIT 80 OFFSET 0	0	0	f	error	2026-04-29 06:19:31.26663-05	\N	\N
213	admin	admin	\N	SELECT "t1"."id" AS "users_id", "t1"."username" AS "users_username", "t1"."role" AS "users_role", "t1"."activo" AS "users_activo", "t1"."created_at" AS "users_created_at" FROM "users" "t1" WHERE ("t1"."role"::text ILIKE '%oracle%') OR ("t1"."username"::text ILIKE '%oracle%') ORDER BY "t1"."id" DESC LIMIT 80 OFFSET 0	0	0	f	error	2026-04-29 06:20:14.664402-05	\N	\N
215	admin	admin	\N	SELECT "t1"."id" AS "query_history_id", "t1"."username" AS "query_history_username", "t1"."created_at" AS "query_history_created_at", "t1"."user_role" AS "query_history_user_role", "t1"."query_text" AS "query_history_query_text" FROM "query_history" "t1" WHERE "t1"."status"::text ILIKE '%dato%' AND "t1"."username"::text ILIKE '%oracle%' ORDER BY "t1"."id" DESC LIMIT 80 OFFSET 0	0	0	f	error	2026-04-29 06:34:14.95596-05	\N	\N
217	admin	admin	\N	SELECT "t1"."id" AS "query_history_id", "t1"."username" AS "query_history_username", "t1"."created_at" AS "query_history_created_at", "t1"."user_role" AS "query_history_user_role", "t1"."query_text" AS "query_history_query_text" FROM "query_history" "t1" WHERE ("t1"."status"::text ILIKE '%oracle%') OR ("t1"."username"::text ILIKE '%oracle%') ORDER BY "t1"."id" DESC LIMIT 80 OFFSET 0	0	0	f	error	2026-04-29 07:12:38.772273-05	\N	\N
219	admin	admin	\N	SELECT * FROM query_history ORDER BY created_at DESC	75	3	f	ok	2026-04-29 08:11:54.554579-05	\N	\N
221	anonymous	admin	execute article 8e598568-5963-4e03-965a-6cfaae251630	workflow execution	149	0	f	error	2026-05-13 11:47:03.266753-05	\N	\N
223	admin	admin	execute article 21961a02-af0e-4237-abb1-dd0e89eccc7f	workflow execution	4	0	f	error	2026-05-13 11:53:01.631407-05	\N	\N
225	anonymous	admin	execute article 21961a02-af0e-4237-abb1-dd0e89eccc7f	{"collection":"ventas","aggregate":[{"$addFields":{"mes":{"$month":"$fecha"},"anio":{"$year":"$fecha"}}},{"$group":{"_id":{"vendedor":"$vendedor","producto":"$producto_codigo","pagado":"$pagado"},"total_ventas":{"$sum":"$total"},"total_unidades":{"$sum":"$cantidad"},"num_operaciones":{"$sum":1},"ticket_promedio":{"$avg":"$total"},"venta_max":{"$max":"$total"}}},{"$group":{"_id":"$_id.vendedor","total_facturado":{"$sum":"$total_ventas"},"total_operaciones":{"$sum":"$num_operaciones"},"monto_cobrado":{"$sum":{"$cond":[{"$eq":["$_id.pagado",true]},"$total_ventas",0]}},"monto_pendiente":{"$sum":{"$cond":[{"$eq":["$_id.pagado",false]},"$total_ventas",0]}},"productos":{"$addToSet":"$_id.producto"}}},{"$addFields":{"pct_cobrado":{"$round":[{"$multiply":[{"$divide":["$monto_cobrado",{"$max":["$total_facturado",1]}]},100]},1]},"num_productos":{"$size":"$productos"}}},{"$sort":{"total_facturado":-1}},{"$project":{"_id":0,"vendedor":"$_id","total_facturado":1,"total_operaciones":1,"monto_cobrado":1,"monto_pendiente":1,"pct_cobrado":1,"num_productos":1}}]}	92	0	f	ok	2026-05-13 11:55:21.158107-05	[]	[]
228	anonymous	admin	execute article 8e598568-5963-4e03-965a-6cfaae251630	WITH stats_depto AS (\n    SELECT\n        d.id                                        AS depto_id,\n        d.nombre                                    AS departamento,\n        COUNT(e.id)                                 AS total_empleados,\n        AVG(e.salario)                              AS salario_promedio,\n        MAX(e.salario)                              AS salario_max,\n        MIN(e.salario)                              AS salario_min,\n        SUM(e.salario)                              AS masa_salarial,\n        d.presupuesto,\n        ROUND(SUM(e.salario) / d.presupuesto * 100, 2) AS uso_presupuesto_pct\n    FROM empleados e\n    JOIN departamentos d ON e.departamento_id = d.id\n    WHERE e.activo = 1\n    GROUP BY d.id, d.nombre, d.presupuesto\n),\nranking_empleados AS (\n    SELECT\n        e.id,\n        CONCAT(e.nombre, ' ', e.apellido)           AS nombre_completo,\n        e.cargo,\n        e.salario,\n        e.fecha_ingreso,\n        d.nombre                                    AS departamento,\n        DATEDIFF(NOW(), e.fecha_ingreso) / 365      AS anos_empresa,\n        RANK() OVER (\n            PARTITION BY e.departamento_id\n            ORDER BY e.salario DESC\n        )                                           AS ranking_depto\n    FROM empleados e\n    JOIN departamentos d ON e.departamento_id = d.id\n    WHERE e.activo = 1\n)\nSELECT\n    r.nombre_completo,\n    r.cargo,\n    r.departamento,\n    r.salario,\n    ROUND(r.anos_empresa, 1)                        AS anos_empresa,\n    r.ranking_depto,\n    ROUND(s.salario_promedio, 2)                    AS promedio_depto,\n    ROUND(r.salario - s.salario_promedio, 2)        AS diferencia_vs_promedio,\n    ROUND(r.salario / s.salario_promedio * 100, 1)  AS pct_vs_promedio,\n    s.uso_presupuesto_pct\nFROM ranking_empleados r\nJOIN stats_depto s ON r.departamento = s.departamento\nORDER BY r.departamento, r.ranking_depto	38	0	f	ok	2026-05-13 12:01:29.866901-05	[]	[]
230	anonymous	admin	execute article abbad29e-ea6e-4401-803a-4fd3457b6ec9	workflow execution	10	0	f	error	2026-05-13 12:10:33.683839-05	\N	\N
237	anonymous	admin	execute article 41428192-6c8f-47c6-a3d9-45a5903d8a42	workflow execution	72	0	f	error	2026-05-13 12:14:05.110817-05	\N	\N
240	anonymous	admin	execute article bed57404-2a43-47dd-8bbc-aa8d555e99f9	INSERT INTO empleados (cedula, nombre, apellido, email, departamento_id, cargo, salario, fecha_ingreso, activo) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1)	46	0	f	ok	2026-05-13 12:14:05.258833-05	["12345678", "Test", "Usuario", "test@test.com", "1", "Dev", "2000", "2025-01-01"]	["cedula", "nombre", "apellido", "email", "departamento_id", "cargo", "salario", "fecha_ingreso"]
231	anonymous	admin	execute article 8e598568-5963-4e03-965a-6cfaae251630	WITH stats_depto AS (\n    SELECT\n        d.id                                        AS depto_id,\n        d.nombre                                    AS departamento,\n        COUNT(e.id)                                 AS total_empleados,\n        AVG(e.salario)                              AS salario_promedio,\n        MAX(e.salario)                              AS salario_max,\n        MIN(e.salario)                              AS salario_min,\n        SUM(e.salario)                              AS masa_salarial,\n        d.presupuesto,\n        ROUND(SUM(e.salario) / d.presupuesto * 100, 2) AS uso_presupuesto_pct\n    FROM empleados e\n    JOIN departamentos d ON e.departamento_id = d.id\n    WHERE e.activo = 1\n    GROUP BY d.id, d.nombre, d.presupuesto\n),\nranking_empleados AS (\n    SELECT\n        e.id,\n        CONCAT(e.nombre, ' ', e.apellido)           AS nombre_completo,\n        e.cargo,\n        e.salario,\n        e.fecha_ingreso,\n        d.nombre                                    AS departamento,\n        DATEDIFF(NOW(), e.fecha_ingreso) / 365      AS anos_empresa,\n        RANK() OVER (\n            PARTITION BY e.departamento_id\n            ORDER BY e.salario DESC\n        )                                           AS ranking_depto\n    FROM empleados e\n    JOIN departamentos d ON e.departamento_id = d.id\n    WHERE e.activo = 1\n)\nSELECT\n    r.nombre_completo,\n    r.cargo,\n    r.departamento,\n    r.salario,\n    ROUND(r.anos_empresa, 1)                        AS anos_empresa,\n    r.ranking_depto,\n    ROUND(s.salario_promedio, 2)                    AS promedio_depto,\n    ROUND(r.salario - s.salario_promedio, 2)        AS diferencia_vs_promedio,\n    ROUND(r.salario / s.salario_promedio * 100, 1)  AS pct_vs_promedio,\n    s.uso_presupuesto_pct\nFROM ranking_empleados r\nJOIN stats_depto s ON r.departamento = s.departamento\nORDER BY r.departamento, r.ranking_depto	19	0	f	ok	2026-05-13 12:10:33.714051-05	[]	[]
232	anonymous	admin	execute article bed57404-2a43-47dd-8bbc-aa8d555e99f9	workflow execution	14	0	f	error	2026-05-13 12:10:33.744736-05	\N	\N
233	anonymous	admin	execute article 25f28d88-312c-4877-abb9-bdeb19383296	SELECT *\nFROM (\n    SELECT\n        e.id,\n        e.nombre || ' ' || e.apellido              AS nombre_completo,\n        e.cargo,\n        e.departamento,\n        e.salario,\n        CASE e.activo WHEN 1 THEN 'Activo' ELSE 'Inactivo' END  AS estado,\n        ROUND(MONTHS_BETWEEN(SYSDATE, e.fecha_ingreso) / 12, 1) AS anos_servicio,\n        RANK() OVER (\n            PARTITION BY e.departamento\n            ORDER BY e.salario DESC\n        )                                           AS ranking_depto\n    FROM empleados e\n    WHERE ($1 IS NULL OR e.departamento = $1)\n      AND e.salario >= NVL($2, 0)\n      AND e.salario <= NVL($3, 999999)\n      AND ($4 IS NULL OR e.activo = $4)\n    ORDER BY e.salario DESC\n)\nWHERE ROWNUM <= 50	177	0	f	ok	2026-05-13 12:10:33.927778-05	["TI", null, null, "1"]	["departamento", "salario_min", "salario_max", "solo_activos"]
234	anonymous	admin	execute article 05ce43a2-2ab0-4de2-84af-7a11b4821d47	workflow execution	2	0	f	error	2026-05-13 12:10:33.936455-05	\N	\N
295	anonymous	admin	{"collection":"empleados","find":{},"limit":1}	{"collection":"empleados","find":{},"limit":1}	10	1	f	ok	2026-05-14 09:39:24.803481-05	[]	[]
296	anonymous	admin	SELECT NOW() AS ahora, '2024-01-15'::date AS fecha, 42 AS n	SELECT NOW() AS ahora, '2024-01-15'::date AS fecha, 42 AS n	5	1	f	ok	2026-05-14 09:39:38.336918-05	[]	[]
297	anonymous	admin	SELECT id, nombre, fecha_ingreso FROM empleados FETCH FIRST 1 ROWS ONLY	SELECT id, nombre, fecha_ingreso FROM empleados FETCH FIRST 1 ROWS ONLY	70	1	f	ok	2026-05-14 09:39:38.591965-05	[]	[]
298	anonymous	admin	SELECT id, nombre, fecha_ingreso, creado_en FROM empleados LIMIT 1	SELECT id, nombre, fecha_ingreso, creado_en FROM empleados LIMIT 1	25	1	f	ok	2026-05-14 09:39:38.688658-05	[]	[]
326	anonymous	admin	SELECT * FROM (SELECT departamento, salario FROM empleados) PIVOT (COUNT(*) AS cnt, AVG(salario) AS prom FOR departamento IN (Q'TI' AS ti, Q'Ventas' AS ventas, Q'RRHH' AS rrhh))	SELECT * FROM (SELECT departamento, salario FROM empleados) PIVOT (COUNT(*) AS cnt, AVG(salario) AS prom FOR departamento IN (Q'TI' AS ti, Q'Ventas' AS ventas, Q'RRHH' AS rrhh))	104	0	f	error	2026-05-14 09:47:41.664882-05	[]	\N
327	anonymous	admin	WITH RECURSIVE serie(n) AS (\n      SELECT 1 UNION ALL SELECT n+1 FROM serie WHERE n < 10\n    ) SELECT array_agg(n) AS numeros, SUM(n) AS suma, AVG(n)::numeric(10,2) AS promedio FROM serie	WITH RECURSIVE serie(n) AS (\n      SELECT 1 UNION ALL SELECT n+1 FROM serie WHERE n < 10\n    ) SELECT array_agg(n) AS numeros, SUM(n) AS suma, AVG(n)::numeric(10,2) AS promedio FROM serie	2	1	f	ok	2026-05-14 09:47:41.835156-05	[]	[]
328	anonymous	admin	SELECT categoria,\n      COUNT(*) AS total,\n      COUNT(*) FILTER (WHERE visible_para = 'todos') AS publicos,\n      COUNT(*) FILTER (WHERE visible_para != 'todos') AS restringidos,\n      ROUND(100.0 * COUNT(*) FILTER (WHERE visible_para='todos') / NULLIF(COUNT(*),0), 1) AS pct_publico,\n      DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS rank_categoria\n    FROM knowledge_base GROUP BY categoria ORDER BY total DESC	SELECT categoria,\n      COUNT(*) AS total,\n      COUNT(*) FILTER (WHERE visible_para = 'todos') AS publicos,\n      COUNT(*) FILTER (WHERE visible_para != 'todos') AS restringidos,\n      ROUND(100.0 * COUNT(*) FILTER (WHERE visible_para='todos') / NULLIF(COUNT(*),0), 1) AS pct_publico,\n      DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS rank_categoria\n    FROM knowledge_base GROUP BY categoria ORDER BY total DESC	6	11	f	ok	2026-05-14 09:47:41.852905-05	[]	[]
329	anonymous	admin	WITH RECURSIVE numeros AS (\n      SELECT 1 AS n UNION ALL SELECT n+1 FROM numeros WHERE n < 8\n    ) SELECT GROUP_CONCAT(n) AS secuencia, SUM(n) AS total FROM numeros	WITH RECURSIVE numeros AS (\n      SELECT 1 AS n UNION ALL SELECT n+1 FROM numeros WHERE n < 8\n    ) SELECT GROUP_CONCAT(n) AS secuencia, SUM(n) AS total FROM numeros	13	1	f	ok	2026-05-14 09:47:43.73758-05	[]	[]
330	anonymous	admin	SELECT d.nombre AS depto,\n      COUNT(e.id) AS num_empleados,\n      JSON_ARRAYAGG(JSON_OBJECT('nombre', e.nombre, 'salario', e.salario)) AS empleados_json,\n      (SELECT MAX(salario) FROM empleados WHERE departamento_id = d.id) AS salario_max\n    FROM departamentos d LEFT JOIN empleados e ON e.departamento_id = d.id\n    GROUP BY d.id, d.nombre HAVING num_empleados > 0	SELECT d.nombre AS depto,\n      COUNT(e.id) AS num_empleados,\n      JSON_ARRAYAGG(JSON_OBJECT('nombre', e.nombre, 'salario', e.salario)) AS empleados_json,\n      (SELECT MAX(salario) FROM empleados WHERE departamento_id = d.id) AS salario_max\n    FROM departamentos d LEFT JOIN empleados e ON e.departamento_id = d.id\n    GROUP BY d.id, d.nombre HAVING num_empleados > 0	4	5	f	ok	2026-05-14 09:47:43.74683-05	[]	[]
340	anonymous	admin	{"collection":"empleados","aggregate":[{"$addFields":{"categoria_salarial":{"$cond":[{"$gte":["$salario",4000]},"alto",{"$cond":[{"$gte":["$salario",3000]},"medio","bajo"]}]}}},{"$group":{"_id":{"depto":"$departamento","categoria":"$categoria_salarial"},"count":{"$sum":1},"nombres":{"$push":"$nombre"}}},{"$sort":{"count":-1}}]}	{"collection":"empleados","aggregate":[{"$addFields":{"categoria_salarial":{"$cond":[{"$gte":["$salario",4000]},"alto",{"$cond":[{"$gte":["$salario",3000]},"medio","bajo"]}]}}},{"$group":{"_id":{"depto":"$departamento","categoria":"$categoria_salarial"},"count":{"$sum":1},"nombres":{"$push":"$nombre"}}},{"$sort":{"count":-1}}]}	3	4	f	ok	2026-05-14 09:47:43.835314-05	[]	[]
355	anonymous	admin	SELECT 1 AS n, NOW() AS ahora	SELECT 1 AS n, NOW() AS ahora	2	1	f	ok	2026-05-14 09:49:05.115254-05	[]	[]
235	anonymous	admin	execute article 21961a02-af0e-4237-abb1-dd0e89eccc7f	{"collection":"ventas","aggregate":[{"$addFields":{"mes":{"$month":"$fecha"},"anio":{"$year":"$fecha"}}},{"$group":{"_id":{"vendedor":"$vendedor","producto":"$producto_codigo","pagado":"$pagado"},"total_ventas":{"$sum":"$total"},"total_unidades":{"$sum":"$cantidad"},"num_operaciones":{"$sum":1},"ticket_promedio":{"$avg":"$total"},"venta_max":{"$max":"$total"}}},{"$group":{"_id":"$_id.vendedor","total_facturado":{"$sum":"$total_ventas"},"total_operaciones":{"$sum":"$num_operaciones"},"monto_cobrado":{"$sum":{"$cond":[{"$eq":["$_id.pagado",true]},"$total_ventas",0]}},"monto_pendiente":{"$sum":{"$cond":[{"$eq":["$_id.pagado",false]},"$total_ventas",0]}},"productos":{"$addToSet":"$_id.producto"}}},{"$addFields":{"pct_cobrado":{"$round":[{"$multiply":[{"$divide":["$monto_cobrado",{"$max":["$total_facturado",1]}]},100]},1]},"num_productos":{"$size":"$productos"}}},{"$sort":{"total_facturado":-1}},{"$project":{"_id":0,"vendedor":"$_id","total_facturado":1,"total_operaciones":1,"monto_cobrado":1,"monto_pendiente":1,"pct_cobrado":1,"num_productos":1}}]}	29	0	f	ok	2026-05-13 12:10:33.972084-05	[]	[]
238	anonymous	admin	execute article abbad29e-ea6e-4401-803a-4fd3457b6ec9	WITH sesiones AS (\n    SELECT\n        username,\n        rol                                               AS rol,\n        ip,\n        estado,\n        fecha,\n        LAG(fecha)  OVER (PARTITION BY username ORDER BY fecha) AS login_anterior,\n        LEAD(fecha) OVER (PARTITION BY username ORDER BY fecha) AS siguiente_evento,\n        ROW_NUMBER() OVER (PARTITION BY username ORDER BY fecha DESC) AS nro_evento\n    FROM login_audit\n    WHERE fecha >= NOW() - INTERVAL '30 days'\n),\nmetricas AS (\n    SELECT\n        username,\n        COUNT(*)                                            AS total_eventos,\n        COUNT(CASE WHEN estado = 'ok'     THEN 1 END)      AS logins_ok,\n        COUNT(CASE WHEN estado = 'fail'   THEN 1 END)      AS logins_fail,\n        COUNT(CASE WHEN estado = 'logout' THEN 1 END)      AS logouts,\n        MIN(fecha)                                          AS primer_evento,\n        MAX(fecha)                                          AS ultimo_evento,\n        COUNT(DISTINCT ip)                                  AS ips_distintas,\n        ROUND(\n            COUNT(CASE WHEN estado = 'fail' THEN 1 END)::numeric\n            / NULLIF(COUNT(*), 0) * 100, 1\n        )                                                   AS pct_fallos\n    FROM login_audit\n    WHERE fecha >= NOW() - INTERVAL '30 days'\n    GROUP BY username\n)\nSELECT\n    m.username,\n    m.total_eventos,\n    m.logins_ok,\n    m.logins_fail,\n    m.logouts,\n    m.ips_distintas,\n    m.pct_fallos,\n    ROUND(EXTRACT(EPOCH FROM (m.ultimo_evento - m.primer_evento)) / 3600, 1) AS horas_activo,\n    m.primer_evento,\n    m.ultimo_evento,\n    CASE\n        WHEN m.pct_fallos > 50   THEN 'ALERTA: muchos fallos'\n        WHEN m.ips_distintas > 3 THEN 'AVISO: multiples IPs'\n        ELSE 'Normal'\n    END                                                     AS alerta\nFROM metricas m\nORDER BY m.pct_fallos DESC, m.total_eventos DESC	10	0	f	ok	2026-05-13 12:14:05.180426-05	[]	[]
239	anonymous	admin	execute article 8e598568-5963-4e03-965a-6cfaae251630	WITH stats_depto AS (\n    SELECT\n        d.id                                        AS depto_id,\n        d.nombre                                    AS departamento,\n        COUNT(e.id)                                 AS total_empleados,\n        AVG(e.salario)                              AS salario_promedio,\n        MAX(e.salario)                              AS salario_max,\n        MIN(e.salario)                              AS salario_min,\n        SUM(e.salario)                              AS masa_salarial,\n        d.presupuesto,\n        ROUND(SUM(e.salario) / d.presupuesto * 100, 2) AS uso_presupuesto_pct\n    FROM empleados e\n    JOIN departamentos d ON e.departamento_id = d.id\n    WHERE e.activo = 1\n    GROUP BY d.id, d.nombre, d.presupuesto\n),\nranking_empleados AS (\n    SELECT\n        e.id,\n        CONCAT(e.nombre, ' ', e.apellido)           AS nombre_completo,\n        e.cargo,\n        e.salario,\n        e.fecha_ingreso,\n        d.nombre                                    AS departamento,\n        DATEDIFF(NOW(), e.fecha_ingreso) / 365      AS anos_empresa,\n        RANK() OVER (\n            PARTITION BY e.departamento_id\n            ORDER BY e.salario DESC\n        )                                           AS ranking_depto\n    FROM empleados e\n    JOIN departamentos d ON e.departamento_id = d.id\n    WHERE e.activo = 1\n)\nSELECT\n    r.nombre_completo,\n    r.cargo,\n    r.departamento,\n    r.salario,\n    ROUND(r.anos_empresa, 1)                        AS anos_empresa,\n    r.ranking_depto,\n    ROUND(s.salario_promedio, 2)                    AS promedio_depto,\n    ROUND(r.salario - s.salario_promedio, 2)        AS diferencia_vs_promedio,\n    ROUND(r.salario / s.salario_promedio * 100, 1)  AS pct_vs_promedio,\n    s.uso_presupuesto_pct\nFROM ranking_empleados r\nJOIN stats_depto s ON r.departamento = s.departamento\nORDER BY r.departamento, r.ranking_depto	17	0	f	ok	2026-05-13 12:14:05.208799-05	[]	[]
241	anonymous	admin	execute article 25f28d88-312c-4877-abb9-bdeb19383296	SELECT *\nFROM (\n    SELECT\n        e.id,\n        e.nombre || ' ' || e.apellido              AS nombre_completo,\n        e.cargo,\n        e.departamento,\n        e.salario,\n        CASE e.activo WHEN 1 THEN 'Activo' ELSE 'Inactivo' END  AS estado,\n        ROUND(MONTHS_BETWEEN(SYSDATE, e.fecha_ingreso) / 12, 1) AS anos_servicio,\n        RANK() OVER (\n            PARTITION BY e.departamento\n            ORDER BY e.salario DESC\n        )                                           AS ranking_depto\n    FROM empleados e\n    WHERE ($1 IS NULL OR e.departamento = $1)\n      AND e.salario >= NVL($2, 0)\n      AND e.salario <= NVL($3, 999999)\n      AND ($4 IS NULL OR e.activo = $4)\n    ORDER BY e.salario DESC\n)\nWHERE ROWNUM <= 50	48	0	f	ok	2026-05-13 12:14:05.310258-05	["TI", null, null, "1"]	["departamento", "salario_min", "salario_max", "solo_activos"]
243	anonymous	admin	execute article 3c2bca46-6a46-46c5-952f-a16b5b26dfbe	SELECT\n    NVL(p.categoria, 'TOTAL GENERAL')           AS categoria,\n    NVL(p.nombre,    '-- Subtotal --')          AS producto,\n    COUNT(v.id)                                 AS num_ventas,\n    SUM(v.total)                                AS total_ventas,\n    AVG(v.total)                                AS ticket_promedio,\n    MAX(v.total)                                AS venta_max,\n    ROUND(\n        RATIO_TO_REPORT(SUM(v.total))\n        OVER (PARTITION BY p.categoria) * 100, 2\n    )                                           AS participacion_categoria_pct,\n    ROUND(\n        RATIO_TO_REPORT(SUM(v.total))\n        OVER () * 100, 2\n    )                                           AS participacion_total_pct,\n    COUNT(CASE WHEN v.pagado = 1 THEN 1 END)    AS pagadas,\n    COUNT(CASE WHEN v.pagado = 0 THEN 1 END)    AS pendientes,\n    SUM(CASE WHEN v.pagado = 0 THEN v.total ELSE 0 END) AS monto_pendiente\nFROM ventas v\nJOIN productos p ON v.producto_id = p.id\nGROUP BY ROLLUP(p.categoria, p.nombre)\nORDER BY p.categoria NULLS LAST, total_ventas DESC NULLS LAST	555	0	f	ok	2026-05-13 12:14:05.876971-05	[]	[]
244	anonymous	admin	execute article 05ce43a2-2ab0-4de2-84af-7a11b4821d47	{"collection":"empleados","find":{"departamento":"TI","salario":{"$gte":"2000"}},"sort":{"salario":-1},"limit":50}	18	0	f	ok	2026-05-13 12:14:05.899907-05	[]	[]
299	anonymous	admin	SELECT id, titulo, visible_para FROM knowledge_base WHERE visible_para = $1 LIMIT 3	SELECT id, titulo, visible_para FROM knowledge_base WHERE visible_para = $1 LIMIT 3	5	3	f	ok	2026-05-14 09:41:36.128819-05	["todos"]	[]
300	anonymous	admin	SELECT id, titulo FROM knowledge_base WHERE visible_para = $1 LIMIT $2	SELECT id, titulo FROM knowledge_base WHERE visible_para = $1 LIMIT $2	2	2	f	ok	2026-05-14 09:41:36.141888-05	["todos", 2]	[]
301	anonymous	admin	SELECT id, titulo, categoria FROM knowledge_base WHERE categoria = {{categoria}} LIMIT 3	SELECT id, titulo, categoria FROM knowledge_base WHERE categoria = {{categoria}} LIMIT 3	6	0	f	error	2026-05-14 09:41:36.155056-05	{"categoria": "General"}	\N
302	anonymous	admin	SELECT id, nombre, salario FROM empleados WHERE departamento_id = $1 ORDER BY salario DESC	SELECT id, nombre, salario FROM empleados WHERE departamento_id = $1 ORDER BY salario DESC	7	6	f	ok	2026-05-14 09:41:36.166822-05	[1]	[]
303	anonymous	admin	SELECT nombre, salario FROM empleados WHERE salario >= {{min_salario}} ORDER BY salario DESC	SELECT nombre, salario FROM empleados WHERE salario >= $1 ORDER BY salario DESC	4	5	f	ok	2026-05-14 09:41:36.174935-05	[3000]	["min_salario"]
394	anonymous	admin	SELECT 1 AS n, NOW() AS ahora	SELECT 1 AS n, NOW() AS ahora	2	1	f	ok	2026-05-14 09:50:50.255977-05	[]	[]
245	anonymous	admin	execute article 21961a02-af0e-4237-abb1-dd0e89eccc7f	{"collection":"ventas","aggregate":[{"$addFields":{"mes":{"$month":"$fecha"},"anio":{"$year":"$fecha"}}},{"$group":{"_id":{"vendedor":"$vendedor","producto":"$producto_codigo","pagado":"$pagado"},"total_ventas":{"$sum":"$total"},"total_unidades":{"$sum":"$cantidad"},"num_operaciones":{"$sum":1},"ticket_promedio":{"$avg":"$total"},"venta_max":{"$max":"$total"}}},{"$group":{"_id":"$_id.vendedor","total_facturado":{"$sum":"$total_ventas"},"total_operaciones":{"$sum":"$num_operaciones"},"monto_cobrado":{"$sum":{"$cond":[{"$eq":["$_id.pagado",true]},"$total_ventas",0]}},"monto_pendiente":{"$sum":{"$cond":[{"$eq":["$_id.pagado",false]},"$total_ventas",0]}},"productos":{"$addToSet":"$_id.producto"}}},{"$addFields":{"pct_cobrado":{"$round":[{"$multiply":[{"$divide":["$monto_cobrado",{"$max":["$total_facturado",1]}]},100]},1]},"num_productos":{"$size":"$productos"}}},{"$sort":{"total_facturado":-1}},{"$project":{"_id":0,"vendedor":"$_id","total_facturado":1,"total_operaciones":1,"monto_cobrado":1,"monto_pendiente":1,"pct_cobrado":1,"num_productos":1}}]}	8	0	f	ok	2026-05-13 12:14:05.91134-05	[]	[]
246	anonymous	admin	execute article 41428192-6c8f-47c6-a3d9-45a5903d8a42	SELECT\n    qh.id,\n    qh.username,\n    qh.user_role                                        AS rol,\n    LEFT(qh.query_text, 100)                            AS consulta,\n    qh.execution_ms,\n    qh.row_count,\n    qh.status,\n    qh.was_cached,\n    qh.created_at,\n    CASE\n        WHEN qh.execution_ms > 5000 THEN 'LENTA'\n        WHEN qh.execution_ms > 1000 THEN 'MEDIA'\n        ELSE 'RAPIDA'\n    END                                                 AS velocidad,\n    ROUND((PERCENT_RANK() OVER (ORDER BY qh.execution_ms) * 100)::numeric, 1) AS percentil\nFROM query_history qh\nWHERE (NULLIF($1, '') IS NULL OR qh.username = $1)\n  AND (NULLIF($2, '') IS NULL OR qh.status = $2)\n  AND qh.created_at >= NOW() - (COALESCE($3, 7) || ' days')::INTERVAL\n  AND qh.execution_ms >= COALESCE($4::int, 0)\nORDER BY qh.created_at DESC\nLIMIT 100	77	0	f	ok	2026-05-13 12:15:06.475945-05	[null, null, "7", "0"]	["usuario", "estado", "dias", "min_ms"]
247	anonymous	admin	execute article abbad29e-ea6e-4401-803a-4fd3457b6ec9	WITH sesiones AS (\n    SELECT\n        username,\n        rol                                               AS rol,\n        ip,\n        estado,\n        fecha,\n        LAG(fecha)  OVER (PARTITION BY username ORDER BY fecha) AS login_anterior,\n        LEAD(fecha) OVER (PARTITION BY username ORDER BY fecha) AS siguiente_evento,\n        ROW_NUMBER() OVER (PARTITION BY username ORDER BY fecha DESC) AS nro_evento\n    FROM login_audit\n    WHERE fecha >= NOW() - INTERVAL '30 days'\n),\nmetricas AS (\n    SELECT\n        username,\n        COUNT(*)                                            AS total_eventos,\n        COUNT(CASE WHEN estado = 'ok'     THEN 1 END)      AS logins_ok,\n        COUNT(CASE WHEN estado = 'fail'   THEN 1 END)      AS logins_fail,\n        COUNT(CASE WHEN estado = 'logout' THEN 1 END)      AS logouts,\n        MIN(fecha)                                          AS primer_evento,\n        MAX(fecha)                                          AS ultimo_evento,\n        COUNT(DISTINCT ip)                                  AS ips_distintas,\n        ROUND(\n            COUNT(CASE WHEN estado = 'fail' THEN 1 END)::numeric\n            / NULLIF(COUNT(*), 0) * 100, 1\n        )                                                   AS pct_fallos\n    FROM login_audit\n    WHERE fecha >= NOW() - INTERVAL '30 days'\n    GROUP BY username\n)\nSELECT\n    m.username,\n    m.total_eventos,\n    m.logins_ok,\n    m.logins_fail,\n    m.logouts,\n    m.ips_distintas,\n    m.pct_fallos,\n    ROUND(EXTRACT(EPOCH FROM (m.ultimo_evento - m.primer_evento)) / 3600, 1) AS horas_activo,\n    m.primer_evento,\n    m.ultimo_evento,\n    CASE\n        WHEN m.pct_fallos > 50   THEN 'ALERTA: muchos fallos'\n        WHEN m.ips_distintas > 3 THEN 'AVISO: multiples IPs'\n        ELSE 'Normal'\n    END                                                     AS alerta\nFROM metricas m\nORDER BY m.pct_fallos DESC, m.total_eventos DESC	9	0	f	ok	2026-05-13 12:15:06.538345-05	[]	[]
248	anonymous	admin	execute article 8e598568-5963-4e03-965a-6cfaae251630	WITH stats_depto AS (\n    SELECT\n        d.id                                        AS depto_id,\n        d.nombre                                    AS departamento,\n        COUNT(e.id)                                 AS total_empleados,\n        AVG(e.salario)                              AS salario_promedio,\n        MAX(e.salario)                              AS salario_max,\n        MIN(e.salario)                              AS salario_min,\n        SUM(e.salario)                              AS masa_salarial,\n        d.presupuesto,\n        ROUND(SUM(e.salario) / d.presupuesto * 100, 2) AS uso_presupuesto_pct\n    FROM empleados e\n    JOIN departamentos d ON e.departamento_id = d.id\n    WHERE e.activo = 1\n    GROUP BY d.id, d.nombre, d.presupuesto\n),\nranking_empleados AS (\n    SELECT\n        e.id,\n        CONCAT(e.nombre, ' ', e.apellido)           AS nombre_completo,\n        e.cargo,\n        e.salario,\n        e.fecha_ingreso,\n        d.nombre                                    AS departamento,\n        DATEDIFF(NOW(), e.fecha_ingreso) / 365      AS anos_empresa,\n        RANK() OVER (\n            PARTITION BY e.departamento_id\n            ORDER BY e.salario DESC\n        )                                           AS ranking_depto\n    FROM empleados e\n    JOIN departamentos d ON e.departamento_id = d.id\n    WHERE e.activo = 1\n)\nSELECT\n    r.nombre_completo,\n    r.cargo,\n    r.departamento,\n    r.salario,\n    ROUND(r.anos_empresa, 1)                        AS anos_empresa,\n    r.ranking_depto,\n    ROUND(s.salario_promedio, 2)                    AS promedio_depto,\n    ROUND(r.salario - s.salario_promedio, 2)        AS diferencia_vs_promedio,\n    ROUND(r.salario / s.salario_promedio * 100, 1)  AS pct_vs_promedio,\n    s.uso_presupuesto_pct\nFROM ranking_empleados r\nJOIN stats_depto s ON r.departamento = s.departamento\nORDER BY r.departamento, r.ranking_depto	39	0	f	ok	2026-05-13 12:15:06.581713-05	[]	[]
249	anonymous	admin	execute article bed57404-2a43-47dd-8bbc-aa8d555e99f9	INSERT INTO empleados (cedula, nombre, apellido, email, departamento_id, cargo, salario, fecha_ingreso, activo) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1)	19	0	f	ok	2026-05-13 12:15:06.60455-05	["TEST-001", "Test", "Usuario", "t@t.com", "1", "Dev", "2000", "2025-01-01"]	["cedula", "nombre", "apellido", "email", "departamento_id", "cargo", "salario", "fecha_ingreso"]
250	anonymous	admin	execute article dc84bf6d-0c42-4802-87ee-e03791db0f14	SELECT\n    i.nombre                                    AS institucion,\n    i.tipo                                      AS tipo_institucion,\n    sps.nombre                                  AS servicio,\n    COUNT(op.id)                                AS total_transacciones,\n    SUM(op.monto)                               AS monto_total,\n    AVG(op.monto)                               AS monto_promedio,\n    MAX(op.monto)                               AS transaccion_mayor,\n    MIN(op.monto)                               AS transaccion_menor,\n    SUM(op.monto * sps.comision)                AS comision_generada,\n    COUNT(CASE WHEN op.estado = 1 THEN 1 END)   AS procesadas,\n    COUNT(CASE WHEN op.estado = 0 THEN 1 END)   AS pendientes,\n    MAX(op.fecha_pago)                          AS ultima_transaccion,\n    DATEDIFF(NOW(), MAX(op.fecha_pago))         AS dias_desde_ultima\nFROM ordenes_pago op\nJOIN servicios_punto_venta sps ON op.sale_point_service_id = sps.id\nJOIN instituciones i           ON sps.institucion_id = i.id\nWHERE op.fecha_pago >= DATE_SUB(NOW(), INTERVAL 100 DAY)\n  AND i.activo = 1\n  AND sps.activo = 1\nGROUP BY i.id, i.nombre, i.tipo, sps.id, sps.nombre, sps.comision\nHAVING total_transacciones > 0\n   AND monto_total > 100\nORDER BY monto_total DESC, total_transacciones DESC	17	0	f	ok	2026-05-13 12:15:06.625774-05	[]	[]
251	anonymous	admin	execute article 19a1e879-7450-473c-9582-23022ec10344	SELECT\n    op.id,\n    i.nombre                            AS institucion,\n    sps.nombre                          AS servicio,\n    op.monto,\n    op.estado,\n    op.fecha_pago,\n    DATEDIFF(NOW(), op.fecha_pago)      AS dias_atras,\n    CASE op.estado WHEN 1 THEN 'Procesada' ELSE 'Pendiente' END AS estado_texto\nFROM ordenes_pago op\nJOIN servicios_punto_venta sps ON op.sale_point_service_id = sps.id\nJOIN instituciones i           ON sps.institucion_id = i.id\nWHERE op.fecha_pago BETWEEN $1 AND $2\n  AND op.monto >= COALESCE($3, 0)\n  AND ($4 IS NULL OR i.nombre LIKE CONCAT('%', $4, '%'))\nORDER BY op.monto DESC	6	0	f	ok	2026-05-13 12:15:06.637085-05	[null, null, null, null]	["fecha_desde", "fecha_hasta", "monto_min", "institucion"]
257	anonymous	admin	SELECT 1 AS test	SELECT 1 AS test	4	1	f	ok	2026-05-14 09:19:13.367557-05	[]	[]
252	anonymous	admin	execute article 25f28d88-312c-4877-abb9-bdeb19383296	SELECT *\nFROM (\n    SELECT\n        e.id,\n        e.nombre || ' ' || e.apellido              AS nombre_completo,\n        e.cargo,\n        e.departamento,\n        e.salario,\n        CASE e.activo WHEN 1 THEN 'Activo' ELSE 'Inactivo' END  AS estado,\n        ROUND(MONTHS_BETWEEN(SYSDATE, e.fecha_ingreso) / 12, 1) AS anos_servicio,\n        RANK() OVER (\n            PARTITION BY e.departamento\n            ORDER BY e.salario DESC\n        )                                           AS ranking_depto\n    FROM empleados e\n    WHERE ($1 IS NULL OR e.departamento = $1)\n      AND e.salario >= NVL($2, 0)\n      AND e.salario <= NVL($3, 999999)\n      AND ($4 IS NULL OR e.activo = $4)\n    ORDER BY e.salario DESC\n)\nWHERE ROWNUM <= 50	6	0	f	ok	2026-05-13 12:15:06.647195-05	["TI", null, null, "1"]	["departamento", "salario_min", "salario_max", "solo_activos"]
253	anonymous	admin	execute article 1ff6addb-b02c-47eb-af86-43825bb2dbba	UPDATE empleados\nSET salario = ROUND(salario * (1 + $1 / 100), 2)\nWHERE ($2 IS NULL OR departamento = $2)\n  AND activo = 1	61	0	f	ok	2026-05-13 12:15:06.712325-05	[null, "TI"]	["pct_aumento", "departamento"]
304	anonymous	admin	SELECT nombre, salario FROM empleados WHERE salario BETWEEN $1 AND $2 ORDER BY salario	SELECT nombre, salario FROM empleados WHERE salario BETWEEN $1 AND $2 ORDER BY salario	3	9	f	ok	2026-05-14 09:41:36.182978-05	[2000, 4000]	[]
305	anonymous	admin	SELECT id, nombre, salario FROM empleados WHERE departamento = $1	SELECT id, nombre, salario FROM empleados WHERE departamento = $1	55	2	f	ok	2026-05-14 09:41:36.243775-05	["TI"]	[]
306	anonymous	admin	SELECT nombre, cargo, salario FROM empleados WHERE cargo = {{cargo}}	SELECT nombre, cargo, salario FROM empleados WHERE ($1 IS NULL OR cargo = $1)	11	1	f	ok	2026-05-14 09:41:36.258098-05	["Gerente"]	["cargo"]
307	anonymous	admin	SELECT nombre, salario FROM empleados WHERE salario > $1 OR salario = $1	SELECT nombre, salario FROM empleados WHERE salario > $1 OR salario = $1	9	1	f	ok	2026-05-14 09:41:36.270806-05	[5000]	[]
308	anonymous	admin	{"collection":"empleados","find":{"departamento":"{{dept}}"}}	{"collection":"empleados","find":{"departamento":"{{dept}}"}}	1	0	f	error	2026-05-14 09:41:36.275134-05	{"dept": "TI"}	\N
309	anonymous	admin	{"collection":"empleados","find":{"salario":{"$gte":{{min}}}}}	{"collection":"empleados","find":{"salario":{"$gte":3000}}}	3	2	f	ok	2026-05-14 09:41:36.282015-05	[]	[]
331	anonymous	admin	SELECT e.nombre, e.salario, e.departamento_id,\n      (SELECT COUNT(*) FROM empleados e2 WHERE e2.departamento_id = e.departamento_id AND e2.salario > e.salario) AS empleados_mejor_pagados,\n      (SELECT ROUND(AVG(salario),2) FROM empleados e3 WHERE e3.departamento_id = e.departamento_id) AS promedio_depto,\n      CASE\n        WHEN e.salario > (SELECT AVG(salario) FROM empleados WHERE departamento_id = e.departamento_id) * 1.2 THEN 'Top'\n        WHEN e.salario < (SELECT AVG(salario) FROM empleados WHERE departamento_id = e.departamento_id) * 0.8 THEN 'Bajo'\n        ELSE 'Medio'\n      END AS banda_salarial\n    FROM empleados e WHERE e.activo = 1 ORDER BY e.salario DESC LIMIT 8	SELECT e.nombre, e.salario, e.departamento_id,\n      (SELECT COUNT(*) FROM empleados e2 WHERE e2.departamento_id = e.departamento_id AND e2.salario > e.salario) AS empleados_mejor_pagados,\n      (SELECT ROUND(AVG(salario),2) FROM empleados e3 WHERE e3.departamento_id = e.departamento_id) AS promedio_depto,\n      CASE\n        WHEN e.salario > (SELECT AVG(salario) FROM empleados WHERE departamento_id = e.departamento_id) * 1.2 THEN 'Top'\n        WHEN e.salario < (SELECT AVG(salario) FROM empleados WHERE departamento_id = e.departamento_id) * 0.8 THEN 'Bajo'\n        ELSE 'Medio'\n      END AS banda_salarial\n    FROM empleados e WHERE e.activo = 1 ORDER BY e.salario DESC LIMIT 8	4	8	f	ok	2026-05-14 09:47:43.756265-05	[]	[]
332	anonymous	admin	SELECT origen, nombre, monto FROM (\n      SELECT 'empleado' AS origen, nombre, salario AS monto FROM empleados WHERE activo=1\n      UNION ALL\n      SELECT 'producto' AS origen, nombre, precio AS monto FROM productos WHERE activo=1\n    ) t ORDER BY monto DESC LIMIT 10	SELECT origen, nombre, monto FROM (\n      SELECT 'empleado' AS origen, nombre, salario AS monto FROM empleados WHERE activo=1\n      UNION ALL\n      SELECT 'producto' AS origen, nombre, precio AS monto FROM productos WHERE activo=1\n    ) t ORDER BY monto DESC LIMIT 10	4	10	f	ok	2026-05-14 09:47:43.76573-05	[]	[]
333	anonymous	admin	SELECT LEVEL AS nivel, LPAD(' ', LEVEL*2) || 'nodo ' || LEVEL AS arbol\n    FROM dual CONNECT BY LEVEL <= 6	SELECT LEVEL AS nivel, LPAD(' ', LEVEL*2) || 'nodo ' || LEVEL AS arbol\n    FROM dual CONNECT BY LEVEL <= 6	4	6	f	ok	2026-05-14 09:47:43.77529-05	[]	[]
334	anonymous	admin	SELECT nombre, salario, departamento,\n      LAG(salario) OVER (PARTITION BY departamento ORDER BY salario) AS sal_anterior,\n      LEAD(salario) OVER (PARTITION BY departamento ORDER BY salario) AS sal_siguiente,\n      ROUND(RATIO_TO_REPORT(salario) OVER (PARTITION BY departamento) * 100, 2) AS pct_del_depto,\n      SUM(salario) OVER (PARTITION BY departamento ORDER BY salario ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS acumulado\n    FROM empleados ORDER BY departamento, salario	SELECT nombre, salario, departamento,\n      LAG(salario) OVER (PARTITION BY departamento ORDER BY salario) AS sal_anterior,\n      LEAD(salario) OVER (PARTITION BY departamento ORDER BY salario) AS sal_siguiente,\n      ROUND(RATIO_TO_REPORT(salario) OVER (PARTITION BY departamento) * 100, 2) AS pct_del_depto,\n      SUM(salario) OVER (PARTITION BY departamento ORDER BY salario ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS acumulado\n    FROM empleados ORDER BY departamento, salario	4	7	f	ok	2026-05-14 09:47:43.784498-05	[]	[]
335	anonymous	admin	SELECT * FROM (\n      SELECT departamento, salario FROM empleados\n    ) PIVOT (\n      COUNT(*) AS cnt, AVG(salario) AS prom\n      FOR departamento IN ('TI' AS ti, 'Ventas' AS ventas, 'RRHH' AS rrhh, 'Gerencia' AS gerencia)\n    )	SELECT * FROM (\n      SELECT departamento, salario FROM empleados\n    ) PIVOT (\n      COUNT(*) AS cnt, AVG(salario) AS prom\n      FOR departamento IN ('TI' AS ti, 'Ventas' AS ventas, 'RRHH' AS rrhh, 'Gerencia' AS gerencia)\n    )	3	1	f	ok	2026-05-14 09:47:43.791794-05	[]	[]
338	anonymous	admin	{"collection":"ventas","aggregate":[{"$lookup":{"from":"productos","localField":"producto_codigo","foreignField":"codigo","as":"producto"}},{"$unwind":"$producto"},{"$group":{"_id":{"categoria":"$producto.categoria","vendedor":"$vendedor"},"ingresos":{"$sum":"$total"},"unidades":{"$sum":"$cantidad"},"ventas":{"$sum":1}}},{"$sort":{"ingresos":-1}},{"$project":{"_id":0,"categoria":"$_id.categoria","vendedor":"$_id.vendedor","ingresos":1,"unidades":1,"ventas":1,"ticket_promedio":{"$round":[{"$divide":["$ingresos","$ventas"]},2]}}}]}	{"collection":"ventas","aggregate":[{"$lookup":{"from":"productos","localField":"producto_codigo","foreignField":"codigo","as":"producto"}},{"$unwind":"$producto"},{"$group":{"_id":{"categoria":"$producto.categoria","vendedor":"$vendedor"},"ingresos":{"$sum":"$total"},"unidades":{"$sum":"$cantidad"},"ventas":{"$sum":1}}},{"$sort":{"ingresos":-1}},{"$project":{"_id":0,"categoria":"$_id.categoria","vendedor":"$_id.vendedor","ingresos":1,"unidades":1,"ventas":1,"ticket_promedio":{"$round":[{"$divide":["$ingresos","$ventas"]},2]}}}]}	3	5	f	ok	2026-05-14 09:47:43.821467-05	[]	[]
356	anonymous	admin	SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name	SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name	2	13	f	ok	2026-05-14 09:49:05.126978-05	[]	[]
254	anonymous	admin	execute article 3c2bca46-6a46-46c5-952f-a16b5b26dfbe	SELECT\n    NVL(p.categoria, 'TOTAL GENERAL')           AS categoria,\n    NVL(p.nombre,    '-- Subtotal --')          AS producto,\n    COUNT(v.id)                                 AS num_ventas,\n    SUM(v.total)                                AS total_ventas,\n    AVG(v.total)                                AS ticket_promedio,\n    MAX(v.total)                                AS venta_max,\n    ROUND(\n        RATIO_TO_REPORT(SUM(v.total))\n        OVER (PARTITION BY p.categoria) * 100, 2\n    )                                           AS participacion_categoria_pct,\n    ROUND(\n        RATIO_TO_REPORT(SUM(v.total))\n        OVER () * 100, 2\n    )                                           AS participacion_total_pct,\n    COUNT(CASE WHEN v.pagado = 1 THEN 1 END)    AS pagadas,\n    COUNT(CASE WHEN v.pagado = 0 THEN 1 END)    AS pendientes,\n    SUM(CASE WHEN v.pagado = 0 THEN v.total ELSE 0 END) AS monto_pendiente\nFROM ventas v\nJOIN productos p ON v.producto_id = p.id\nGROUP BY ROLLUP(p.categoria, p.nombre)\nORDER BY p.categoria NULLS LAST, total_ventas DESC NULLS LAST	6	0	f	ok	2026-05-13 12:15:06.722336-05	[]	[]
255	anonymous	admin	execute article 05ce43a2-2ab0-4de2-84af-7a11b4821d47	{"collection":"empleados","find":{"departamento":"TI","salario":{"$gte":"2000"}},"sort":{"salario":-1},"limit":50}	6	0	f	ok	2026-05-13 12:15:06.731139-05	[]	[]
256	anonymous	admin	execute article 21961a02-af0e-4237-abb1-dd0e89eccc7f	{"collection":"ventas","aggregate":[{"$addFields":{"mes":{"$month":"$fecha"},"anio":{"$year":"$fecha"}}},{"$group":{"_id":{"vendedor":"$vendedor","producto":"$producto_codigo","pagado":"$pagado"},"total_ventas":{"$sum":"$total"},"total_unidades":{"$sum":"$cantidad"},"num_operaciones":{"$sum":1},"ticket_promedio":{"$avg":"$total"},"venta_max":{"$max":"$total"}}},{"$group":{"_id":"$_id.vendedor","total_facturado":{"$sum":"$total_ventas"},"total_operaciones":{"$sum":"$num_operaciones"},"monto_cobrado":{"$sum":{"$cond":[{"$eq":["$_id.pagado",true]},"$total_ventas",0]}},"monto_pendiente":{"$sum":{"$cond":[{"$eq":["$_id.pagado",false]},"$total_ventas",0]}},"productos":{"$addToSet":"$_id.producto"}}},{"$addFields":{"pct_cobrado":{"$round":[{"$multiply":[{"$divide":["$monto_cobrado",{"$max":["$total_facturado",1]}]},100]},1]},"num_productos":{"$size":"$productos"}}},{"$sort":{"total_facturado":-1}},{"$project":{"_id":0,"vendedor":"$_id","total_facturado":1,"total_operaciones":1,"monto_cobrado":1,"monto_pendiente":1,"pct_cobrado":1,"num_productos":1}}]}	5	0	f	ok	2026-05-13 12:15:06.740136-05	[]	[]
258	anonymous	admin	SELECT column_name, data_type FROM user_tab_columns WHERE table_name='EMPLEADOS' ORDER BY column_id	SELECT column_name, data_type FROM user_tab_columns WHERE table_name='EMPLEADOS' ORDER BY column_id	207	8	f	ok	2026-05-14 09:24:21.577069-05	[]	[]
259	anonymous	admin	SELECT column_name, data_type FROM user_tab_columns WHERE table_name='DEPARTAMENTOS' ORDER BY column_id	SELECT column_name, data_type FROM user_tab_columns WHERE table_name='DEPARTAMENTOS' ORDER BY column_id	41	4	f	ok	2026-05-14 09:24:21.691399-05	[]	[]
260	anonymous	admin	SELECT column_name, data_type FROM user_tab_columns WHERE table_name='PRODUCTOS' ORDER BY column_id	SELECT column_name, data_type FROM user_tab_columns WHERE table_name='PRODUCTOS' ORDER BY column_id	39	7	f	ok	2026-05-14 09:24:21.890543-05	[]	[]
261	anonymous	admin	SELECT column_name, data_type FROM user_tab_columns WHERE table_name='VENTAS' ORDER BY column_id	SELECT column_name, data_type FROM user_tab_columns WHERE table_name='VENTAS' ORDER BY column_id	39	8	f	ok	2026-05-14 09:24:22.055349-05	[]	[]
262	anonymous	admin	SELECT column_name, data_type FROM user_tab_columns WHERE table_name='EMPLEADOS' ORDER BY column_id	SELECT column_name, data_type FROM user_tab_columns WHERE table_name='EMPLEADOS' ORDER BY column_id	4	8	f	ok	2026-05-14 09:24:36.082589-05	[]	[]
263	anonymous	admin	SELECT column_name||':'||data_type AS col FROM user_tab_columns WHERE table_name='DEPARTAMENTOS' ORDER BY column_id	SELECT column_name||':'||data_type AS col FROM user_tab_columns WHERE table_name='DEPARTAMENTOS' ORDER BY column_id	186	4	f	ok	2026-05-14 09:31:58.256768-05	[]	[]
264	anonymous	admin	SELECT column_name||':'||data_type AS col FROM user_tab_columns WHERE table_name='PRODUCTOS' ORDER BY column_id	SELECT column_name||':'||data_type AS col FROM user_tab_columns WHERE table_name='PRODUCTOS' ORDER BY column_id	57	7	f	ok	2026-05-14 09:31:58.373034-05	[]	[]
265	anonymous	admin	SELECT column_name||':'||data_type AS col FROM user_tab_columns WHERE table_name='VENTAS' ORDER BY column_id	SELECT column_name||':'||data_type AS col FROM user_tab_columns WHERE table_name='VENTAS' ORDER BY column_id	43	8	f	ok	2026-05-14 09:31:58.466472-05	[]	[]
266	anonymous	admin	SELECT GROUP_CONCAT(CONCAT(column_name,':',data_type)) AS cols FROM information_schema.columns WHERE table_schema='testdb' AND table_name='instituciones'	SELECT GROUP_CONCAT(CONCAT(column_name,':',data_type)) AS cols FROM information_schema.columns WHERE table_schema='testdb' AND table_name='instituciones'	96	1	f	ok	2026-05-14 09:31:58.611412-05	[]	[]
267	anonymous	admin	{"collection":"empleados","find":{},"limit":1}	{"collection":"empleados","find":{},"limit":1}	81	1	f	ok	2026-05-14 09:32:06.467366-05	[]	[]
268	anonymous	admin	{"collection":"ventas","find":{},"limit":1}	{"collection":"ventas","find":{},"limit":1}	9	1	f	ok	2026-05-14 09:32:06.549477-05	[]	[]
269	anonymous	admin	{"collection":"productos","find":{},"limit":1}	{"collection":"productos","find":{},"limit":1}	7	1	f	ok	2026-05-14 09:32:06.640639-05	[]	[]
270	anonymous	admin	{"collection":"auditoria","find":{},"limit":1}	{"collection":"auditoria","find":{},"limit":1}	6	1	f	ok	2026-05-14 09:32:06.733991-05	[]	[]
310	anonymous	admin	{"collection":"empleados","find":{"departamento":"{{dept}}"}}	{"collection":"empleados","find":{"departamento":"TI"}}	6	2	f	ok	2026-05-14 09:43:15.612764-05	[]	[]
311	anonymous	admin	{"collection":"empleados","find":{"departamento":"{{dept}}","activo":{{act}}}}	{"collection":"empleados","find":{"departamento":"RRHH","activo":true}}	4	1	f	ok	2026-05-14 09:43:15.637949-05	[]	[]
336	anonymous	admin	WITH ventas_agg AS (\n      SELECT producto_id, COUNT(*) AS num_ventas, SUM(total) AS ingresos, SUM(cantidad) AS unidades\n      FROM ventas GROUP BY producto_id\n    ), con_ranking AS (\n      SELECT va.*, p.nombre, p.categoria, p.precio,\n        RANK() OVER (ORDER BY va.ingresos DESC) AS rank_global,\n        ROUND(va.ingresos / NULLIF(va.unidades,0), 2) AS precio_promedio_venta\n      FROM ventas_agg va JOIN productos p ON p.id = va.producto_id\n    )\n    SELECT rank_global, nombre, categoria, num_ventas, unidades, ingresos, precio_promedio_venta,\n      CASE WHEN precio_promedio_venta > precio THEN 'sobre lista' ELSE 'bajo lista' END AS comparacion\n    FROM con_ranking ORDER BY rank_global	WITH ventas_agg AS (\n      SELECT producto_id, COUNT(*) AS num_ventas, SUM(total) AS ingresos, SUM(cantidad) AS unidades\n      FROM ventas GROUP BY producto_id\n    ), con_ranking AS (\n      SELECT va.*, p.nombre, p.categoria, p.precio,\n        RANK() OVER (ORDER BY va.ingresos DESC) AS rank_global,\n        ROUND(va.ingresos / NULLIF(va.unidades,0), 2) AS precio_promedio_venta\n      FROM ventas_agg va JOIN productos p ON p.id = va.producto_id\n    )\n    SELECT rank_global, nombre, categoria, num_ventas, unidades, ingresos, precio_promedio_venta,\n      CASE WHEN precio_promedio_venta > precio THEN 'sobre lista' ELSE 'bajo lista' END AS comparacion\n    FROM con_ranking ORDER BY rank_global	5	4	f	ok	2026-05-14 09:47:43.80227-05	[]	[]
271	anonymous	admin	SELECT 1 AS n, NOW() AS ahora	SELECT 1 AS n, NOW() AS ahora	5	1	f	ok	2026-05-14 09:33:19.299494-05	[]	[]
272	anonymous	admin	SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name	SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name	2	13	f	ok	2026-05-14 09:33:19.31443-05	[]	[]
273	anonymous	admin	SELECT visible_para, COUNT(*) AS total FROM knowledge_base GROUP BY visible_para ORDER BY total DESC	SELECT visible_para, COUNT(*) AS total FROM knowledge_base GROUP BY visible_para ORDER BY total DESC	7	2	f	ok	2026-05-14 09:33:19.32639-05	[]	[]
274	anonymous	admin	WITH ranked AS (\n      SELECT id, titulo, categoria, ROW_NUMBER() OVER (PARTITION BY categoria ORDER BY created_at DESC) AS rn\n      FROM knowledge_base\n    ) SELECT categoria, COUNT(*) AS arts FROM ranked GROUP BY categoria ORDER BY arts DESC LIMIT 5	WITH ranked AS (\n      SELECT id, titulo, categoria, ROW_NUMBER() OVER (PARTITION BY categoria ORDER BY created_at DESC) AS rn\n      FROM knowledge_base\n    ) SELECT categoria, COUNT(*) AS arts FROM ranked GROUP BY categoria ORDER BY arts DESC LIMIT 5	10	0	f	error	2026-05-14 09:33:19.339649-05	[]	\N
275	anonymous	admin	SELECT k.titulo, k.categoria,\n      (SELECT COUNT(*) FROM article_executions ae WHERE ae.article_id = k.id) AS ejecuciones\n    FROM knowledge_base k\n    WHERE k.id IN (SELECT id FROM knowledge_base ORDER BY created_at DESC LIMIT 10)\n    ORDER BY ejecuciones DESC LIMIT 5	SELECT k.titulo, k.categoria,\n      (SELECT COUNT(*) FROM article_executions ae WHERE ae.article_id = k.id) AS ejecuciones\n    FROM knowledge_base k\n    WHERE k.id IN (SELECT id FROM knowledge_base ORDER BY created_at DESC LIMIT 10)\n    ORDER BY ejecuciones DESC LIMIT 5	4	0	f	error	2026-05-14 09:33:19.36238-05	[]	\N
276	anonymous	admin	SELECT 1+1 AS suma	SELECT 1+1 AS suma	6	1	f	ok	2026-05-14 09:33:19.372529-05	[]	[]
277	anonymous	admin	SELECT id, nombre, salario FROM empleados ORDER BY salario DESC LIMIT 3	SELECT id, nombre, salario FROM empleados ORDER BY salario DESC LIMIT 3	6	3	f	ok	2026-05-14 09:33:19.381852-05	[]	[]
278	anonymous	admin	SELECT d.nombre AS depto, COUNT(e.id) AS emps, ROUND(AVG(e.salario),2) AS sal_prom\n    FROM departamentos d LEFT JOIN empleados e ON e.departamento_id = d.id\n    GROUP BY d.id, d.nombre HAVING COUNT(e.id) > 0 ORDER BY sal_prom DESC	SELECT d.nombre AS depto, COUNT(e.id) AS emps, ROUND(AVG(e.salario),2) AS sal_prom\n    FROM departamentos d LEFT JOIN empleados e ON e.departamento_id = d.id\n    GROUP BY d.id, d.nombre HAVING COUNT(e.id) > 0 ORDER BY sal_prom DESC	9	5	f	ok	2026-05-14 09:33:19.394125-05	[]	[]
279	anonymous	admin	SELECT nombre, departamento_id, salario,\n      RANK() OVER (PARTITION BY departamento_id ORDER BY salario DESC) AS rango_salarial\n    FROM empleados WHERE activo = 1 ORDER BY departamento_id, rango_salarial LIMIT 8	SELECT nombre, departamento_id, salario,\n      RANK() OVER (PARTITION BY departamento_id ORDER BY salario DESC) AS rango_salarial\n    FROM empleados WHERE activo = 1 ORDER BY departamento_id, rango_salarial LIMIT 8	8	8	f	ok	2026-05-14 09:33:19.405689-05	[]	[]
280	anonymous	admin	WITH dept_stats AS (\n      SELECT departamento_id, AVG(salario) AS avg_sal FROM empleados GROUP BY departamento_id\n    )\n    SELECT e.nombre, e.salario, ds.avg_sal,\n      CASE WHEN e.salario > ds.avg_sal THEN 'sobre promedio' ELSE 'bajo promedio' END AS clasificacion\n    FROM empleados e JOIN dept_stats ds ON ds.departamento_id = e.departamento_id\n    ORDER BY e.salario DESC LIMIT 6	WITH dept_stats AS (\n      SELECT departamento_id, AVG(salario) AS avg_sal FROM empleados GROUP BY departamento_id\n    )\n    SELECT e.nombre, e.salario, ds.avg_sal,\n      CASE WHEN e.salario > ds.avg_sal THEN 'sobre promedio' ELSE 'bajo promedio' END AS clasificacion\n    FROM empleados e JOIN dept_stats ds ON ds.departamento_id = e.departamento_id\n    ORDER BY e.salario DESC LIMIT 6	13	6	f	ok	2026-05-14 09:33:19.422628-05	[]	[]
281	anonymous	admin	SELECT\n      p.nombre AS producto, p.categoria,\n      COUNT(DISTINCT ped.id) AS num_pedidos,\n      COALESCE(SUM(ped.cantidad),0) AS unidades_vendidas,\n      COALESCE(SUM(ped.precio_total),0) AS ingresos,\n      (SELECT GROUP_CONCAT(DISTINCT emp.nombre) FROM pedidos pe\n        JOIN empleados emp ON emp.id = pe.empleado_id WHERE pe.producto_id = p.id) AS vendedores\n    FROM productos p\n    LEFT JOIN pedidos ped ON ped.producto_id = p.id\n    GROUP BY p.id, p.nombre, p.categoria\n    ORDER BY ingresos DESC LIMIT 5	SELECT\n      p.nombre AS producto, p.categoria,\n      COUNT(DISTINCT ped.id) AS num_pedidos,\n      COALESCE(SUM(ped.cantidad),0) AS unidades_vendidas,\n      COALESCE(SUM(ped.precio_total),0) AS ingresos,\n      (SELECT GROUP_CONCAT(DISTINCT emp.nombre) FROM pedidos pe\n        JOIN empleados emp ON emp.id = pe.empleado_id WHERE pe.producto_id = p.id) AS vendedores\n    FROM productos p\n    LEFT JOIN pedidos ped ON ped.producto_id = p.id\n    GROUP BY p.id, p.nombre, p.categoria\n    ORDER BY ingresos DESC LIMIT 5	37	5	f	ok	2026-05-14 09:33:19.463548-05	[]	[]
282	anonymous	admin	SELECT 1 AS n FROM dual	SELECT 1 AS n FROM dual	17	1	f	ok	2026-05-14 09:33:19.48437-05	[]	[]
283	anonymous	admin	SELECT id, nombre, salario FROM empleados ORDER BY salario DESC FETCH FIRST 3 ROWS ONLY	SELECT id, nombre, salario FROM empleados ORDER BY salario DESC FETCH FIRST 3 ROWS ONLY	195	3	f	ok	2026-05-14 09:33:19.682765-05	[]	[]
284	anonymous	admin	SELECT departamento, COUNT(*) AS emps, ROUND(AVG(salario),2) AS prom,\n      MAX(salario) AS max_sal, MIN(salario) AS min_sal\n    FROM empleados GROUP BY departamento ORDER BY prom DESC	SELECT departamento, COUNT(*) AS emps, ROUND(AVG(salario),2) AS prom,\n      MAX(salario) AS max_sal, MIN(salario) AS min_sal\n    FROM empleados GROUP BY departamento ORDER BY prom DESC	12	5	f	ok	2026-05-14 09:33:19.698239-05	[]	[]
285	anonymous	admin	SELECT v.cliente, p.nombre AS producto, v.total,\n      SUM(v.total) OVER (PARTITION BY v.cliente) AS total_cliente,\n      RANK() OVER (ORDER BY v.total DESC) AS rank_venta\n    FROM ventas v JOIN productos p ON p.id = v.producto_id\n    ORDER BY rank_venta FETCH FIRST 6 ROWS ONLY	SELECT v.cliente, p.nombre AS producto, v.total,\n      SUM(v.total) OVER (PARTITION BY v.cliente) AS total_cliente,\n      RANK() OVER (ORDER BY v.total DESC) AS rank_venta\n    FROM ventas v JOIN productos p ON p.id = v.producto_id\n    ORDER BY rank_venta FETCH FIRST 6 ROWS ONLY	69	4	f	ok	2026-05-14 09:33:19.770904-05	[]	[]
286	anonymous	admin	WITH ventas_por_producto AS (\n      SELECT producto_id, SUM(total) AS ingreso_total, SUM(cantidad) AS unidades\n      FROM ventas GROUP BY producto_id\n    ), ranking AS (\n      SELECT vp.*, p.nombre, p.categoria,\n        ROW_NUMBER() OVER (PARTITION BY p.categoria ORDER BY vp.ingreso_total DESC) AS rn\n      FROM ventas_por_producto vp JOIN productos p ON p.id = vp.producto_id\n    )\n    SELECT categoria, nombre, ingreso_total, unidades FROM ranking WHERE rn <= 2 ORDER BY categoria, ingreso_total DESC	WITH ventas_por_producto AS (\n      SELECT producto_id, SUM(total) AS ingreso_total, SUM(cantidad) AS unidades\n      FROM ventas GROUP BY producto_id\n    ), ranking AS (\n      SELECT vp.*, p.nombre, p.categoria,\n        ROW_NUMBER() OVER (PARTITION BY p.categoria ORDER BY vp.ingreso_total DESC) AS rn\n      FROM ventas_por_producto vp JOIN productos p ON p.id = vp.producto_id\n    )\n    SELECT categoria, nombre, ingreso_total, unidades FROM ranking WHERE rn <= 2 ORDER BY categoria, ingreso_total DESC	141	4	f	ok	2026-05-14 09:33:19.915831-05	[]	[]
287	anonymous	admin	{"collection":"empleados","find":{},"limit":3}	{"collection":"empleados","find":{},"limit":3}	4	3	f	ok	2026-05-14 09:33:19.922634-05	[]	[]
288	anonymous	admin	{"collection":"empleados","find":{"activo":true},"limit":5}	{"collection":"empleados","find":{"activo":true},"limit":5}	6	4	f	ok	2026-05-14 09:33:19.931823-05	[]	[]
289	anonymous	admin	{"collection":"empleados","aggregate":[{"$group":{"_id":"$departamento","total":{"$sum":1},"salario_prom":{"$avg":"$salario"}}},{"$sort":{"salario_prom":-1}}]}	{"collection":"empleados","aggregate":[{"$group":{"_id":"$departamento","total":{"$sum":1},"salario_prom":{"$avg":"$salario"}}},{"$sort":{"salario_prom":-1}}]}	30	3	f	ok	2026-05-14 09:33:19.965439-05	[]	[]
290	anonymous	admin	{"collection":"ventas","aggregate":[{"$group":{"_id":"$vendedor","ventas":{"$sum":1},"ingresos":{"$sum":"$total"},"ticket_prom":{"$avg":"$total"}}},{"$match":{"ingresos":{"$gt":0}}},{"$sort":{"ingresos":-1}},{"$limit":5}]}	{"collection":"ventas","aggregate":[{"$group":{"_id":"$vendedor","ventas":{"$sum":1},"ingresos":{"$sum":"$total"},"ticket_prom":{"$avg":"$total"}}},{"$match":{"ingresos":{"$gt":0}}},{"$sort":{"ingresos":-1}},{"$limit":5}]}	7	3	f	ok	2026-05-14 09:33:19.975738-05	[]	[]
291	anonymous	admin	{"collection":"ventas","aggregate":[{"$lookup":{"from":"productos","localField":"producto_codigo","foreignField":"codigo","as":"prod"}},{"$unwind":"$prod"},{"$group":{"_id":"$prod.categoria","ingresos":{"$sum":"$total"},"unidades":{"$sum":"$cantidad"}}},{"$sort":{"ingresos":-1}}]}	{"collection":"ventas","aggregate":[{"$lookup":{"from":"productos","localField":"producto_codigo","foreignField":"codigo","as":"prod"}},{"$unwind":"$prod"},{"$group":{"_id":"$prod.categoria","ingresos":{"$sum":"$total"},"unidades":{"$sum":"$cantidad"}}},{"$sort":{"ingresos":-1}}]}	11	4	f	ok	2026-05-14 09:33:19.989893-05	[]	[]
292	anonymous	admin	{"collection":"conciliacion","find":{},"limit":2}	{"collection":"conciliacion","find":{},"limit":2}	6	2	f	ok	2026-05-14 09:33:20.001523-05	[]	[]
293	anonymous	admin	{"collection":"productos","countDocuments":{}}	{"collection":"productos","countDocuments":{}}	4	1	f	ok	2026-05-14 09:33:20.009869-05	[]	[]
312	anonymous	admin	WITH RECURSIVE serie(n) AS (\n      SELECT 1 UNION ALL SELECT n+1 FROM serie WHERE n < 10\n    ) SELECT array_agg(n) AS numeros, SUM(n) AS suma, AVG(n)::numeric(10,2) AS promedio FROM serie	WITH RECURSIVE serie(n) AS (\n      SELECT 1 UNION ALL SELECT n+1 FROM serie WHERE n < 10\n    ) SELECT array_agg(n) AS numeros, SUM(n) AS suma, AVG(n)::numeric(10,2) AS promedio FROM serie	6	1	f	ok	2026-05-14 09:44:46.459523-05	[]	[]
313	anonymous	admin	SELECT categoria,\n      COUNT(*) AS total,\n      COUNT(*) FILTER (WHERE visible_para = 'todos') AS publicos,\n      COUNT(*) FILTER (WHERE visible_para != 'todos') AS restringidos,\n      ROUND(100.0 * COUNT(*) FILTER (WHERE visible_para='todos') / NULLIF(COUNT(*),0), 1) AS pct_publico,\n      DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS rank_categoria\n    FROM knowledge_base GROUP BY categoria ORDER BY total DESC	SELECT categoria,\n      COUNT(*) AS total,\n      COUNT(*) FILTER (WHERE visible_para = 'todos') AS publicos,\n      COUNT(*) FILTER (WHERE visible_para != 'todos') AS restringidos,\n      ROUND(100.0 * COUNT(*) FILTER (WHERE visible_para='todos') / NULLIF(COUNT(*),0), 1) AS pct_publico,\n      DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS rank_categoria\n    FROM knowledge_base GROUP BY categoria ORDER BY total DESC	6	11	f	ok	2026-05-14 09:44:46.47635-05	[]	[]
314	anonymous	admin	WITH RECURSIVE numeros AS (\n      SELECT 1 AS n UNION ALL SELECT n+1 FROM numeros WHERE n < 8\n    ) SELECT GROUP_CONCAT(n) AS secuencia, SUM(n) AS total FROM numeros	WITH RECURSIVE numeros AS (\n      SELECT 1 AS n UNION ALL SELECT n+1 FROM numeros WHERE n < 8\n    ) SELECT GROUP_CONCAT(n) AS secuencia, SUM(n) AS total FROM numeros	14	1	f	ok	2026-05-14 09:44:47.82446-05	[]	[]
315	anonymous	admin	SELECT d.nombre AS depto,\n      COUNT(e.id) AS num_empleados,\n      JSON_ARRAYAGG(JSON_OBJECT('nombre', e.nombre, 'salario', e.salario)) AS empleados_json,\n      (SELECT MAX(salario) FROM empleados WHERE departamento_id = d.id) AS salario_max\n    FROM departamentos d LEFT JOIN empleados e ON e.departamento_id = d.id\n    GROUP BY d.id, d.nombre HAVING num_empleados > 0	SELECT d.nombre AS depto,\n      COUNT(e.id) AS num_empleados,\n      JSON_ARRAYAGG(JSON_OBJECT('nombre', e.nombre, 'salario', e.salario)) AS empleados_json,\n      (SELECT MAX(salario) FROM empleados WHERE departamento_id = d.id) AS salario_max\n    FROM departamentos d LEFT JOIN empleados e ON e.departamento_id = d.id\n    GROUP BY d.id, d.nombre HAVING num_empleados > 0	11	5	f	ok	2026-05-14 09:44:47.839671-05	[]	[]
316	anonymous	admin	SELECT e.nombre, e.salario, e.departamento_id,\n      (SELECT COUNT(*) FROM empleados e2 WHERE e2.departamento_id = e.departamento_id AND e2.salario > e.salario) AS empleados_mejor_pagados,\n      (SELECT ROUND(AVG(salario),2) FROM empleados e3 WHERE e3.departamento_id = e.departamento_id) AS promedio_depto,\n      CASE\n        WHEN e.salario > (SELECT AVG(salario) FROM empleados WHERE departamento_id = e.departamento_id) * 1.2 THEN 'Top'\n        WHEN e.salario < (SELECT AVG(salario) FROM empleados WHERE departamento_id = e.departamento_id) * 0.8 THEN 'Bajo'\n        ELSE 'Medio'\n      END AS banda_salarial\n    FROM empleados e WHERE e.activo = 1 ORDER BY e.salario DESC LIMIT 8	SELECT e.nombre, e.salario, e.departamento_id,\n      (SELECT COUNT(*) FROM empleados e2 WHERE e2.departamento_id = e.departamento_id AND e2.salario > e.salario) AS empleados_mejor_pagados,\n      (SELECT ROUND(AVG(salario),2) FROM empleados e3 WHERE e3.departamento_id = e.departamento_id) AS promedio_depto,\n      CASE\n        WHEN e.salario > (SELECT AVG(salario) FROM empleados WHERE departamento_id = e.departamento_id) * 1.2 THEN 'Top'\n        WHEN e.salario < (SELECT AVG(salario) FROM empleados WHERE departamento_id = e.departamento_id) * 0.8 THEN 'Bajo'\n        ELSE 'Medio'\n      END AS banda_salarial\n    FROM empleados e WHERE e.activo = 1 ORDER BY e.salario DESC LIMIT 8	5	8	f	ok	2026-05-14 09:44:47.848361-05	[]	[]
317	anonymous	admin	SELECT origen, nombre, monto FROM (\n      SELECT 'empleado' AS origen, nombre, salario AS monto FROM empleados WHERE activo=1\n      UNION ALL\n      SELECT 'producto' AS origen, nombre, precio AS monto FROM productos WHERE activo=1\n    ) t ORDER BY monto DESC LIMIT 10	SELECT origen, nombre, monto FROM (\n      SELECT 'empleado' AS origen, nombre, salario AS monto FROM empleados WHERE activo=1\n      UNION ALL\n      SELECT 'producto' AS origen, nombre, precio AS monto FROM productos WHERE activo=1\n    ) t ORDER BY monto DESC LIMIT 10	4	10	f	ok	2026-05-14 09:44:47.857292-05	[]	[]
318	anonymous	admin	SELECT LEVEL AS nivel, LPAD(' ', LEVEL*2) || 'nodo ' || LEVEL AS arbol\n    FROM dual CONNECT BY LEVEL <= 6	SELECT LEVEL AS nivel, LPAD(' ', LEVEL*2) || 'nodo ' || LEVEL AS arbol\n    FROM dual CONNECT BY LEVEL <= 6	64	6	f	ok	2026-05-14 09:44:47.924563-05	[]	[]
339	anonymous	admin	{"collection":"conciliacion","aggregate":[{"$unwind":"$details"},{"$group":{"_id":"$details.transactionTypeSwitch","total_transacciones":{"$sum":1},"valor_total":{"$sum":"$details.transactionValueSwitch"},"reconciliados":{"$sum":"$details.reconciled"}}},{"$sort":{"valor_total":-1}}]}	{"collection":"conciliacion","aggregate":[{"$unwind":"$details"},{"$group":{"_id":"$details.transactionTypeSwitch","total_transacciones":{"$sum":1},"valor_total":{"$sum":"$details.transactionValueSwitch"},"reconciliados":{"$sum":"$details.reconciled"}}},{"$sort":{"valor_total":-1}}]}	3	3	f	ok	2026-05-14 09:47:43.828063-05	[]	[]
342	anonymous	admin	SELECT categoria,\n      COUNT(*) AS total,\n      COUNT(*) FILTER (WHERE visible_para = 'todos') AS publicos,\n      COUNT(*) FILTER (WHERE visible_para != 'todos') AS restringidos,\n      ROUND(100.0 * COUNT(*) FILTER (WHERE visible_para='todos') / NULLIF(COUNT(*),0), 1) AS pct_publico,\n      DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS rank_categoria\n    FROM knowledge_base GROUP BY categoria ORDER BY total DESC	SELECT categoria,\n      COUNT(*) AS total,\n      COUNT(*) FILTER (WHERE visible_para = 'todos') AS publicos,\n      COUNT(*) FILTER (WHERE visible_para != 'todos') AS restringidos,\n      ROUND(100.0 * COUNT(*) FILTER (WHERE visible_para='todos') / NULLIF(COUNT(*),0), 1) AS pct_publico,\n      DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS rank_categoria\n    FROM knowledge_base GROUP BY categoria ORDER BY total DESC	2	11	f	ok	2026-05-14 09:47:49.909516-05	[]	[]
343	anonymous	admin	WITH RECURSIVE numeros AS (\n      SELECT 1 AS n UNION ALL SELECT n+1 FROM numeros WHERE n < 8\n    ) SELECT GROUP_CONCAT(n) AS secuencia, SUM(n) AS total FROM numeros	WITH RECURSIVE numeros AS (\n      SELECT 1 AS n UNION ALL SELECT n+1 FROM numeros WHERE n < 8\n    ) SELECT GROUP_CONCAT(n) AS secuencia, SUM(n) AS total FROM numeros	2	1	f	ok	2026-05-14 09:47:51.463186-05	[]	[]
344	anonymous	admin	SELECT d.nombre AS depto,\n      COUNT(e.id) AS num_empleados,\n      JSON_ARRAYAGG(JSON_OBJECT('nombre', e.nombre, 'salario', e.salario)) AS empleados_json,\n      (SELECT MAX(salario) FROM empleados WHERE departamento_id = d.id) AS salario_max\n    FROM departamentos d LEFT JOIN empleados e ON e.departamento_id = d.id\n    GROUP BY d.id, d.nombre HAVING num_empleados > 0	SELECT d.nombre AS depto,\n      COUNT(e.id) AS num_empleados,\n      JSON_ARRAYAGG(JSON_OBJECT('nombre', e.nombre, 'salario', e.salario)) AS empleados_json,\n      (SELECT MAX(salario) FROM empleados WHERE departamento_id = d.id) AS salario_max\n    FROM departamentos d LEFT JOIN empleados e ON e.departamento_id = d.id\n    GROUP BY d.id, d.nombre HAVING num_empleados > 0	3	5	f	ok	2026-05-14 09:47:51.470744-05	[]	[]
345	anonymous	admin	SELECT e.nombre, e.salario, e.departamento_id,\n      (SELECT COUNT(*) FROM empleados e2 WHERE e2.departamento_id = e.departamento_id AND e2.salario > e.salario) AS empleados_mejor_pagados,\n      (SELECT ROUND(AVG(salario),2) FROM empleados e3 WHERE e3.departamento_id = e.departamento_id) AS promedio_depto,\n      CASE\n        WHEN e.salario > (SELECT AVG(salario) FROM empleados WHERE departamento_id = e.departamento_id) * 1.2 THEN 'Top'\n        WHEN e.salario < (SELECT AVG(salario) FROM empleados WHERE departamento_id = e.departamento_id) * 0.8 THEN 'Bajo'\n        ELSE 'Medio'\n      END AS banda_salarial\n    FROM empleados e WHERE e.activo = 1 ORDER BY e.salario DESC LIMIT 8	SELECT e.nombre, e.salario, e.departamento_id,\n      (SELECT COUNT(*) FROM empleados e2 WHERE e2.departamento_id = e.departamento_id AND e2.salario > e.salario) AS empleados_mejor_pagados,\n      (SELECT ROUND(AVG(salario),2) FROM empleados e3 WHERE e3.departamento_id = e.departamento_id) AS promedio_depto,\n      CASE\n        WHEN e.salario > (SELECT AVG(salario) FROM empleados WHERE departamento_id = e.departamento_id) * 1.2 THEN 'Top'\n        WHEN e.salario < (SELECT AVG(salario) FROM empleados WHERE departamento_id = e.departamento_id) * 0.8 THEN 'Bajo'\n        ELSE 'Medio'\n      END AS banda_salarial\n    FROM empleados e WHERE e.activo = 1 ORDER BY e.salario DESC LIMIT 8	3	8	f	ok	2026-05-14 09:47:51.478721-05	[]	[]
346	anonymous	admin	SELECT origen, nombre, monto FROM (\n      SELECT 'empleado' AS origen, nombre, salario AS monto FROM empleados WHERE activo=1\n      UNION ALL\n      SELECT 'producto' AS origen, nombre, precio AS monto FROM productos WHERE activo=1\n    ) t ORDER BY monto DESC LIMIT 10	SELECT origen, nombre, monto FROM (\n      SELECT 'empleado' AS origen, nombre, salario AS monto FROM empleados WHERE activo=1\n      UNION ALL\n      SELECT 'producto' AS origen, nombre, precio AS monto FROM productos WHERE activo=1\n    ) t ORDER BY monto DESC LIMIT 10	2	10	f	ok	2026-05-14 09:47:51.483788-05	[]	[]
347	anonymous	admin	SELECT LEVEL AS nivel, LPAD(' ', LEVEL*2) || 'nodo ' || LEVEL AS arbol\n    FROM dual CONNECT BY LEVEL <= 6	SELECT LEVEL AS nivel, LPAD(' ', LEVEL*2) || 'nodo ' || LEVEL AS arbol\n    FROM dual CONNECT BY LEVEL <= 6	3	6	f	ok	2026-05-14 09:47:51.491451-05	[]	[]
348	anonymous	admin	SELECT nombre, salario, departamento,\n      LAG(salario) OVER (PARTITION BY departamento ORDER BY salario) AS sal_anterior,\n      LEAD(salario) OVER (PARTITION BY departamento ORDER BY salario) AS sal_siguiente,\n      ROUND(RATIO_TO_REPORT(salario) OVER (PARTITION BY departamento) * 100, 2) AS pct_del_depto,\n      SUM(salario) OVER (PARTITION BY departamento ORDER BY salario ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS acumulado\n    FROM empleados ORDER BY departamento, salario	SELECT nombre, salario, departamento,\n      LAG(salario) OVER (PARTITION BY departamento ORDER BY salario) AS sal_anterior,\n      LEAD(salario) OVER (PARTITION BY departamento ORDER BY salario) AS sal_siguiente,\n      ROUND(RATIO_TO_REPORT(salario) OVER (PARTITION BY departamento) * 100, 2) AS pct_del_depto,\n      SUM(salario) OVER (PARTITION BY departamento ORDER BY salario ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS acumulado\n    FROM empleados ORDER BY departamento, salario	3	7	f	ok	2026-05-14 09:47:51.498549-05	[]	[]
349	anonymous	admin	SELECT * FROM (\n      SELECT departamento, salario FROM empleados\n    ) PIVOT (\n      COUNT(*) AS cnt, AVG(salario) AS prom\n      FOR departamento IN ('TI' AS ti, 'Ventas' AS ventas, 'RRHH' AS rrhh, 'Gerencia' AS gerencia)\n    )	SELECT * FROM (\n      SELECT departamento, salario FROM empleados\n    ) PIVOT (\n      COUNT(*) AS cnt, AVG(salario) AS prom\n      FOR departamento IN ('TI' AS ti, 'Ventas' AS ventas, 'RRHH' AS rrhh, 'Gerencia' AS gerencia)\n    )	2	1	f	ok	2026-05-14 09:47:51.504565-05	[]	[]
350	anonymous	admin	WITH ventas_agg AS (\n      SELECT producto_id, COUNT(*) AS num_ventas, SUM(total) AS ingresos, SUM(cantidad) AS unidades\n      FROM ventas GROUP BY producto_id\n    ), con_ranking AS (\n      SELECT va.*, p.nombre, p.categoria, p.precio,\n        RANK() OVER (ORDER BY va.ingresos DESC) AS rank_global,\n        ROUND(va.ingresos / NULLIF(va.unidades,0), 2) AS precio_promedio_venta\n      FROM ventas_agg va JOIN productos p ON p.id = va.producto_id\n    )\n    SELECT rank_global, nombre, categoria, num_ventas, unidades, ingresos, precio_promedio_venta,\n      CASE WHEN precio_promedio_venta > precio THEN 'sobre lista' ELSE 'bajo lista' END AS comparacion\n    FROM con_ranking ORDER BY rank_global	WITH ventas_agg AS (\n      SELECT producto_id, COUNT(*) AS num_ventas, SUM(total) AS ingresos, SUM(cantidad) AS unidades\n      FROM ventas GROUP BY producto_id\n    ), con_ranking AS (\n      SELECT va.*, p.nombre, p.categoria, p.precio,\n        RANK() OVER (ORDER BY va.ingresos DESC) AS rank_global,\n        ROUND(va.ingresos / NULLIF(va.unidades,0), 2) AS precio_promedio_venta\n      FROM ventas_agg va JOIN productos p ON p.id = va.producto_id\n    )\n    SELECT rank_global, nombre, categoria, num_ventas, unidades, ingresos, precio_promedio_venta,\n      CASE WHEN precio_promedio_venta > precio THEN 'sobre lista' ELSE 'bajo lista' END AS comparacion\n    FROM con_ranking ORDER BY rank_global	3	4	f	ok	2026-05-14 09:47:51.51246-05	[]	[]
357	anonymous	admin	SELECT visible_para, COUNT(*) AS total FROM knowledge_base GROUP BY visible_para ORDER BY total DESC	SELECT visible_para, COUNT(*) AS total FROM knowledge_base GROUP BY visible_para ORDER BY total DESC	2	2	f	ok	2026-05-14 09:49:05.135626-05	[]	[]
351	anonymous	admin	{"collection":"empleados","aggregate":[{"$facet":{"por_departamento":[{"$group":{"_id":"$departamento","total":{"$sum":1},"salario_prom":{"$avg":"$salario"}}}],"por_estado":[{"$group":{"_id":"$activo","total":{"$sum":1}}}],"rango_salarial":[{"$bucket":{"groupBy":"$salario","boundaries":[0,3000,4000,5000,10000],"default":"otros","output":{"count":{"$sum":1},"empleados":{"$push":"$nombre"}}}}]}}]}	{"collection":"empleados","aggregate":[{"$facet":{"por_departamento":[{"$group":{"_id":"$departamento","total":{"$sum":1},"salario_prom":{"$avg":"$salario"}}}],"por_estado":[{"$group":{"_id":"$activo","total":{"$sum":1}}}],"rango_salarial":[{"$bucket":{"groupBy":"$salario","boundaries":[0,3000,4000,5000,10000],"default":"otros","output":{"count":{"$sum":1},"empleados":{"$push":"$nombre"}}}}]}}]}	3	1	f	ok	2026-05-14 09:47:51.519191-05	[]	[]
352	anonymous	admin	{"collection":"ventas","aggregate":[{"$lookup":{"from":"productos","localField":"producto_codigo","foreignField":"codigo","as":"producto"}},{"$unwind":"$producto"},{"$group":{"_id":{"categoria":"$producto.categoria","vendedor":"$vendedor"},"ingresos":{"$sum":"$total"},"unidades":{"$sum":"$cantidad"},"ventas":{"$sum":1}}},{"$sort":{"ingresos":-1}},{"$project":{"_id":0,"categoria":"$_id.categoria","vendedor":"$_id.vendedor","ingresos":1,"unidades":1,"ventas":1,"ticket_promedio":{"$round":[{"$divide":["$ingresos","$ventas"]},2]}}}]}	{"collection":"ventas","aggregate":[{"$lookup":{"from":"productos","localField":"producto_codigo","foreignField":"codigo","as":"producto"}},{"$unwind":"$producto"},{"$group":{"_id":{"categoria":"$producto.categoria","vendedor":"$vendedor"},"ingresos":{"$sum":"$total"},"unidades":{"$sum":"$cantidad"},"ventas":{"$sum":1}}},{"$sort":{"ingresos":-1}},{"$project":{"_id":0,"categoria":"$_id.categoria","vendedor":"$_id.vendedor","ingresos":1,"unidades":1,"ventas":1,"ticket_promedio":{"$round":[{"$divide":["$ingresos","$ventas"]},2]}}}]}	3	5	f	ok	2026-05-14 09:47:51.52612-05	[]	[]
353	anonymous	admin	{"collection":"conciliacion","aggregate":[{"$unwind":"$details"},{"$group":{"_id":"$details.transactionTypeSwitch","total_transacciones":{"$sum":1},"valor_total":{"$sum":"$details.transactionValueSwitch"},"reconciliados":{"$sum":"$details.reconciled"}}},{"$sort":{"valor_total":-1}}]}	{"collection":"conciliacion","aggregate":[{"$unwind":"$details"},{"$group":{"_id":"$details.transactionTypeSwitch","total_transacciones":{"$sum":1},"valor_total":{"$sum":"$details.transactionValueSwitch"},"reconciliados":{"$sum":"$details.reconciled"}}},{"$sort":{"valor_total":-1}}]}	3	3	f	ok	2026-05-14 09:47:51.532618-05	[]	[]
354	anonymous	admin	{"collection":"empleados","aggregate":[{"$addFields":{"categoria_salarial":{"$cond":[{"$gte":["$salario",4000]},"alto",{"$cond":[{"$gte":["$salario",3000]},"medio","bajo"]}]}}},{"$group":{"_id":{"depto":"$departamento","categoria":"$categoria_salarial"},"count":{"$sum":1},"nombres":{"$push":"$nombre"}}},{"$sort":{"count":-1}}]}	{"collection":"empleados","aggregate":[{"$addFields":{"categoria_salarial":{"$cond":[{"$gte":["$salario",4000]},"alto",{"$cond":[{"$gte":["$salario",3000]},"medio","bajo"]}]}}},{"$group":{"_id":{"depto":"$departamento","categoria":"$categoria_salarial"},"count":{"$sum":1},"nombres":{"$push":"$nombre"}}},{"$sort":{"count":-1}}]}	3	4	f	ok	2026-05-14 09:47:51.539647-05	[]	[]
358	anonymous	admin	WITH ranked AS (\n      SELECT id, titulo, categoria, ROW_NUMBER() OVER (PARTITION BY categoria ORDER BY created_at DESC) AS rn\n      FROM knowledge_base\n    ) SELECT categoria, COUNT(*) AS arts FROM ranked GROUP BY categoria ORDER BY arts DESC LIMIT 5	WITH ranked AS (\n      SELECT id, titulo, categoria, ROW_NUMBER() OVER (PARTITION BY categoria ORDER BY created_at DESC) AS rn\n      FROM knowledge_base\n    ) SELECT categoria, COUNT(*) AS arts FROM ranked GROUP BY categoria ORDER BY arts DESC LIMIT 5	4	0	f	error	2026-05-14 09:49:05.143065-05	[]	\N
359	anonymous	admin	SELECT k.titulo, k.categoria,\n      (SELECT COUNT(*) FROM article_executions ae WHERE ae.article_id = k.id) AS ejecuciones\n    FROM knowledge_base k\n    WHERE k.id IN (SELECT id FROM knowledge_base ORDER BY created_at DESC LIMIT 10)\n    ORDER BY ejecuciones DESC LIMIT 5	SELECT k.titulo, k.categoria,\n      (SELECT COUNT(*) FROM article_executions ae WHERE ae.article_id = k.id) AS ejecuciones\n    FROM knowledge_base k\n    WHERE k.id IN (SELECT id FROM knowledge_base ORDER BY created_at DESC LIMIT 10)\n    ORDER BY ejecuciones DESC LIMIT 5	2	0	f	error	2026-05-14 09:49:05.149756-05	[]	\N
360	anonymous	admin	SELECT 1+1 AS suma	SELECT 1+1 AS suma	4	1	f	ok	2026-05-14 09:49:05.15849-05	[]	[]
361	anonymous	admin	SELECT id, nombre, salario FROM empleados ORDER BY salario DESC LIMIT 3	SELECT id, nombre, salario FROM empleados ORDER BY salario DESC LIMIT 3	3	3	f	ok	2026-05-14 09:49:05.1653-05	[]	[]
362	anonymous	admin	SELECT d.nombre AS depto, COUNT(e.id) AS emps, ROUND(AVG(e.salario),2) AS sal_prom\n    FROM departamentos d LEFT JOIN empleados e ON e.departamento_id = d.id\n    GROUP BY d.id, d.nombre HAVING COUNT(e.id) > 0 ORDER BY sal_prom DESC	SELECT d.nombre AS depto, COUNT(e.id) AS emps, ROUND(AVG(e.salario),2) AS sal_prom\n    FROM departamentos d LEFT JOIN empleados e ON e.departamento_id = d.id\n    GROUP BY d.id, d.nombre HAVING COUNT(e.id) > 0 ORDER BY sal_prom DESC	4	5	f	ok	2026-05-14 09:49:05.17294-05	[]	[]
363	anonymous	admin	SELECT nombre, departamento_id, salario,\n      RANK() OVER (PARTITION BY departamento_id ORDER BY salario DESC) AS rango_salarial\n    FROM empleados WHERE activo = 1 ORDER BY departamento_id, rango_salarial LIMIT 8	SELECT nombre, departamento_id, salario,\n      RANK() OVER (PARTITION BY departamento_id ORDER BY salario DESC) AS rango_salarial\n    FROM empleados WHERE activo = 1 ORDER BY departamento_id, rango_salarial LIMIT 8	4	8	f	ok	2026-05-14 09:49:05.180484-05	[]	[]
364	anonymous	admin	WITH dept_stats AS (\n      SELECT departamento_id, AVG(salario) AS avg_sal FROM empleados GROUP BY departamento_id\n    )\n    SELECT e.nombre, e.salario, ds.avg_sal,\n      CASE WHEN e.salario > ds.avg_sal THEN 'sobre promedio' ELSE 'bajo promedio' END AS clasificacion\n    FROM empleados e JOIN dept_stats ds ON ds.departamento_id = e.departamento_id\n    ORDER BY e.salario DESC LIMIT 6	WITH dept_stats AS (\n      SELECT departamento_id, AVG(salario) AS avg_sal FROM empleados GROUP BY departamento_id\n    )\n    SELECT e.nombre, e.salario, ds.avg_sal,\n      CASE WHEN e.salario > ds.avg_sal THEN 'sobre promedio' ELSE 'bajo promedio' END AS clasificacion\n    FROM empleados e JOIN dept_stats ds ON ds.departamento_id = e.departamento_id\n    ORDER BY e.salario DESC LIMIT 6	3	6	f	ok	2026-05-14 09:49:05.187715-05	[]	[]
365	anonymous	admin	SELECT\n      p.nombre AS producto, p.categoria,\n      COUNT(DISTINCT ped.id) AS num_pedidos,\n      COALESCE(SUM(ped.cantidad),0) AS unidades_vendidas,\n      COALESCE(SUM(ped.precio_total),0) AS ingresos,\n      (SELECT GROUP_CONCAT(DISTINCT emp.nombre) FROM pedidos pe\n        JOIN empleados emp ON emp.id = pe.empleado_id WHERE pe.producto_id = p.id) AS vendedores\n    FROM productos p\n    LEFT JOIN pedidos ped ON ped.producto_id = p.id\n    GROUP BY p.id, p.nombre, p.categoria\n    ORDER BY ingresos DESC LIMIT 5	SELECT\n      p.nombre AS producto, p.categoria,\n      COUNT(DISTINCT ped.id) AS num_pedidos,\n      COALESCE(SUM(ped.cantidad),0) AS unidades_vendidas,\n      COALESCE(SUM(ped.precio_total),0) AS ingresos,\n      (SELECT GROUP_CONCAT(DISTINCT emp.nombre) FROM pedidos pe\n        JOIN empleados emp ON emp.id = pe.empleado_id WHERE pe.producto_id = p.id) AS vendedores\n    FROM productos p\n    LEFT JOIN pedidos ped ON ped.producto_id = p.id\n    GROUP BY p.id, p.nombre, p.categoria\n    ORDER BY ingresos DESC LIMIT 5	4	5	f	ok	2026-05-14 09:49:05.195615-05	[]	[]
366	anonymous	admin	SELECT 1 AS n FROM dual	SELECT 1 AS n FROM dual	3	1	f	ok	2026-05-14 09:49:05.203279-05	[]	[]
367	anonymous	admin	SELECT id, nombre, salario FROM empleados ORDER BY salario DESC FETCH FIRST 3 ROWS ONLY	SELECT id, nombre, salario FROM empleados ORDER BY salario DESC FETCH FIRST 3 ROWS ONLY	3	3	f	ok	2026-05-14 09:49:05.210842-05	[]	[]
368	anonymous	admin	SELECT departamento, COUNT(*) AS emps, ROUND(AVG(salario),2) AS prom,\n      MAX(salario) AS max_sal, MIN(salario) AS min_sal\n    FROM empleados GROUP BY departamento ORDER BY prom DESC	SELECT departamento, COUNT(*) AS emps, ROUND(AVG(salario),2) AS prom,\n      MAX(salario) AS max_sal, MIN(salario) AS min_sal\n    FROM empleados GROUP BY departamento ORDER BY prom DESC	3	5	f	ok	2026-05-14 09:49:05.218308-05	[]	[]
369	anonymous	admin	SELECT v.cliente, p.nombre AS producto, v.total,\n      SUM(v.total) OVER (PARTITION BY v.cliente) AS total_cliente,\n      RANK() OVER (ORDER BY v.total DESC) AS rank_venta\n    FROM ventas v JOIN productos p ON p.id = v.producto_id\n    ORDER BY rank_venta FETCH FIRST 6 ROWS ONLY	SELECT v.cliente, p.nombre AS producto, v.total,\n      SUM(v.total) OVER (PARTITION BY v.cliente) AS total_cliente,\n      RANK() OVER (ORDER BY v.total DESC) AS rank_venta\n    FROM ventas v JOIN productos p ON p.id = v.producto_id\n    ORDER BY rank_venta FETCH FIRST 6 ROWS ONLY	4	4	f	ok	2026-05-14 09:49:05.226365-05	[]	[]
370	anonymous	admin	WITH ventas_por_producto AS (\n      SELECT producto_id, SUM(total) AS ingreso_total, SUM(cantidad) AS unidades\n      FROM ventas GROUP BY producto_id\n    ), ranking AS (\n      SELECT vp.*, p.nombre, p.categoria,\n        ROW_NUMBER() OVER (PARTITION BY p.categoria ORDER BY vp.ingreso_total DESC) AS rn\n      FROM ventas_por_producto vp JOIN productos p ON p.id = vp.producto_id\n    )\n    SELECT categoria, nombre, ingreso_total, unidades FROM ranking WHERE rn <= 2 ORDER BY categoria, ingreso_total DESC	WITH ventas_por_producto AS (\n      SELECT producto_id, SUM(total) AS ingreso_total, SUM(cantidad) AS unidades\n      FROM ventas GROUP BY producto_id\n    ), ranking AS (\n      SELECT vp.*, p.nombre, p.categoria,\n        ROW_NUMBER() OVER (PARTITION BY p.categoria ORDER BY vp.ingreso_total DESC) AS rn\n      FROM ventas_por_producto vp JOIN productos p ON p.id = vp.producto_id\n    )\n    SELECT categoria, nombre, ingreso_total, unidades FROM ranking WHERE rn <= 2 ORDER BY categoria, ingreso_total DESC	3	4	f	ok	2026-05-14 09:49:05.233404-05	[]	[]
371	anonymous	admin	{"collection":"empleados","find":{},"limit":3}	{"collection":"empleados","find":{},"limit":3}	4	3	f	ok	2026-05-14 09:49:05.240939-05	[]	[]
372	anonymous	admin	{"collection":"empleados","find":{"activo":true},"limit":5}	{"collection":"empleados","find":{"activo":true},"limit":5}	2	4	f	ok	2026-05-14 09:49:05.24656-05	[]	[]
373	anonymous	admin	{"collection":"empleados","aggregate":[{"$group":{"_id":"$departamento","total":{"$sum":1},"salario_prom":{"$avg":"$salario"}}},{"$sort":{"salario_prom":-1}}]}	{"collection":"empleados","aggregate":[{"$group":{"_id":"$departamento","total":{"$sum":1},"salario_prom":{"$avg":"$salario"}}},{"$sort":{"salario_prom":-1}}]}	3	3	f	ok	2026-05-14 09:49:05.254176-05	[]	[]
374	anonymous	admin	{"collection":"ventas","aggregate":[{"$group":{"_id":"$vendedor","ventas":{"$sum":1},"ingresos":{"$sum":"$total"},"ticket_prom":{"$avg":"$total"}}},{"$match":{"ingresos":{"$gt":0}}},{"$sort":{"ingresos":-1}},{"$limit":5}]}	{"collection":"ventas","aggregate":[{"$group":{"_id":"$vendedor","ventas":{"$sum":1},"ingresos":{"$sum":"$total"},"ticket_prom":{"$avg":"$total"}}},{"$match":{"ingresos":{"$gt":0}}},{"$sort":{"ingresos":-1}},{"$limit":5}]}	3	3	f	ok	2026-05-14 09:49:05.262053-05	[]	[]
375	anonymous	admin	{"collection":"ventas","aggregate":[{"$lookup":{"from":"productos","localField":"producto_codigo","foreignField":"codigo","as":"prod"}},{"$unwind":"$prod"},{"$group":{"_id":"$prod.categoria","ingresos":{"$sum":"$total"},"unidades":{"$sum":"$cantidad"}}},{"$sort":{"ingresos":-1}}]}	{"collection":"ventas","aggregate":[{"$lookup":{"from":"productos","localField":"producto_codigo","foreignField":"codigo","as":"prod"}},{"$unwind":"$prod"},{"$group":{"_id":"$prod.categoria","ingresos":{"$sum":"$total"},"unidades":{"$sum":"$cantidad"}}},{"$sort":{"ingresos":-1}}]}	3	4	f	ok	2026-05-14 09:49:05.268537-05	[]	[]
376	anonymous	admin	{"collection":"conciliacion","find":{},"limit":2}	{"collection":"conciliacion","find":{},"limit":2}	3	2	f	ok	2026-05-14 09:49:05.275385-05	[]	[]
377	anonymous	admin	{"collection":"productos","countDocuments":{}}	{"collection":"productos","countDocuments":{}}	3	1	f	ok	2026-05-14 09:49:05.281882-05	[]	[]
378	anonymous	admin	SELECT id, titulo, visible_para FROM knowledge_base WHERE visible_para = $1 LIMIT 3	SELECT id, titulo, visible_para FROM knowledge_base WHERE visible_para = $1 LIMIT 3	2	3	f	ok	2026-05-14 09:49:05.427046-05	["todos"]	[]
379	anonymous	admin	SELECT id, titulo FROM knowledge_base WHERE visible_para = $1 LIMIT $2	SELECT id, titulo FROM knowledge_base WHERE visible_para = $1 LIMIT $2	2	2	f	ok	2026-05-14 09:49:05.437975-05	["todos", 2]	[]
380	anonymous	admin	SELECT id, titulo, categoria FROM knowledge_base WHERE categoria = {{categoria}} LIMIT 3	SELECT id, titulo, categoria FROM knowledge_base WHERE categoria = {{categoria}} LIMIT 3	3	0	f	error	2026-05-14 09:49:05.446573-05	{"categoria": "General"}	\N
381	anonymous	admin	SELECT id, nombre, salario FROM empleados WHERE departamento_id = $1 ORDER BY salario DESC	SELECT id, nombre, salario FROM empleados WHERE departamento_id = $1 ORDER BY salario DESC	5	6	f	ok	2026-05-14 09:49:05.457203-05	[1]	[]
382	anonymous	admin	SELECT nombre, salario FROM empleados WHERE salario >= {{min_salario}} ORDER BY salario DESC	SELECT nombre, salario FROM empleados WHERE salario >= $1 ORDER BY salario DESC	4	5	f	ok	2026-05-14 09:49:05.464901-05	[3000]	["min_salario"]
383	anonymous	admin	SELECT nombre, salario FROM empleados WHERE salario BETWEEN $1 AND $2 ORDER BY salario	SELECT nombre, salario FROM empleados WHERE salario BETWEEN $1 AND $2 ORDER BY salario	3	9	f	ok	2026-05-14 09:49:05.472288-05	[2000, 4000]	[]
384	anonymous	admin	SELECT id, nombre, salario FROM empleados WHERE departamento = $1	SELECT id, nombre, salario FROM empleados WHERE departamento = $1	6	2	f	ok	2026-05-14 09:49:05.482189-05	["TI"]	[]
385	anonymous	admin	SELECT nombre, cargo, salario FROM empleados WHERE cargo = {{cargo}}	SELECT nombre, cargo, salario FROM empleados WHERE ($1 IS NULL OR cargo = $1)	2	1	f	ok	2026-05-14 09:49:05.488706-05	["Gerente"]	["cargo"]
386	anonymous	admin	SELECT nombre, salario FROM empleados WHERE salario > $1 OR salario = $1	SELECT nombre, salario FROM empleados WHERE salario > $1 OR salario = $1	3	1	f	ok	2026-05-14 09:49:05.495302-05	[5000]	[]
387	anonymous	admin	{"collection":"empleados","find":{"departamento":"{{dept}}"}}	{"collection":"empleados","find":{"departamento":"TI"}}	2	2	f	ok	2026-05-14 09:49:05.501894-05	[]	[]
388	anonymous	admin	{"collection":"empleados","find":{"salario":{"$gte":{{min}}}}}	{"collection":"empleados","find":{"salario":{"$gte":3000}}}	3	2	f	ok	2026-05-14 09:49:05.508042-05	[]	[]
389	anonymous	admin	DO $$ BEGIN RAISE NOTICE %, (SELECT COUNT(*) FROM knowledge_base); END $$;	DO $$ BEGIN RAISE NOTICE %, (SELECT COUNT(*) FROM knowledge_base); END $$;	11	0	f	error	2026-05-14 09:49:05.614701-05	[]	\N
390	anonymous	admin	SELECT id, titulo FROM knowledge_base ORDER BY created_at LIMIT 2 FOR UPDATE	SELECT id, titulo FROM knowledge_base ORDER BY created_at LIMIT 2 FOR UPDATE	2	0	f	error	2026-05-14 09:49:05.684255-05	[]	\N
391	anonymous	admin	SELECT string_agg(column_name, ', ' ORDER BY ordinal_position) AS cols FROM information_schema.columns WHERE table_name='knowledge_base'	SELECT string_agg(column_name, ', ' ORDER BY ordinal_position) AS cols FROM information_schema.columns WHERE table_name='knowledge_base'	3	1	f	ok	2026-05-14 09:49:34.704851-05	[]	[]
392	anonymous	admin	SELECT id FROM knowledge_base ORDER BY created_at DESC LIMIT 3	SELECT id FROM knowledge_base ORDER BY created_at DESC LIMIT 3	2	0	f	error	2026-05-14 09:49:34.757897-05	[]	\N
393	anonymous	admin	WITH r AS (SELECT id, ROW_NUMBER() OVER (ORDER BY created_at DESC) rn FROM knowledge_base) SELECT COUNT(*) FROM r	WITH r AS (SELECT id, ROW_NUMBER() OVER (ORDER BY created_at DESC) rn FROM knowledge_base) SELECT COUNT(*) FROM r	2	0	f	error	2026-05-14 09:49:34.821849-05	[]	\N
395	anonymous	admin	SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name	SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name	2	13	f	ok	2026-05-14 09:50:50.269161-05	[]	[]
396	anonymous	admin	SELECT visible_para, COUNT(*) AS total FROM knowledge_base GROUP BY visible_para ORDER BY total DESC	SELECT visible_para, COUNT(*) AS total FROM knowledge_base GROUP BY visible_para ORDER BY total DESC	3	2	f	ok	2026-05-14 09:50:50.278128-05	[]	[]
397	anonymous	admin	WITH ranked AS (\n      SELECT id, titulo, categoria, ROW_NUMBER() OVER (PARTITION BY categoria ORDER BY fecha DESC) AS rn\n      FROM knowledge_base\n    ) SELECT categoria, COUNT(*) AS arts FROM ranked GROUP BY categoria ORDER BY arts DESC LIMIT 5	WITH ranked AS (\n      SELECT id, titulo, categoria, ROW_NUMBER() OVER (PARTITION BY categoria ORDER BY fecha DESC) AS rn\n      FROM knowledge_base\n    ) SELECT categoria, COUNT(*) AS arts FROM ranked GROUP BY categoria ORDER BY arts DESC LIMIT 5	3	5	f	ok	2026-05-14 09:50:50.285495-05	[]	[]
398	anonymous	admin	SELECT k.titulo, k.categoria,\n      (SELECT COUNT(*) FROM article_executions ae WHERE ae.article_id = k.id) AS ejecuciones\n    FROM knowledge_base k\n    WHERE k.id IN (SELECT id FROM knowledge_base ORDER BY fecha DESC LIMIT 10)\n    ORDER BY ejecuciones DESC LIMIT 5	SELECT k.titulo, k.categoria,\n      (SELECT COUNT(*) FROM article_executions ae WHERE ae.article_id = k.id) AS ejecuciones\n    FROM knowledge_base k\n    WHERE k.id IN (SELECT id FROM knowledge_base ORDER BY fecha DESC LIMIT 10)\n    ORDER BY ejecuciones DESC LIMIT 5	4	0	f	error	2026-05-14 09:50:50.293044-05	[]	\N
399	anonymous	admin	SELECT 1+1 AS suma	SELECT 1+1 AS suma	2	1	f	ok	2026-05-14 09:50:50.300391-05	[]	[]
400	anonymous	admin	SELECT id, nombre, salario FROM empleados ORDER BY salario DESC LIMIT 3	SELECT id, nombre, salario FROM empleados ORDER BY salario DESC LIMIT 3	2	3	f	ok	2026-05-14 09:50:50.306821-05	[]	[]
401	anonymous	admin	SELECT d.nombre AS depto, COUNT(e.id) AS emps, ROUND(AVG(e.salario),2) AS sal_prom\n    FROM departamentos d LEFT JOIN empleados e ON e.departamento_id = d.id\n    GROUP BY d.id, d.nombre HAVING COUNT(e.id) > 0 ORDER BY sal_prom DESC	SELECT d.nombre AS depto, COUNT(e.id) AS emps, ROUND(AVG(e.salario),2) AS sal_prom\n    FROM departamentos d LEFT JOIN empleados e ON e.departamento_id = d.id\n    GROUP BY d.id, d.nombre HAVING COUNT(e.id) > 0 ORDER BY sal_prom DESC	2	5	f	ok	2026-05-14 09:50:50.313269-05	[]	[]
402	anonymous	admin	SELECT nombre, departamento_id, salario,\n      RANK() OVER (PARTITION BY departamento_id ORDER BY salario DESC) AS rango_salarial\n    FROM empleados WHERE activo = 1 ORDER BY departamento_id, rango_salarial LIMIT 8	SELECT nombre, departamento_id, salario,\n      RANK() OVER (PARTITION BY departamento_id ORDER BY salario DESC) AS rango_salarial\n    FROM empleados WHERE activo = 1 ORDER BY departamento_id, rango_salarial LIMIT 8	2	8	f	ok	2026-05-14 09:50:50.319463-05	[]	[]
403	anonymous	admin	WITH dept_stats AS (\n      SELECT departamento_id, AVG(salario) AS avg_sal FROM empleados GROUP BY departamento_id\n    )\n    SELECT e.nombre, e.salario, ds.avg_sal,\n      CASE WHEN e.salario > ds.avg_sal THEN 'sobre promedio' ELSE 'bajo promedio' END AS clasificacion\n    FROM empleados e JOIN dept_stats ds ON ds.departamento_id = e.departamento_id\n    ORDER BY e.salario DESC LIMIT 6	WITH dept_stats AS (\n      SELECT departamento_id, AVG(salario) AS avg_sal FROM empleados GROUP BY departamento_id\n    )\n    SELECT e.nombre, e.salario, ds.avg_sal,\n      CASE WHEN e.salario > ds.avg_sal THEN 'sobre promedio' ELSE 'bajo promedio' END AS clasificacion\n    FROM empleados e JOIN dept_stats ds ON ds.departamento_id = e.departamento_id\n    ORDER BY e.salario DESC LIMIT 6	2	6	f	ok	2026-05-14 09:50:50.32691-05	[]	[]
404	anonymous	admin	SELECT\n      p.nombre AS producto, p.categoria,\n      COUNT(DISTINCT ped.id) AS num_pedidos,\n      COALESCE(SUM(ped.cantidad),0) AS unidades_vendidas,\n      COALESCE(SUM(ped.precio_total),0) AS ingresos,\n      (SELECT GROUP_CONCAT(DISTINCT emp.nombre) FROM pedidos pe\n        JOIN empleados emp ON emp.id = pe.empleado_id WHERE pe.producto_id = p.id) AS vendedores\n    FROM productos p\n    LEFT JOIN pedidos ped ON ped.producto_id = p.id\n    GROUP BY p.id, p.nombre, p.categoria\n    ORDER BY ingresos DESC LIMIT 5	SELECT\n      p.nombre AS producto, p.categoria,\n      COUNT(DISTINCT ped.id) AS num_pedidos,\n      COALESCE(SUM(ped.cantidad),0) AS unidades_vendidas,\n      COALESCE(SUM(ped.precio_total),0) AS ingresos,\n      (SELECT GROUP_CONCAT(DISTINCT emp.nombre) FROM pedidos pe\n        JOIN empleados emp ON emp.id = pe.empleado_id WHERE pe.producto_id = p.id) AS vendedores\n    FROM productos p\n    LEFT JOIN pedidos ped ON ped.producto_id = p.id\n    GROUP BY p.id, p.nombre, p.categoria\n    ORDER BY ingresos DESC LIMIT 5	2	5	f	ok	2026-05-14 09:50:50.333275-05	[]	[]
405	anonymous	admin	SELECT 1 AS n FROM dual	SELECT 1 AS n FROM dual	42	1	f	ok	2026-05-14 09:50:50.378964-05	[]	[]
406	anonymous	admin	SELECT id, nombre, salario FROM empleados ORDER BY salario DESC FETCH FIRST 3 ROWS ONLY	SELECT id, nombre, salario FROM empleados ORDER BY salario DESC FETCH FIRST 3 ROWS ONLY	3	3	f	ok	2026-05-14 09:50:50.384955-05	[]	[]
407	anonymous	admin	SELECT departamento, COUNT(*) AS emps, ROUND(AVG(salario),2) AS prom,\n      MAX(salario) AS max_sal, MIN(salario) AS min_sal\n    FROM empleados GROUP BY departamento ORDER BY prom DESC	SELECT departamento, COUNT(*) AS emps, ROUND(AVG(salario),2) AS prom,\n      MAX(salario) AS max_sal, MIN(salario) AS min_sal\n    FROM empleados GROUP BY departamento ORDER BY prom DESC	3	5	f	ok	2026-05-14 09:50:50.391834-05	[]	[]
408	anonymous	admin	SELECT v.cliente, p.nombre AS producto, v.total,\n      SUM(v.total) OVER (PARTITION BY v.cliente) AS total_cliente,\n      RANK() OVER (ORDER BY v.total DESC) AS rank_venta\n    FROM ventas v JOIN productos p ON p.id = v.producto_id\n    ORDER BY rank_venta FETCH FIRST 6 ROWS ONLY	SELECT v.cliente, p.nombre AS producto, v.total,\n      SUM(v.total) OVER (PARTITION BY v.cliente) AS total_cliente,\n      RANK() OVER (ORDER BY v.total DESC) AS rank_venta\n    FROM ventas v JOIN productos p ON p.id = v.producto_id\n    ORDER BY rank_venta FETCH FIRST 6 ROWS ONLY	4	4	f	ok	2026-05-14 09:50:50.398355-05	[]	[]
409	anonymous	admin	WITH ventas_por_producto AS (\n      SELECT producto_id, SUM(total) AS ingreso_total, SUM(cantidad) AS unidades\n      FROM ventas GROUP BY producto_id\n    ), ranking AS (\n      SELECT vp.*, p.nombre, p.categoria,\n        ROW_NUMBER() OVER (PARTITION BY p.categoria ORDER BY vp.ingreso_total DESC) AS rn\n      FROM ventas_por_producto vp JOIN productos p ON p.id = vp.producto_id\n    )\n    SELECT categoria, nombre, ingreso_total, unidades FROM ranking WHERE rn <= 2 ORDER BY categoria, ingreso_total DESC	WITH ventas_por_producto AS (\n      SELECT producto_id, SUM(total) AS ingreso_total, SUM(cantidad) AS unidades\n      FROM ventas GROUP BY producto_id\n    ), ranking AS (\n      SELECT vp.*, p.nombre, p.categoria,\n        ROW_NUMBER() OVER (PARTITION BY p.categoria ORDER BY vp.ingreso_total DESC) AS rn\n      FROM ventas_por_producto vp JOIN productos p ON p.id = vp.producto_id\n    )\n    SELECT categoria, nombre, ingreso_total, unidades FROM ranking WHERE rn <= 2 ORDER BY categoria, ingreso_total DESC	4	4	f	ok	2026-05-14 09:50:50.405454-05	[]	[]
410	anonymous	admin	{"collection":"empleados","find":{},"limit":3}	{"collection":"empleados","find":{},"limit":3}	3	3	f	ok	2026-05-14 09:50:50.412574-05	[]	[]
411	anonymous	admin	{"collection":"empleados","find":{"activo":true},"limit":5}	{"collection":"empleados","find":{"activo":true},"limit":5}	2	4	f	ok	2026-05-14 09:50:50.418443-05	[]	[]
412	anonymous	admin	{"collection":"empleados","aggregate":[{"$group":{"_id":"$departamento","total":{"$sum":1},"salario_prom":{"$avg":"$salario"}}},{"$sort":{"salario_prom":-1}}]}	{"collection":"empleados","aggregate":[{"$group":{"_id":"$departamento","total":{"$sum":1},"salario_prom":{"$avg":"$salario"}}},{"$sort":{"salario_prom":-1}}]}	3	3	f	ok	2026-05-14 09:50:50.425333-05	[]	[]
413	anonymous	admin	{"collection":"ventas","aggregate":[{"$group":{"_id":"$vendedor","ventas":{"$sum":1},"ingresos":{"$sum":"$total"},"ticket_prom":{"$avg":"$total"}}},{"$match":{"ingresos":{"$gt":0}}},{"$sort":{"ingresos":-1}},{"$limit":5}]}	{"collection":"ventas","aggregate":[{"$group":{"_id":"$vendedor","ventas":{"$sum":1},"ingresos":{"$sum":"$total"},"ticket_prom":{"$avg":"$total"}}},{"$match":{"ingresos":{"$gt":0}}},{"$sort":{"ingresos":-1}},{"$limit":5}]}	3	3	f	ok	2026-05-14 09:50:50.431028-05	[]	[]
414	anonymous	admin	{"collection":"ventas","aggregate":[{"$lookup":{"from":"productos","localField":"producto_codigo","foreignField":"codigo","as":"prod"}},{"$unwind":"$prod"},{"$group":{"_id":"$prod.categoria","ingresos":{"$sum":"$total"},"unidades":{"$sum":"$cantidad"}}},{"$sort":{"ingresos":-1}}]}	{"collection":"ventas","aggregate":[{"$lookup":{"from":"productos","localField":"producto_codigo","foreignField":"codigo","as":"prod"}},{"$unwind":"$prod"},{"$group":{"_id":"$prod.categoria","ingresos":{"$sum":"$total"},"unidades":{"$sum":"$cantidad"}}},{"$sort":{"ingresos":-1}}]}	3	4	f	ok	2026-05-14 09:50:50.436667-05	[]	[]
415	anonymous	admin	{"collection":"conciliacion","find":{},"limit":2}	{"collection":"conciliacion","find":{},"limit":2}	2	2	f	ok	2026-05-14 09:50:50.442166-05	[]	[]
416	anonymous	admin	{"collection":"productos","countDocuments":{}}	{"collection":"productos","countDocuments":{}}	3	1	f	ok	2026-05-14 09:50:50.448617-05	[]	[]
417	anonymous	admin	SELECT id, titulo, visible_para FROM knowledge_base WHERE visible_para = $1 LIMIT 3	SELECT id, titulo, visible_para FROM knowledge_base WHERE visible_para = $1 LIMIT 3	2	3	f	ok	2026-05-14 09:50:50.547939-05	["todos"]	[]
418	anonymous	admin	SELECT id, titulo FROM knowledge_base WHERE visible_para = $1 LIMIT $2	SELECT id, titulo FROM knowledge_base WHERE visible_para = $1 LIMIT $2	2	2	f	ok	2026-05-14 09:50:50.55954-05	["todos", 2]	[]
419	anonymous	admin	SELECT id, titulo, categoria FROM knowledge_base WHERE categoria = {{categoria}} LIMIT 3	SELECT id, titulo, categoria FROM knowledge_base WHERE categoria = {{categoria}} LIMIT 3	2	0	f	error	2026-05-14 09:50:50.565287-05	{"categoria": "General"}	\N
420	anonymous	admin	SELECT id, nombre, salario FROM empleados WHERE departamento_id = $1 ORDER BY salario DESC	SELECT id, nombre, salario FROM empleados WHERE departamento_id = $1 ORDER BY salario DESC	1	6	f	ok	2026-05-14 09:50:50.571365-05	[1]	[]
421	anonymous	admin	SELECT nombre, salario FROM empleados WHERE salario >= {{min_salario}} ORDER BY salario DESC	SELECT nombre, salario FROM empleados WHERE salario >= $1 ORDER BY salario DESC	2	5	f	ok	2026-05-14 09:50:50.5774-05	[3000]	["min_salario"]
422	anonymous	admin	SELECT nombre, salario FROM empleados WHERE salario BETWEEN $1 AND $2 ORDER BY salario	SELECT nombre, salario FROM empleados WHERE salario BETWEEN $1 AND $2 ORDER BY salario	1	9	f	ok	2026-05-14 09:50:50.582957-05	[2000, 4000]	[]
423	anonymous	admin	SELECT id, nombre, salario FROM empleados WHERE departamento = $1	SELECT id, nombre, salario FROM empleados WHERE departamento = $1	3	2	f	ok	2026-05-14 09:50:50.590262-05	["TI"]	[]
424	anonymous	admin	SELECT nombre, cargo, salario FROM empleados WHERE cargo = {{cargo}}	SELECT nombre, cargo, salario FROM empleados WHERE ($1 IS NULL OR cargo = $1)	2	1	f	ok	2026-05-14 09:50:50.595654-05	["Gerente"]	["cargo"]
425	anonymous	admin	SELECT nombre, salario FROM empleados WHERE salario > $1 OR salario = $1	SELECT nombre, salario FROM empleados WHERE salario > $1 OR salario = $1	2	1	f	ok	2026-05-14 09:50:50.601306-05	[5000]	[]
426	anonymous	admin	{"collection":"empleados","find":{"departamento":"{{dept}}"}}	{"collection":"empleados","find":{"departamento":"TI"}}	3	2	f	ok	2026-05-14 09:50:50.607582-05	[]	[]
427	anonymous	admin	{"collection":"empleados","find":{"salario":{"$gte":{{min}}}}}	{"collection":"empleados","find":{"salario":{"$gte":3000}}}	2	2	f	ok	2026-05-14 09:50:50.613398-05	[]	[]
428	anonymous	admin	WITH RECURSIVE serie(n) AS (\n      SELECT 1 UNION ALL SELECT n+1 FROM serie WHERE n < 10\n    ) SELECT array_agg(n) AS numeros, SUM(n) AS suma, AVG(n)::numeric(10,2) AS promedio FROM serie	WITH RECURSIVE serie(n) AS (\n      SELECT 1 UNION ALL SELECT n+1 FROM serie WHERE n < 10\n    ) SELECT array_agg(n) AS numeros, SUM(n) AS suma, AVG(n)::numeric(10,2) AS promedio FROM serie	2	1	f	ok	2026-05-14 09:50:50.719581-05	[]	[]
429	anonymous	admin	SELECT categoria,\n      COUNT(*) AS total,\n      COUNT(*) FILTER (WHERE visible_para = 'todos') AS publicos,\n      COUNT(*) FILTER (WHERE visible_para != 'todos') AS restringidos,\n      ROUND(100.0 * COUNT(*) FILTER (WHERE visible_para='todos') / NULLIF(COUNT(*),0), 1) AS pct_publico,\n      DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS rank_categoria\n    FROM knowledge_base GROUP BY categoria ORDER BY total DESC	SELECT categoria,\n      COUNT(*) AS total,\n      COUNT(*) FILTER (WHERE visible_para = 'todos') AS publicos,\n      COUNT(*) FILTER (WHERE visible_para != 'todos') AS restringidos,\n      ROUND(100.0 * COUNT(*) FILTER (WHERE visible_para='todos') / NULLIF(COUNT(*),0), 1) AS pct_publico,\n      DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS rank_categoria\n    FROM knowledge_base GROUP BY categoria ORDER BY total DESC	2	11	f	ok	2026-05-14 09:50:50.730901-05	[]	[]
430	anonymous	admin	WITH RECURSIVE numeros AS (\n      SELECT 1 AS n UNION ALL SELECT n+1 FROM numeros WHERE n < 8\n    ) SELECT GROUP_CONCAT(n) AS secuencia, SUM(n) AS total FROM numeros	WITH RECURSIVE numeros AS (\n      SELECT 1 AS n UNION ALL SELECT n+1 FROM numeros WHERE n < 8\n    ) SELECT GROUP_CONCAT(n) AS secuencia, SUM(n) AS total FROM numeros	2	1	f	ok	2026-05-14 09:50:52.151682-05	[]	[]
431	anonymous	admin	SELECT d.nombre AS depto,\n      COUNT(e.id) AS num_empleados,\n      JSON_ARRAYAGG(JSON_OBJECT('nombre', e.nombre, 'salario', e.salario)) AS empleados_json,\n      (SELECT MAX(salario) FROM empleados WHERE departamento_id = d.id) AS salario_max\n    FROM departamentos d LEFT JOIN empleados e ON e.departamento_id = d.id\n    GROUP BY d.id, d.nombre HAVING num_empleados > 0	SELECT d.nombre AS depto,\n      COUNT(e.id) AS num_empleados,\n      JSON_ARRAYAGG(JSON_OBJECT('nombre', e.nombre, 'salario', e.salario)) AS empleados_json,\n      (SELECT MAX(salario) FROM empleados WHERE departamento_id = d.id) AS salario_max\n    FROM departamentos d LEFT JOIN empleados e ON e.departamento_id = d.id\n    GROUP BY d.id, d.nombre HAVING num_empleados > 0	1	5	f	ok	2026-05-14 09:50:52.15719-05	[]	[]
468	anonymous	admin	UPDATE empleados SET salario = salario - 100 WHERE id IN (1, 3)	UPDATE empleados SET salario = salario - 100 WHERE id IN (1, 3)	20	0	f	ok	2026-05-14 11:14:51.643472-05	[]	[]
469	anonymous	admin	UPDATE empleados SET salario = salario + 1 WHERE id = -999	UPDATE empleados SET salario = salario + 1 WHERE id = -999	5	0	f	ok	2026-05-14 11:14:51.655764-05	[]	[]
432	anonymous	admin	SELECT e.nombre, e.salario, e.departamento_id,\n      (SELECT COUNT(*) FROM empleados e2 WHERE e2.departamento_id = e.departamento_id AND e2.salario > e.salario) AS empleados_mejor_pagados,\n      (SELECT ROUND(AVG(salario),2) FROM empleados e3 WHERE e3.departamento_id = e.departamento_id) AS promedio_depto,\n      CASE\n        WHEN e.salario > (SELECT AVG(salario) FROM empleados WHERE departamento_id = e.departamento_id) * 1.2 THEN 'Top'\n        WHEN e.salario < (SELECT AVG(salario) FROM empleados WHERE departamento_id = e.departamento_id) * 0.8 THEN 'Bajo'\n        ELSE 'Medio'\n      END AS banda_salarial\n    FROM empleados e WHERE e.activo = 1 ORDER BY e.salario DESC LIMIT 8	SELECT e.nombre, e.salario, e.departamento_id,\n      (SELECT COUNT(*) FROM empleados e2 WHERE e2.departamento_id = e.departamento_id AND e2.salario > e.salario) AS empleados_mejor_pagados,\n      (SELECT ROUND(AVG(salario),2) FROM empleados e3 WHERE e3.departamento_id = e.departamento_id) AS promedio_depto,\n      CASE\n        WHEN e.salario > (SELECT AVG(salario) FROM empleados WHERE departamento_id = e.departamento_id) * 1.2 THEN 'Top'\n        WHEN e.salario < (SELECT AVG(salario) FROM empleados WHERE departamento_id = e.departamento_id) * 0.8 THEN 'Bajo'\n        ELSE 'Medio'\n      END AS banda_salarial\n    FROM empleados e WHERE e.activo = 1 ORDER BY e.salario DESC LIMIT 8	2	8	f	ok	2026-05-14 09:50:52.162932-05	[]	[]
433	anonymous	admin	SELECT origen, nombre, monto FROM (\n      SELECT 'empleado' AS origen, nombre, salario AS monto FROM empleados WHERE activo=1\n      UNION ALL\n      SELECT 'producto' AS origen, nombre, precio AS monto FROM productos WHERE activo=1\n    ) t ORDER BY monto DESC LIMIT 10	SELECT origen, nombre, monto FROM (\n      SELECT 'empleado' AS origen, nombre, salario AS monto FROM empleados WHERE activo=1\n      UNION ALL\n      SELECT 'producto' AS origen, nombre, precio AS monto FROM productos WHERE activo=1\n    ) t ORDER BY monto DESC LIMIT 10	2	10	f	ok	2026-05-14 09:50:52.168481-05	[]	[]
434	anonymous	admin	SELECT LEVEL AS nivel, LPAD(' ', LEVEL*2) || 'nodo ' || LEVEL AS arbol\n    FROM dual CONNECT BY LEVEL <= 6	SELECT LEVEL AS nivel, LPAD(' ', LEVEL*2) || 'nodo ' || LEVEL AS arbol\n    FROM dual CONNECT BY LEVEL <= 6	2	6	f	ok	2026-05-14 09:50:52.174187-05	[]	[]
435	anonymous	admin	SELECT nombre, salario, departamento,\n      LAG(salario) OVER (PARTITION BY departamento ORDER BY salario) AS sal_anterior,\n      LEAD(salario) OVER (PARTITION BY departamento ORDER BY salario) AS sal_siguiente,\n      ROUND(RATIO_TO_REPORT(salario) OVER (PARTITION BY departamento) * 100, 2) AS pct_del_depto,\n      SUM(salario) OVER (PARTITION BY departamento ORDER BY salario ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS acumulado\n    FROM empleados ORDER BY departamento, salario	SELECT nombre, salario, departamento,\n      LAG(salario) OVER (PARTITION BY departamento ORDER BY salario) AS sal_anterior,\n      LEAD(salario) OVER (PARTITION BY departamento ORDER BY salario) AS sal_siguiente,\n      ROUND(RATIO_TO_REPORT(salario) OVER (PARTITION BY departamento) * 100, 2) AS pct_del_depto,\n      SUM(salario) OVER (PARTITION BY departamento ORDER BY salario ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS acumulado\n    FROM empleados ORDER BY departamento, salario	2	7	f	ok	2026-05-14 09:50:52.180274-05	[]	[]
436	anonymous	admin	SELECT * FROM (\n      SELECT departamento, salario FROM empleados\n    ) PIVOT (\n      COUNT(*) AS cnt, AVG(salario) AS prom\n      FOR departamento IN ('TI' AS ti, 'Ventas' AS ventas, 'RRHH' AS rrhh, 'Gerencia' AS gerencia)\n    )	SELECT * FROM (\n      SELECT departamento, salario FROM empleados\n    ) PIVOT (\n      COUNT(*) AS cnt, AVG(salario) AS prom\n      FOR departamento IN ('TI' AS ti, 'Ventas' AS ventas, 'RRHH' AS rrhh, 'Gerencia' AS gerencia)\n    )	2	1	f	ok	2026-05-14 09:50:52.186353-05	[]	[]
437	anonymous	admin	WITH ventas_agg AS (\n      SELECT producto_id, COUNT(*) AS num_ventas, SUM(total) AS ingresos, SUM(cantidad) AS unidades\n      FROM ventas GROUP BY producto_id\n    ), con_ranking AS (\n      SELECT va.*, p.nombre, p.categoria, p.precio,\n        RANK() OVER (ORDER BY va.ingresos DESC) AS rank_global,\n        ROUND(va.ingresos / NULLIF(va.unidades,0), 2) AS precio_promedio_venta\n      FROM ventas_agg va JOIN productos p ON p.id = va.producto_id\n    )\n    SELECT rank_global, nombre, categoria, num_ventas, unidades, ingresos, precio_promedio_venta,\n      CASE WHEN precio_promedio_venta > precio THEN 'sobre lista' ELSE 'bajo lista' END AS comparacion\n    FROM con_ranking ORDER BY rank_global	WITH ventas_agg AS (\n      SELECT producto_id, COUNT(*) AS num_ventas, SUM(total) AS ingresos, SUM(cantidad) AS unidades\n      FROM ventas GROUP BY producto_id\n    ), con_ranking AS (\n      SELECT va.*, p.nombre, p.categoria, p.precio,\n        RANK() OVER (ORDER BY va.ingresos DESC) AS rank_global,\n        ROUND(va.ingresos / NULLIF(va.unidades,0), 2) AS precio_promedio_venta\n      FROM ventas_agg va JOIN productos p ON p.id = va.producto_id\n    )\n    SELECT rank_global, nombre, categoria, num_ventas, unidades, ingresos, precio_promedio_venta,\n      CASE WHEN precio_promedio_venta > precio THEN 'sobre lista' ELSE 'bajo lista' END AS comparacion\n    FROM con_ranking ORDER BY rank_global	3	4	f	ok	2026-05-14 09:50:52.193315-05	[]	[]
438	anonymous	admin	{"collection":"empleados","aggregate":[{"$facet":{"por_departamento":[{"$group":{"_id":"$departamento","total":{"$sum":1},"salario_prom":{"$avg":"$salario"}}}],"por_estado":[{"$group":{"_id":"$activo","total":{"$sum":1}}}],"rango_salarial":[{"$bucket":{"groupBy":"$salario","boundaries":[0,3000,4000,5000,10000],"default":"otros","output":{"count":{"$sum":1},"empleados":{"$push":"$nombre"}}}}]}}]}	{"collection":"empleados","aggregate":[{"$facet":{"por_departamento":[{"$group":{"_id":"$departamento","total":{"$sum":1},"salario_prom":{"$avg":"$salario"}}}],"por_estado":[{"$group":{"_id":"$activo","total":{"$sum":1}}}],"rango_salarial":[{"$bucket":{"groupBy":"$salario","boundaries":[0,3000,4000,5000,10000],"default":"otros","output":{"count":{"$sum":1},"empleados":{"$push":"$nombre"}}}}]}}]}	2	1	f	ok	2026-05-14 09:50:52.199249-05	[]	[]
439	anonymous	admin	{"collection":"ventas","aggregate":[{"$lookup":{"from":"productos","localField":"producto_codigo","foreignField":"codigo","as":"producto"}},{"$unwind":"$producto"},{"$group":{"_id":{"categoria":"$producto.categoria","vendedor":"$vendedor"},"ingresos":{"$sum":"$total"},"unidades":{"$sum":"$cantidad"},"ventas":{"$sum":1}}},{"$sort":{"ingresos":-1}},{"$project":{"_id":0,"categoria":"$_id.categoria","vendedor":"$_id.vendedor","ingresos":1,"unidades":1,"ventas":1,"ticket_promedio":{"$round":[{"$divide":["$ingresos","$ventas"]},2]}}}]}	{"collection":"ventas","aggregate":[{"$lookup":{"from":"productos","localField":"producto_codigo","foreignField":"codigo","as":"producto"}},{"$unwind":"$producto"},{"$group":{"_id":{"categoria":"$producto.categoria","vendedor":"$vendedor"},"ingresos":{"$sum":"$total"},"unidades":{"$sum":"$cantidad"},"ventas":{"$sum":1}}},{"$sort":{"ingresos":-1}},{"$project":{"_id":0,"categoria":"$_id.categoria","vendedor":"$_id.vendedor","ingresos":1,"unidades":1,"ventas":1,"ticket_promedio":{"$round":[{"$divide":["$ingresos","$ventas"]},2]}}}]}	4	5	f	ok	2026-05-14 09:50:52.205674-05	[]	[]
470	anonymous	admin	INSERT INTO departamentos (nombre, ubicacion, presupuesto, activo) VALUES ('TEMP_DEL', 'X', 999, 1)	INSERT INTO departamentos (nombre, ubicacion, presupuesto, activo) VALUES ('TEMP_DEL', 'X', 999, 1)	14	0	f	ok	2026-05-14 11:14:51.674836-05	[]	[]
471	anonymous	admin	DELETE FROM departamentos WHERE nombre = 'TEMP_DEL'	DELETE FROM departamentos WHERE nombre = 'TEMP_DEL'	16	0	f	ok	2026-05-14 11:14:51.697007-05	[]	[]
440	anonymous	admin	{"collection":"conciliacion","aggregate":[{"$unwind":"$details"},{"$group":{"_id":"$details.transactionTypeSwitch","total_transacciones":{"$sum":1},"valor_total":{"$sum":"$details.transactionValueSwitch"},"reconciliados":{"$sum":"$details.reconciled"}}},{"$sort":{"valor_total":-1}}]}	{"collection":"conciliacion","aggregate":[{"$unwind":"$details"},{"$group":{"_id":"$details.transactionTypeSwitch","total_transacciones":{"$sum":1},"valor_total":{"$sum":"$details.transactionValueSwitch"},"reconciliados":{"$sum":"$details.reconciled"}}},{"$sort":{"valor_total":-1}}]}	2	3	f	ok	2026-05-14 09:50:52.211151-05	[]	[]
441	anonymous	admin	{"collection":"empleados","aggregate":[{"$addFields":{"categoria_salarial":{"$cond":[{"$gte":["$salario",4000]},"alto",{"$cond":[{"$gte":["$salario",3000]},"medio","bajo"]}]}}},{"$group":{"_id":{"depto":"$departamento","categoria":"$categoria_salarial"},"count":{"$sum":1},"nombres":{"$push":"$nombre"}}},{"$sort":{"count":-1}}]}	{"collection":"empleados","aggregate":[{"$addFields":{"categoria_salarial":{"$cond":[{"$gte":["$salario",4000]},"alto",{"$cond":[{"$gte":["$salario",3000]},"medio","bajo"]}]}}},{"$group":{"_id":{"depto":"$departamento","categoria":"$categoria_salarial"},"count":{"$sum":1},"nombres":{"$push":"$nombre"}}},{"$sort":{"count":-1}}]}	2	4	f	ok	2026-05-14 09:50:52.216797-05	[]	[]
442	anonymous	admin	SELECT table_name, column_name, data_type FROM information_schema.columns WHERE (table_name='article_executions' AND column_name='article_id') OR (table_name='knowledge_base' AND column_name='id')	SELECT table_name, column_name, data_type FROM information_schema.columns WHERE (table_name='article_executions' AND column_name='article_id') OR (table_name='knowledge_base' AND column_name='id')	44	2	f	ok	2026-05-14 09:51:47.126235-05	[]	[]
443	anonymous	admin	SELECT id, nombre, salario FROM empleados WHERE id IN (1,3)	SELECT id, nombre, salario FROM empleados WHERE id IN (1,3)	18	2	f	ok	2026-05-14 10:02:21.020973-05	[]	[]
444	anonymous	admin	UPDATE empleados SET salario = salario + 100 WHERE id IN (1, 3)	UPDATE empleados SET salario = salario + 100 WHERE id IN (1, 3)	22	0	f	ok	2026-05-14 10:02:21.061954-05	[]	[]
445	anonymous	admin	UPDATE empleados SET salario = salario - 100 WHERE id IN (1, 3)	UPDATE empleados SET salario = salario - 100 WHERE id IN (1, 3)	18	0	f	ok	2026-05-14 10:02:21.08543-05	[]	[]
446	anonymous	admin	SELECT id, titulo, categoria FROM knowledge_base LIMIT 1	SELECT id, titulo, categoria FROM knowledge_base LIMIT 1	4	1	f	ok	2026-05-14 10:02:21.094115-05	[]	[]
447	anonymous	admin	SELECT subcategoria FROM knowledge_base WHERE id = '2939eb64-5ff4-4e35-8937-003dc88b725c'	SELECT subcategoria FROM knowledge_base WHERE id = '2939eb64-5ff4-4e35-8937-003dc88b725c'	1	1	f	ok	2026-05-14 10:02:21.099348-05	[]	[]
448	anonymous	admin	UPDATE knowledge_base SET subcategoria = 'PRUEBA_TEMP' WHERE id = '2939eb64-5ff4-4e35-8937-003dc88b725c'	UPDATE knowledge_base SET subcategoria = 'PRUEBA_TEMP' WHERE id = '2939eb64-5ff4-4e35-8937-003dc88b725c'	6	1	f	ok	2026-05-14 10:02:21.109268-05	[]	[]
449	anonymous	admin	UPDATE empleados SET salario = salario + 1 WHERE id = -999	UPDATE empleados SET salario = salario + 1 WHERE id = -999	5	0	f	ok	2026-05-14 10:02:22.831057-05	[]	[]
450	anonymous	admin	INSERT INTO departamentos (nombre, ubicacion, presupuesto, activo) VALUES ('TEMP_TEST_DEPT', 'Test', 1000, 1)	INSERT INTO departamentos (nombre, ubicacion, presupuesto, activo) VALUES ('TEMP_TEST_DEPT', 'Test', 1000, 1)	16	0	f	ok	2026-05-14 10:02:22.852108-05	[]	[]
451	anonymous	admin	DELETE FROM departamentos WHERE nombre = 'TEMP_TEST_DEPT'	DELETE FROM departamentos WHERE nombre = 'TEMP_TEST_DEPT'	21	0	f	ok	2026-05-14 10:02:22.878716-05	[]	[]
452	anonymous	admin	UPDATE empleados SET salario = salario + 50 WHERE id = 1	UPDATE empleados SET salario = salario + 50 WHERE id = 1	67	0	f	ok	2026-05-14 10:02:22.950114-05	[]	[]
453	anonymous	admin	UPDATE empleados SET salario = salario - 50 WHERE id = 1	UPDATE empleados SET salario = salario - 50 WHERE id = 1	10	0	f	ok	2026-05-14 10:02:22.963375-05	[]	[]
454	anonymous	admin	UPDATE empleados SET salario = salario + 100 WHERE id IN (1, 3)	UPDATE empleados SET salario = salario + 100 WHERE id IN (1, 3)	53	0	f	ok	2026-05-14 11:12:55.77711-05	[]	[]
455	anonymous	admin	UPDATE empleados SET salario = salario - 100 WHERE id IN (1, 3)	UPDATE empleados SET salario = salario - 100 WHERE id IN (1, 3)	19	0	f	ok	2026-05-14 11:12:55.814749-05	[]	[]
456	anonymous	admin	UPDATE empleados SET salario = salario + 1 WHERE id = -999	UPDATE empleados SET salario = salario + 1 WHERE id = -999	7	0	f	ok	2026-05-14 11:12:55.828001-05	[]	[]
457	anonymous	admin	INSERT INTO departamentos (nombre, ubicacion, presupuesto, activo) VALUES ('TEMP_DEL', 'X', 999, 1)	INSERT INTO departamentos (nombre, ubicacion, presupuesto, activo) VALUES ('TEMP_DEL', 'X', 999, 1)	20	0	f	ok	2026-05-14 11:12:55.85336-05	[]	[]
458	anonymous	admin	DELETE FROM departamentos WHERE nombre = 'TEMP_DEL'	DELETE FROM departamentos WHERE nombre = 'TEMP_DEL'	19	0	f	ok	2026-05-14 11:12:55.876436-05	[]	[]
459	anonymous	admin	-- PASO 1: subir salario de Ventas\n   UPDATE empleados SET salario = salario + 200 WHERE departamento_id = 2;\n   -- PASO 2: subir presupuesto del depto\n   UPDATE departamentos SET presupuesto = presupuesto + 5000 WHERE id = 2;	-- PASO 1: subir salario de Ventas\n   UPDATE empleados SET salario = salario + 200 WHERE departamento_id = 2;\n   -- PASO 2: subir presupuesto del depto\n   UPDATE departamentos SET presupuesto = presupuesto + 5000 WHERE id = 2;	34	0	f	ok	2026-05-14 11:12:55.91459-05	[]	[]
460	anonymous	admin	UPDATE empleados SET salario = salario - 200 WHERE departamento_id = 2; UPDATE departamentos SET presupuesto = presupuesto - 5000 WHERE id = 2;	UPDATE empleados SET salario = salario - 200 WHERE departamento_id = 2; UPDATE departamentos SET presupuesto = presupuesto - 5000 WHERE id = 2;	33	0	f	ok	2026-05-14 11:12:55.952963-05	[]	[]
461	anonymous	admin	UPDATE empleados SET salario = salario + 50 WHERE id = 1	UPDATE empleados SET salario = salario + 50 WHERE id = 1	103	0	f	ok	2026-05-14 11:12:56.062347-05	[]	[]
462	anonymous	admin	UPDATE empleados SET salario = salario - 50 WHERE id = 1	UPDATE empleados SET salario = salario - 50 WHERE id = 1	10	0	f	ok	2026-05-14 11:12:56.077338-05	[]	[]
463	anonymous	admin	SELECT id, subcategoria FROM knowledge_base LIMIT 1	SELECT id, subcategoria FROM knowledge_base LIMIT 1	4	1	f	ok	2026-05-14 11:12:56.08589-05	[]	[]
464	anonymous	admin	UPDATE knowledge_base SET subcategoria = 'TEMP_PRUEBA' WHERE id = '9515ae4e-df33-42ef-bf7d-58f4f287a34e'	UPDATE knowledge_base SET subcategoria = 'TEMP_PRUEBA' WHERE id = '9515ae4e-df33-42ef-bf7d-58f4f287a34e'	6	1	f	ok	2026-05-14 11:12:56.095806-05	[]	[]
465	anonymous	admin	UPDATE knowledge_base SET subcategoria = 'Otros temas' WHERE id = '9515ae4e-df33-42ef-bf7d-58f4f287a34e'	UPDATE knowledge_base SET subcategoria = 'Otros temas' WHERE id = '9515ae4e-df33-42ef-bf7d-58f4f287a34e'	3	1	f	ok	2026-05-14 11:12:56.101506-05	[]	[]
466	anonymous	admin	SELECT 1 AS n	SELECT 1 AS n	1	1	f	ok	2026-05-14 11:12:56.105754-05	[]	[]
467	anonymous	admin	UPDATE empleados SET salario = salario + 100 WHERE id IN (1, 3)	UPDATE empleados SET salario = salario + 100 WHERE id IN (1, 3)	44	0	f	ok	2026-05-14 11:14:51.603232-05	[]	[]
472	anonymous	admin	-- PASO 1: subir salario de Ventas\n   UPDATE empleados SET salario = salario + 200 WHERE departamento_id = 2;\n   -- PASO 2: subir presupuesto del depto\n   UPDATE departamentos SET presupuesto = presupuesto + 5000 WHERE id = 2;	-- PASO 1: subir salario de Ventas\n   UPDATE empleados SET salario = salario + 200 WHERE departamento_id = 2;\n   -- PASO 2: subir presupuesto del depto\n   UPDATE departamentos SET presupuesto = presupuesto + 5000 WHERE id = 2;	32	0	f	ok	2026-05-14 11:14:51.733447-05	[]	[]
473	anonymous	admin	UPDATE empleados SET salario = salario - 200 WHERE departamento_id = 2; UPDATE departamentos SET presupuesto = presupuesto - 5000 WHERE id = 2;	UPDATE empleados SET salario = salario - 200 WHERE departamento_id = 2; UPDATE departamentos SET presupuesto = presupuesto - 5000 WHERE id = 2;	29	0	f	ok	2026-05-14 11:14:51.768488-05	[]	[]
474	anonymous	admin	UPDATE empleados SET salario = salario + 50 WHERE id = 1	UPDATE empleados SET salario = salario + 50 WHERE id = 1	51	0	f	ok	2026-05-14 11:14:51.825526-05	[]	[]
475	anonymous	admin	UPDATE empleados SET salario = salario - 50 WHERE id = 1	UPDATE empleados SET salario = salario - 50 WHERE id = 1	9	0	f	ok	2026-05-14 11:14:51.840066-05	[]	[]
476	anonymous	admin	SELECT id, subcategoria FROM knowledge_base LIMIT 1	SELECT id, subcategoria FROM knowledge_base LIMIT 1	5	1	f	ok	2026-05-14 11:14:51.850275-05	[]	[]
477	anonymous	admin	UPDATE knowledge_base SET subcategoria = 'TEMP_PRUEBA' WHERE id = '8cd91481-64a8-4af1-b598-c75302515e62'	UPDATE knowledge_base SET subcategoria = 'TEMP_PRUEBA' WHERE id = '8cd91481-64a8-4af1-b598-c75302515e62'	5	1	f	ok	2026-05-14 11:14:51.859679-05	[]	[]
478	anonymous	admin	UPDATE knowledge_base SET subcategoria = 'otros temas' WHERE id = '8cd91481-64a8-4af1-b598-c75302515e62'	UPDATE knowledge_base SET subcategoria = 'otros temas' WHERE id = '8cd91481-64a8-4af1-b598-c75302515e62'	4	1	f	ok	2026-05-14 11:14:51.866674-05	[]	[]
479	anonymous	admin	SELECT 1 AS n	SELECT 1 AS n	2	1	f	ok	2026-05-14 11:14:51.87147-05	[]	[]
480	anonymous	admin	UPDATE empleados SET salario = salario + 100 WHERE id IN (1, 3)	UPDATE empleados SET salario = salario + 100 WHERE id IN (1, 3)	40	2	f	ok	2026-05-14 11:18:39.196699-05	[]	[]
481	anonymous	admin	UPDATE empleados SET salario = salario - 100 WHERE id IN (1, 3)	UPDATE empleados SET salario = salario - 100 WHERE id IN (1, 3)	17	2	f	ok	2026-05-14 11:18:39.229924-05	[]	[]
482	anonymous	admin	UPDATE empleados SET salario = salario + 1 WHERE id = -999	UPDATE empleados SET salario = salario + 1 WHERE id = -999	5	0	f	ok	2026-05-14 11:18:39.239602-05	[]	[]
483	anonymous	admin	INSERT INTO departamentos (nombre, ubicacion, presupuesto, activo) VALUES ('TEMP_DEL', 'X', 999, 1)	INSERT INTO departamentos (nombre, ubicacion, presupuesto, activo) VALUES ('TEMP_DEL', 'X', 999, 1)	12	1	f	ok	2026-05-14 11:18:39.25615-05	[]	[]
484	anonymous	admin	DELETE FROM departamentos WHERE nombre = 'TEMP_DEL'	DELETE FROM departamentos WHERE nombre = 'TEMP_DEL'	20	1	f	ok	2026-05-14 11:18:39.281267-05	[]	[]
485	anonymous	admin	-- PASO 1: subir salario de Ventas\n   UPDATE empleados SET salario = salario + 200 WHERE departamento_id = 2;\n   -- PASO 2: subir presupuesto del depto\n   UPDATE departamentos SET presupuesto = presupuesto + 5000 WHERE id = 2;	-- PASO 1: subir salario de Ventas\n   UPDATE empleados SET salario = salario + 200 WHERE departamento_id = 2;\n   -- PASO 2: subir presupuesto del depto\n   UPDATE departamentos SET presupuesto = presupuesto + 5000 WHERE id = 2;	30	0	f	ok	2026-05-14 11:18:39.31505-05	[]	[]
486	anonymous	admin	UPDATE empleados SET salario = salario - 200 WHERE departamento_id = 2; UPDATE departamentos SET presupuesto = presupuesto - 5000 WHERE id = 2;	UPDATE empleados SET salario = salario - 200 WHERE departamento_id = 2; UPDATE departamentos SET presupuesto = presupuesto - 5000 WHERE id = 2;	28	0	f	ok	2026-05-14 11:18:39.346749-05	[]	[]
487	anonymous	admin	UPDATE empleados SET salario = salario + 50 WHERE id = 1	UPDATE empleados SET salario = salario + 50 WHERE id = 1	41	1	f	ok	2026-05-14 11:18:39.391481-05	[]	[]
488	anonymous	admin	UPDATE empleados SET salario = salario - 50 WHERE id = 1	UPDATE empleados SET salario = salario - 50 WHERE id = 1	6	1	f	ok	2026-05-14 11:18:39.401672-05	[]	[]
489	anonymous	admin	SELECT id, subcategoria FROM knowledge_base LIMIT 1	SELECT id, subcategoria FROM knowledge_base LIMIT 1	4	1	f	ok	2026-05-14 11:18:39.40945-05	[]	[]
490	anonymous	admin	UPDATE knowledge_base SET subcategoria = 'TEMP_PRUEBA' WHERE id = '2939eb64-5ff4-4e35-8937-003dc88b725c'	UPDATE knowledge_base SET subcategoria = 'TEMP_PRUEBA' WHERE id = '2939eb64-5ff4-4e35-8937-003dc88b725c'	5	1	f	ok	2026-05-14 11:18:39.418668-05	[]	[]
491	anonymous	admin	UPDATE knowledge_base SET subcategoria = 'PRUEBA_TEMP' WHERE id = '2939eb64-5ff4-4e35-8937-003dc88b725c'	UPDATE knowledge_base SET subcategoria = 'PRUEBA_TEMP' WHERE id = '2939eb64-5ff4-4e35-8937-003dc88b725c'	3	1	f	ok	2026-05-14 11:18:39.425454-05	[]	[]
492	anonymous	admin	SELECT 1 AS n	SELECT 1 AS n	1	1	f	ok	2026-05-14 11:18:39.429817-05	[]	[]
493	anonymous	admin	SELECT id, titulo, subcategoria FROM knowledge_base WHERE subcategoria IN ('PRUEBA_TEMP','TEMP_PRUEBA')	SELECT id, titulo, subcategoria FROM knowledge_base WHERE subcategoria IN ('PRUEBA_TEMP','TEMP_PRUEBA')	36	1	f	ok	2026-05-14 11:19:29.932814-05	[]	[]
494	anonymous	admin	UPDATE knowledge_base SET subcategoria = 'General' WHERE id = '2939eb64-5ff4-4e35-8937-003dc88b725c'	UPDATE knowledge_base SET subcategoria = 'General' WHERE id = '2939eb64-5ff4-4e35-8937-003dc88b725c'	6	1	f	ok	2026-05-14 11:20:05.867649-05	[]	[]
495	anonymous	admin	SELECT COUNT(*) AS total FROM empleados	SELECT COUNT(*) AS total FROM empleados	2	1	f	ok	2026-05-14 11:20:28.647871-05	[]	[]
496	anonymous	admin	SELECT d.nombre, COUNT(e.id) c FROM departamentos d LEFT JOIN empleados e ON e.departamento_id=d.id GROUP BY d.id,d.nombre ORDER BY c DESC LIMIT 3	SELECT d.nombre, COUNT(e.id) c FROM departamentos d LEFT JOIN empleados e ON e.departamento_id=d.id GROUP BY d.id,d.nombre ORDER BY c DESC LIMIT 3	6	3	f	ok	2026-05-14 11:20:28.736293-05	[]	[]
497	anonymous	admin	{"collection":"empleados","aggregate":[{"$group":{"_id":"$departamento","n":{"$sum":1}}}]}	{"collection":"empleados","aggregate":[{"$group":{"_id":"$departamento","n":{"$sum":1}}}]}	22	3	f	ok	2026-05-14 11:20:28.846588-05	[]	[]
498	anonymous	admin	{"collection":"empleados","updateOne":{"filter":{"departamento":"TI"},"update":{"$set":{"activo":true}}}}	{"collection":"empleados","updateOne":{"filter":{"departamento":"TI"},"update":{"$set":{"activo":true}}}}	10	0	f	ok	2026-05-14 11:20:28.928728-05	[]	[]
499	anonymous	admin	UPDATE empleados SET salario = salario + 10 WHERE id = 1	UPDATE empleados SET salario = salario + 10 WHERE id = 1	39	1	f	ok	2026-05-14 11:24:49.427721-05	[]	[]
500	anonymous	admin	UPDATE empleados SET salario = salario - 10 WHERE id = 1	UPDATE empleados SET salario = salario - 10 WHERE id = 1	17	1	f	ok	2026-05-14 11:24:49.503079-05	[]	[]
517	anonymous	admin	UPDATE empleados SET salario = salario - 300 WHERE departamento_id = 1	UPDATE empleados SET salario = salario - 300 WHERE departamento_id = 1	16	6	f	ok	2026-05-14 11:28:59.522346-05	[]	[]
518	anonymous	admin	UPDATE departamentos SET presupuesto = presupuesto - 10000 WHERE id = 1	UPDATE departamentos SET presupuesto = presupuesto - 10000 WHERE id = 1	15	1	f	ok	2026-05-14 11:28:59.54162-05	[]	[]
501	anonymous	admin	INSERT INTO knowledge_base (id, titulo, contenido_md, categoria, subcategoria, tipo_solucion, script_json, visible_para, creado_por_username)\n   VALUES ('aaaa1111-2222-3333-4444-555566667777', 'TEST E2E write-detail', 'test', 'Test', 'General', 'script', '{"modo":"script","origen":"manual-sql","databaseId":"mysql_test","target_database":"mysql_test","sql":"-- PASO 1: Aumentar salario de Tecnología\\nUPDATE empleados SET salario = salario + 300 WHERE departamento_id = 1;\\n-- PASO 2: Ajustar presupuesto del departamento\\nUPDATE departamentos SET presupuesto = presupuesto + 10000 WHERE id = 1;"}', 'admin', 'admin')\n   ON CONFLICT (id) DO UPDATE SET script_json = EXCLUDED.script_json	INSERT INTO knowledge_base (id, titulo, contenido_md, categoria, subcategoria, tipo_solucion, script_json, visible_para, creado_por_username)\n   VALUES ('aaaa1111-2222-3333-4444-555566667777', 'TEST E2E write-detail', 'test', 'Test', 'General', 'script', '{"modo":"script","origen":"manual-sql","databaseId":"mysql_test","target_database":"mysql_test","sql":"-- PASO 1: Aumentar salario de Tecnología\\nUPDATE empleados SET salario = salario + 300 WHERE departamento_id = 1;\\n-- PASO 2: Ajustar presupuesto del departamento\\nUPDATE departamentos SET presupuesto = presupuesto + 10000 WHERE id = 1;"}', 'admin', 'admin')\n   ON CONFLICT (id) DO UPDATE SET script_json = EXCLUDED.script_json	68	0	f	error	2026-05-14 11:27:35.509899-05	[]	\N
502	anonymous	admin	execute article aaaa1111-2222-3333-4444-555566667777	workflow execution	3	0	f	error	2026-05-14 11:27:35.525371-05	\N	\N
503	anonymous	admin	UPDATE empleados SET salario = salario - 300 WHERE departamento_id = 1	UPDATE empleados SET salario = salario - 300 WHERE departamento_id = 1	20	6	f	ok	2026-05-14 11:27:35.549124-05	[]	[]
504	anonymous	admin	UPDATE departamentos SET presupuesto = presupuesto - 10000 WHERE id = 1	UPDATE departamentos SET presupuesto = presupuesto - 10000 WHERE id = 1	18	1	f	ok	2026-05-14 11:27:35.571987-05	[]	[]
505	anonymous	admin	DELETE FROM knowledge_base WHERE id = 'aaaa1111-2222-3333-4444-555566667777'	DELETE FROM knowledge_base WHERE id = 'aaaa1111-2222-3333-4444-555566667777'	6	0	f	ok	2026-05-14 11:27:35.583495-05	[]	[]
506	anonymous	admin	SELECT column_name, is_nullable, column_default FROM information_schema.columns WHERE table_name='knowledge_base' AND is_nullable='NO'	SELECT column_name, is_nullable, column_default FROM information_schema.columns WHERE table_name='knowledge_base' AND is_nullable='NO'	14	5	f	ok	2026-05-14 11:27:50.886281-05	[]	[]
507	anonymous	admin	INSERT INTO knowledge_base (id, titulo, contenido_md, categoria, subcategoria, tipo_solucion, script_json, visible_para) VALUES ('aaaa1111-2222-3333-4444-555566667777', 'TEST', 'x', 'Test', 'General', 'script', '{}', 'admin')	INSERT INTO knowledge_base (id, titulo, contenido_md, categoria, subcategoria, tipo_solucion, script_json, visible_para) VALUES ('aaaa1111-2222-3333-4444-555566667777', 'TEST', 'x', 'Test', 'General', 'script', '{}', 'admin')	1	0	f	error	2026-05-14 11:27:50.938747-05	[]	\N
508	anonymous	admin	SELECT data_type FROM information_schema.columns WHERE table_name='knowledge_base' AND column_name='creado_por'	SELECT data_type FROM information_schema.columns WHERE table_name='knowledge_base' AND column_name='creado_por'	3	1	f	ok	2026-05-14 11:28:19.605456-05	[]	[]
509	anonymous	admin	SELECT creado_por FROM knowledge_base LIMIT 1	SELECT creado_por FROM knowledge_base LIMIT 1	1	1	f	ok	2026-05-14 11:28:19.662556-05	[]	[]
510	anonymous	admin	INSERT INTO knowledge_base (id, titulo, contenido, contenido_md, categoria, subcategoria, tipo_solucion, script_json, visible_para, creado_por, creado_por_username)\n   VALUES ('aaaa1111-2222-3333-4444-555566667777', 'TEST E2E write-detail', 'test', 'test', 'Test', 'General', 'script', '{"modo":"script","origen":"manual-sql","databaseId":"mysql_test","target_database":"mysql_test","sql":"-- PASO 1: Aumentar salario de Tecnología\\nUPDATE empleados SET salario = salario + 300 WHERE departamento_id = 1;\\n-- PASO 2: Ajustar presupuesto del departamento\\nUPDATE departamentos SET presupuesto = presupuesto + 10000 WHERE id = 1;"}', 'admin', '747bd085-6444-49a4-9919-64660808316c', 'admin')\n   ON CONFLICT (id) DO UPDATE SET script_json = EXCLUDED.script_json	INSERT INTO knowledge_base (id, titulo, contenido, contenido_md, categoria, subcategoria, tipo_solucion, script_json, visible_para, creado_por, creado_por_username)\n   VALUES ('aaaa1111-2222-3333-4444-555566667777', 'TEST E2E write-detail', 'test', 'test', 'Test', 'General', 'script', '{"modo":"script","origen":"manual-sql","databaseId":"mysql_test","target_database":"mysql_test","sql":"-- PASO 1: Aumentar salario de Tecnología\\nUPDATE empleados SET salario = salario + 300 WHERE departamento_id = 1;\\n-- PASO 2: Ajustar presupuesto del departamento\\nUPDATE departamentos SET presupuesto = presupuesto + 10000 WHERE id = 1;"}', 'admin', '747bd085-6444-49a4-9919-64660808316c', 'admin')\n   ON CONFLICT (id) DO UPDATE SET script_json = EXCLUDED.script_json	5	1	f	ok	2026-05-14 11:28:35.24869-05	[]	[]
511	anonymous	admin	execute article aaaa1111-2222-3333-4444-555566667777	workflow execution	2	0	f	error	2026-05-14 11:28:35.259646-05	\N	\N
512	anonymous	admin	UPDATE empleados SET salario = salario - 300 WHERE departamento_id = 1	UPDATE empleados SET salario = salario - 300 WHERE departamento_id = 1	19	6	f	ok	2026-05-14 11:28:35.28417-05	[]	[]
513	anonymous	admin	UPDATE departamentos SET presupuesto = presupuesto - 10000 WHERE id = 1	UPDATE departamentos SET presupuesto = presupuesto - 10000 WHERE id = 1	15	1	f	ok	2026-05-14 11:28:35.303903-05	[]	[]
514	anonymous	admin	DELETE FROM knowledge_base WHERE id = 'aaaa1111-2222-3333-4444-555566667777'	DELETE FROM knowledge_base WHERE id = 'aaaa1111-2222-3333-4444-555566667777'	5	1	f	ok	2026-05-14 11:28:35.315007-05	[]	[]
515	anonymous	admin	INSERT INTO knowledge_base (id, titulo, contenido, contenido_md, categoria, subcategoria, tipo_solucion, script_json, visible_para, creado_por, creado_por_username)\n   VALUES ('aaaa1111-2222-3333-4444-555566667777', 'TEST E2E write-detail', 'test', 'test', 'Test', 'General', 'script', '{"modo":"script","origen":"manual-sql","databaseId":"mysql_test","target_database":"mysql_test","sql":"-- PASO 1: Aumentar salario de Tecnología\\nUPDATE empleados SET salario = salario + 300 WHERE departamento_id = 1;\\n-- PASO 2: Ajustar presupuesto del departamento\\nUPDATE departamentos SET presupuesto = presupuesto + 10000 WHERE id = 1;"}', 'admin', '747bd085-6444-49a4-9919-64660808316c', 'admin')\n   ON CONFLICT (id) DO UPDATE SET script_json = EXCLUDED.script_json	INSERT INTO knowledge_base (id, titulo, contenido, contenido_md, categoria, subcategoria, tipo_solucion, script_json, visible_para, creado_por, creado_por_username)\n   VALUES ('aaaa1111-2222-3333-4444-555566667777', 'TEST E2E write-detail', 'test', 'test', 'Test', 'General', 'script', '{"modo":"script","origen":"manual-sql","databaseId":"mysql_test","target_database":"mysql_test","sql":"-- PASO 1: Aumentar salario de Tecnología\\nUPDATE empleados SET salario = salario + 300 WHERE departamento_id = 1;\\n-- PASO 2: Ajustar presupuesto del departamento\\nUPDATE departamentos SET presupuesto = presupuesto + 10000 WHERE id = 1;"}', 'admin', '747bd085-6444-49a4-9919-64660808316c', 'admin')\n   ON CONFLICT (id) DO UPDATE SET script_json = EXCLUDED.script_json	3	1	f	ok	2026-05-14 11:28:59.446472-05	[]	[]
516	anonymous	admin	execute article aaaa1111-2222-3333-4444-555566667777	workflow execution	47	0	f	ok	2026-05-14 11:28:59.503628-05	[]	[]
519	anonymous	admin	DELETE FROM knowledge_base WHERE id = 'aaaa1111-2222-3333-4444-555566667777'	DELETE FROM knowledge_base WHERE id = 'aaaa1111-2222-3333-4444-555566667777'	2	1	f	ok	2026-05-14 11:28:59.546764-05	[]	[]
520	anonymous	admin	SELECT 'texto con ; punto y coma' AS frase, 1 AS n	SELECT 'texto con ; punto y coma' AS frase, 1 AS n	70	0	f	error	2026-05-14 11:37:18.09549-05	[]	\N
521	anonymous	admin	SELECT 1 AS uno -- comentario; con punto y coma\n	SELECT 1 AS uno -- comentario; con punto y coma\n	2	0	f	error	2026-05-14 11:37:18.12462-05	[]	\N
522	anonymous	admin	SELECT 1 AS n /* esto; no debe; partir */	SELECT 1 AS n /* esto; no debe; partir */	0	0	f	error	2026-05-14 11:37:18.131928-05	[]	\N
523	anonymous	admin	SELECT 1 AS a; SELECT 2 AS b;	SELECT 1 AS a; SELECT 2 AS b;	7	0	f	ok	2026-05-14 11:37:18.144591-05	[]	[]
524	anonymous	admin	WITH RECURSIVE serie(n) AS (\n      SELECT 1 UNION ALL SELECT n+1 FROM serie WHERE n < 50\n    ),\n    pares AS (SELECT n FROM serie WHERE n % 2 = 0),\n    stats AS (\n      SELECT COUNT(*) AS total, SUM(n) AS suma, AVG(n)::numeric(10,2) AS promedio,\n             MIN(n) AS minimo, MAX(n) AS maximo\n      FROM pares\n    ),\n    ranked AS (\n      SELECT n, ROW_NUMBER() OVER (ORDER BY n DESC) AS rk,\n             SUM(n) OVER (ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS acum\n      FROM pares\n    )\n    SELECT s.total, s.suma, s.promedio, s.minimo, s.maximo,\n           (SELECT COUNT(*) FROM ranked WHERE rk <= 5) AS top5_count,\n           (SELECT MAX(acum) FROM ranked) AS acumulado_max\n    FROM stats s	WITH RECURSIVE serie(n) AS (\n      SELECT 1 UNION ALL SELECT n+1 FROM serie WHERE n < 50\n    ),\n    pares AS (SELECT n FROM serie WHERE n % 2 = 0),\n    stats AS (\n      SELECT COUNT(*) AS total, SUM(n) AS suma, AVG(n)::numeric(10,2) AS promedio,\n             MIN(n) AS minimo, MAX(n) AS maximo\n      FROM pares\n    ),\n    ranked AS (\n      SELECT n, ROW_NUMBER() OVER (ORDER BY n DESC) AS rk,\n             SUM(n) OVER (ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS acum\n      FROM pares\n    )\n    SELECT s.total, s.suma, s.promedio, s.minimo, s.maximo,\n           (SELECT COUNT(*) FROM ranked WHERE rk <= 5) AS top5_count,\n           (SELECT MAX(acum) FROM ranked) AS acumulado_max\n    FROM stats s	5	1	f	ok	2026-05-14 11:37:18.155384-05	[]	[]
525	anonymous	admin	SELECT e.nombre, e.cargo, d.nombre AS depto, d.ubicacion,\n      (SELECT COUNT(*) FROM pedidos p WHERE p.empleado_id = e.id) AS num_pedidos,\n      (SELECT COALESCE(SUM(p.precio_total),0) FROM pedidos p WHERE p.empleado_id = e.id) AS total_vendido,\n      CASE\n        WHEN e.salario > (SELECT AVG(salario) FROM empleados) THEN\n          CASE WHEN e.activo = 1 THEN 'Alto-Activo' ELSE 'Alto-Inactivo' END\n        ELSE 'Normal'\n      END AS clasificacion,\n      RANK() OVER (PARTITION BY e.departamento_id ORDER BY e.salario DESC) AS rank_depto\n    FROM empleados e\n    JOIN departamentos d ON d.id = e.departamento_id\n    LEFT JOIN pedidos pd ON pd.empleado_id = e.id\n    LEFT JOIN productos pr ON pr.id = pd.producto_id\n    GROUP BY e.id, e.nombre, e.cargo, d.nombre, d.ubicacion, e.salario, e.activo, e.departamento_id\n    ORDER BY total_vendido DESC, rank_depto\n    LIMIT 15	SELECT e.nombre, e.cargo, d.nombre AS depto, d.ubicacion,\n      (SELECT COUNT(*) FROM pedidos p WHERE p.empleado_id = e.id) AS num_pedidos,\n      (SELECT COALESCE(SUM(p.precio_total),0) FROM pedidos p WHERE p.empleado_id = e.id) AS total_vendido,\n      CASE\n        WHEN e.salario > (SELECT AVG(salario) FROM empleados) THEN\n          CASE WHEN e.activo = 1 THEN 'Alto-Activo' ELSE 'Alto-Inactivo' END\n        ELSE 'Normal'\n      END AS clasificacion,\n      RANK() OVER (PARTITION BY e.departamento_id ORDER BY e.salario DESC) AS rank_depto\n    FROM empleados e\n    JOIN departamentos d ON d.id = e.departamento_id\n    LEFT JOIN pedidos pd ON pd.empleado_id = e.id\n    LEFT JOIN productos pr ON pr.id = pd.producto_id\n    GROUP BY e.id, e.nombre, e.cargo, d.nombre, d.ubicacion, e.salario, e.activo, e.departamento_id\n    ORDER BY total_vendido DESC, rank_depto\n    LIMIT 15	25	11	f	ok	2026-05-14 11:37:18.186824-05	[]	[]
526	anonymous	admin	WITH ventas_resumen AS (\n      SELECT producto_id, SUM(total) AS ingresos, COUNT(*) AS num_ventas, SUM(cantidad) AS unidades\n      FROM ventas GROUP BY producto_id\n    )\n    SELECT p.nombre, p.categoria, vr.ingresos, vr.num_ventas, vr.unidades,\n      ROUND(vr.ingresos / NULLIF(vr.unidades,0), 2) AS precio_prom,\n      RANK() OVER (PARTITION BY p.categoria ORDER BY vr.ingresos DESC) AS rk,\n      ROUND(RATIO_TO_REPORT(vr.ingresos) OVER () * 100, 2) AS pct_total\n    FROM ventas_resumen vr\n    JOIN productos p ON p.id = vr.producto_id\n    ORDER BY vr.ingresos DESC	WITH ventas_resumen AS (\n      SELECT producto_id, SUM(total) AS ingresos, COUNT(*) AS num_ventas, SUM(cantidad) AS unidades\n      FROM ventas GROUP BY producto_id\n    )\n    SELECT p.nombre, p.categoria, vr.ingresos, vr.num_ventas, vr.unidades,\n      ROUND(vr.ingresos / NULLIF(vr.unidades,0), 2) AS precio_prom,\n      RANK() OVER (PARTITION BY p.categoria ORDER BY vr.ingresos DESC) AS rk,\n      ROUND(RATIO_TO_REPORT(vr.ingresos) OVER () * 100, 2) AS pct_total\n    FROM ventas_resumen vr\n    JOIN productos p ON p.id = vr.producto_id\n    ORDER BY vr.ingresos DESC	100	4	f	ok	2026-05-14 11:37:18.292782-05	[]	[]
527	anonymous	admin	CREATE TABLE IF NOT EXISTS tmp_test_tabla (id INT PRIMARY KEY, nombre VARCHAR(50), creado TIMESTAMP DEFAULT CURRENT_TIMESTAMP)	CREATE TABLE IF NOT EXISTS tmp_test_tabla (id INT PRIMARY KEY, nombre VARCHAR(50), creado TIMESTAMP DEFAULT CURRENT_TIMESTAMP)	191	0	f	ok	2026-05-14 11:37:18.48698-05	[]	[]
528	anonymous	admin	INSERT INTO tmp_test_tabla (id, nombre) VALUES (1, 'prueba'), (2, 'otra')	INSERT INTO tmp_test_tabla (id, nombre) VALUES (1, 'prueba'), (2, 'otra')	20	2	f	ok	2026-05-14 11:37:18.512416-05	[]	[]
529	anonymous	admin	ALTER TABLE tmp_test_tabla ADD COLUMN activo TINYINT DEFAULT 1	ALTER TABLE tmp_test_tabla ADD COLUMN activo TINYINT DEFAULT 1	65	0	f	ok	2026-05-14 11:37:18.581749-05	[]	[]
530	anonymous	admin	SELECT * FROM tmp_test_tabla ORDER BY id	SELECT * FROM tmp_test_tabla ORDER BY id	4	2	f	ok	2026-05-14 11:37:18.589508-05	[]	[]
531	anonymous	admin	DROP TABLE IF EXISTS tmp_test_tabla	DROP TABLE IF EXISTS tmp_test_tabla	51	0	f	ok	2026-05-14 11:37:18.645287-05	[]	[]
532	anonymous	admin	CREATE TABLE IF NOT EXISTS tmp_pg_test (id SERIAL PRIMARY KEY, dato TEXT);\n            INSERT INTO tmp_pg_test (dato) VALUES ('a'), ('b'), ('c');\n            DROP TABLE tmp_pg_test;	CREATE TABLE IF NOT EXISTS tmp_pg_test (id SERIAL PRIMARY KEY, dato TEXT);\n            INSERT INTO tmp_pg_test (dato) VALUES ('a'), ('b'), ('c');\n            DROP TABLE tmp_pg_test;	37	0	f	ok	2026-05-14 11:37:18.686834-05	[]	[]
533	anonymous	admin	SELECT 'texto con ; punto y coma' AS frase, 1 AS n	SELECT 'texto con ; punto y coma' AS frase, 1 AS n	3	1	f	ok	2026-05-14 11:40:31.091294-05	[]	[]
534	anonymous	admin	SELECT 1 AS uno -- comentario; con punto y coma	SELECT 1 AS uno -- comentario; con punto y coma	3	1	f	ok	2026-05-14 11:40:31.111994-05	[]	[]
535	anonymous	admin	SELECT 1 AS n /* esto; no debe; partir */	SELECT 1 AS n /* esto; no debe; partir */	15	1	f	ok	2026-05-14 11:40:31.132962-05	[]	[]
536	anonymous	admin	SELECT 1 AS a; SELECT 2 AS b;	SELECT 1 AS a; SELECT 2 AS b;	5	0	f	ok	2026-05-14 11:40:31.142948-05	[]	[]
548	anonymous	admin	SELECT COUNT(*) AS num_fks FROM information_schema.table_constraints WHERE constraint_type='FOREIGN KEY' AND table_schema='public'	SELECT COUNT(*) AS num_fks FROM information_schema.table_constraints WHERE constraint_type='FOREIGN KEY' AND table_schema='public'	6	1	f	ok	2026-05-14 11:41:19.918212-05	[]	[]
537	anonymous	admin	WITH RECURSIVE serie(n) AS (\n      SELECT 1 UNION ALL SELECT n+1 FROM serie WHERE n < 50\n    ),\n    pares AS (SELECT n FROM serie WHERE n % 2 = 0),\n    stats AS (\n      SELECT COUNT(*) AS total, SUM(n) AS suma, AVG(n)::numeric(10,2) AS promedio,\n             MIN(n) AS minimo, MAX(n) AS maximo\n      FROM pares\n    ),\n    ranked AS (\n      SELECT n, ROW_NUMBER() OVER (ORDER BY n DESC) AS rk,\n             SUM(n) OVER (ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS acum\n      FROM pares\n    )\n    SELECT s.total, s.suma, s.promedio, s.minimo, s.maximo,\n           (SELECT COUNT(*) FROM ranked WHERE rk <= 5) AS top5_count,\n           (SELECT MAX(acum) FROM ranked) AS acumulado_max\n    FROM stats s	WITH RECURSIVE serie(n) AS (\n      SELECT 1 UNION ALL SELECT n+1 FROM serie WHERE n < 50\n    ),\n    pares AS (SELECT n FROM serie WHERE n % 2 = 0),\n    stats AS (\n      SELECT COUNT(*) AS total, SUM(n) AS suma, AVG(n)::numeric(10,2) AS promedio,\n             MIN(n) AS minimo, MAX(n) AS maximo\n      FROM pares\n    ),\n    ranked AS (\n      SELECT n, ROW_NUMBER() OVER (ORDER BY n DESC) AS rk,\n             SUM(n) OVER (ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS acum\n      FROM pares\n    )\n    SELECT s.total, s.suma, s.promedio, s.minimo, s.maximo,\n           (SELECT COUNT(*) FROM ranked WHERE rk <= 5) AS top5_count,\n           (SELECT MAX(acum) FROM ranked) AS acumulado_max\n    FROM stats s	3	1	f	ok	2026-05-14 11:40:31.152739-05	[]	[]
538	anonymous	admin	SELECT e.nombre, e.cargo, d.nombre AS depto, d.ubicacion,\n      (SELECT COUNT(*) FROM pedidos p WHERE p.empleado_id = e.id) AS num_pedidos,\n      (SELECT COALESCE(SUM(p.precio_total),0) FROM pedidos p WHERE p.empleado_id = e.id) AS total_vendido,\n      CASE\n        WHEN e.salario > (SELECT AVG(salario) FROM empleados) THEN\n          CASE WHEN e.activo = 1 THEN 'Alto-Activo' ELSE 'Alto-Inactivo' END\n        ELSE 'Normal'\n      END AS clasificacion,\n      RANK() OVER (PARTITION BY e.departamento_id ORDER BY e.salario DESC) AS rank_depto\n    FROM empleados e\n    JOIN departamentos d ON d.id = e.departamento_id\n    LEFT JOIN pedidos pd ON pd.empleado_id = e.id\n    LEFT JOIN productos pr ON pr.id = pd.producto_id\n    GROUP BY e.id, e.nombre, e.cargo, d.nombre, d.ubicacion, e.salario, e.activo, e.departamento_id\n    ORDER BY total_vendido DESC, rank_depto\n    LIMIT 15	SELECT e.nombre, e.cargo, d.nombre AS depto, d.ubicacion,\n      (SELECT COUNT(*) FROM pedidos p WHERE p.empleado_id = e.id) AS num_pedidos,\n      (SELECT COALESCE(SUM(p.precio_total),0) FROM pedidos p WHERE p.empleado_id = e.id) AS total_vendido,\n      CASE\n        WHEN e.salario > (SELECT AVG(salario) FROM empleados) THEN\n          CASE WHEN e.activo = 1 THEN 'Alto-Activo' ELSE 'Alto-Inactivo' END\n        ELSE 'Normal'\n      END AS clasificacion,\n      RANK() OVER (PARTITION BY e.departamento_id ORDER BY e.salario DESC) AS rank_depto\n    FROM empleados e\n    JOIN departamentos d ON d.id = e.departamento_id\n    LEFT JOIN pedidos pd ON pd.empleado_id = e.id\n    LEFT JOIN productos pr ON pr.id = pd.producto_id\n    GROUP BY e.id, e.nombre, e.cargo, d.nombre, d.ubicacion, e.salario, e.activo, e.departamento_id\n    ORDER BY total_vendido DESC, rank_depto\n    LIMIT 15	7	11	f	ok	2026-05-14 11:40:31.165707-05	[]	[]
539	anonymous	admin	WITH ventas_resumen AS (\n      SELECT producto_id, SUM(total) AS ingresos, COUNT(*) AS num_ventas, SUM(cantidad) AS unidades\n      FROM ventas GROUP BY producto_id\n    )\n    SELECT p.nombre, p.categoria, vr.ingresos, vr.num_ventas, vr.unidades,\n      ROUND(vr.ingresos / NULLIF(vr.unidades,0), 2) AS precio_prom,\n      RANK() OVER (PARTITION BY p.categoria ORDER BY vr.ingresos DESC) AS rk,\n      ROUND(RATIO_TO_REPORT(vr.ingresos) OVER () * 100, 2) AS pct_total\n    FROM ventas_resumen vr\n    JOIN productos p ON p.id = vr.producto_id\n    ORDER BY vr.ingresos DESC	WITH ventas_resumen AS (\n      SELECT producto_id, SUM(total) AS ingresos, COUNT(*) AS num_ventas, SUM(cantidad) AS unidades\n      FROM ventas GROUP BY producto_id\n    )\n    SELECT p.nombre, p.categoria, vr.ingresos, vr.num_ventas, vr.unidades,\n      ROUND(vr.ingresos / NULLIF(vr.unidades,0), 2) AS precio_prom,\n      RANK() OVER (PARTITION BY p.categoria ORDER BY vr.ingresos DESC) AS rk,\n      ROUND(RATIO_TO_REPORT(vr.ingresos) OVER () * 100, 2) AS pct_total\n    FROM ventas_resumen vr\n    JOIN productos p ON p.id = vr.producto_id\n    ORDER BY vr.ingresos DESC	45	4	f	ok	2026-05-14 11:40:31.216719-05	[]	[]
540	anonymous	admin	CREATE TABLE IF NOT EXISTS tmp_test_tabla (id INT PRIMARY KEY, nombre VARCHAR(50), creado TIMESTAMP DEFAULT CURRENT_TIMESTAMP)	CREATE TABLE IF NOT EXISTS tmp_test_tabla (id INT PRIMARY KEY, nombre VARCHAR(50), creado TIMESTAMP DEFAULT CURRENT_TIMESTAMP)	101	0	f	ok	2026-05-14 11:40:31.322758-05	[]	[]
541	anonymous	admin	INSERT INTO tmp_test_tabla (id, nombre) VALUES (1, 'prueba'), (2, 'otra')	INSERT INTO tmp_test_tabla (id, nombre) VALUES (1, 'prueba'), (2, 'otra')	17	2	f	ok	2026-05-14 11:40:31.345631-05	[]	[]
542	anonymous	admin	ALTER TABLE tmp_test_tabla ADD COLUMN activo TINYINT DEFAULT 1	ALTER TABLE tmp_test_tabla ADD COLUMN activo TINYINT DEFAULT 1	56	0	f	ok	2026-05-14 11:40:31.406971-05	[]	[]
543	anonymous	admin	SELECT * FROM tmp_test_tabla ORDER BY id	SELECT * FROM tmp_test_tabla ORDER BY id	3	2	f	ok	2026-05-14 11:40:31.413608-05	[]	[]
544	anonymous	admin	DROP TABLE IF EXISTS tmp_test_tabla	DROP TABLE IF EXISTS tmp_test_tabla	48	0	f	ok	2026-05-14 11:40:31.46745-05	[]	[]
545	anonymous	admin	CREATE TABLE IF NOT EXISTS tmp_pg_test (id SERIAL PRIMARY KEY, dato TEXT);\n            INSERT INTO tmp_pg_test (dato) VALUES ('a'), ('b'), ('c');\n            DROP TABLE tmp_pg_test;	CREATE TABLE IF NOT EXISTS tmp_pg_test (id SERIAL PRIMARY KEY, dato TEXT);\n            INSERT INTO tmp_pg_test (dato) VALUES ('a'), ('b'), ('c');\n            DROP TABLE tmp_pg_test;	31	0	f	ok	2026-05-14 11:40:31.502546-05	[]	[]
546	anonymous	admin	CREATE TABLE IF NOT EXISTS tmp_carga (id INT PRIMARY KEY, valor INT, etiqueta VARCHAR(60));\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (1, 7, 'fila 1; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (2, 14, 'fila 2; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (3, 21, 'fila 3; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (4, 28, 'fila 4; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (5, 35, 'fila 5; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (6, 42, 'fila 6; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (7, 49, 'fila 7; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (8, 56, 'fila 8; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (9, 63, 'fila 9; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (10, 70, 'fila 10; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (11, 77, 'fila 11; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (12, 84, 'fila 12; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (13, 91, 'fila 13; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (14, 98, 'fila 14; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (15, 105, 'fila 15; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (16, 112, 'fila 16; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (17, 119, 'fila 17; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (18, 126, 'fila 18; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (19, 133, 'fila 19; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (20, 140, 'fila 20; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (21, 147, 'fila 21; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (22, 154, 'fila 22; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (23, 161, 'fila 23; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (24, 168, 'fila 24; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (25, 175, 'fila 25; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (26, 182, 'fila 26; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (27, 189, 'fila 27; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (28, 196, 'fila 28; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (29, 203, 'fila 29; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (30, 210, 'fila 30; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (31, 217, 'fila 31; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (32, 224, 'fila 32; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (33, 231, 'fila 33; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (34, 238, 'fila 34; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (35, 245, 'fila 35; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (36, 252, 'fila 36; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (37, 259, 'fila 37; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (38, 266, 'fila 38; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (39, 273, 'fila 39; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (40, 280, 'fila 40; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (41, 287, 'fila 41; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (42, 294, 'fila 42; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (43, 301, 'fila 43; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (44, 308, 'fila 44; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (45, 315, 'fila 45; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (46, 322, 'fila 46; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (47, 329, 'fila 47; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (48, 336, 'fila 48; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (49, 343, 'fila 49; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (50, 350, 'fila 50; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (51, 357, 'fila 51; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (52, 364, 'fila 52; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (53, 371, 'fila 53; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (54, 378, 'fila 54; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (55, 385, 'fila 55; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (56, 392, 'fila 56; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (57, 399, 'fila 57; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (58, 406, 'fila 58; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (59, 413, 'fila 59; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (60, 420, 'fila 60; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (61, 427, 'fila 61; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (62, 434, 'fila 62; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (63, 441, 'fila 63; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (64, 448, 'fila 64; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (65, 455, 'fila 65; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (66, 462, 'fila 66; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (67, 469, 'fila 67; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (68, 476, 'fila 68; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (69, 483, 'fila 69; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (70, 490, 'fila 70; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (71, 497, 'fila 71; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (72, 504, 'fila 72; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (73, 511, 'fila 73; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (74, 518, 'fila 74; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (75, 525, 'fila 75; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (76, 532, 'fila 76; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (77, 539, 'fila 77; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (78, 546, 'fila 78; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (79, 553, 'fila 79; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (80, 560, 'fila 80; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (81, 567, 'fila 81; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (82, 574, 'fila 82; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (83, 581, 'fila 83; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (84, 588, 'fila 84; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (85, 595, 'fila 85; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (86, 602, 'fila 86; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (87, 609, 'fila 87; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (88, 616, 'fila 88; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (89, 623, 'fila 89; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (90, 630, 'fila 90; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (91, 637, 'fila 91; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (92, 644, 'fila 92; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (93, 651, 'fila 93; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (94, 658, 'fila 94; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (95, 665, 'fila 95; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (96, 672, 'fila 96; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (97, 679, 'fila 97; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (98, 686, 'fila 98; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (99, 693, 'fila 99; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (100, 700, 'fila 100; con punto y coma en el texto');\nSELECT COUNT(*) AS total, SUM(valor) AS suma, MAX(valor) AS maximo FROM tmp_carga;\nDROP TABLE tmp_carga;	CREATE TABLE IF NOT EXISTS tmp_carga (id INT PRIMARY KEY, valor INT, etiqueta VARCHAR(60));\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (1, 7, 'fila 1; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (2, 14, 'fila 2; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (3, 21, 'fila 3; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (4, 28, 'fila 4; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (5, 35, 'fila 5; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (6, 42, 'fila 6; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (7, 49, 'fila 7; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (8, 56, 'fila 8; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (9, 63, 'fila 9; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (10, 70, 'fila 10; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (11, 77, 'fila 11; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (12, 84, 'fila 12; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (13, 91, 'fila 13; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (14, 98, 'fila 14; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (15, 105, 'fila 15; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (16, 112, 'fila 16; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (17, 119, 'fila 17; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (18, 126, 'fila 18; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (19, 133, 'fila 19; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (20, 140, 'fila 20; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (21, 147, 'fila 21; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (22, 154, 'fila 22; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (23, 161, 'fila 23; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (24, 168, 'fila 24; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (25, 175, 'fila 25; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (26, 182, 'fila 26; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (27, 189, 'fila 27; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (28, 196, 'fila 28; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (29, 203, 'fila 29; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (30, 210, 'fila 30; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (31, 217, 'fila 31; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (32, 224, 'fila 32; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (33, 231, 'fila 33; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (34, 238, 'fila 34; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (35, 245, 'fila 35; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (36, 252, 'fila 36; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (37, 259, 'fila 37; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (38, 266, 'fila 38; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (39, 273, 'fila 39; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (40, 280, 'fila 40; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (41, 287, 'fila 41; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (42, 294, 'fila 42; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (43, 301, 'fila 43; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (44, 308, 'fila 44; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (45, 315, 'fila 45; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (46, 322, 'fila 46; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (47, 329, 'fila 47; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (48, 336, 'fila 48; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (49, 343, 'fila 49; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (50, 350, 'fila 50; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (51, 357, 'fila 51; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (52, 364, 'fila 52; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (53, 371, 'fila 53; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (54, 378, 'fila 54; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (55, 385, 'fila 55; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (56, 392, 'fila 56; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (57, 399, 'fila 57; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (58, 406, 'fila 58; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (59, 413, 'fila 59; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (60, 420, 'fila 60; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (61, 427, 'fila 61; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (62, 434, 'fila 62; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (63, 441, 'fila 63; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (64, 448, 'fila 64; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (65, 455, 'fila 65; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (66, 462, 'fila 66; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (67, 469, 'fila 67; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (68, 476, 'fila 68; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (69, 483, 'fila 69; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (70, 490, 'fila 70; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (71, 497, 'fila 71; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (72, 504, 'fila 72; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (73, 511, 'fila 73; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (74, 518, 'fila 74; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (75, 525, 'fila 75; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (76, 532, 'fila 76; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (77, 539, 'fila 77; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (78, 546, 'fila 78; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (79, 553, 'fila 79; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (80, 560, 'fila 80; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (81, 567, 'fila 81; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (82, 574, 'fila 82; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (83, 581, 'fila 83; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (84, 588, 'fila 84; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (85, 595, 'fila 85; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (86, 602, 'fila 86; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (87, 609, 'fila 87; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (88, 616, 'fila 88; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (89, 623, 'fila 89; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (90, 630, 'fila 90; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (91, 637, 'fila 91; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (92, 644, 'fila 92; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (93, 651, 'fila 93; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (94, 658, 'fila 94; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (95, 665, 'fila 95; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (96, 672, 'fila 96; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (97, 679, 'fila 97; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (98, 686, 'fila 98; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (99, 693, 'fila 99; con punto y coma en el texto');\nINSERT INTO tmp_carga (id, valor, etiqueta) VALUES (100, 700, 'fila 100; con punto y coma en el texto');\nSELECT COUNT(*) AS total, SUM(valor) AS suma, MAX(valor) AS maximo FROM tmp_carga;\nDROP TABLE tmp_carga;	1664	0	f	ok	2026-05-14 11:41:19.879205-05	[]	[]
547	anonymous	admin	SELECT table_name, COUNT(*) AS num_columnas FROM information_schema.columns WHERE table_schema='public' GROUP BY table_name ORDER BY table_name	SELECT table_name, COUNT(*) AS num_columnas FROM information_schema.columns WHERE table_schema='public' GROUP BY table_name ORDER BY table_name	14	13	f	ok	2026-05-14 11:41:19.907673-05	[]	[]
549	anonymous	admin	SELECT table_name, COUNT(*) AS cols FROM information_schema.columns WHERE table_schema='testdb' GROUP BY table_name	SELECT table_name, COUNT(*) AS cols FROM information_schema.columns WHERE table_schema='testdb' GROUP BY table_name	6	7	f	ok	2026-05-14 11:41:19.929896-05	[]	[]
550	anonymous	admin	SELECT table_name, COUNT(*) AS cols FROM user_tab_columns GROUP BY table_name	SELECT table_name, COUNT(*) AS cols FROM user_tab_columns GROUP BY table_name	208	4	f	ok	2026-05-14 11:41:20.141515-05	[]	[]
551	anonymous	admin	{"collection":"conciliacion","find":{},"limit":1}	{"collection":"conciliacion","find":{},"limit":1}	8	1	f	ok	2026-05-14 11:41:20.156627-05	[]	[]
552	anonymous	admin	SELECT\n  ARRAY[1,2,3] AS arreglo,\n  '{"a":1,"b":[2,3]}'::jsonb -> 'b' AS json_extract,\n  generate_series(1,3) AS serie,\n  CASE WHEN true THEN 'sí' ELSE 'no' END AS condicional,\n  EXTRACT(YEAR FROM NOW()) AS anio	SELECT\n  ARRAY[1,2,3] AS arreglo,\n  '{"a":1,"b":[2,3]}'::jsonb -> 'b' AS json_extract,\n  generate_series(1,3) AS serie,\n  CASE WHEN true THEN 'sí' ELSE 'no' END AS condicional,\n  EXTRACT(YEAR FROM NOW()) AS anio	3	3	f	ok	2026-05-14 11:41:20.167865-05	[]	[]
553	anonymous	admin	SELECT id, nombre FROM empleados WHERE nombre = {{nombre}} OR cargo LIKE {{patron}}	SELECT id, nombre FROM empleados WHERE ($1 IS NULL OR nombre = $1) OR cargo LIKE $2	18	1	f	ok	2026-05-14 11:42:10.143491-05	["Juan; DROP", "%Gerente%"]	["nombre", "patron"]
554	anonymous	admin	SELECT count(*) AS n, string_agg(tablename, ', ' ORDER BY tablename) AS tablas FROM pg_tables WHERE schemaname='public'	SELECT count(*) AS n, string_agg(tablename, ', ' ORDER BY tablename) AS tablas FROM pg_tables WHERE schemaname='public'	8	1	f	ok	2026-05-14 12:02:55.394277-05	[]	[]
555	anonymous	admin	SELECT current_database() AS db, inet_server_addr()::text AS host, inet_server_port() AS port, version() AS ver	SELECT current_database() AS db, inet_server_addr()::text AS host, inet_server_port() AS port, version() AS ver	2	1	f	ok	2026-05-14 12:02:55.484434-05	[]	[]
\.


--
-- Data for Name: roles; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.roles (id, nombre, descripcion) FROM stdin;
c81054bb-9237-4139-b68f-ee8e669a690c	admin	Administrador
0a3ac834-14cc-4c52-9db4-2a2c2ade2f44	user	Usuario
\.


--
-- Data for Name: semantic_learning; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.semantic_learning (id, db_fingerprint, normalized_term, original_term, target_kind, target_name, table_name, confidence, hit_count, created_at, last_seen) FROM stdin;
1	runtime:::1_128|5432|knowledge_base|public|postgres	usan	usan	table	users		0.850	1	2026-04-22 12:21:34.66225-05	2026-04-22 12:21:34.66225-05
2	runtime:::1_128|5432|knowledge_base|public|postgres	sistema	sistema	table	users		0.850	1	2026-04-22 12:21:34.668707-05	2026-04-22 12:21:34.668707-05
3	runtime:::1_128|5432|knowledge_base|public|postgres	ultimamente	ultimamente	table	users		0.850	1	2026-04-22 12:21:34.669558-05	2026-04-22 12:21:34.669558-05
4	runtime:::1_128|5432|knowledge_base|public|postgres	limit	limit	table	users		0.850	1	2026-04-22 12:30:10.985643-05	2026-04-22 12:30:10.985643-05
5	runtime:::1_128|5432|knowledge_base|public|postgres	f074b61c	f074b61c	table	users		0.850	1	2026-04-22 12:30:16.222613-05	2026-04-22 12:30:16.222613-05
6	runtime:::1_128|5432|knowledge_base|public|postgres	16e6	16e6	table	users		0.850	1	2026-04-22 12:30:16.223705-05	2026-04-22 12:30:16.223705-05
7	runtime:::1_128|5432|knowledge_base|public|postgres	4a8e	4a8e	table	users		0.850	1	2026-04-22 12:30:16.224635-05	2026-04-22 12:30:16.224635-05
8	runtime:::1_128|5432|knowledge_base|public|postgres	a614	a614	table	users		0.850	1	2026-04-22 12:30:16.225351-05	2026-04-22 12:30:16.225351-05
9	runtime:::1_128|5432|knowledge_base|public|postgres	30946b4b056b	30946b4b056b	table	users		0.850	1	2026-04-22 12:30:16.22598-05	2026-04-22 12:30:16.22598-05
10	runtime:::1_128|5432|knowledge_base|public|postgres	oracle	oracle	column	role	users	0.700	1	2026-04-29 06:20:14.605936-05	2026-04-29 06:20:14.605936-05
\.


--
-- Data for Name: sessions; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.sessions (id, user_id, token, activo, created_at) FROM stdin;
47930ced-5724-4c80-9923-f67e8b6ff3ba	f074b61c-16e6-4a8e-a614-30946b4b056b	token_123	t	2026-04-09 10:40:36.53255
4490389c-285f-46ec-bd80-0528891ab403	747bd085-6444-49a4-9919-64660808316c	mock-jwt-token-1778548319483	t	2026-05-11 20:11:59.483883
94b1b50d-4cfc-416b-8a8e-9ce15936373b	747bd085-6444-49a4-9919-64660808316c	mock-jwt-token-1778548472082	t	2026-05-11 20:14:32.083398
e405495a-f035-4cc2-8732-be9e82ca94d7	747bd085-6444-49a4-9919-64660808316c	mock-jwt-token-1778549322687	t	2026-05-11 20:28:42.687847
8c2dad5f-85f3-4bb6-919a-ba92c8c84718	747bd085-6444-49a4-9919-64660808316c	mock-jwt-token-1778555162897	t	2026-05-11 22:06:02.899006
dec408f0-57c0-475e-bb84-7ac737bace75	747bd085-6444-49a4-9919-64660808316c	mock-jwt-token-1778592623018	t	2026-05-12 08:30:23.019063
bc893411-a58d-4866-a9ca-eeb8fa75d1f4	747bd085-6444-49a4-9919-64660808316c	mock-jwt-token-1778605260634	t	2026-05-12 12:01:00.635287
6baa0c15-e759-4677-aef1-bccea4c58728	747bd085-6444-49a4-9919-64660808316c	mock-jwt-token-1778605279828	t	2026-05-12 12:01:19.829198
24f91088-f3c3-4cba-9e23-7ca7ad731139	747bd085-6444-49a4-9919-64660808316c	mock-jwt-token-1778622220380	t	2026-05-12 16:43:40.381792
5ac33a6a-a71b-410f-be66-ec1fa08e8954	09b3c854-21a2-4974-942a-41c40ca146de	mock-jwt-token-1778622235710	t	2026-05-12 16:43:55.711464
1bc413b3-81cc-44aa-9d85-19f3b1b8ea11	747bd085-6444-49a4-9919-64660808316c	mock-jwt-token-1778622243270	t	2026-05-12 16:44:03.272294
015a94b9-0d22-4510-bf20-477b3012969a	747bd085-6444-49a4-9919-64660808316c	mock-jwt-token-1778642052571	t	2026-05-12 22:14:12.572216
2bc3b79b-9205-45d8-9823-0d9dc6be0dd6	09b3c854-21a2-4974-942a-41c40ca146de	mock-jwt-token-1778645704936	t	2026-05-12 23:15:04.937962
cf9c4d10-221e-468e-9bb1-3de923f77ded	747bd085-6444-49a4-9919-64660808316c	mock-jwt-token-1778645758742	t	2026-05-12 23:15:58.742555
ba917ed8-7223-41c3-84c2-be7e79ad5a2a	747bd085-6444-49a4-9919-64660808316c	mock-jwt-token-1778645768533	t	2026-05-12 23:16:08.534179
bdb0784e-a999-4655-90cf-cbb3263c2b9f	09b3c854-21a2-4974-942a-41c40ca146de	mock-jwt-token-1778646288429	t	2026-05-12 23:24:48.430268
742c9650-4fe1-4192-b361-630c6f061837	747bd085-6444-49a4-9919-64660808316c	mock-jwt-token-1778646956412	t	2026-05-12 23:35:56.413692
201b85b6-1a0a-41bb-9479-8aa062591841	09b3c854-21a2-4974-942a-41c40ca146de	mock-jwt-token-1778647017406	t	2026-05-12 23:36:57.408018
2585bf6a-3386-490e-97b5-7a093b0b5c46	747bd085-6444-49a4-9919-64660808316c	mock-jwt-token-1778647165277	t	2026-05-12 23:39:25.278103
2308be3f-6025-4634-b44d-37423e89857b	09b3c854-21a2-4974-942a-41c40ca146de	mock-jwt-token-1778647296656	t	2026-05-12 23:41:36.656953
51608d12-4763-4074-8c71-c734cc70847e	747bd085-6444-49a4-9919-64660808316c	mock-jwt-token-1778648206372	t	2026-05-12 23:56:46.372905
1563ebfa-9383-4b2c-a732-9e11fc6e8168	747bd085-6444-49a4-9919-64660808316c	mock-jwt-token-1778683753845	t	2026-05-13 09:49:13.846807
5c8b2021-8921-410b-8d3f-a49d110b6557	747bd085-6444-49a4-9919-64660808316c	mock-jwt-token-1778684582808	f	2026-05-13 10:03:02.808981
0d1409ea-02f2-4427-b74e-ae9e4be66b61	747bd085-6444-49a4-9919-64660808316c	mock-jwt-token-1778686645664	f	2026-05-13 10:37:25.665156
47cf1a4e-e99e-4acd-9eaa-aa77a1a5ec6a	747bd085-6444-49a4-9919-64660808316c	mock-jwt-token-1778687110292	f	2026-05-13 10:45:10.295519
ed192da8-0fa0-425a-b38a-41aea066036a	09b3c854-21a2-4974-942a-41c40ca146de	mock-jwt-token-1778693294159	f	2026-05-13 12:28:14.159918
6f6280c7-0465-4284-b986-a1eb9e1ff499	747bd085-6444-49a4-9919-64660808316c	mock-jwt-token-1778693315494	f	2026-05-13 12:28:35.495953
017e14a5-a6a7-4726-a443-56cdc2a20d84	09b3c854-21a2-4974-942a-41c40ca146de	mock-jwt-token-1778693334901	f	2026-05-13 12:28:54.905637
cf18b40a-22a9-4ddf-a0db-9e745bb7c6cf	747bd085-6444-49a4-9919-64660808316c	mock-jwt-token-1778693552574	t	2026-05-13 12:32:32.576459
d97709f5-6869-4625-87cc-775108caaeca	747bd085-6444-49a4-9919-64660808316c	mock-jwt-token-1778719133788	t	2026-05-13 19:38:53.78917
29370e57-9771-44cd-addb-80f55ad98b41	747bd085-6444-49a4-9919-64660808316c	mock-jwt-token-1778765093937	t	2026-05-14 08:24:53.938631
a0b99dcf-c6ab-48bc-8bea-cd6118062f48	747bd085-6444-49a4-9919-64660808316c	mock-jwt-token-1778775917801	t	2026-05-14 11:25:17.802045
\.


--
-- Data for Name: system_events; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.system_events (id, evento, usuario, rol, detalle, ip, fecha) FROM stdin;
1	excel_descarga	admin	admin	{"filas": 3, "archivo": "resultado_2026-05-13.xlsx", "columnas": 13, "articulo_id": "b946ef9b-f6e1-4ce2-a9dd-b3f33c5d991e", "articulo_titulo": "Error en transferencia bancaria."}	::1	2026-05-12 23:39:36.301903
2	excel_descarga	user	user	{"filas": 3, "archivo": "resultado_2026-05-13.xlsx", "columnas": 13, "articulo_id": "b946ef9b-f6e1-4ce2-a9dd-b3f33c5d991e", "articulo_titulo": "Error en transferencia bancaria."}	::1	2026-05-12 23:41:42.731932
\.


--
-- Data for Name: users; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.users (id, username, password, role, created_at, bloqueado, role_id, activo) FROM stdin;
f074b61c-16e6-4a8e-a614-30946b4b056b	test_user	$2a$06$UGHUbKbNPEGii3FrG9nqR.y9Dx96wRMbq0LziUbTxP9xDHSg3gO/O	user	2026-04-09 10:40:36.53255	f	0a3ac834-14cc-4c52-9db4-2a2c2ade2f44	t
09b3c854-21a2-4974-942a-41c40ca146de	user	$2a$10$irRXvAOaFTZcLY3o/PiXDOsx733B.5HSH5zKKuaFU2f9M5OK8mMvG	user	2026-03-31 18:43:59.547827	f	0a3ac834-14cc-4c52-9db4-2a2c2ade2f44	t
747bd085-6444-49a4-9919-64660808316c	admin	$2a$10$13CxhWanZyp.XPs450qrHO7wZN643UUlYBOI6nY3uT0XGd9GFob0G	admin	2026-03-31 18:43:59.544842	f	c81054bb-9237-4139-b68f-ee8e669a690c	t
\.


--
-- Name: article_changes_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.article_changes_id_seq', 38, true);


--
-- Name: article_executions_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.article_executions_id_seq', 96, true);


--
-- Name: comments_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.comments_id_seq', 17, true);


--
-- Name: error_logs_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.error_logs_id_seq', 1, false);


--
-- Name: login_audit_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.login_audit_id_seq', 29, true);


--
-- Name: logs_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.logs_id_seq', 4, true);


--
-- Name: query_history_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.query_history_id_seq', 555, true);


--
-- Name: semantic_learning_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.semantic_learning_id_seq', 10, true);


--
-- Name: system_events_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.system_events_id_seq', 2, true);


--
-- Name: article_changes article_changes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.article_changes
    ADD CONSTRAINT article_changes_pkey PRIMARY KEY (id);


--
-- Name: article_executions article_executions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.article_executions
    ADD CONSTRAINT article_executions_pkey PRIMARY KEY (id);


--
-- Name: comments comments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.comments
    ADD CONSTRAINT comments_pkey PRIMARY KEY (id);


--
-- Name: error_logs error_logs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.error_logs
    ADD CONSTRAINT error_logs_pkey PRIMARY KEY (id);


--
-- Name: knowledge_base knowledge_base_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.knowledge_base
    ADD CONSTRAINT knowledge_base_pkey PRIMARY KEY (id);


--
-- Name: login_audit login_audit_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.login_audit
    ADD CONSTRAINT login_audit_pkey PRIMARY KEY (id);


--
-- Name: logs logs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.logs
    ADD CONSTRAINT logs_pkey PRIMARY KEY (id);


--
-- Name: query_history query_history_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.query_history
    ADD CONSTRAINT query_history_pkey PRIMARY KEY (id);


--
-- Name: roles roles_nombre_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.roles
    ADD CONSTRAINT roles_nombre_key UNIQUE (nombre);


--
-- Name: roles roles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.roles
    ADD CONSTRAINT roles_pkey PRIMARY KEY (id);


--
-- Name: semantic_learning semantic_learning_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.semantic_learning
    ADD CONSTRAINT semantic_learning_pkey PRIMARY KEY (id);


--
-- Name: sessions sessions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT sessions_pkey PRIMARY KEY (id);


--
-- Name: system_events system_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.system_events
    ADD CONSTRAINT system_events_pkey PRIMARY KEY (id);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: users users_username_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_username_key UNIQUE (username);


--
-- Name: idx_articulos_creado_por; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_articulos_creado_por ON public.knowledge_base USING btree (creado_por);


--
-- Name: idx_articulos_tags; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_articulos_tags ON public.knowledge_base USING gin (tags);


--
-- Name: idx_comments_article_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_comments_article_id ON public.comments USING btree (article_id);


--
-- Name: idx_comments_parent_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_comments_parent_id ON public.comments USING btree (parent_id);


--
-- Name: idx_error_logs_fecha; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_error_logs_fecha ON public.error_logs USING btree (fecha DESC);


--
-- Name: idx_error_logs_modulo; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_error_logs_modulo ON public.error_logs USING btree (modulo);


--
-- Name: idx_logs_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_logs_created_at ON public.logs USING btree (created_at);


--
-- Name: idx_logs_usuario; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_logs_usuario ON public.logs USING btree (usuario);


--
-- Name: idx_query_history_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_query_history_created_at ON public.query_history USING btree (created_at DESC);


--
-- Name: idx_query_history_username; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_query_history_username ON public.query_history USING btree (username);


--
-- Name: idx_semantic_learning_fingerprint; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_semantic_learning_fingerprint ON public.semantic_learning USING btree (db_fingerprint);


--
-- Name: idx_semantic_learning_last_seen; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_semantic_learning_last_seen ON public.semantic_learning USING btree (last_seen DESC);


--
-- Name: idx_semantic_learning_term; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_semantic_learning_term ON public.semantic_learning USING btree (normalized_term);


--
-- Name: idx_semantic_learning_unique_scope; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_semantic_learning_unique_scope ON public.semantic_learning USING btree (db_fingerprint, normalized_term, target_kind, target_name, table_name);


--
-- Name: idx_usuarios_username; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_usuarios_username ON public.users USING btree (username);


--
-- Name: comments comments_parent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.comments
    ADD CONSTRAINT comments_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES public.comments(id) ON DELETE CASCADE;


--
-- Name: logs fk_logs_users; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.logs
    ADD CONSTRAINT fk_logs_users FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE SET NULL;


--
-- Name: sessions fk_sessions_user_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT fk_sessions_user_id FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: sessions fk_sessions_users; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT fk_sessions_users FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: users fk_users_role_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT fk_users_role_id FOREIGN KEY (role_id) REFERENCES public.roles(id);


--
-- Name: users fk_users_roles; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT fk_users_roles FOREIGN KEY (role_id) REFERENCES public.roles(id) ON DELETE SET NULL;


--
-- Name: knowledge_base knowledge_base_creado_por_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.knowledge_base
    ADD CONSTRAINT knowledge_base_creado_por_fkey FOREIGN KEY (creado_por) REFERENCES public.users(id);


--
-- PostgreSQL database dump complete
--

\unrestrict 0If1jujhwoiSFqQ6e3tPfFJ6YacESqjG8dhH5SGbH6eev1yazQegMfNpgeBGUc7

