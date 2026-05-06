# Sistema Multi-DB con IA — Documentación técnica

## Índice
1. [Visión general](#1-visión-general)
2. [Stack tecnológico](#2-stack-tecnológico)
3. [Cómo el backend se conecta a las bases de datos](#3-cómo-el-backend-se-conecta-a-las-bases-de-datos)
4. [Cómo se extrae y muestra información](#4-cómo-se-extrae-y-muestra-información)
5. [Detección automática de base de datos al insertar SQL](#5-detección-automática-de-base-de-datos-al-insertar-sql)
6. [Sistema de IA — cómo Ollama se conecta al backend](#6-sistema-de-ia--cómo-ollama-se-conecta-al-backend)
7. [Flujo completo de una consulta en lenguaje natural](#7-flujo-completo-de-una-consulta-en-lenguaje-natural)
8. [Docker — cómo están containerizados los servicios](#8-docker--cómo-están-containerizados-los-servicios)
9. [Archivos clave](#9-archivos-clave)

---

## 1. Visión general

El sistema es un **motor de consultas SQL multi-base** que permite:

- Ejecutar SQL directamente sobre **PostgreSQL** y **Oracle** (y MySQL en el futuro).
- Escribir preguntas en lenguaje natural ("dame las tablas de oracle") y obtener resultados reales.
- Detectar automáticamente a qué base de datos pertenece una consulta SQL ingresada manualmente.
- Mostrar resultados, mensajes de consola (`RAISE NOTICE`, `DBMS_OUTPUT`) y errores de forma legible.

```
Usuario (navegador)
      │
      ▼
  Frontend (Next.js)          ← Panel SQL, tabla de resultados, IA assistant
      │  HTTP / fetch
      ▼
  Backend (Node.js / Express) ← app.js + QueryRoute.js
      │
      ├──► PostgreSQL (pg.Pool)
      ├──► Oracle     (oracledb thin mode)
      └──► Ollama     (deepseek-coder — IA local)
```

---

## 2. Stack tecnológico

| Capa | Tecnología |
|------|-----------|
| Frontend | Next.js 15, React, Tailwind CSS |
| Backend | Node.js 20 (ESM), Express |
| Base de datos 1 | PostgreSQL 15 — pool `pg` |
| Base de datos 2 | Oracle XE 21c — driver `oracledb` (thin mode, sin Instant Client) |
| IA local | Ollama + modelo `deepseek-coder` |
| Configuración multi-DB | `config/multidb.databases.json` |
| Contenedores | Docker Compose v2 |

---

## 3. Cómo el backend se conecta a las bases de datos

### Archivo de configuración central
`backend/config/multidb.databases.json` define todas las bases de datos disponibles:

```json
{
  "databases": [
    {
      "id": "pg_main",       "type": "postgres",
      "host": "localhost",   "port": 5432,
      "database": "knowledge_base",
      "user": "postgres",    "password": "...",
      "enabled": true
    },
    {
      "id": "oracle_test",   "type": "oracle",
      "host": "localhost",   "port": 1521,
      "database": "XEPDB1",
      "user": "APP",         "password": "...",
      "enabled": true
    }
  ]
}
```

En Docker, `multidb.databases.docker.json` usa los nombres de servicio (`postgres`, `oracle`) en lugar de `localhost`.

### MultiDbRegistry
`backend/src/infrastructure/distributed/MultiDbRegistry.js` carga ese JSON al arrancar el servidor y mantiene una conexión o pool activo por cada entrada:

- **PostgreSQL** → crea un `pg.Pool` con los parámetros del JSON.
- **Oracle** → usa `oracledb` en **thin mode** (pure JavaScript, sin Oracle Client instalado).

El registry expone el método principal:
```js
registry.executeCompiledQuery({ databaseId, sql, params })
```
Ese método enruta la consulta al driver correcto según el `type` del database con ese `id`.

### Limpieza de SQL antes de ejecutar (Oracle)
Oracle rechaza el terminador `/` de SQL*Plus. El backend lo elimina automáticamente:
```js
const cleanSql = sql.trimEnd().replace(/\s*\n\s*\/\s*$/, '').trimEnd();
```

---

## 4. Cómo se extrae y muestra información

### Flujo de ejecución directa (`/api/query/execute-generated`)

```
Frontend envía: { sql, databaseId }
       │
       ▼
  backend/src/app.js  →  executeCompiledQuery({ databaseId, sql })
       │
       ├── PostgreSQL → pool.query(sql) → rows[]
       │       └── RAISE NOTICE capturado → resumenHumano
       │
       └── Oracle → connection.execute(sql, [], { outFormat: OBJECT })
               ├── rows[] normales
               └── DBMS_OUTPUT.GET_LINES → columna `output` en cada fila
```

### Tipos de respuesta que el backend produce

| Situación | Lo que devuelve |
|-----------|----------------|
| Query normal con filas | `{ resultado: [...rows], metadata: { total } }` |
| PL/SQL con `RAISE NOTICE` | `{ resumenHumano: "texto del NOTICE", resultado: [] }` |
| PL/SQL con `DBMS_OUTPUT` | `{ resultado: [{ output: "línea" }, ...] }` |
| Error de sintaxis SQL | `{ success: false, error: "mensaje" }` |

### Renderizado en el frontend

- **Filas normales** → tabla con columnas y filas.
- **`resultado[0].output` existe** → bloque terminal ámbar con etiqueta `DBMS_OUTPUT:`.
- **`resumenHumano` con saltos de línea** → bloque terminal azul con etiqueta `NOTICE:`.
- **`resumenHumano` de una sola línea** → badge de texto debajo del SQL.

---

## 5. Detección automática de base de datos al insertar SQL

Cuando el usuario escribe SQL manualmente sin especificar `databaseId`:

### Paso 1 — Hint explícito del frontend
El usuario selecciona "Oracle" o "Postgres" en el selector del panel → el frontend envía `databaseHint`.

### Paso 2 — Palabras clave en el SQL

| Detecta | Indica |
|---------|--------|
| `USER_TABLES`, `USER_TAB_COLUMNS`, `ROWNUM`, `DBMS_OUTPUT`, `dual` | Oracle |
| `information_schema`, `pg_`, `SERIAL`, `ILIKE`, `RETURNING` | PostgreSQL |
| `SHOW TABLES`, `AUTO_INCREMENT`, `ENGINE=InnoDB` | MySQL |

### Paso 3 — Base primaria por defecto
Si no hay pistas, se usa la base marcada con `"primary": true` en el JSON de configuración.

---

## 6. Sistema de IA — cómo Ollama se conecta al backend

### Componentes

```
OllamaClient.js          ← cliente HTTP liviano
      │
      ▼
http://localhost:11434/api/generate   ← Ollama (proceso local o contenedor)
      │
      ▼
modelo: deepseek-coder   ← LLM especializado en código/SQL
```

### Variables de entorno

```env
OLLAMA_URL=http://localhost:11434/api/generate
OLLAMA_MODEL=deepseek-coder
OLLAMA_TIMEOUT_MS=2800
```

`OllamaClient.js` normaliza la URL automáticamente — si recibe solo la raíz (`http://host:port`), añade `/api/generate` solo.

### Extracción de JSON robusta
El cliente extrae el JSON de la respuesta de Ollama con 4 estrategias en cascada:
1. `JSON.parse(text)` directo
2. Bloques de código fenced ` ```json ``` `
3. Slice por `{` y `}` más externo
4. **Regex por campo** (funciona aunque el JSON esté truncado a mitad de la `explanation`)

### Warmup al arrancar
Al iniciarse el servidor se hace una llamada silenciosa con `"warmup"` para cargar el modelo en memoria antes de la primera consulta real.

---

## 7. Flujo completo de una consulta en lenguaje natural

El usuario escribe: *"dame las tablas de oracle"*

```
Frontend (SqlAssistantPanel.jsx)
  POST /api/query/generate  { text: "dame las tablas de oracle" }
         │
         ▼
  backend/src/app.js
         │
         ├── PASO 0: Ollama (siempre primero para lenguaje natural)
         │       │
         │       ├─ Recopila esquema de TODAS las DBs habilitadas
         │       │     Oracle: SELECT table_name, column_name FROM USER_TAB_COLUMNS
         │       │     Postgres: SELECT table_name, column_name FROM information_schema.columns
         │       │
         │       ├─ Construye prompt con reglas de sintaxis + ejemplos + esquema real
         │       │
         │       ├─ Llama Ollama (timeout 30s, temperature 0.1)
         │       │     Respuesta: {"sql":"SELECT table_name FROM USER_TABLES","database_id":"oracle_test"}
         │       │
         │       ├─ Sanitiza SQL generado:
         │       │     • Quita backticks  `tabla` → tabla
         │       │     • Postgres + ROWNUM → convierte a LIMIT
         │       │     • Oracle + LIMIT   → convierte a WHERE ROWNUM <=
         │       │     • Elimina WHERE placeholders hallucinated ('your-value')
         │       │
         │       └─ Devuelve: { query: { sql, databaseId }, success: true }
         │
         ├── PASO 1 (fallback): Motor semántico determinístico
         │
         └── PASO 2 (fallback): Error con sugerencias

Frontend recibe { query: { sql, databaseId } }
  │
  └─ POST /api/query/execute-generated { sql, databaseId: "oracle_test" }
         │
         ▼
  MultiDbRegistry → Oracle → rows: [{ TABLE_NAME: "USERS" }, ...]
         │
         ▼
  { resultado: [...rows] }  →  Frontend renderiza tabla
```

---

## 8. Docker — cómo están containerizados los servicios

### Servicios en `docker-compose.yml`

| Servicio | Imagen | Puerto | Descripción |
|---------|--------|--------|-------------|
| `postgres` | `postgres:15-alpine` | 5432 | Importa `database_schema_complete.sql` automáticamente |
| `oracle` | `gvenzl/oracle-xe:21-slim` | 1521 | Oracle XE 21c, crea usuario APP + tablas en XEPDB1 |
| `ollama` | `ollama/ollama:latest` | 11434 | IA local, volumen persistente para el modelo |
| `backend` | `./backend/Dockerfile` | 3002 | Espera a que postgres y oracle estén healthy |
| `frontend` | `./frontend/Dockerfile` | 3000 | Llama al backend via `http://backend:3002` |

### Por qué `gvenzl/oracle-xe:21-slim`
- No requiere cuenta Oracle Container Registry
- ~1.5 GB vs ~8 GB de la imagen oficial
- `APP_USER` / `APP_USER_PASSWORD` crea el usuario automáticamente en XEPDB1
- Incluye `healthcheck.sh` — el backend espera la señal antes de arrancar

### Diferencia entre config local y Docker

| Archivo | Usa | Para |
|---------|-----|------|
| `multidb.databases.json` | `localhost` | Desarrollo local |
| `multidb.databases.docker.json` | `postgres`, `oracle` | Contenedores Docker |

### Flujo de red en Docker

```
Browser → :3000 (frontend container)
             │ /api/query/generate
             ▼
         frontend container
             │ http://backend:3002 (red interna Docker)
             ▼
         backend container
             ├── postgres:5432  (red interna Docker)
             ├── oracle:1521    (red interna Docker)
             └── ollama:11434   (red interna Docker)
```

---

## 9. Archivos clave

```
backend/
├── .env                                     ← URLs, credenciales, modelo Ollama
├── .env.docker.example                      ← plantilla para Docker
├── config/
│   ├── multidb.databases.json               ← bases para desarrollo local
│   ├── multidb.databases.docker.json        ← bases para Docker
│   └── multidb.databases.example.json       ← plantilla documentada
└── src/
    ├── app.js                               ← servidor Express + todos los endpoints
    │   ├── POST /api/query/generate         ← PASO 0 (Ollama) + motor semántico
    │   └── POST /api/query/execute-generated← ejecuta SQL en la DB indicada
    └── infrastructure/
        ├── ai/OllamaClient.js               ← cliente HTTP para Ollama
        └── distributed/
            └── MultiDbRegistry.js           ← pool/conexión por DB + router

docker-compose.yml                           ← orquestación completa
docker/oracle/
├── 01-create-user.sql                       ← crea usuario APP en XEPDB1
└── 02-create-tables.sql                     ← tablas + datos de prueba Oracle

frontend/
└── components/
    ├── SqlAssistantPanel.jsx                ← panel de lenguaje natural
    └── app/components/ContentPanel.tsx      ← visualización de resultados
```
