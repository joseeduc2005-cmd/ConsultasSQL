# Sistema Multi-DB con IA — Documentación técnica

## Índice
1. [Visión general](#1-visión-general)
2. [Stack tecnológico](#2-stack-tecnológico)
3. [Cómo el backend se conecta a las bases de datos](#3-cómo-el-backend-se-conecta-a-las-bases-de-datos)
4. [Cómo se extrae y muestra información](#4-cómo-se-extrae-y-muestra-información)
5. [Detección automática de base de datos al insertar SQL](#5-detección-automática-de-base-de-datos-al-insertar-sql)
6. [Sistema de IA — cómo Ollama se conecta al backend](#6-sistema-de-ia--cómo-ollama-se-conecta-al-backend)
7. [Flujo completo de una consulta en lenguaje natural](#7-flujo-completo-de-una-consulta-en-lenguaje-natural)
8. [Archivos clave](#8-archivos-clave)

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
      ├──► Oracle     (oracledb)
      └──► Ollama     (deepseek-coder — IA local)
```

---

## 2. Stack tecnológico

| Capa | Tecnología |
|------|-----------|
| Frontend | Next.js 14, React, Tailwind CSS |
| Backend | Node.js (ESM), Express |
| Base de datos 1 | PostgreSQL 15 — pool `pg` |
| Base de datos 2 | Oracle XE 21c — driver `oracledb` |
| IA local | Ollama + modelo `deepseek-coder` |
| Configuración multi-DB | `config/multidb.databases.json` |

---

## 3. Cómo el backend se conecta a las bases de datos

### Archivo de configuración central
`backend/config/multidb.databases.json` define todas las bases de datos disponibles:

```json
{
  "databases": [
    {
      "id": "pg_main",
      "label": "Postgres Principal",
      "type": "postgres",
      "host": "localhost", "port": 5432,
      "database": "knowledge_base",
      "user": "postgres", "password": "admin123",
      "enabled": true
    },
    {
      "id": "oracle_test",
      "label": "Oracle Test Local",
      "type": "oracle",
      "host": "localhost", "port": 1521,
      "database": "XEPDB1",
      "user": "APP", "password": "1234",
      "enabled": true
    }
  ]
}
```

### MultiDbRegistry
`backend/src/infrastructure/distributed/MultiDbRegistry.js` carga ese JSON al arrancar el servidor y mantiene una conexión o pool activo por cada entrada:

- **PostgreSQL** → crea un `pg.Pool` con los parámetros del JSON.
- **Oracle** → usa `oracledb.getConnection()` bajo demanda (o pool configurable).

El registry expone el método principal:
```js
registry.executeCompiledQuery({ databaseId, sql, params })
```
Ese método enruta la consulta al driver correcto según el `type` del database con ese `id`.

### Limpieza de SQL antes de ejecutar (Oracle)
Oracle rechaza el terminador `/` de SQL*Plus. Antes de llamar a `connection.execute()` se elimina automáticamente:
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
       ├── PostgreSQL → pool.query(sql) → devuelve rows[]
       │       └── RAISE NOTICE capturado → resumenHumano
       │
       └── Oracle → connection.execute(sql, [], { outFormat: OBJECT })
               ├── rows[] normales
               └── DBMS_OUTPUT.GET_LINES → columna `output` en cada fila
```

### Tipos de respuesta que el backend produce

| Situación | Lo que devuelve el backend |
|-----------|--------------------------|
| Query normal con filas | `{ resultado: [...rows], metadata: { total } }` |
| PL/SQL con `RAISE NOTICE` (Postgres) | `{ resumenHumano: "texto del NOTICE", resultado: [] }` |
| PL/SQL con `DBMS_OUTPUT` (Oracle) | `{ resultado: [{ output: "línea" }, ...] }` |
| Error de sintaxis SQL | `{ success: false, error: "mensaje" }` |

### Renderizado en el frontend

`frontend/app/components/ContentPanel.tsx` y `frontend/components/SqlAssistantPanel.jsx` inspeccionan la respuesta:

- **Filas normales** → tabla con columnas y filas.
- **`resultado[0].output` existe** → bloque terminal ámbar con etiqueta `DBMS_OUTPUT:`.
- **`resumenHumano` con saltos de línea** → bloque terminal azul con etiqueta `NOTICE:`.
- **`resumenHumano` de una sola línea** → badge de texto debajo del SQL.

---

## 5. Detección automática de base de datos al insertar SQL

Cuando el usuario escribe SQL manualmente en el editor y ejecuta, el sistema puede recibir el SQL sin un `databaseId` explícito. En ese caso el backend aplica este orden de detección:

### Paso 1 — Hint explícito del frontend
Si el usuario seleccionó manualmente "Oracle" o "Postgres" en el selector de base de datos del panel, el frontend envía `databaseHint: "oracle"`. El backend respeta ese hint.

### Paso 2 — Palabras clave en el SQL
`MultiDbRegistry` o el motor semántico analiza el SQL buscando sintaxis característica:

| Detecta | Indica |
|---------|--------|
| `USER_TABLES`, `USER_TAB_COLUMNS`, `ROWNUM`, `DBMS_OUTPUT`, `dual` | Oracle |
| `information_schema`, `pg_`, `SERIAL`, `ILIKE`, `RETURNING` | PostgreSQL |
| `SHOW TABLES`, `AUTO_INCREMENT`, `ENGINE=InnoDB` | MySQL |

### Paso 3 — Base primaria por defecto
Si no hay ninguna pista, se usa la base marcada con `"primary": true` en `multidb.databases.json` (normalmente `pg_main`).

### Paso 4 — Ejecución y reintento
Si la primera ejecución falla con un error de compatibilidad (ej. tabla no existe en Postgres pero sí en Oracle), el motor distribuido puede reintentar en la siguiente base disponible.

---

## 6. Sistema de IA — cómo Ollama se conecta al backend

### Componentes

```
OllamaClient.js          ← cliente HTTP liviano hacia la API REST de Ollama
      │
      ▼
http://localhost:11434/api/generate   ← Ollama (proceso local)
      │
      ▼
modelo: deepseek-coder   ← LLM especializado en código/SQL
```

### Variables de entorno (`backend/.env`)

```env
OLLAMA_URL=http://localhost:11434/api/generate
OLLAMA_MODEL=deepseek-coder
OLLAMA_TIMEOUT_MS=2800        # timeout por defecto (warmup/otras llamadas)
```

### `OllamaClient.js` — qué hace

Envía un POST a Ollama con el prompt y espera un JSON de respuesta:
```js
fetch(OLLAMA_URL, {
  method: 'POST',
  body: JSON.stringify({ model, prompt, stream: false })
})
```
La respuesta de Ollama es `{ response: "texto generado" }`. El cliente extrae el texto e intenta parsearlo como JSON (con fallback a extracción por `{...}`).

El cliente normaliza la URL automáticamente: si `OLLAMA_URL` apunta a la raíz (`http://host:port`) le agrega `/api/generate` solo.

### Warmup al arrancar
Al iniciarse el servidor se hace una llamada silenciosa con texto `"warmup"` para que Ollama cargue el modelo en memoria antes de la primera consulta real.

---

## 7. Flujo completo de una consulta en lenguaje natural

El usuario escribe en el **Panel SQL Automático**: *"dame las tablas de oracle"*

```
Frontend (SqlAssistantPanel.jsx)
  POST /api/query/generate  body: { text: "dame las tablas de oracle" }
         │
         ▼
  backend/src/app.js  —  POST /api/query/generate
         │
         ├── PASO 0: ¿Es lenguaje natural? → SÍ → OLLAMA
         │       │
         │       ├─ Recopilar esquema de todas las DBs habilitadas:
         │       │     Oracle: SELECT table_name, column_name FROM USER_TAB_COLUMNS
         │       │     Postgres: SELECT table_name, column_name FROM information_schema.columns
         │       │
         │       ├─ Construir prompt para deepseek-coder:
         │       │     - Reglas de sintaxis (LIMIT para PG, ROWNUM para Oracle, sin backticks)
         │       │     - Ejemplos de SQL por tipo
         │       │     - Esquema real de las tablas
         │       │     - Pregunta del usuario
         │       │
         │       ├─ Llamar Ollama (timeout 30s)
         │       │     Respuesta: {"sql":"SELECT table_name FROM USER_TABLES","database_id":"oracle_test","explanation":"..."}
         │       │
         │       ├─ Sanitizar SQL generado:
         │       │     - Quitar backticks → identificadores limpios
         │       │     - Si es Postgres y tiene ROWNUM → convertir a LIMIT
         │       │     - Si es Oracle y tiene LIMIT → convertir a WHERE ROWNUM <=
         │       │
         │       └─ Devolver al frontend: { query: { sql, databaseId }, success: true }
         │
         ├── PASO 1 (fallback): Motor semántico distribuido
         │       Analiza el texto, busca entidades en el esquema aprendido,
         │       genera SQL por coincidencia semántica.
         │
         └── PASO 2 (fallback): Respuesta de error con sugerencias
```

```
Frontend recibe { query: { sql, databaseId } }
  │
  └─ POST /api/query/execute-generated  body: { sql, databaseId }
         │
         ▼
  MultiDbRegistry.executeCompiledQuery({ databaseId: "oracle_test", sql: "SELECT..." })
         │
         ▼
  Oracle driver → rows: [{ TABLE_NAME: "USERS" }, { TABLE_NAME: "ORDERS" }, ...]
         │
         ▼
  { resultado: [...rows], metadata: { total: N } }
         │
         ▼
  Frontend renderiza tabla con columnas TABLE_NAME
```

### Por qué el motor semántico falla con preguntas de lenguaje natural sobre DBs
El motor semántico está optimizado para preguntas del tipo *"usuarios activos"* → busca la entidad `usuarios` en el esquema aprendido. Cuando la pregunta es *"dame las tablas de oracle"*, interpreta "oracle" como un filtro de datos (ILIKE '%oracle%') en vez de un nombre de motor. Por eso el **PASO 0 con Ollama va siempre primero** para preguntas que no son SQL directo.

---

## 8. Archivos clave

```
backend/
├── .env                                     ← URLs, credenciales, modelo Ollama
├── config/
│   └── multidb.databases.json               ← Configuración de bases de datos
└── src/
    ├── app.js                               ← Servidor Express + todos los endpoints
    │   ├── POST /api/query/generate         ← Paso 0 (Ollama) + motor semántico
    │   ├── POST /api/query/execute-generated← Ejecuta SQL en la DB indicada
    │   └── POST /api/query                  ← Endpoint directo (sin generación)
    ├── routes/
    │   └── QueryRoute.js                    ← Funciones auxiliares de query
    └── infrastructure/
        ├── ai/
        │   └── OllamaClient.js              ← Cliente HTTP para Ollama
        └── distributed/
            └── MultiDbRegistry.js           ← Pool/conexión por DB + router de ejecución

frontend/
└── components/
    ├── SqlAssistantPanel.jsx                ← Panel de lenguaje natural + resultados
    └── app/components/
        └── ContentPanel.tsx                 ← Visualización de resultados del editor SQL
```

### Flujo de datos resumido

```
Pregunta en lenguaje natural
  → /api/query/generate (Ollama genera SQL + databaseId)
    → /api/query/execute-generated (MultiDbRegistry ejecuta en la DB correcta)
      → Frontend renderiza filas / terminal / notice
```

```
SQL manual del editor
  → /api/query/execute-generated (con databaseId del hint o autodetectado)
    → MultiDbRegistry ejecuta en la DB correcta
      → Frontend renderiza resultado
```
