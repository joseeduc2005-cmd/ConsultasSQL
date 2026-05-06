# ConsultasSQL — Motor de Consultas Multibase con IA Local

Sistema de consultas en lenguaje natural sobre múltiples bases de datos (PostgreSQL, Oracle, MySQL) con IA local via Ollama. No requiere ninguna API de IA externa.

---

## Índice

1. [Arquitectura](#1-arquitectura)
2. [Requisitos](#2-requisitos)
3. [Inicio rápido con Docker](#3-inicio-rápido-con-docker)
4. [Instalación manual](#4-instalación-manual)
5. [Registrar bases de datos](#5-registrar-bases-de-datos)
6. [Variables de entorno](#6-variables-de-entorno-env)
7. [Soporte multibase y SQL complejo](#7-soporte-multibase-y-sql-complejo)
8. [Flujo de una consulta](#8-flujo-de-una-consulta)
9. [Endpoints principales](#9-endpoints-principales)
10. [Panel de administración](#10-panel-de-administración)
11. [Tests](#11-tests)
12. [Estructura de carpetas](#12-estructura-de-carpetas)

---

## 1. Arquitectura

```
frontend/   →  Next.js 15 (App Router)
backend/    →  Node.js + Express
               ├── Motor IA (Ollama local, deepseek-coder)
               ├── Motor SQL determinístico (QueryBuilder)
               ├── Motor distribuido multibase (MultiDatabaseEngine)
               ├── Motor PL/SQL procedural (PlSqlInterpreter)
               └── Registro multibase (MultiDbRegistry)
```

El sistema tiene **dos capas de inteligencia**:

- **Determinística**: resuelve el 80 % de las consultas por patrones (IDs, rankings, conteos, filtros de texto) sin llamar a la IA. Velocidad < 50 ms.
- **IA local (Ollama)**: se activa cuando la capa determinística no tiene confianza suficiente. Timeout configurable, sin datos enviados a servidores externos.

---

## 2. Requisitos

### Con Docker (recomendado)
- Docker Desktop 24+
- Docker Compose v2

### Sin Docker (manual)

| Herramienta | Versión mínima |
|---|---|
| Node.js | 18+ |
| npm | 9+ |
| PostgreSQL | 12+ |
| Oracle XE | 21c (opcional) |
| Ollama | cualquier versión estable |
| Modelo IA | `deepseek-coder` |

---

## 3. Inicio rápido con Docker

```bash
git clone https://github.com/joseeduc2005-cmd/ConsultasSQL.git
cd ConsultasSQL

# 1. Copiar plantilla de variables de entorno
cp backend/.env.docker.example backend/.env.docker

# 2. Levantar todos los servicios
#    (PostgreSQL, Oracle XE 21c, Ollama, Backend, Frontend)
docker compose up --build

# 3. Primera vez — cargar el modelo de IA
#    (en otra terminal mientras Oracle inicializa ~3-5 min)
docker exec -it practicas-ollama ollama pull deepseek-coder
```

Servicios disponibles:

| URL | Servicio |
|---|---|
| http://localhost:3000 | Frontend (Next.js) |
| http://localhost:3002 | Backend API |
| localhost:5432 | PostgreSQL |
| localhost:1521 | Oracle XE 21c |
| localhost:11434 | Ollama |

> **Oracle XE tarda ~3-5 minutos en arrancar** la primera vez. El backend espera automáticamente a que esté listo.

---

## 4. Instalación manual

### 4.1 Clonar e instalar dependencias

```bash
git clone https://github.com/joseeduc2005-cmd/ConsultasSQL.git
cd ConsultasSQL

cd backend && npm install
cd ../frontend && npm install
```

### 4.2 Configurar bases de datos

```bash
# Copiar plantilla y editar con tus datos
cp backend/config/multidb.databases.example.json backend/config/multidb.databases.json
```

### 4.3 Crear schema PostgreSQL (primera vez)

```bash
psql -U postgres -d knowledge_base -f database_schema_complete.sql
```

### 4.4 Instalar Ollama y el modelo

```bash
# Instalar desde https://ollama.com
ollama pull deepseek-coder
```

### 4.5 Iniciar el sistema

**Terminal 1 – Backend:**
```bash
cd backend
npm run dev        # http://localhost:3002
```

**Terminal 2 – Frontend:**
```bash
cd frontend
npm run dev        # http://localhost:3000
```

**Windows — script automático:**
```powershell
.\start-dev.ps1
```

---

## 5. Registrar bases de datos

Toda la configuración de bases vive en un solo archivo:

```
backend/config/multidb.databases.json
```

El archivo `backend/config/multidb.databases.example.json` tiene plantillas para Postgres, Oracle y MySQL.

### Agregar una base Oracle

```json
{
  "id": "oracle_rrhh",
  "label": "Oracle RRHH",
  "type": "oracle",
  "host": "10.0.0.25",
  "port": 1521,
  "database": "XEPDB1",
  "user": "hr",
  "password": "tu_contraseña",
  "enabled": true,
  "schema": { "tables": [] }
}
```

### Agregar una base MySQL

```json
{
  "id": "mysql_ventas",
  "label": "MySQL Ventas",
  "type": "mysql",
  "host": "10.0.0.30",
  "port": 3306,
  "database": "ventas",
  "user": "app_user",
  "password": "tu_contraseña",
  "enabled": true,
  "schema": { "tables": [] }
}
```

> El backend detecta cambios en este archivo automáticamente (hot reload) sin reiniciar.

---

## 6. Variables de entorno (.env)

| Variable | Descripción | Valor por defecto |
|---|---|---|
| `PORT` | Puerto del backend | `3002` |
| `NODE_ENV` | Entorno | `development` |
| `JWT_SECRET` | Secreto para tokens JWT | — (obligatorio) |
| `OLLAMA_URL` | URL del servicio Ollama | `http://localhost:11434/api/generate` |
| `OLLAMA_MODEL` | Modelo IA | `deepseek-coder` |
| `OLLAMA_TIMEOUT_MS` | Timeout de respuesta IA | `2800` |
| `MULTI_DB_CONFIG_FILE` | Ruta al registro de bases | `./config/multidb.databases.json` |
| `FORCE_DISTRIBUTED_MULTI_ENTITY` | Resolución distribuida multi-entidad | `true` |
| `DEBUG_QUERY_ENGINE` | Logs técnicos del motor | `false` |
| `CORS_ALLOWED_ORIGINS` | Orígenes permitidos | `http://localhost:3000` |
| `SQL_QUERY_TIMEOUT_MS` | Timeout de queries SQL | `8000` |

---

## 7. Soporte multibase y SQL complejo

### Motores soportados

| Motor | Conexión | Introspección automática | Ejecución SQL | Merge distribuido |
|---|---|---|---|---|
| PostgreSQL | `pg` | Sí | Sí | Sí |
| Oracle | `oracledb` (thin mode) | Sí | Sí | Sí |
| MySQL | `mysql2` | Sí | Sí | Sí |

### SQL complejo soportado

- `SELECT` simples, con `JOIN`, `GROUP BY`, `HAVING`, subqueries
- `INSERT`, `UPDATE`, `DELETE`
- Scripts procedurales: `DECLARE`, `BEGIN...END`, `FOR ... LOOP`, `IF...THEN...ELSE`
- Scripts `DO $$...$$` de PostgreSQL con `RAISE NOTICE`
- Bloques PL/SQL Oracle con `DBMS_OUTPUT`
- Múltiples sentencias encadenadas con `;`

---

## 8. Flujo de una consulta

```
Usuario escribe: "dame las tablas de oracle"
        ↓
[PASO 0 — IA local Ollama]     — genera SQL + detecta BD destino
        ↓ (si falla o no aplica)
[Capa determinística]          — detecta intent por patrones
        ↓
[MultiDatabaseEngine]          — detecta si necesita 1 o N bases
        ↓
[QueryBuilder / adapters]      — genera SQL por motor
        ↓
[Ejecución + merge]            — combina resultados
        ↓
Frontend muestra tabla / terminal DBMS_OUTPUT / NOTICE
```

---

## 9. Endpoints principales

```
POST /api/query                    — consulta en lenguaje natural
POST /api/query/generate           — genera SQL sin ejecutar
POST /api/query/execute-generated  — ejecuta SQL generado
POST /api/sql/manual               — SQL manual / scripts
GET  /api/distributed/databases    — bases registradas
GET  /health                       — estado del sistema
```

---

## 10. Panel de administración

Accede a `/admin` con rol `admin`. Permite:

- Ejecutar scripts SQL manuales (INSERT, UPDATE, DELETE, PL/SQL, DO$$)
- Ver historial de consultas
- Consultar estado de las bases registradas
- Gestionar usuarios y roles

---

## 11. Tests

```bash
cd backend
npm test
```

| Archivo | Qué valida |
|---|---|
| `primary-config.test.js` | Resolución de base primaria |
| `distributed.test.js` | Motor distribuido y merge |
| `intent.test.js` | Interpretación de intents |
| `relevance.test.js` | Clasificación de consultas DB vs no-DB |
| `scoring.test.js` | Scoring de entidades |
| `plsql-interpreter.test.js` | Scripts procedurales |
| `error-analysis.test.js` | Motor de análisis de errores |

---

## 12. Estructura de carpetas

```
ConsultasSQL/
├── docker-compose.yml                 ← todos los servicios Docker
├── database_schema_complete.sql       ← schema inicial PostgreSQL
├── docker/oracle/                     ← scripts de init Oracle XE
│
├── backend/
│   ├── Dockerfile
│   ├── .env                           ← variables de runtime (no subir)
│   ├── .env.docker.example            ← plantilla para Docker
│   ├── config/
│   │   ├── multidb.databases.json          ← registro de bases (no subir)
│   │   ├── multidb.databases.example.json  ← plantilla
│   │   └── multidb.databases.docker.json   ← config para Docker
│   └── src/
│       ├── app.js                     ← entrypoint Express
│       ├── routes/QueryRoute.js       ← ruta principal de consultas IA
│       └── infrastructure/
│           ├── ai/OllamaClient.js     ← cliente IA local
│           └── distributed/
│               ├── MultiDbRegistry.js
│               └── MultiDatabaseEngine.js
│
└── frontend/
    ├── Dockerfile
    ├── app/
    │   ├── dashboard/    ← panel de consultas
    │   ├── admin/        ← administración
    │   └── api/          ← proxies al backend
    └── components/       ← SqlAssistantPanel, ContentPanel, etc.
```
