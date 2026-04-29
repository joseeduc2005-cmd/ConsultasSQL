# ConsultasSQL — Motor de Consultas Multibase con IA Local

Sistema de consultas en lenguaje natural sobre múltiples bases de datos (PostgreSQL, Oracle, MySQL) con IA local via Ollama. No requiere ninguna API de IA externa.

---

## Índice

1. [Arquitectura](#1-arquitectura)
2. [Requisitos](#2-requisitos)
3. [Configuración inicial](#3-configuración-inicial)
4. [Cómo iniciar el sistema](#4-cómo-iniciar-el-sistema)
5. [Registrar bases de datos](#5-registrar-bases-de-datos)
6. [Variables de entorno (.env)](#6-variables-de-entorno-env)
7. [Soporte multibase y SQL complejo](#7-soporte-multibase-y-sql-complejo)
8. [Flujo de una consulta](#8-flujo-de-una-consulta)
9. [Endpoints principales](#9-endpoints-principales)
10. [Panel de administración](#10-panel-de-administración)
11. [Tests](#11-tests)
12. [Despliegue en producción](#12-despliegue-en-producción)
13. [Estructura de carpetas](#13-estructura-de-carpetas)

---

## 1. Arquitectura

```
frontend/   →  Next.js 15 (App Router)
backend/    →  Node.js + Express
               ├── Motor IA (Ollama local, llama3)
               ├── Motor SQL determinístico (QueryBuilder)
               ├── Motor distribuido multibase (MultiDatabaseEngine)
               ├── Motor PL/SQL procedural (PlSqlInterpreter)
               └── Registro multibase (MultiDbRegistry)
```

El sistema tiene **dos capas de inteligencia**:

- **Determinística**: resuelve el 80 % de las consultas por patrones (IDs, rankings, conteos, filtros de texto) sin llamar a la IA. Velocidad < 50 ms.
- **IA local (Ollama)**: se activa solo cuando la capa determinística no tiene confianza suficiente. Timeout configurable, sin datos enviados a servidores externos.

---

## 2. Requisitos

| Herramienta | Versión mínima |
|---|---|
| Node.js | 18+ |
| npm | 9+ |
| PostgreSQL | 12+ (base principal del backend) |
| Ollama | cualquier versión estable |
| Modelo IA | llama3 (o el que configures en `.env`) |

Oracle y MySQL son opcionales; solo se necesitan si los registras como bases distribuidas.

---

## 3. Configuración inicial

### 3.1 Clonar e instalar

```bash
git clone https://github.com/joseeduc2005-cmd/ConsultasSQL.git
cd ConsultasSQL

# Backend
cd backend
npm install

# Frontend
cd ../frontend
npm install
```

### 3.2 Registrar las bases de datos

Editar `backend/config/multidb.databases.json`. Agrega **todas** las bases que el sistema debe consultar. No hay jerarquía — todas las bases con `"enabled": true` son iguales para el motor SQL y la IA:

```json
{
  "databases": [
    {
      "id": "oracle_rrhh",
      "label": "Oracle RRHH",
      "type": "oracle",
      "enabled": true,
      "host": "10.0.0.25",
      "port": 1521,
      "database": "XEPDB1",
      "user": "hr",
      "password": "tu_password"
    },
    {
      "id": "pg_operaciones",
      "label": "Postgres Operaciones",
      "type": "postgres",
      "enabled": true,
      "host": "localhost",
      "port": 5432,
      "database": "operaciones",
      "user": "postgres",
      "password": "tu_password"
    }
  ]
}
```

> **Sobre el campo `primary`:** es completamente **opcional**. Si lo usas, sirve únicamente como preferencia para que el backend elija qué postgres usar para sus tablas internas (historial de consultas, aprendizaje semántico). **No afecta** cuáles bases son consultadas — todas las bases con `"enabled": true` son accesibles para SQL y para la IA, independientemente de si tienen `primary: true`, `primary: false`, o si el campo no existe.
>
> Si registras Oracle y Postgres juntos sin poner `primary` en ninguno, el sistema funciona normalmente — ambas bases se consultan y el backend usa el primer postgres encontrado para sus tablas internas.

### 3.3 Crear el schema base (primera vez)

```bash
cd backend
# Aplica el schema SQL inicial
psql -U tu_usuario -d nombre_de_tu_base -f ../database_schema_complete.sql
```

### 3.4 Instalar Ollama y el modelo

```bash
# Instalar Ollama desde https://ollama.com
ollama pull llama3
```

---

## 4. Cómo iniciar el sistema

### Opción A — Dos terminales separadas (recomendado)

**Terminal 1 – Backend:**
```bash
cd backend
npm run dev
# escucha en http://localhost:3002
```

**Terminal 2 – Frontend:**
```bash
cd frontend
npm run dev
# escucha en http://localhost:3000
```

### Opción B — Script PowerShell (Windows)

```powershell
.\start-dev.ps1
```

### Opción C — Producción

```bash
# Backend
cd backend
npm start

# Frontend
cd frontend
npm run build
npm start
# escucha en http://localhost:3001
```

### Verificar que el sistema está vivo

```
GET http://localhost:3002/health
GET http://localhost:3002/api/distributed/databases
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
  "primary": false,
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
  "primary": false,
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

### Claves de merge para consultas distribuidas

Si quieres que el motor una tablas de distintas bases, declara las relaciones:

```json
"semanticLearning": {
  "tableAliases": { "empleados": ["employees"] },
  "columnKeywords": { "empleado": ["employee_id", "user_id"] }
}
```

---

## 6. Variables de entorno (.env)

El archivo `backend/.env` controla el runtime del backend, **no** el registro de bases de datos.

| Variable | Descripción | Valor por defecto |
|---|---|---|
| `PORT` | Puerto del backend | `3002` |
| `NODE_ENV` | Entorno (`development` / `production`) | `development` |
| `JWT_SECRET` | Secreto para firmar tokens | — (obligatorio) |
| `JWT_EXPIRE` | Duración del token | `7d` |
| `OLLAMA_URL` | URL del servicio Ollama | `http://localhost:11434` |
| `OLLAMA_MODEL` | Modelo a usar | `llama3` |
| `OLLAMA_TIMEOUT_MS` | Timeout de respuesta IA | `2800` |
| `MULTI_DB_CONFIG_FILE` | Ruta al archivo de registro multibase | `./config/multidb.databases.json` |
| `FORCE_DISTRIBUTED_MULTI_ENTITY` | Forzar resolución distribuida para consultas multi-entidad | `true` |
| `DEBUG_QUERY_ENGINE` | Habilita logs técnicos de decisión y ruteo | `false` |
| `CORS_ALLOWED_ORIGINS` | Orígenes permitidos separados por coma | `http://localhost:3000,...` |
| `API_RATE_LIMIT_MAX` | Máximo de requests por ventana | `100` |
| `SQL_QUERY_TIMEOUT_MS` | Timeout de queries SQL | `8000` |

`DATABASE_URL` ya no es necesaria si `multidb.databases.json` tiene una entrada `primary: true`. Se mantiene como fallback.

---

## 7. Soporte multibase y SQL complejo

### Motores soportados

| Motor | Conexión | Introspección automática de schema | Ejecución SQL | Merge distribuido |
|---|---|---|---|---|
| PostgreSQL | `pg` | Sí | Sí | Sí |
| Oracle | `oracledb` | Sí | Sí | Sí |
| MySQL | `mysql2` | Sí | Sí | Sí |

### SQL complejo soportado

El `PlSqlInterpreter` interpreta y ejecuta:

- `SELECT` simples, con `JOIN`, `GROUP BY`, `HAVING`, subqueries
- `INSERT`, `UPDATE`, `DELETE` con confirmación previa
- Scripts procedurales: `DECLARE`, `BEGIN...END`, `FOR ... LOOP`, `IF...THEN...ELSE`, `SELECT INTO`, `RAISE NOTICE`
- Scripts `DO $$...$$` de PostgreSQL
- Múltiples sentencias encadenadas con `;`

El motor adapta automáticamente la sintaxis según el motor destino:
- `LIMIT` (Postgres/MySQL) vs `FETCH FIRST N ROWS ONLY` (Oracle)
- `LOWER(CAST(col AS TEXT))` (Postgres) vs `LOWER(TO_CHAR(col))` (Oracle) vs `LOWER(CAST(col AS CHAR))` (MySQL)
- Binding de parámetros: `$1` (Postgres) vs `:p1` (Oracle) vs `?` (MySQL)

### Consultas multi-entidad entre distintas bases

Cuando `FORCE_DISTRIBUTED_MULTI_ENTITY=true`, consultas como "usuarios con sus logs de Oracle" se ejecutan así:

1. Detección de entidades → `users` (Postgres) + `logs` (Oracle)
2. Planificación de merge por claves declaradas o claves foráneas inferidas
3. Ejecución paralela en cada base
4. Merge en memoria con prefijo por base (`users_id`, `logs_id`, etc.)

---

## 8. Flujo de una consulta

```
Usuario escribe: "usuarios activos con más de 3 logs"
        ↓
[DatabaseRelevanceValidator]  — ¿es sobre BD? Si no → rechaza con mensaje
        ↓
[Capa determinística]         — detecta intent: usuarios + logs + count > 3
        ↓ (si no tiene confianza suficiente)
[Ollama local]                — normaliza intent, timeout 2.8s
        ↓
[MultiDatabaseEngine]         — detecta si necesita 1 o N bases
        ↓
[QueryBuilder / adapters]     — genera SQL por motor
        ↓
[Ejecución + merge]           — combina resultados
        ↓
[buildProductionSqlResponse]  — limpia el payload (solo datos relevantes)
        ↓
Frontend muestra tabla / gráfica
```

---

## 9. Endpoints principales

### Consulta en lenguaje natural
```
POST /api/query
Body: { "query": "usuarios activos", "limit": 50 }
```

### Consulta SQL manual / script
```
POST /api/sql/manual
Body: { "sql": "SELECT * FROM users WHERE active = true", "params": {} }
```

### Consulta distribuida explícita
```
POST /api/query/distributed
Body: { "query": "usuarios con logs", "limit": 20 }
```

### Ver bases registradas
```
GET /api/distributed/databases
```

### Estado del sistema
```
GET /health
```

### Sugerencias de consulta
```
GET /api/query/suggestions
```

---

## 10. Panel de administración

El frontend incluye un panel en `/admin` accesible con rol `admin`. Desde ahí se puede:

- Ejecutar scripts SQL manuales (INSERT, UPDATE, DELETE, scripts complejos)
- Ver historial de consultas
- Consultar el estado de las bases registradas

---

## 11. Tests

```bash
cd backend
npm test
```

Suite de tests cubiertos:

| Archivo | Qué valida |
|---|---|
| `primary-config.test.js` | Resolución de base primaria desde registro multibase |
| `distributed.test.js` | Motor distribuido y merge entre bases |
| `intent.test.js` | Interpretación de intents semánticos |
| `relevance.test.js` | Clasificación de consultas DB vs no-DB |
| `scoring.test.js` | Sistema de scoring de entidades |
| `vocabulary.test.js` | Vocabulario semántico y alias |
| `plsql-interpreter.test.js` | Interpretación de scripts procedurales |
| `error-analysis.test.js` | Motor de análisis de errores |
| `validation.test.js` | Validación de inputs y SQL |

---

## 12. Despliegue en producción

1. Cambiar `NODE_ENV=production` en `.env`
2. Cambiar `JWT_SECRET` por un secreto seguro y aleatorio
3. Ajustar `CORS_ALLOWED_ORIGINS` con el dominio real del frontend
4. Poner `DEBUG_QUERY_ENGINE=false`
5. Asegurar que Ollama esté corriendo y accesible en `OLLAMA_URL`
6. Verificar que `backend/config/multidb.databases.json` tenga las credenciales correctas
7. El archivo `multidb.databases.json` **no debe subirse a git** con credenciales reales — agrégalo a `.gitignore` o usa variables de entorno para la contraseña si el entorno lo permite

---

## 13. Estructura de carpetas

```
ConsultasSQL/
├── README.md                          ← este archivo
├── database_schema_complete.sql       ← schema inicial de la BD principal
├── start-dev.ps1                      ← script de inicio rápido (Windows)
│
├── backend/
│   ├── .env                           ← configuración de runtime
│   ├── config/
│   │   ├── multidb.databases.json     ← registro de todas las bases (fuente de verdad)
│   │   ├── multidb.databases.example.json
│   │   └── README.md
│   ├── src/
│   │   ├── app.js                     ← entrypoint del backend
│   │   ├── bootstrap.js               ← arranque multibase
│   │   ├── routes/
│   │   │   ├── QueryRoute.js          ← ruta principal de consultas IA
│   │   │   └── DynamicRoutes.js
│   │   └── infrastructure/
│   │       ├── ai/OllamaClient.js     ← cliente IA local
│   │       ├── database/SchemaDetector.js
│   │       ├── distributed/
│   │       │   ├── MultiDbRegistry.js
│   │       │   ├── MultiDatabaseEngine.js
│   │       │   └── DistributedQueryOrchestrator.js
│   │       ├── query/
│   │       │   ├── QueryBuilder.js
│   │       │   ├── PlSqlInterpreter.js
│   │       │   ├── QueryIntelligenceEngine.js
│   │       │   └── DatabaseRelevanceValidator.js
│   │       └── universal-db/
│   │           ├── adapters/
│   │           │   ├── PostgresAdapter.js
│   │           │   ├── OracleAdapter.js
│   │           │   └── MySqlAdapter.js
│   │           └── schema/introspectionQueries.js
│   └── tests/
│
└── frontend/
    ├── app/
    │   ├── dashboard/      ← dashboard de consultas
    │   ├── admin/          ← panel de administración
    │   └── api/            ← proxies al backend
    └── components/
```

**Terminal 2 - Backend:**
```bash
cd backend
npm install
npm run start
```
→ http://localhost:3002

### Opción 2: Script Automático (Windows)

```bash
.\start-dev.ps1
```

## ⚙️ Configuración

Crear `.env` en `backend/`:

```bash
PORT=3002
NODE_ENV=development
DATABASE_URL=postgresql://usuario:contraseña@localhost:5432/nombre_db
JWT_SECRET=tu_secreto_seguro
JWT_EXPIRE=7d
```

## 📂 Estructura del Proyecto

```
backend/
├── src/
│   ├── app.js                              # Aplicación principal
│   └── infrastructure/
│       ├── database/SchemaDetector.js       # Auto-detección de BD
│       ├── cache/SchemaCache.js             # Cache inteligente
│       └── query/QueryBuilder.js            # Generador SQL

frontend/
├── components/
│   ├── AutoQueryGenerator.jsx              # Componente principal
│   └── ...
├── hooks/
│   └── useDynamicSchema.js                 # Hook para cargar schema
└── app/
    └── ...

.env                                         # Configuración
SETUP.md                                     # Guía de instalación
```

## 📡 API Endpoints

| Método | Endpoint | Descripción |
|--------|----------|-------------|
| GET | `/api/db/schema-full` | Schema completo de BD |
| GET | `/api/query/analyze?text=...` | Análisis de entrada |
| POST | `/api/query/generate` | Generar SQL |
| POST | `/api/query/execute-generated` | Ejecutar query |

## 🎯 Ejemplo de Uso

```javascript
// Frontend - Hook
import useDynamicSchema from '@/hooks/useDynamicSchema'

export function MyComponent() {
  const { tablas, schema, loading } = useDynamicSchema()

  if (loading) return <div>Cargando...</div>

  return <select>
    {tablas.map(t => <option key={t}>{t}</option>)}
  </select>
}
```

```javascript
// Frontend - Componente
import AutoQueryGenerator from '@/components/AutoQueryGenerator'

export default function Page() {
  return <AutoQueryGenerator />
}
```

## 🔄 Cambiar BD (2 minutos)

```bash
# 1. Actualizar .env
DATABASE_URL=postgresql://user:pass@server:5432/nueva_bd

# 2. Reiniciar backend
npm run start

# ✅ Sistema auto-detecta TODO automáticamente
```

## 🛠️ Desarrollo

```bash
# Frontend
cd frontend && npm run lint      # Linting
cd frontend && npm run build     # Build producción

# Backend  
cd backend && npm run dev        # Desarrollo con watch
cd backend && npm start          # Producción
```

## 🔒 Seguridad

- ✅ Solo SELECT permitido (previene DELETE/DROP)
- ✅ Validación de SQL
- ✅ Role-based access control
- ✅ Auditoría de queries
- ✅ Timeout en queries (5 segundos)

## 📊 Performance

- **Primera carga**: ~500ms (detecta schema)
- **Cachéd**: <100ms
- **Timeout query**: 5 segundos
- **Max conexiones DB**: 20

## 📖 Documentación

Ver `SETUP.md` para:
- Instalación detallada
- Configuración avanzada
- Troubleshooting
- Comandos disponibles

## 🚀 Deploying

```bash
# Frontend
cd frontend && npm run build && npm start

# Backend
cd backend && npm install --production && npm start
```

Usar variables de entorno para configuración en producción.

## 📝 Historial de Cambios

**v1.0.0** (Actual)
- Auto-detección de PostgreSQL
- Generación de SQL sin IA
- Cache inteligente
- API REST completa
- Frontend dinámico

## 📞 Soporte

1. Verifica `.env` está configurado correctamente
2. Confirma que PostgreSQL es accesible
3. Ver logs del backend: `npm run start`
4. Verifica tablas: `psql $DATABASE_URL -c "\dt"`

## 📄 Licencia

MIT

---

**Sistema listo para producción.** ✨
