# Módulo de soporte — absorción de ticket v2.0

Centro de soporte del equipo de Sistemas (tickets, diagnósticos automáticos, biblioteca
SQL y vigilancia) integrado dentro de ConsultasSQL. Antes era un proyecto aparte
(`ticket_v2.0`) con su propio servidor Fastify, su propio SQLite y su propio frontend.

Esto documenta **qué se movió, qué cambió y qué falta**.

---

## 1. Estado actual

| Parte | Estado |
|---|---|
| Backend: tickets, biblioteca SQL, diagnósticos, vigilancia | **Integrado y probado** |
| Base de conocimiento migrada de SQLite a PostgreSQL | **Integrada** — esquema `soporte` |
| Frontend (6 pantallas) | **Pendiente** — sigue viviendo en `ticket_v2.0` |
| Interpretar mensaje (`/api/interpretar`) | **Pendiente** — depende del modelo de lenguaje |

---

## 2. Por qué el módulo está en TypeScript

`backend/src/soporte/` es TypeScript dentro de un backend que es JavaScript. No hay paso
de compilación: **Node 22.18+ borra los tipos y ejecuta los `.ts` directamente**.

Funciona porque el proyecto original se escribió con `erasableSyntaxOnly: true`, que
garantiza que no hay `enum` ni `namespace` — nada que necesite generar código. Gracias a
eso, 1 850 de sus 3 177 líneas de backend se movieron **sin tocar una sola línea**, y se
conservó todo su tipado, que es de lo mejor que tiene.

> **Requisito:** Node >= 22.18. Está declarado en `backend/package.json` (`engines`) y
> en el `Dockerfile` (`node:22-alpine`).

---

## 3. Qué se movió y qué se reescribió

```
backend/src/soporte/
├── config.ts             REESCRITO  ← era config/env.ts; ahora lee el .env del backend
├── index.ts              NUEVO      ← arranque del módulo
├── rutas.ts              REESCRITO  ← eran 5 archivos de rutas Fastify
├── http/errores.ts       tal cual
├── knowledge/
│   ├── esquema.sql       NUEVO      ← DDL de PostgreSQL
│   ├── db.ts             REESCRITO  ← era node:sqlite síncrono; ahora pg asíncrono
│   ├── tickets.ts        REESCRITO
│   ├── consultas.ts      REESCRITO
│   ├── diagnosticos.ts   REESCRITO
│   ├── alertas.ts        REESCRITO
│   └── semillas.ts       REESCRITO  ← misma lógica, ahora asíncrona
├── monitors/
│   ├── monitores.ts      tal cual   ← los 6 monitores
│   └── programador.ts    REESCRITO  ← asíncrono
├── oracle/               tal cual   ← catálogo, guardia, fuente Oracle y fuente demo
├── playbooks/            tal cual   ← los 4 diagnósticos y su motor
└── seed/                 tal cual   ← 11 tickets y 111 consultas
```

---

## 4. La base de conocimiento en PostgreSQL

Vive en el esquema **`soporte`** de `knowledge_base`, separada de las tablas de A
(`users`, `roles`, `logs`, `query_history`…) para que los nombres no compitan.

`backend/src/soporte/knowledge/esquema.sql` es la única fuente de la verdad y **el
backend lo aplica en cada arranque**. Es idempotente, así que sirve igual para una base
nueva que para una que ya está en producción — no hace falta tocar `db-init/`.

### Traducciones desde SQLite

| SQLite (ticket v2.0) | PostgreSQL |
|---|---|
| `INTEGER PRIMARY KEY` | `GENERATED ALWAYS AS IDENTITY` |
| JSON guardado como texto (`entrada`, `resultado`, `datos`) | `jsonb` — ahora consultable |
| `datetime('now','localtime')` | `timestamptz DEFAULT now()` |
| Tabla virtual `tickets_fts` (FTS5) + reindexado manual | columna generada `busqueda tsvector` + índice GIN |
| `remove_diacritics` del tokenizador | `soporte.sin_tildes()`, con `translate()` |
| `bm25()` (menor es mejor) | `ts_rank()` (mayor es mejor) |

La búsqueda de casos similares ya no necesita reindexar al guardar: el `tsvector` es una
columna generada y se mantiene solo. Se eligió `translate()` en vez de la extensión
`unaccent` porque `unaccent` exige superusuario y no es `IMMUTABLE`, y una columna
generada solo admite funciones inmutables.

Las fechas de día (`creado_en`, `resuelto_en`, `ticket_pasos.fecha`) se quedaron como
`text` a propósito: vienen del SIR como cadenas y se muestran tal cual, sin zona horaria.

---

## 5. Cómo está enganchado

Tres puntos en `backend/src/app.js`:

```js
import { crearSoporteRouter, iniciarSoporte } from './soporte/index.ts';

app.use('/api/soporte', crearSoporteRouter({ log: … }));   // registro de rutas

await iniciarSoporte({ obtenerPool: () => pool, log: … }); // dentro de startServer()
```

`iniciarSoporte` crea el esquema, siembra la base la primera vez y arranca la vigilancia
si está encendida. Nunca lanza: si algo falla, el módulo queda inutilizable pero el resto
del backend arranca igual.

Se le pasa un **getter** del pool, no el pool: `app.js` reemplaza su pool en caliente
cuando cambia `multidb.databases.json`, y capturarlo una vez dejaría a soporte hablando
con una conexión ya cerrada.

---

## 6. Endpoints

Todos bajo `/api/soporte`. Devuelven JSON; los errores son `{ "error": "mensaje" }`.

| Método | Ruta | Qué hace |
|---|---|---|
| GET | `/estado` | Versión, estado de Oracle, vigilancia y umbrales |
| GET | `/resumen` | Indicadores del panel |
| GET | `/tickets?q=&estado=&sistema=` | Listado con filtros |
| GET | `/tickets/:numero` | Ticket con sus pasos y casos similares |
| POST | `/tickets` | Crear |
| PATCH | `/tickets/:numero` | Actualizar campos o estado |
| POST | `/tickets/:numero/pasos` | Agregar paso manual |
| GET | `/consultas?q=&categoria=&riesgo=` | Biblioteca SQL + categorías |
| GET | `/consultas/:codigo` | Una consulta |
| GET | `/playbooks` | Diagnósticos disponibles con sus campos |
| POST | `/playbooks/:id/ejecutar` | Ejecuta un diagnóstico |
| GET | `/diagnosticos?limite=` | Historial |
| GET | `/diagnosticos/:id` | Resultado completo guardado |
| POST | `/diagnosticos/:id/registrar` | Lo agrega como paso del ticket |
| GET | `/monitores` | Monitores con su última ejecución |
| POST | `/monitores/:id/ejecutar` | Ejecuta un monitor ahora |
| GET | `/alertas?estado=` | Alertas |
| PATCH | `/alertas/:id` | `abierta` / `atendida` / `resuelta` |

---

## 7. Configuración

Todo en `backend/.env` (ver `.env.example`).

| Variable | Por defecto | Qué hace |
|---|---|---|
| `SOPORTE_MODO` | — | `demo` fuerza datos ficticios sin tocar Oracle |
| `SOPORTE_ORACLE_DB_ID` | `oracle` | Qué entrada de `multidb.databases.json` usar |
| `SOPORTE_ORACLE_USUARIO/_CLAVE/_CONEXION` | — | Credenciales propias; tienen prioridad sobre el JSON |
| `SOPORTE_ORACLE_MAX_FILAS` | `500` | Tope de filas por consulta de diagnóstico |
| `VIGILANCIA` | `off` | `on` activa los 6 monitores en segundo plano |
| `MAX_INTENTOS_PIN` / `MAX_INTENTOS_CLAVE` | `3` | Umbrales de bloqueo de los diagnósticos |

**La vigilancia arranca apagada**, al revés que en ticket v2.0. Encenderla hace que seis
monitores consulten Oracle cada cierto tiempo en segundo plano; es una decisión que se
toma a propósito, no un efecto colateral de desplegar.

### Oracle

El módulo mantiene **su propio pool** de `oracledb` en modo thin, aparte del
`MultiDbRegistry`. Es a propósito: así los diagnósticos conservan sus tres barreras
originales, que siguen intactas —

1. `oracle/catalogo.ts`: lista cerrada de sentencias; no hay SQL libre.
2. `oracle/guardia.ts`: valida que cada sentencia sea `SELECT` o `WITH`.
3. `SET TRANSACTION READ ONLY` y `rollback()` en cada consulta.

Si los DBA entregan un usuario de solo lectura, basta con poner las tres variables
`SOPORTE_ORACLE_*` — no hay que tocar código.

---

## 8. Lo que falta

1. **Frontend**: las 6 pantallas siguen en `ticket_v2.0` (React 19 + Vite + Tailwind 4).
   Para traerlas al Next de A hay que subir A a Tailwind 4 y React 19 primero. Los tokens
   de color no chocan: A define 16 variables CSS y B 64, sin ninguna en común.
2. **Interpretar mensaje**: `interpreter/` no se movió porque usa el modelo de lenguaje.
3. **`pnpm importar`**: `leerDashboardHtml()` ya está portado en `knowledge/semillas.ts`,
   falta el script de línea de comandos que lo invoque.
4. **Revisar** si conviene que el motor de consultas de A indexe el esquema `soporte`.
   Hoy lo detecta y lo agrega a su vocabulario; puede ser útil (preguntar por tickets en
   lenguaje natural) o ruido para la generación de SQL.

---

## 9. Un arreglo colateral

`infrastructure/security/responseSecurity.js` corrompía cualquier respuesta que fuera un
array de nivel superior: `capRows()` hacía `{ ...payload }`, lo que convierte `[a, b]` en
`{0: a, 1: b}`. No se había notado porque todos los endpoints de A devuelven
`{ success, data }`. Los de soporte sí devuelven arrays, y ahí saltó. Ya está corregido.
