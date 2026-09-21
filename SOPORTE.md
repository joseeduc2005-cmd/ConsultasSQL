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
| Frontend de A migrado a Tailwind 4 + React 19 | **Hecho** — prerrequisito para traer las pantallas |
| Frontend: Panel, Vigilancia, Biblioteca, Tickets y Diagnósticos | **Integradas** — 5 de 6 |
| Navegación propia y entrada desde ConsultasSQL | **Hecho** — ver sección 15 |
| Acceso restringido a rol admin | **Hecho** — dos barreras, ver sección 12 |
| Interpretar mensaje (pantalla y `/api/interpretar`) | **Pendiente** — depende del modelo de lenguaje |

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

1. **Frontend**: solo falta Interpretar mensaje, que usa el modelo de lenguaje. La ruta
   comodín `app/soporte/[...pendiente]` la cubre mientras tanto y hace de 404 del módulo.
2. **Vigilancia en producción**: arranca apagada. Encenderla (`VIGILANCIA=on`) es una
   decisión a tomar cuando el usuario de Oracle de solo lectura esté listo.
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

---

## 10. Migración del frontend de A a Tailwind 4 + React 19

Prerrequisito para traer las pantallas de soporte, que están escritas contra Tailwind 4.
Se hizo con el codemod oficial (`@tailwindcss/upgrade`), no a mano: A usaba 143 veces
`rounded`, 38 `shadow`, 26 `ring`, 20 `blur` y 10 `outline-none`, y todas cambian de valor
o de nombre en la versión 4.

Qué cambió:

- `tailwind.config.mjs` eliminado — estaba vacío (sin `theme` ni `plugins`).
- `postcss.config.mjs` usa `@tailwindcss/postcss`; `autoprefixer` ya no hace falta.
- `globals.css`: `@tailwind base/components/utilities` → `@import 'tailwindcss'`, más la
  capa de compatibilidad que el codemod añade para el color de borde por defecto, que en
  la versión 4 pasa de `gray-200` a `currentColor`.
- 10 archivos `.tsx` con las utilidades renombradas.
- React y React-DOM a 19.3. No hubo nada que adaptar: el código no tenía `useRef()` sin
  argumento, ni `propTypes`, ni `defaultProps`, ni `forwardRef`, ni `ReactDOM.render`.

### Verificación

| Comprobación | Resultado |
|---|---|
| Compilación de producción | correcta |
| Estilos computados de `/login` (58 elementos) | **cero diferencias** contra la versión anterior |
| Clases que se generaban antes y ya no | **cero**, tras corregir un `border-opacity-20` |

### Dos cosas que cambian a la vista, a propósito

1. **`border-opacity-20`** en `DynamicForm.tsx` pasó a `border-current/20`. La versión 4
   eliminó las utilidades `*-opacity-*`. El codemod no la tocó porque el archivo estaba
   fuera de los `content` de la configuración vieja.

2. **`frontend/components/` nunca se estaba escaneando.** Los `content` de Tailwind 3 solo
   incluían `./app/**` y `./src/**`, así que las clases de `SqlAssistantBubble.jsx` y de
   los siete archivos de `WorkflowBuilder/` jamás se generaron. La detección automática de
   la versión 4 sí los ve: aparecen 31 clases nuevas, entre ellas `bottom-8 right-8 h-14
   w-14`, que son la posición y el tamaño de la burbuja flotante del asistente SQL. Es
   decir, esos componentes llevaban tiempo renderizándose a medio estilar y ahora se ven
   como se diseñaron. El CSS pasó de 59 KB a 91 KB casi todo por esto.

   **Conviene mirar el panel de administración y la burbuja del asistente**, que son los
   dos sitios donde se nota.

---

## 11. El frontend: sistema de diseño y pantalla piloto

`frontend/soporte/` contiene el código de interfaz traído de ticket v2.0:

```
frontend/soporte/
├── components/ui/          9 componentes (badge, card, code-block, form, …)
├── components/layout/      solo EncabezadoPagina; el Shell no se trajo
├── lib/                    api, types, formato, utils, claves de caché
└── features/biblioteca/    la pantalla piloto
```

Los imports `@/…` del proyecto original se reescribieron a `@/soporte/…`, que con el
alias de ConsultasSQL (`@/*` → raíz del frontend) resuelve solo.

### Los tokens, acotados

Los 64 tokens de diseño viven en `app/globals.css`. Ninguno choca con los 16 de A —se
comprobó uno por uno—, pero **lo que sí hubo que acotar es todo lo que el original
aplicaba a `body` y a `*`**: `bg-background`, `text-foreground` y `border-border`
globales habrían repintado ConsultasSQL entera. Ahora cuelgan de `.soporte-raiz`, la
clase que pone el layout del módulo.

Dos detalles de esa integración:

- **El tema oscuro.** El original usaba la clase `.dark`; ConsultasSQL usa
  `data-theme="dark"`. Se redefinió el variante: `@custom-variant dark
  (&:where([data-theme='dark'], [data-theme='dark'] *))`. Así el módulo sigue el
  interruptor de tema que ya existe, sin cablear nada.

- **Las tipografías.** A usa la utilidad `font-mono` 14 veces, así que redefinir
  `--font-mono` en `@theme` le habría cambiado los bloques de código. En vez de eso,
  `--font-sans` y `--font-mono` se redefinen como variables normales sobre
  `.soporte-raiz`: como las utilidades de Tailwind 4 compilan a `font-family:
  var(--font-mono)` y las variables se heredan, dentro del módulo resuelven a Inter y
  JetBrains Mono, y fuera siguen resolviendo a las de Tailwind. Verificado.

### El proxy

`app/api/soporte/[...ruta]/route.ts` es una sola ruta comodín que reenvía los 18
endpoints al backend, reutilizando `fwd()` y `proxyRaw()` de `backend-client.ts`. Como el
resto de handlers de A, la llamada al backend nunca sale del servidor.

### Adaptación de react-router a Next

Fue mecánico: `Link to=` → `Link href=`, y `useSearchParams` de react-router (que
devuelve un par valor/actualizador) por el de `next/navigation` más `useRouter().replace`.
La página se envuelve en `<Suspense>` porque Next lo exige para `useSearchParams`.

### Verificación

- La pantalla carga las 111 consultas a través del proxy; buscador, filtro por categoría
  y segmentado de riesgo funcionan; la selección se refleja en la URL.
- El tema oscuro responde al `data-theme` de ConsultasSQL.
- `/login` de A: todas las propiedades computadas idénticas salvo `border-color`, que pasa
  de `rgb(229, 231, 235)` a `oklch(0.928 0.006 264.531)` — es el mismo gris de Tailwind
  escrito en oklch, comprobado pintando ambos en un canvas: diferencia de 0 en los tres
  canales.
- Ninguna clase que se generara antes dejó de generarse.

> **Nota de método.** Una primera verificación de la migración a Tailwind 4 dio "cero
> diferencias" contra una compilación obsoleta: había quedado un `next-server` vivo
> sirviendo el build anterior. Al levantar un servidor hay que matar el proceso `next-server`,
> no el `pnpm` que lo lanza, y confirmar que el hash del CSS servido cambió.

---

## 12. Acceso restringido a rol admin

El módulo consulta la Oracle de producción y muestra los tickets con su investigación
completa, así que va entero detrás del rol `admin`. Hay dos barreras:

**En el navegador.** `app/soporte/layout.tsx` se exporta envuelto en el `withAuth` que
ConsultasSQL ya tenía, con rol `admin`. No se fía de `localStorage`: confirma contra
`/api/auth/me`. Sin sesión manda a `/login`; con sesión de usuario normal, a `/dashboard`.

**En el backend.** `soloAdminSoporte` protege `/api/soporte/*` entero. A diferencia de
`adminRoute`, **no acepta la cabecera `x-user-role`**: la pone el navegador y cualquiera
podría mandarla. El rol sale solo de la sesión asociada al token.

Comprobado endpoint por endpoint:

| Petición | Respuesta |
|---|---|
| Sin autenticación | 401 |
| Token inválido | 401 |
| Solo la cabecera `x-user-role: admin` | 401 |
| Sesión de usuario normal | 403 |
| Sesión de usuario normal + `x-user-role: admin` | 403 |
| Sesión de admin | 200 |

La capa de API del módulo tuvo que aprender a autenticarse: en ticket v2.0 el backend no
tenía usuarios y `fetch` iba sin cabeceras. Ahora `soporte/lib/api.ts` adjunta el
`Authorization: Bearer` que guarda el login, igual que el resto del frontend.

---

## 13. Un arreglo de fondo en el lienzo

`.app-shell` pinta el `body` con un degradado **translúcido y sin color de base**, y
`html` no tiene fondo: el lienzo de la página queda sin pintar. En las pantallas de
ConsultasSQL no se nota porque su contenido llena la ventana, pero las de soporte son más
largas y al hacer scroll asomaba una banda negra.

Se arregla con `body:has(.soporte-raiz) { background: var(--background) }`, que pinta el
body solo cuando hay una pantalla del módulo montada.

**La regla va fuera de `@layer` a propósito.** `.app-shell` también está fuera, y en la
cascada de CSS lo no estratificado gana a lo estratificado: puesta dentro de `@layer base`
el selector coincidía pero la declaración nunca se aplicaba —`getComputedStyle(body)`
seguía devolviendo el degradado—. Fuera de la capa gana por especificidad (0,1,1 contra
0,1,0). Verificado que en `/login` la regla no aplica y el degradado de A sigue intacto.

---

## 14. Tickets y Diagnósticos

Las dos últimas pantallas, que son las que llevan parámetros de ruta:

| Ruta | Pantalla |
|---|---|
| `/soporte/tickets` | Listado con filtros por estado y sistema, buscador y alta de tickets |
| `/soporte/tickets/[numero]` | Detalle: secciones, línea de tiempo de la investigación y casos similares |
| `/soporte/diagnostico` | Los 4 diagnósticos y el historial reciente |
| `/soporte/diagnostico/[id]` | Formulario y ejecución |
| `/soporte/diagnostico/historial/[id]` | Resultado guardado |

`historial` es un segmento estático y convive sin ambigüedad con `[id]`: Next resuelve
primero lo estático.

La adaptación desde react-router añadió dos patrones a los ya vistos:

- `useNavigate()` → `useRouter()` de `next/navigation`, con `navegar(x)` → `router.push(x)`.
- El listado de tickets guarda sus filtros en la URL con `setParams(p, { replace: true })`,
  que pasa a `router.replace()` con la ruta y los parámetros, sin scroll.
- `useParams()` de Next tipa los segmentos como `string | string[]`, así que las
  desestructuraciones llevan genérico: `useParams<{ numero: string }>()`.

`ICONO_PLAYBOOK` quedó solo en `soporte/lib/iconos.tsx`; la copia que traía
`diagnostico/paginas.tsx` se eliminó para que no puedan divergir.

### Verificación

Se recorrió el flujo completo con una sesión de admin: listar y filtrar tickets, abrir un
detalle con su investigación, ejecutar el diagnóstico de MAC duplicada, y **registrarlo
como paso del ticket 2026091974** — el paso aparece en la línea de tiempo con el SQL
utilizado, la evidencia y la conclusión. El historial abre el resultado guardado. Los
datos de prueba se borraron al terminar.

---

## 15. Navegación

ConsultasSQL **no tenía navegación global**: `Sidebar.tsx` es el árbol de categorías y
artículos del panel de administración —se alimenta de props, no de rutas— y `Header.tsx`
ni siquiera está en uso. Las pantallas se enlazaban entre sí con `router.push` sueltos.

Así que "engancharlo a la barra lateral" se resolvió en dos piezas:

**Dentro del módulo**, se trajo la barra de navegación de ticket v2.0
(`soporte/components/layout/barra-lateral.tsx`), con el `NavLink` de react-router
cambiado por `next/link` más `usePathname` para la pestaña activa. Incluye las insignias
de alertas abiertas y tickets en curso, el estado de Oracle y de la vigilancia, y un
enlace de vuelta. En pantallas pequeñas se convierte en menú desplegable que se cierra
solo al navegar.

La cabecera (`cabecera.tsx`) viene del Shell original con dos cambios: el título se deduce
de la ruta en vez de los `handle` de react-router, y **el tema usa el `ThemeProvider` de
ConsultasSQL**, de modo que el interruptor del módulo y el de la aplicación son el mismo.

**Desde ConsultasSQL**, un enlace "Centro de Soporte" en las cabeceras del panel de
administración y del panel SQL, visible solo con rol admin.

`Interpretar mensaje` aparece en la barra marcada como *pendiente*: lleva al marcador que
explica que depende del modelo de lenguaje. La entrada desaparece cuando esa pantalla
exista.
