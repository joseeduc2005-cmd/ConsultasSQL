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
| Integrado dentro del panel de administración | **Hecho** — ver sección 15 |
| Acceso restringido a rol admin | **Hecho** — dos barreras, ver sección 12 |
| Puente Biblioteca → motor de consultas | **Hecho** — ver sección 16 |
| Correcciones publicadas como artículos | **Hecho** — ver sección 20 |
| Diagnóstico al crear el ticket y publicación de la solución | **Hecho** — ver sección 21 |
| Oracle de pruebas en Docker | **Hecho** — ver sección 22 |
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

## 15. Dentro del panel de administración

El Centro de Soporte **es una sección del panel de administración**, no una aplicación
aparte. Todo cuelga de `/admin`: `/admin/resumen`, `/admin/tickets`, `/admin/biblioteca`,
`/admin/diagnostico`, `/admin/vigilancia`. Un grupo de rutas de Next —la carpeta
`app/admin/(soporte)/`— les da un layout propio sin aparecer en la URL.

**La barra lateral es una sola y no cambia nunca.** Tiene arriba los accesos del Centro de
Soporte y debajo el árbol de categorías de artículos, en las dos secciones. Al pulsar una
sección solo se reemplaza el contenido de la derecha.

| Pieza | Dónde |
|---|---|
| `app/components/CabeceraPanel.tsx` | La barra superior, extraída de `admin/page.tsx` para que ambas secciones usen una sola |
| `app/components/NavegacionSoporte.tsx` | Los accesos del módulo, con el lenguaje visual del panel |
| `app/components/BarraLateralPanel.tsx` | La barra completa: accesos de soporte + categorías. La usan las dos secciones |
| `app/admin/soporte/layout.tsx` | Arma el marco para las pantallas del módulo |

En `/admin` la barra recibe los artículos que la página ya tiene cargados; en las pantallas
de soporte no hay quien se los pase, así que los pide ella misma. Al abrir un artículo desde
una pantalla de soporte se vuelve a `/admin?articulo=<id>`, que lo selecciona y limpia la URL.

`.soporte-raiz` se aplica **solo al área de contenido**, no al marco: así los tokens del
módulo no repintan la cabecera ni la barra, que son del panel.

Hubo dos intentos previos fallidos, y el motivo es el mismo en los dos. El primero montó el
módulo en `/soporte`, con cabecera y barra propias: dos aplicaciones compartiendo un enlace.
El segundo compartió la cabecera pero no la barra, así que al entrar al módulo las
categorías desaparecían y seguía sintiéndose como salir del panel. Lo que hace que se sienta
una sola herramienta no es compartir las rutas ni la cabecera: es que **no desaparezca nada
de la barra al navegar**.

`Interpretar mensaje` no aparece en la navegación porque todavía no existe; la ruta
comodín la cubre si alguien llega por un enlace.

---

## 16. El puente con el motor de consultas

Las 111 consultas de la Biblioteca son un corpus curado a mano que mapea **lenguaje de
negocio en español** a las tablas reales de producción. El motor guarda justo ese tipo de
conocimiento en `semantic_learning`, y antes de esto tenía 10 filas que había inferido
solo, de mala calidad (`"sistema" → users` con 0.85 de confianza).

`backend/src/soporte/vocabulario.ts` deriva el vocabulario de la Biblioteca y lo escribe
ahí. **No interviene ningún modelo de lenguaje**: es conteo, normalización y análisis del
SQL.

### Cómo puntúa

Para cada consulta toma los términos del nombre y la categoría, y las tablas y columnas
del SQL. Después mide, para cada pareja término→objetivo, cuán exclusiva es. Tres
correcciones hicieron la diferencia entre un vocabulario útil y uno equivocado:

1. **Peso por columnas tocadas, no por aparición.** Estar en un `JOIN` vale 1; cada
   columna que la consulta realmente lee sobre esa tabla suma 2. El alias del SQL dice a
   qué tabla pertenece cada columna.
2. **Penalización a las tablas omnipresentes** (IDF). `usuario` sale en casi todas las
   consultas por arrastre de JOINs y no distingue nada.
3. **JOINs con coma.** El corpus usa la sintaxis antigua de Oracle (`FROM a u, b p`). El
   primer extractor solo leía la tabla que seguía a `FROM` y perdía el resto.

Sin las tres, `"mac"` terminaba apuntando a `usuario`. Con ellas apunta a `pos_view`, que
es donde vive `pos_mac`. Como el motor se queda con el mapeo de **mayor confianza** por
término, acertar el ganador es todo lo que importa.

Resultado sobre las 111 consultas: **45 términos → tabla y 24 → columna**. Algunos:

| término | tabla | apoyado por |
|---|---|---|
| `parametro` | `parametro` | 7 consultas |
| `acta` | `actacomprobantes` | 4 |
| `representante` | `corresponsalrepresentante` | 2 |
| `mac` | `pos_view` | 6 |
| `transaccion` | `transacciones_view` | 2 |

### Por qué no ensucia las consultas a PostgreSQL

Era la duda principal. **No la hay**: `QueryIntelligenceEngine` comprueba que la tabla
exista en el esquema que está consultando antes de usar un mapeo aprendido. Un término
que apunta a una tabla de Oracle simplemente se descarta cuando la consulta va a
PostgreSQL.

Por eso todo se escribe bajo la **misma huella** que usa el motor al leer
(`schemaDetector.getConnectionFingerprint()`): el cargador filtra por ella, y sembrar bajo
otra habría dejado las filas escritas pero nunca leídas.

### Operación

Se siembra solo en el primer arranque. Para regenerarlo tras ampliar la Biblioteca:

```
GET  /api/soporte/vocabulario   ← previsualiza sin escribir nada
POST /api/soporte/vocabulario   ← regenera y recarga la caché del motor
```

Es idempotente: borra lo que sembró antes y vuelve a escribir. Las filas que el motor
aprendió por su cuenta **no se tocan** — se distinguen por la columna `origen`, que se
añade de forma aditiva con el mismo patrón `ADD COLUMN IF NOT EXISTS` que ya usa `app.js`.

Con `SOPORTE_VOCABULARIO=off` la siembra automática se desactiva.

---

## 17. Base de datos del ticket

Los tickets siempre se guardan en PostgreSQL, en el esquema `soporte`. Oracle no almacena
nada: solo se lee, desde los diagnósticos y los monitores. El campo **Base de datos** del
ticket no cambia eso — dice **contra qué base se investiga el caso**, que es otra cosa.

Hace falta porque el motor tiene seis bases registradas y los tickets vienen de cuatro
sistemas distintos: un caso de Botón de Pago se investiga contra `boton`, no contra la
Oracle de Corresponsales, y antes no había dónde anotarlo.

- La columna `soporte.tickets.base_datos` guarda el id de `multidb.databases.json`.
- El formulario lo ofrece desde `/api/distributed/databases?showAll=true`. El `showAll` es
  necesario: por defecto ese endpoint devuelve solo las conectadas y no primarias, y un
  ticket se registra igual aunque la base esté caída.
- `/api/soporte/estado` expone `baseDiagnosticos`, la base contra la que corren los cuatro
  diagnósticos. Si no coincide con la del ticket, el detalle **oculta la lista de
  diagnósticos** y explica por qué: su SQL está escrito contra tablas concretas de
  Corresponsales y no funcionaría en otra base. En modo demostración no hay base asociada
  y se muestran todos.

---

## 18. Estado de Oracle: por qué no bloquea la interfaz

La cabecera y la barra lateral consultan `/api/soporte/estado` cada minuto. Con Oracle
inalcanzable, cada llamada esperaba a que el driver se rindiera —unos 20 segundos— y la
interfaz se quedaba colgada. Se arregló por dos lados:

1. **El pool deja de esperar indefinidamente.** `connectTimeout`, `transportConnectTimeout`
   y `queueTimeout` se fijan desde `SOPORTE_ORACLE_CONECTAR_TIMEOUT_S` (6 s por defecto).
   Esto también acota los diagnósticos, no solo el sondeo.
2. **El sondeo se cachea 30 s y se refresca por detrás.** Si hay un valor anterior se
   devuelve al instante aunque esté vencido, y el sondeo nuevo corre en segundo plano.
   Solo la primerísima llamada llega a esperar, y como máximo 2,5 s.

Además se lanza un sondeo al arrancar (`precalentarEstadoOracle`), así que en la práctica
ni esa primera llamada espera.

Medido con la Oracle de producción inalcanzable: **de 20 s por llamada a 7 ms**.

---

## 19. Qué hace y qué no hace un diagnóstico

Un diagnóstico **no aplica la corrección**. Devuelve el veredicto, los hallazgos y la
corrección **ya redactada con los valores reales**, marcada como escritura y con su
responsable. La ejecuta una persona.

Ejemplo real (`acceso-corresponsal` sobre un usuario bloqueado):

```
veredicto: problema — Bloqueado por intentos fallidos de PIN

  [alta] Bloqueado por intentos fallidos de PIN
         usr_intentos_pin = 3 (máximo configurado: 3).
    → acción: Resetear los intentos de PIN.
      riesgo: escritura · responsable: Administración de Aplicaciones · biblioteca: U10
      sql:    UPDATE CORRESPONSALES.USUARIO SET USR_INTENTOS_PIN = 0
              WHERE USR_CODIGO = 3003;
              COMMIT;
```

El `USR_CODIGO = 3003` no es un hueco por rellenar: el diagnóstico ya resolvió de qué
usuario se trata. Lo que falta es que alguien lo ejecute.

Es la regla de oro que venía del flujo anterior y que se conservó tal cual: *toda sentencia
de escritura se marca como riesgosa y la ejecuta una persona*. El tipo `AccionSugerida` lo
dice en su comentario, y no existe ningún camino en el código que ejecute una acción de
escritura sobre Oracle.

---

## 20. De un diagnóstico a un artículo ejecutable

El diagnóstico redacta la corrección, pero antes eso moría en la pantalla del
diagnóstico: había que copiar el SQL a mano y el conocimiento no se acumulaba en ningún
sitio. Ahora las correcciones viven en el árbol de categorías del panel, donde el equipo
ya busca.

### Un catálogo, no un artículo por ticket

`backend/src/soporte/correcciones.ts` declara **cada corrección una sola vez**, con su
plantilla SQL y los campos que necesita. De esa única declaración salen dos cosas:

1. La acción que muestra el diagnóstico, con los valores del caso ya resueltos.
2. El artículo de `knowledge_base`, con `script_json` y `campos_formulario`.

Antes las plantillas estaban sueltas dentro de cada playbook. Sacarlas al catálogo evita
que el SQL del diagnóstico y el del artículo se separen con el tiempo.

Son **cinco correcciones**, no una por ticket. Generar un artículo por cada caso llenaría
el árbol de entradas de un solo uso, cada una con un `USR_CODIGO` distinto quemado dentro.
En su lugar hay cinco artículos parametrizados y reutilizables:

| Artículo | Categoría | Campos |
|---|---|---|
| Reactivar un usuario desactivado | Corresponsales / Acceso y usuarios | `usr_codigo` |
| Resetear los intentos de PIN | Corresponsales / Acceso y usuarios | `usr_codigo` |
| Resetear intentos y reactivar por nombre de usuario | Corresponsales / Acceso y usuarios | `usuario` |
| Alinear la sucursal del usuario con la del corresponsal | Corresponsales / Sucursales | `codigo_sucursal`, `cedula` |
| Corregir la sucursal en fisja.clientes | Corresponsales / Sucursales | `codigo_sucursal`, `codigo_socio` |

La acción del diagnóstico ahora lleva `correccion` y `parametros` además del SQL resuelto:

```json
{ "descripcion": "Resetear los intentos de PIN.",
  "correccion": "resetear-pin",
  "parametros": { "usr_codigo": 3003 },
  "sql": "UPDATE CORRESPONSALES.USUARIO SET USR_INTENTOS_PIN = 0 WHERE USR_CODIGO = 3003; COMMIT;" }
```

Con eso se puede enlazar al artículo llevando el formulario ya rellenado.

### Publicados como documentación, todavía no ejecutables

```
GET  /api/soporte/correcciones            ← el catálogo
POST /api/soporte/correcciones/publicar   ← los publica o actualiza
```

Idempotente: se reconocen por el título y se actualizan en vez de duplicarse.

**Por defecto se publican con `tipo_solucion = 'lectura'`.** El `script_json` y los
`campos_formulario` quedan guardados, pero el panel no muestra el botón de ejecutar. Para
encenderlo basta `{"ejecutable": true}` en el POST — pero es una decisión que no es
técnica y hay dos cosas que confirmar antes:

1. Si el usuario de Oracle del motor (`u11` en `multidb.databases.json`) tiene permiso de
   escritura sobre `CORRESPONSALES.USUARIO` y `fisja.clientes`.
2. Si Administración de Aplicaciones acepta que la corrección se aplique desde el panel en
   vez de desde su cliente. Seguiría ejecutándola una persona, pero cambia con qué usuario
   corre y dónde queda el rastro: sería `u11`, y el registro iría a `article_executions`.

La regla del módulo de soporte no cambia en ningún caso: **los diagnósticos no ejecutan
escrituras**. Leen con su propio pool y su usuario restringido. Si algún día se enciende el
botón, la ejecución iría por el camino de ConsultasSQL, con su usuario y su auditoría: son
dos conexiones distintas, no una con permisos mezclados.

---

## 21. Del ticket a la solución publicada

Al crear un ticket, el backend saca MAC, usuario o cédula de su título y descripción
—con reglas, **sin modelo de lenguaje**—, elige el diagnóstico que corresponde y lo
ejecuta, dejando el resultado como primer paso de investigación.

`soporte/interpreter/` es la parte del intérprete de ticket v2.0 que no depende de IA:
`extraer.ts` (identificadores) y `reglas.ts` (clasificación). El modelo sigue fuera.

La respuesta de `POST /tickets` incluye qué pasó:

```json
{ "ejecutado": true, "playbook": "acceso-corresponsal", "diagnosticoId": 8, "veredicto": "problema" }
{ "ejecutado": false, "motivo": "El ticket no trae MAC, usuario ni cédula con los que investigar." }
```

**El backend no inventa SQL.** Reconoce el problema entre los que los diagnósticos saben
comprobar y produce la corrección de su catálogo con los valores del caso. Ante un caso
nuevo devuelve «sin hallazgos» y sigue haciendo falta una persona.

### El botón «Revisar error»

En el detalle del ticket, la tarjeta **Investigación** lleva un botón **Revisar error**:
el backend deduce del texto qué comprobar, ejecuta el diagnóstico, **muestra el resultado
ahí mismo** y lo deja registrado como paso.

Se dejó dentro de Investigación en vez de reemplazarla: los pasos son el historial del
caso, que es lo que hace útil el ticket después. Lo que sí desaparece es su estado vacío,
que antes solo decía «sin pasos».

`POST /tickets/:numero/revisar` devuelve el diagnóstico completo, no solo el resumen.

### Publicar la solución

`POST /diagnosticos/:id/publicar` convierte el diagnóstico en un artículo bajo
`<sistema> / Soluciones`: qué se revisó paso a paso, qué se encontró con su severidad, y
el SQL ya resuelto con el enlace a la plantilla reutilizable. En la pantalla del
diagnóstico es el botón **Publicar como solución**.

Es a petición y no automático: un diagnóstico sin hallazgos no merece artículo, y
publicarlos todos llenaría el árbol. Rechaza publicar si no hubo hallazgos, y republicar
el mismo caso lo actualiza en vez de duplicarlo.

---

## 22. Oracle de pruebas

`backend/src/soporte/oracle/pruebas.sql` levanta el esquema de Corresponsales en una
Oracle local, con los mismos escenarios del modo demostración: MAC duplicada, usuario
bloqueado por PIN, sucursal desincronizada y transacciones sin movimiento de tendero.

```bash
docker run -d --name oracle-pruebas -p 1521:1521   -e ORACLE_PASSWORD=admin123 -e APP_USER=testuser -e APP_USER_PASSWORD=testpass123   gvenzl/oracle-free:23-slim
# ~3-5 min hasta "DATABASE IS READY TO USE"
docker cp backend/src/soporte/oracle/pruebas.sql oracle-pruebas:/tmp/
docker exec oracle-pruebas sqlplus -S sys/admin123@localhost/FREEPDB1 as sysdba @/tmp/pruebas.sql
```

Luego se apunta el módulo ahí:

```bash
cd backend && SOPORTE_ORACLE_DB_ID=oracle_local node src/app.js
```

En Oracle un esquema es un usuario, así que el script crea `CORRESPONSALES`, `FISJA` y
`JST_PERSONAS` y concede `SELECT` a `TESTUSER`, que es con el que se conecta el módulo.
Es idempotente.

Verificado contra él: los **cuatro diagnósticos** devuelven veredicto correcto y los
**cinco monitores** detectan su hallazgo.
