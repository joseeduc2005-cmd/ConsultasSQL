# Conexiones, temas ejecutables y cómo busca

Tres cosas en un sitio: con qué credenciales entrar desde DBeaver, qué temas ejecutan
contra qué base, y qué consulta hace el sistema para decidir qué sale primero.

---

## Credenciales para DBeaver

Todas son locales. Las dos de producción están en la tabla por completitud, pero **no se
alcanzan sin la red interna**, y a esas no hay que escribirles nada.

| Base | Motor | Host | Puerto | Base / Servicio | Usuario | Contraseña |
|---|---|---|---|---|---|---|
| `pg_main` | PostgreSQL 16 | localhost | 5432 | `knowledge_base` | `postgres` | `admin123` |
| `demo_cooperativa` | PostgreSQL 16 | localhost | 5432 | `demo_cooperativa` | `postgres` | `admin123` |
| `oracle_local` | Oracle 23ai Free | localhost | 1521 | `FREEPDB1` | `testuser` | `testpass123` |
| `mysql_test` | MySQL 8 | localhost | 3307 | `testdb` | `testuser` | `testpass123` |
| `mongo_local` | MongoDB 7 | localhost | 27017 | `testdb` | `admin` | `admin123` |
| `boton` | PostgreSQL | 172.20.90.33 | 5438 | `chasdb` | `gestion` | *(en el JSON)* |
| `oracle` | Oracle XE 21c | 172.17.211.71 | 1521 | `desajab.jardinazuayo.fin.ec` | `u11` | *(en el JSON)* |

Las tres locales de Docker viven en contenedores y **hay que arrancarlos** después de
reiniciar:

```bash
docker start oracle-pruebas practicas-mongo practicas-mysql
```

Oracle tarda uno o dos minutos en abrir la base. Para Mongo, la cadena de conexión lleva
`?authSource=admin` — sin eso rechaza la contraseña aunque sea la correcta.

**`admin123` está en el historial de git.** Para la demostración no bloquea; antes de
abrirlo a la cooperativa hay que cambiarla.

---

## Qué temas ejecutan, y contra qué

De los **23 temas**, nueve ejecutan algo (`dinámico`) y catorce son de lectura. Los de
lectura no consultan nada: explican la causa y ya.

### Contra PostgreSQL · `demo_cooperativa`

| Categoría | Tema | Pide |
|---|---|---|
| Socios | Consultar un socio por su cédula | `cedula` |
| Socios | Ver las cuentas y el saldo de un socio | `cedula` |
| Corresponsales | Buscar POS que comparten la misma MAC | — |
| Corresponsales | El corresponsal no puede entrar: revisar su usuario y la MAC | `cedula` |
| Reportes | Cuántas transacciones tiene un POS en un rango de fechas | `codigo_pos`, `desde`, `hasta` |
| Recaudaciones | Una recaudación se cobró pero no imprime el comprobante | `referencia` |
| Actas y pagos | Por qué un acta de pago no se puede corregir | `numero_acta` |

### Contra Oracle · `oracle_local`

| Categoría | Tema | Pide |
|---|---|---|
| Reportes | Carga de cada área de la mesa de servicios | — |
| Socios | Los casos de un socio por su cédula (Oracle) | `cedula` |

Estos dos usan el dialecto de Oracle de verdad — `CONNECT BY` para recorrer el árbol de
grupos, `TO_CHAR` con formato de fecha — y sirven para enseñar que el mismo sistema habla
con las dos bases sin que el que consulta se entere de cuál es cuál.

La base de Oracle tiene 488 casos con la forma del export de GLPI: categorías de hasta
cuatro niveles, grupos jerárquicos, escalamientos y tiempos. Se monta con:

```bash
cd backend
node herramientas/demo-oracle-mesa.mjs "C:/Users/USER/Downloads/glpi (1).csv"
```

**La categoría no decide contra qué base se ejecuta.** Eso lo dice el campo
`databaseId` del tema, y por eso hay temas de *Socios* y de *Reportes* en las dos bases.
La categoría es solo para encontrarlo.

---

## Qué consulta hace para decidir qué sale primero

Esto es lo que corre contra `knowledge_base` cada vez que alguien busca. Está en
[catalogo-postgres.ts](backend/src/soporte/adaptadores/postgres/catalogo-postgres.ts).

```sql
WITH p(termino, peso) AS (SELECT * FROM unnest($4::text[], $5::float8[]))
SELECT * FROM (
  SELECT k.id::text, k.titulo, k.descripcion, k.categoria,
         k.script_json->>'sql' AS sql,

         -- Qué parte del peso de la pregunta cubre este tema.
         (SELECT coalesce(sum(p.peso), 0) FROM p
           WHERE k.busqueda @@ to_tsquery('spanish', p.termino)) / $6 AS cobertura,

         -- Cuántas veces se abrió este tema tras buscar con estas palabras.
         (SELECT coalesce(sum(a.veces), 0) FROM soporte.aprendizaje a
           WHERE a.articulo = k.id::text AND a.termino = ANY($4::text[])) AS aprendido,

         ts_rank(k.busqueda, q) * (CASE WHEN k.script_json IS NOT NULL THEN 1.4 ELSE 1 END) AS relevancia
  FROM knowledge_base k, to_tsquery('spanish', $1) q
  WHERE k.busqueda @@ q
    AND ($3 = false OR coalesce(k.visible_para, 'todos') = 'todos')
) s
ORDER BY cobertura + least(aprendido * 0.03, 0.25) DESC, relevancia DESC
LIMIT $2
```

### Qué pesa, y cuánto

**La cobertura manda.** Es qué parte del peso de la pregunta cubre el tema, de 0 a 1. Un
tema que toca todas las palabras importantes gana a uno que toca una sola, por muy repetida
que esté esa una. Es el criterio principal y por eso va primero en el `ORDER BY`.

**El uso empuja, pero poco.** Cada vez que alguien busca algo y abre un tema, esa pareja
palabra → tema suma. Ese empuje vale `0,03` por visita **con tope en `0,25`** sobre una
cobertura que llega a 1. O sea: el uso puede desempatar entre dos temas parecidos, pero
**no puede subir un tema que no tiene nada que ver** por mucho que lo hayan abierto. Sin
tope, un tema popular acabaría saliendo para todo.

**Un tema con script vale 1,4 veces más** en la relevancia, que es el criterio de desempate.
La razón: además de explicar, trae la consulta — si dos temas sirven igual, el que devuelve
el dato ahorra un paso.

**La categoría no pesa nada.** Ni aparece en el orden. Sirve para navegar el árbol, no para
buscar: alguien que escribe *"el tendero ve datos de otro negocio"* no sabe —ni tiene por
qué— que eso está en *Corresponsales*.

Los tickets resueltos se buscan con una consulta hermana, con la diferencia de que exigen
`estado = 'resuelto'` y que tengan causa raíz o solución escrita: un ticket abierto no es
un precedente, es un problema.

### Cómo medirlo

```bash
cd backend && node herramientas/prueba-busqueda.mjs
```

Son 1.185 preguntas con erratas, sin tildes y con ruido. La última medición: **84,9% al
primer resultado y 90,7% entre los tres primeros.**
