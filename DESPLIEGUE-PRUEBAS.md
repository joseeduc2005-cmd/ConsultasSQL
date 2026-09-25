# Centro de Soporte — puesta en el ambiente de pruebas

Qué hace falta para dejarlo andando con datos reales, en modo lectura, y qué decidir
antes de dejarlo escribir.

---

## 1. Variables del backend

En `backend/.env`:

```bash
# Oracle contra la que investigan los diagnósticos.
# Es el id de una entrada de config/multidb.databases.json.
SOPORTE_ORACLE_DB_ID=oracle

# Llave de escritura. Déjela en `off` para el piloto.
SOPORTE_CORRECCIONES_EJECUTABLES=off

# Vigilancia automática. Encenderla es una decisión aparte (ver punto 7).
VIGILANCIA=off
```

Con la llave en `off` las soluciones se publican como **documentación**: guardan su SQL
y su formulario, se ven en el árbol de categorías, pero el panel no muestra el botón de
ejecutar. Es lo que corresponde mientras no se confirmen los permisos.

## 2. Crear el archivo de bases

`backend/config/multidb.databases.json` **no está en el repositorio**: lleva las
contraseñas de Oracle y Postgres en texto plano y está en `.gitignore`. Por eso al
clonar esa carpeta aparece vacía. Hay que partir del ejemplo:

```bash
cp backend/config/multidb.databases.example.json backend/config/multidb.databases.json
```

Y reemplazar los `CAMBIAR`, `HOST-INTERNO` y `USUARIO` por los valores reales. Lo mismo
con `backend/.env`, que sale de `backend/.env.example`.

El campo `schema` de cada base se llena solo: el motor introspecciona al arrancar.

### Sin Oracle todavía

Se puede desplegar sin ella. Todo lo que vive en `knowledge` funciona igual: artículos,
categorías, Biblioteca SQL, tickets, y los scripts contra Postgres, MySQL o Mongo.

Lo único que espera es el diagnóstico: «Revisar error» responde que no hay base
configurada y el ticket queda registrado. Cuando se añada la conexión, se pulsa otra vez
y ya investiga. **No inventa nada mientras tanto** — los datos de demostración solo salen
si alguien pide `SOPORTE_MODO=demo` a propósito.

## 3. Marcar las bases en `multidb.databases.json`

Una base solo sirve para investigar tickets si lo declara:

```json
{
  "id": "oracle",
  "label": "Oracle XE 21c (Produccion)",
  "grupo": "produccion",
  "color": "rojo",
  "soporte": { "tickets": true, "catalogo": "corresponsales" }
}
```

El `catalogo` no es decoración: las consultas del módulo están escritas contra
`CORRESPONSALES.USUARIO`, `FISJA.CLIENTES` y `JST_PERSONAS.VWT_CLIENTE`. Apuntarlas a
una base sin esos esquemas no da un mal diagnóstico, da un error.

El selector del ticket solo ofrece las marcadas, y avisa con `⚠` las de `grupo:
produccion`.

**Ojo:** editar este archivo con el sistema arriba dispara una recarga en caliente. El
login ya la sobrevive, pero conviene hacerlo en una ventana tranquila.

## 4. Si la base de conocimiento es antigua

No hace falta migrarla a mano ni tocar el `db-init`. El esquema se aplica solo en cada
arranque, y es idempotente:

- El backend garantiza en `knowledge_base` las columnas `tipo_solucion`, `script_json`,
  `campos_formulario` y `visible_para`.
- El módulo de soporte garantiza además `categoria`, `subcategoria`, `tags` y
  `actualizado`, que nadie más aseguraba y que necesita para publicar una solución.
- El esquema `soporte` completo (tickets, pasos, diagnósticos, consultas, alertas) se
  crea si no existe.

O sea: apunte el backend a la base vieja, arránquelo, y queda al día. Lo único que
conviene es **respaldarla antes**, como con cualquier cambio de esquema.

## 5. Permisos de Oracle

Hoy el módulo se conecta con el usuario que tenga la entrada de multidb. Lo correcto es
pedir a los DBA un usuario propio de **solo lectura** con `SELECT` sobre los esquemas de
arriba, y configurarlo sin tocar código:

```bash
SOPORTE_ORACLE_USUARIO=soporte_lectura
SOPORTE_ORACLE_CLAVE=...
SOPORTE_ORACLE_CONEXION=host:1521/servicio
```

Tiene precedencia sobre `SOPORTE_ORACLE_DB_ID`. Mientras tanto, la única barrera es el
guardián de solo lectura del código, que rechaza cualquier sentencia que no sea SELECT y
abre cada consulta con `SET TRANSACTION READ ONLY`.

## 6. Comprobaciones antes de abrir

```bash
# Pruebas del módulo. No tocan ninguna base.
cd backend && npm run test:soporte

# La fuente Oracle se recupera sola de un reinicio de la base.
node herramientas/prueba-recuperacion-oracle.mjs

# El login sobrevive a una recarga de la config multibase.
node herramientas/prueba-recarga-login.mjs
```

Y una vez conectado a la Oracle real, lo más importante:

```bash
# Calibra los umbrales de la exploración contra los datos de verdad. Solo lectura.
SOPORTE_ORACLE_DB_ID=oracle node herramientas/calibrar-exploracion.mjs > calibracion.txt
```

Los umbrales actuales (mínimo 8 usuarios de referencia, 75 % de cuota) se fijaron contra
una base de pruebas con 41 usuarios inventados. El calibrador mide **a qué porcentaje del
padrón real se le propondría una corrección**: si sale por encima del 40 %, el umbral no
discrimina y hay que subirlo — una corrección que aplica a media cooperativa no está
señalando ningún problema.

## 7. Qué NO encender todavía

**`SOPORTE_CORRECCIONES_EJECUTABLES=on`.** Enciende el botón de ejecutar de los artículos
publicados. Antes hace falta que Administración de Aplicaciones confirme dos cosas: que
el usuario de Oracle tiene permiso de escritura, y que acepta que las correcciones se
apliquen desde el panel en vez de desde su cliente.

**`VIGILANCIA=on`.** Pone a correr los monitores en segundo plano contra Oracle. Conviene
esperar a ver qué carga generan los diagnósticos a demanda.

## 8. Qué esperar del piloto

Medido contra los 10 tickets reales del equipo, el sistema **responde algo útil en los
10**. Eso no quiere decir que los resuelva: quiere decir que a ninguno le contesta «sin
hallazgos». En dos de ellos lo que responde es que el caso no tiene causa documentada y
qué habría que mirar, que es lo honesto.

La mitad responden sin poder consultar la base, porque el ticket no trae MAC,
usuario ni cédula: explican la causa ya investigada y piden el dato que falta. Eso es
esperado, no una falla.

Lo que conviene mirar durante el piloto:

- Cuántos tickets caen en «sin hallazgos», y de qué tratan. Cada uno es una regla que
  falta, y con el caso real delante se escribe rápido.
- Si las propuestas de la exploración coinciden con lo que el técnico habría hecho.
  Ahí se decide si vale la pena encender la escritura.
- Si algún diagnóstico tarda o carga la base más de lo aceptable.
