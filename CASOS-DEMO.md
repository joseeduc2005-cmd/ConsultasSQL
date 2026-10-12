# Casos para probar el sistema

Cada caso trae lo mismo: **qué escribir**, **qué le va a pedir el sistema**, **qué poner**
y **qué tiene que salir**. Si sale otra cosa, está anotado a qué mirar.

Los siete primeros ya tienen tema de ayuda: se buscan y se ejecutan. Los tres últimos
**solo tienen la explicación escrita, no la consulta** — se busca, sale el tema que cuenta
la causa, y después hay que ir a pedirle el dato a alguien. Esos tres se resuelven en vivo
añadiendo el SQL dinámico, y el SQL está escrito aquí para pegar.

Todo lo de la demostración lleva `[demo]` en el título, y se quita con una sola orden.

---

## Antes de empezar

El backend y PostgreSQL están arriba, y la base de demostración y su contenido están
montados. Solo falta el frontend:

```bash
cd frontend
pnpm dev
```

Y entrar en http://localhost:3000 como administrador.

Si hubiera que volver a montarlo todo desde cero:

```bash
cd backend
node herramientas/demo-cooperativa.mjs
node herramientas/demo-contenido.mjs
node src/app.js
```

Para comprobar que la demostración funciona antes de exponer —busca, pide el dato,
ejecuta y verifica el resultado de los diez casos, sin tocar nada:

```bash
cd backend && node herramientas/prueba-demo.mjs
```

Tiene que acabar en `LA DEMOSTRACIÓN FUNCIONA DE PUNTA A PUNTA`.

---

## Los datos que hay

Base `demo_cooperativa`: 12 socios, 16 cuentas, 5 corresponsales, 5 POS, 516
transacciones, 24 recaudaciones, 5 actas. Todo inventado, pero coherente entre sí.

### Los temas de ayuda

| | |
|---|---|
| 14 de **lectura** | escritos de los casos que el equipo investigó de verdad. Explican la causa; no consultan nada. |
| 7 **dinámicos** `[demo]` | piden un dato y lo van a buscar a la base. |

Esa diferencia es la mitad de lo que hay que enseñar: lo que ya había era memoria escrita,
y lo que se añade es que además mire.

### Socios

| Cédula | Nombre | Oficina | Estado | Cuentas |
|---|---|---|---|---|
| `0110000000` | María Fernanda Tenesaca Quizhpi | Matriz Cuenca | activo | 2 activas |
| `0110000137` | Byron Patricio Quizhpe Loja | Azogues | activo | 1 |
| `0110000274` | Rosa Elena Pauta Guamán | Paute | activo | 1 |
| `0110000411` | José Luis Chuchuca Ordóñez | Gualaceo | **bloqueado** | 2 **bloqueadas** |
| `0110000548` | Ana Lucía Morocho Sigcha | Sígsig | activo | 1 |
| `0110000685` | Carlos Andrés Peñaloza Vera | Cañar | activo | 1 |
| `0110000959` | Segundo Manuel Lema Tigse | Azogues | **inactivo** | 1 |
| `0110001096` | Jessica Paola Narváez Bermeo | Paute | activo | 1 |
| `0110001233` | Luis Alberto Cabrera Zhingre | Gualaceo | activo | 2 |

### Puntos de atención

| POS | Local | MAC | Estado del POS | Estado del local | Transacciones (30 días) |
|---|---|---|---|---|---|
| `POS-1001` | Tienda Don Byron | `00:1B:44:11:3A:B7` | activo | activo | **444** |
| `POS-1002` | Guel Cyber Café | `00:1B:44:11:3A:B7` | **inactivo** | **inactivo** | 24 |
| `POS-1003` | Despensa La Esperanza | `00:1B:44:11:3A:C2` | activo | activo | 24 |
| `POS-1004` | Bazar Mi Rincón | `00:1B:44:11:3A:C3` | activo | activo | 24 |
| `POS-1005` | Minimarket Doña Ana | `00:1B:44:11:3A:D4` | activo | activo | 0 |

Los dos primeros comparten MAC. Eso es el caso 1.

### Usuarios del portal de corresponsal

| Usuario | Cédula | MAC registrada | POS con esa MAC |
|---|---|---|---|
| `byron.quizhpe` | `0110000137` | `00:1B:44:11:3A:B7` | **2** — duplicada |
| `jessica.narvaez` | `0110001096` | `00:1B:44:11:3A:C2` | 1 — correcto |
| `luis.cabrera` | `0110001233` | `00:1B:44:11:3A:C3` | 1 — correcto |
| `ana.morocho` | `0110000548` | `00:1B:44:11:3A:E1` | **0** — no existe |

### Recaudaciones y actas

| Referencia | Institución | Monto | Estado | Comprobante |
|---|---|---|---|---|
| `REC-240000` | MUNICIPIO GAD MEJIA — agua potable | 3,00 | cobrada | `CMP-900000` |
| `REC-240065` | MUNICIPIO GAD MEJIA — predios | 42,55 | **anulada** | ninguno (no se cobró) |
| `REC-240143` | CONSEJO PROVINCIAL DEL AZUAY | 126,01 | **cobrada** | **ninguno** |

| Acta | Periodo | Valor | Estado | Pago |
|---|---|---|---|---|
| `53656` | 2026-07 | 142,16 | cerrada | **C — cerrado** |
| `53657` | 2026-08 | 145,27 | abierta | sin pago |

---

# Casos con solución

## 1 · «El tendero ve los datos de otro negocio»

**Escriba en buscar:**

> entra al pos y le salen datos de otro negocio

Sale de primero **Buscar POS que comparten la misma MAC**. Nadie escribió «tendero» ni
«otro negocio» en ese título.

**No pide ningún dato.** Ejecútelo directamente.

**Tiene que salir una fila:**

```
00:1B:44:11:3A:B7 · 2 puntos · Tienda Don Byron (POS-1001) · Guel Cyber Café (POS-1002)
```

**Qué decir:** dos POS distintos con la misma MAC. Uno de los locales está inactivo, y por
eso el problema aparece unos días y otros no — que es exactamente lo que reportó el
corresponsal y lo que costaba de creer.

---

## 2 · «No puedo ver los datos del socio»

**Escriba:**

> quiero saber los datos de un socio por la cedula

Sale **Consultar un socio por su cédula**.

**Le pide:** `cedula`. **Ponga** `0110000411`.

**Tiene que salir:**

```
0110000411 · José Luis Chuchuca Ordóñez · Gualaceo · bloqueado
```

**Qué decir:** el que atiende no necesita SQL ni pedirle nada a sistemas. Escribe la cédula
y tiene el dato. Y el dato ya es media respuesta: está bloqueado.

Pruebe también `0110000000` (un socio normal) para que se vea que no siempre hay problema.

---

## 3 · «El socio no puede retirar en el corresponsal»

**Escriba:**

> el socio no puede retirar en el corresponsal

Sale **Ver las cuentas y el saldo de un socio**.

**Le pide:** `cedula`. **Ponga** `0110000411`.

**Tienen que salir dos filas, las dos bloqueadas:**

```
4100031676 · ahorros   · 125.48 · bloqueada
4100031677 · corriente · 125.48 · bloqueada
```

**Qué decir:** no era el corresponsal ni el POS. Las cuentas están bloqueadas. Ese es el
tipo de llamada que se cierra en un minuto en vez de en dos días.

---

## 4 · «El corresponsal no puede entrar: usuario no encontrado»

Este es el bueno, porque **el mismo síntoma tiene dos causas distintas** y el sistema las
separa.

**Escriba:**

> el corresponsal no puede ingresar le dice usuario no encontrado

Sale **El corresponsal no puede entrar: revisar su usuario y la MAC**.

**Le pide:** `cedula`.

**Primero ponga `0110000548`** (Ana Lucía):

```
ana.morocho · activo · MAC 00:1B:44:11:3A:E1 · 7 intentos fallidos
pos_con_esa_mac: 0        cuales: (vacío)
```

La MAC registrada no es de ningún equipo. Por eso al buscar por MAC y usuario no encuentra
nada, y el mensaje que devuelve —«usuario no encontrado»— manda a mirar donde no está.

**Ahora ponga `0110000137`** (Byron), mismo tema:

```
byron.quizhpe · activo · MAC 00:1B:44:11:3A:B7 · 0 intentos fallidos
pos_con_esa_mac: 2        cuales: POS-1001 (activo) · POS-1002 (inactivo)
```

Aquí la MAC existe, pero está en dos equipos y uno está inactivo.

**Qué decir:** el síntoma es idéntico y la causa no. Una se arregla asignándole bien el
equipo; la otra, quitando la MAC duplicada del equipo que ya no se usa. Distinguirlas es
la mitad del trabajo, y es lo que queda guardado para el siguiente.

---

## 5 · «El reporte da error cuando pido un mes»

**Escriba:**

> el reporte de transacciones da error cuando pido un mes

Sale **Cuántas transacciones tiene un POS en un rango de fechas**.

**Le pide tres datos:** `codigo_pos`, `desde`, `hasta`.

**Ponga** `POS-1001` · `2026-09-01` · `2026-12-31`:

```
POS-1001 · Tienda Don Byron · 444 transacciones
diagnostico: pasa del limite de 300: por eso el reporte da error
```

**Ahora `POS-1003`**, mismas fechas:

```
POS-1003 · Despensa La Esperanza · 24 transacciones
diagnostico: dentro del limite: el reporte deberia salir
```

**Qué decir:** no es que el reporte esté caído. Tiene un límite de 300 y cuando el rango
trae más, falla en vez de recortar. Con esto se le puede responder al usuario en el momento
—«pídalo por semanas»— en vez de abrir un ticket y esperar.

---

## 6 · «Se cobró pero no sale el comprobante»

**Escriba:**

> se cobro la recaudacion pero no sale el comprobante

Sale **Una recaudación se cobró pero no imprime el comprobante**.

**Le pide:** `referencia`. **Ponga** `REC-240143`:

```
REC-240143 · CONSEJO PROVINCIAL DEL AZUAY · 126.01 · cobrada
comprobante: NO SE GENERO
diagnostico: cobrada sin comprobante: hay que reimprimirlo desde respaldo
```

**Ahora ponga `REC-240065`**, para el contraste:

```
REC-240065 · MUNICIPIO GAD MEJIA - predios · 42.55 · anulada
diagnostico: anulada: no hay comprobante porque no se cobro
```

**Qué decir:** son dos cosas que se parecen y no son lo mismo. En una hay plata cobrada y
falta el papel; en la otra no hay nada cobrado. Contestar lo uno por lo otro es el error
que cuesta caro, y aquí no hay que acordarse de la diferencia: la dice la consulta.

---

## 7 · «No me deja corregir el acta»

**Escriba:**

> no me deja corregir el acta de pago del corresponsal

Sale **Por qué un acta de pago no se puede corregir**.

**Le pide:** `numero_acta`. **Ponga** `53656`:

```
53656 · 2026-07 · 142.16 · cerrada · pago C
diagnostico: el pago esta cerrado: la aplicacion no deja corregir el acta
```

**Ahora ponga `53657`:**

```
53657 · 2026-08 · 145.27 · abierta · pago -
diagnostico: sin pago cerrado: se puede corregir desde la pantalla
```

**Qué decir — y este es el momento importante:** el sistema dice *por qué* está bloqueada,
con el número y el motivo, listo para escalar. Lo que **no** hace es desbloquearla. No
ejecuta UPDATE ni DELETE contra ninguna base, y no hay una opción para activarlo. Por eso
se puede dejar en manos de cualquiera: lo peor que puede hacer alguien aquí es leer.

Si alguien pregunta si eso no limita: sí, limita. Y es la razón por la que esto se puede
abrir a toda la cooperativa sin que nadie tenga que vigilarlo.

---

# Casos a medias: hay explicación, falta el dato

Estos tres son los más interesantes, y conviene entender por qué antes de enseñarlos.

La base **ya tiene catorce temas** escritos de los casos que el equipo investigó de verdad.
Los tres de abajo salen al buscarlos —uno con confianza **alta**. Pero los catorce son de
**lectura**: explican la causa, y ninguno consulta nada. Así que quien los lee entiende qué
pasó y **sigue sin el dato**: tiene que ir a pedirle a alguien que mire en la base.

Ese es el hueco que se tapa en vivo. No es que el sistema no sepa: es que sabía explicar y
no sabía mirar.

Al crear cada tema:

- **Tipo de solución:** `dinamico`
- **Base de datos:** `Cooperativa (demostración)`
- **Visible para:** todos
- Pegar el SQL y guardar. Los datos que pide salen solos de los `:nombre` del SQL.

---

## 8 · «¿Qué formato se exige para la contraseña y el PIN?»

**Escriba:**

> que formato tiene que tener el pin del corresponsal

Sale **El cambio de contraseña o PIN «se guarda» pero no persiste: qué formato exige**, con
confianza media. Ábralo: explica el caso y las reglas tal como se escribieron.

**Y ahí se acaba.** Es texto. Si mañana cambian la política, ese texto miente y nadie se
enterará hasta que alguien se queje. Para saber qué se exige **hoy** hay que mirar la tabla.

**Cree el tema.** Título: *Qué formato se exige ahora mismo para la contraseña y el PIN*

```sql
SELECT credencial,
       longitud_min,
       longitud_max,
       solo_numeros,
       exige_mayuscula,
       exige_minuscula,
       exige_digito,
       exige_simbolo,
       no_repetir_ultimas,
       prohibe_consecutivos,
       prohibe_cedula
  FROM politica_seguridad
 WHERE credencial = :credencial
   AND vigente = true
```

**Le pedirá:** `credencial`. **Ponga** `pin`:

```
pin · 4 a 4 · solo numeros: sí · exige digito: sí
no repetir las ultimas: 1 · prohibe consecutivos: sí · prohibe la cedula: sí
```

**Y ahora** `contrasena`:

```
contrasena · 8 a 15 · mayuscula, minuscula, digito y simbolo
no repetir las ultimas: 3
```

**Qué decir:** lo escrito envejece; la consulta no. Un tema de lectura dice lo que era
verdad el día que se escribió, y uno dinámico dice lo que es verdad ahora. Los dos sirven,
y conviene tener los dos —pero cuando la pregunta es «qué se exige», la respuesta tiene que
venir de la base.

---

## 9 · «Las transacciones salen con la oficina equivocada»

**Escriba:**

> la contabilizacion sale con otra oficina y descuadra

Sale **Descuadre contable tras cambiar de oficina** con confianza **alta**. Ábralo: la causa
está explicada completa —la contabilización lee la oficina de una tabla que ya no se
actualiza, y el reporte la lee de otra que sí.

**Perfecto. Y ahora qué.** Porque saber la causa no dice *a quién le pasa*, y eso es lo que
pregunta quien llama. Para saberlo hay que comparar las dos tablas, socio por socio.

**Cree el tema.** Título: *A qué socios les descuadra la oficina de la contabilización*

```sql
SELECT s.cedula,
       s.nombres || ' ' || s.apellidos AS socio,
       s.sucursal AS oficina_del_socio,
       f.sucursal AS oficina_que_usa_la_contabilizacion,
       f.actualizado AS ultima_actualizacion,
       CASE WHEN f.sucursal <> s.sucursal
            THEN 'descuadra: la contabilizacion usa la oficina antigua'
            ELSE 'cuadra' END AS diagnostico
  FROM socios s
  JOIN clientes_fisja f ON f.cedula = s.cedula
 WHERE s.cedula = :cedula
```

**Le pedirá:** `cedula`. **Ponga** `0110000000`:

```
María Fernanda Tenesaca Quizhpi
oficina_del_socio: Matriz Cuenca
oficina_que_usa_la_contabilizacion: Yanuncay   (actualizada en 2021)
diagnostico: descuadra: la contabilizacion usa la oficina antigua
```

**Pruebe `0110000274`** para ver uno que cuadra. **`0110000685`** es el otro que descuadra.

**Qué decir:** el tema de lectura explica el problema una vez; este responde la pregunta
cada vez que alguien llama. Y es un caso que **no se ve mirando una sola tabla** —parecía un
descuadre de plata y era un dato desincronizado.

---

## 10 · «Están fallando las recargas»

**Escriba:**

> estan fallando las recargas

Sale **La recarga se cobra pero el saldo no llega al celular**. Ábralo: en el caso real era
una caída del proveedor externo, ajena a la cooperativa.

**Pero eso fue aquella vez.** Hoy no se sabe si es lo mismo, y antes de escribirle al
proveedor hay que ver si los rechazos están concentrados en un servicio o repartidos por
todo. Si están repartidos, el problema es nuestro.

**Cree el tema.** Título: *Transacciones rechazadas por tipo en un rango de fechas*

```sql
SELECT t.tipo,
       count(*)::int AS total,
       sum(CASE WHEN t.estado = 'rechazada' THEN 1 ELSE 0 END)::int AS rechazadas
  FROM transacciones t
 WHERE t.fecha >= :desde
   AND t.fecha <= :hasta
 GROUP BY t.tipo
 ORDER BY rechazadas DESC
```

**Le pedirá:** `desde` y `hasta`. **Ponga** `2026-09-01` y `2026-12-31`:

```
recaudacion   · 129 total ·  24 rechazadas
pago_servicio ·  ...
deposito      ·  ...
retiro        ·  ...
```

**Qué decir:** el tema de lectura dice qué pasó una vez; este dice qué está pasando ahora.
Con los dos juntos, el que atiende no empieza de cero ni se queda con la respuesta de hace
tres meses.

---

# Lo mismo, pero entrando como un usuario cualquiera

Esto conviene enseñarlo, porque es lo que decide si el sistema sirve a toda la cooperativa
o solo al equipo de soporte.

**Salga de la sesión y entre como `user` / `password123`** (o con la cuenta de cualquiera
sin rol de administrador). Verá el **Portal de Soporte**, que es otra pantalla: sin panel,
sin tickets ajenos, sin biblioteca.

Abra **Socios → Consultar un socio por su cédula**. Debajo de la sentencia hay un recuadro:

> Para consultarlo hace falta este dato: **Cédula**  → `0110000411` → **Consultar**

Sale la fila, y al lado **Descargar CSV**, que abre en Excel con las tildes bien.

Pruebe también el de **Reportes**, que pide tres datos y dos son fechas: ahí se ve que el
formulario sale solo del SQL —nadie lo dibujó— y que las fechas traen su calendario.

**Qué decir:** quien atiende ya no tiene que pedirle a sistemas que le corra una consulta.
La corre él, y solo las que un administrador publicó para todos.

**Y la pregunta que va a salir —«entonces cualquiera puede sacar datos»— tiene respuesta
concreta:**

| | |
|---|---|
| Un tema marcado **solo para administradores** | el usuario no lo ve, y si adivina el enlace recibe el mismo «no se encontró» que si no existiera |
| Un tema de **lectura** | no ejecuta nada, porque no hay nada que ejecutar |
| **Escribir** | ningún tema, de nadie, puede llevar UPDATE, DELETE, INSERT ni DROP contra ninguna base |
| **Mandar otra consulta** desde el navegador | no sirve de nada: se ejecuta la sentencia del tema, no lo que llegue en la petición |
| **Quién consultó qué** | queda registrado igual que si lo hubiera hecho soporte: usuario, tema, con qué datos, cuántas filas |

Lo que decide quién ve qué es **la visibilidad del tema**, que la pone quien lo publica. Es
una sola decisión y está en un sitio.

Se puede comprobar entero, sin tocar nada:

```bash
cd backend && node herramientas/prueba-ejecucion-usuario.mjs
```

---

# Lo demás que hay que enseñar

## El aporte que espera revisión

En **Aportes** hay uno pendiente de `maria.tenesaca`:
*«El socio no puede retirar y el corresponsal funciona bien»*. Está contado **por pasos**, y
uno de ellos trae la consulta con la que lo comprobó.

**Acéptelo.** Se convierte en tema de ayuda, con el nombre de quien lo aportó. Vuelva a
buscar «el socio no puede retirar» y ya sale también ese.

**Qué decir:** esto es lo que hace que la base crezca sola. Nadie se sienta a documentar: se
sube lo que se resolvió, el día que se resolvió, y soporte decide si sirve para todos.

## Los tickets

| Número | Estado | Pasos |
|---|---|---|
| `DEMO-0002` | resuelto | 2 — con causa raíz y solución |
| `DEMO-0003` | resuelto | 4 — la MAC que no existe, paso a paso |
| `DEMO-0001` | investigación | 3 |
| `DEMO-0004` | investigación | 2 — el acta bloqueada |
| `DEMO-0005` | **abierto** | **0** |

Abra `DEMO-0003`: se ve la investigación entera, con el SQL de cada paso y lo que salió.
Eso es lo que queda cuando alguien resuelve algo.

Y **enseñe `DEMO-0005`**, el abierto sin un solo paso. Está a propósito: en una bandeja real
siempre hay uno así. Enseñarlo es más creíble que enseñar todo cerrado.

## Que queda constancia

Si preguntan por control:

```sql
SELECT cuando, tipo, quien, que FROM auditoria_unificada ORDER BY cuando DESC LIMIT 20;
```

Cada consulta ejecutada —quién, cuándo, cuánto tardó—, cada entrada y salida, cada cambio
de un tema, y también las lecturas: quién consultó el dato de qué socio.

---

# Antes de exponer

- [ ] `cd backend && node herramientas/prueba-demo.mjs` acaba en **PUNTA A PUNTA**
- [ ] `cd backend && node herramientas/prueba-ejecucion-usuario.mjs` acaba en **SOLO LO SUYO**
- [ ] El frontend responde en http://localhost:3000
- [ ] Entrar como administrador y ver el panel con los indicadores poblados
- [ ] Probar los casos **1, 4 y 7** una vez: son los que más se entienden
- [ ] Entrar una vez como `user` y ejecutar un tema: es la parte que más preguntan
- [ ] Tener este fichero abierto en una pestaña para los valores

**Mongo, MySQL y Oracle de pruebas no están arriba** (Docker está apagado), y el Oracle de
producción no se alcanza sin la red interna. No hace falta para nada de esto: todo lo de
arriba consulta PostgreSQL. Pero si alguien pregunta por Oracle, la respuesta honesta es
que conectarlo es configuración, no desarrollo — y no prometa enseñarlo hoy.

## Para dejarlo como estaba

```bash
cd backend
node herramientas/demo-contenido.mjs --borrar
node herramientas/demo-cooperativa.mjs --borrar
```

Lo primero quita solo lo marcado `[demo]`; lo que haya subido gente de verdad no se toca. Lo
segundo borra la base entera. Después conviene quitar la entrada `demo_cooperativa` de
`backend/config/multidb.databases.json`.
