# Guion para la demostración

Lo que hay que conseguir que entiendan en diez minutos: **el sistema guarda lo que el
equipo ya sabe, y lo devuelve cuando hace falta — incluido el dato concreto.**

No hace falta explicar la arquitectura. Hace falta que vean el recorrido.

---

## Antes de empezar

```bash
cd backend
node herramientas/demo-cooperativa.mjs
node herramientas/demo-contenido.mjs
npm run dev
```

Y el frontend en otra terminal. Para dejarlo como estaba:

```bash
node herramientas/demo-contenido.mjs --borrar
node herramientas/demo-cooperativa.mjs --borrar
```

### Lo que queda montado

| | |
|---|---|
| Base `demo_cooperativa` | 12 socios, 16 cuentas, 5 corresponsales, 5 POS, 516 transacciones, 24 recaudaciones, 5 actas |
| 7 temas de ayuda dinámicos | consultan esa base y piden el dato que les falta |
| 14 temas de lectura | los que ya había, escritos de los casos reales |
| 1 aporte pendiente | de `maria.tenesaca`, esperando revisión |
| 5 tickets | dos resueltos con su causa raíz, dos en investigación y uno abierto sin pasos |

Para comprobar que todo funciona antes de exponer —busca, pide el dato, ejecuta y verifica
los diez casos, sin tocar nada:

```bash
node herramientas/prueba-demo.mjs
```

**Los casos uno por uno, con qué escribir y qué tiene que salir, están en
[CASOS-DEMO.md](CASOS-DEMO.md).** Este fichero es el recorrido; ese es el guión de mano.

### Cédulas que puede escribir

| Cédula | Quién | Para enseñar |
|---|---|---|
| `0110000000` | María Fernanda Tenesaca · Matriz Cuenca | Un socio normal, 2 cuentas |
| `0110000274` | Rosa Elena Pauta · Paute | Un socio normal, 1 cuenta |
| `0110000411` | José Luis Chuchuca · Gualaceo | **Bloqueado, y sus cuentas también** |

La tercera es la buena: enseña que el sistema no solo trae datos, trae **la respuesta
a por qué algo no funciona**.

---

## El guion

### 1. «Tengo un problema» — 2 minutos

Entre como un usuario normal y escriba, con sus palabras:

> el tendero entra y ve los datos de otro negocio

El sistema devuelve el tema de las MAC repetidas **en primer lugar**, sin que nadie
haya escrito esas palabras en el título.

**Lo que hay que decir:** no es un buscador de palabras. Entiende de qué se habla
porque aprende de lo que el equipo ha ido subiendo.

### 2. «Y me da el dato» — 3 minutos

Abra ese tema y ejecútelo. Salen los dos POS que comparten MAC, con el nombre de los
dos locales.

Ahora abra **Consultar un socio por su cédula** y escriba `0110000411`.

> José Luis Chuchuca Ordóñez · Gualaceo · **bloqueado**

Y luego **Ver las cuentas y el saldo**, con la misma cédula: las dos cuentas salen
**bloqueadas**.

**Lo que hay que decir:** el que atiende no necesita saber SQL ni pedirle nada a
sistemas. Escribe la cédula y tiene la respuesta.

**Y lo importante:** el sistema **no puede modificar nada**. Solo consulta. Por eso se
puede dejar en manos de cualquiera sin miedo — no hay forma de que alguien borre o
cambie un dato desde aquí, aunque quiera.

### 3. «Lo que yo resolví también sirve» — 3 minutos

Entre a **Aportes**. Hay uno esperando, de maria.tenesaca: un socio que no podía
retirar. Está contado **por pasos**, y uno de ellos trae la consulta con la que lo
comprobó.

Acéptelo. Se convierte en un tema de ayuda, con su nombre.

Vuelva a buscar: ya sale.

**Lo que hay que decir:** esto es lo que hace que la base crezca sola. Nadie tiene que
sentarse a documentar: se sube lo que se resolvió, en el momento en que se resolvió.

### 4. «Y queda constancia» — 2 minutos

Si preguntan por control, enséñelo:

- Cada consulta ejecutada queda registrada: quién, cuándo, cuánto tardó
- Cada entrada y salida del sistema
- Cada cambio de un tema de ayuda
- También las lecturas — quién consultó el dato de qué socio

```sql
SELECT cuando, tipo, quien, que FROM auditoria_unificada ORDER BY cuando DESC LIMIT 20;
```

---

## Si preguntan

**«¿Puede dañar la base?»**
No. El sistema no ejecuta UPDATE, DELETE, INSERT ni nada que escriba, contra ninguna
base. Está bloqueado en el código y no hay una opción para activarlo.

**«¿Y si alguien escribe un SQL malo?»**
Se rechaza al guardarlo, no al ejecutarlo. Quien escriba un artículo con un UPDATE
recibe el motivo en ese momento.

**«¿Sirve para otra base, no solo esta?»**
Sí. Ahora mismo hay varias conectadas y el sistema descubrió sus tablas solo. La base
de la demostración se añadió al fichero de configuración y apareció sola.

**«¿Quién puede ver qué?»**
Un tema puede ser para todos o solo para administradores. El usuario normal no ve los
restringidos, y no hay forma de saltárselo desde el navegador.

**«¿Cuántas veces acierta?»**
Medido sobre 1.185 preguntas, con erratas, sin tildes y con ruido: **84,9% al primer
resultado y 90,7% entre los tres primeros.** Se puede volver a medir con
`node herramientas/prueba-busqueda.mjs`.

---

## Lo que conviene NO prometer

Dígalo si sale, pero no lo ofrezca:

- **No corrige nada.** Dice qué pasa y cómo se arregló antes; arreglarlo lo hace una
  persona.
- **La calidad depende de lo que se suba.** Con diez temas responde a diez cosas. Lo
  que lo hace valioso es el uso, no la instalación.
- **Oracle no está conectado** en el ambiente de pruebas. Lo que se ve consulta
  PostgreSQL; conectar Oracle es configuración, no desarrollo.
