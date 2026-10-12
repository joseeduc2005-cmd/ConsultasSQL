/**
 * Monta una base de demostración para enseñar el sistema.
 *
 * El sistema se entiende cuando alguien escribe una cédula y le sale el dato. Para eso
 * hace falta una base que consultar, y la de conocimiento no sirve: ahí viven los temas
 * de ayuda, no los socios. Esto crea una base aparte, `demo_cooperativa`, con la forma
 * que tiene de verdad una cooperativa —socios, cuentas, corresponsales, POS y
 * transacciones— y datos inventados pero coherentes entre sí.
 *
 * Coherentes importa: en la demostración alguien va a buscar un socio concreto y luego
 * sus transacciones, y si los números no cuadran la demostración se cae sola.
 *
 * Los datos son inventados. Las cédulas no corresponden a personas reales: se generan
 * con un patrón reconocible (empiezan por 01) y los nombres salen de una lista corta.
 *
 *   node herramientas/demo-cooperativa.mjs            crear y llenar
 *   node herramientas/demo-cooperativa.mjs --borrar   quitar la base entera
 *
 * Después hay que registrarla en config/multidb.databases.json; el script imprime la
 * entrada lista para pegar.
 */
import 'dotenv/config';
import pg from 'pg';
import { readFileSync } from 'node:fs';

const BORRAR = process.argv.includes('--borrar');
const BASE = 'demo_cooperativa';

const config = JSON.parse(readFileSync('config/multidb.databases.json', 'utf8'));
const pg_main = config.databases.find((d) => d.id === 'pg_main');
if (!pg_main) {
  console.error('No hay una base pg_main en config/multidb.databases.json');
  process.exit(1);
}
const conexion = { host: pg_main.host, port: pg_main.port, user: pg_main.user, password: pg_main.password };

// ── La base ───────────────────────────────────────────────────────────────────
const admin = new pg.Pool({ ...conexion, database: 'postgres' });

if (BORRAR) {
  // Se cierran las conexiones ajenas primero: PostgreSQL no deja borrar una base en uso,
  // y el backend la tiene abierta si está corriendo.
  await admin.query(
    `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`,
    [BASE],
  );
  await admin.query(`DROP DATABASE IF EXISTS ${BASE}`);
  console.log(`Base ${BASE} eliminada.`);
  await admin.end();
  process.exit(0);
}

const { rowCount: existe } = await admin.query('SELECT 1 FROM pg_database WHERE datname = $1', [BASE]);
if (!existe) {
  await admin.query(`CREATE DATABASE ${BASE}`);
  console.log(`Base ${BASE} creada.`);
} else {
  console.log(`Base ${BASE} ya existía; se vuelve a llenar.`);
}
await admin.end();

const db = new pg.Pool({ ...conexion, database: BASE });

// ── El esquema ────────────────────────────────────────────────────────────────
//
// Nombres en castellano y con la forma del negocio, no genéricos: lo que se enseña es
// que el sistema entiende de qué habla la gente, y para eso las columnas tienen que
// llamarse como las llama la gente.
await db.query(`
  DROP TABLE IF EXISTS transacciones, cuentas, puntos_atencion, corresponsales, socios CASCADE;

  CREATE TABLE socios (
    id            SERIAL PRIMARY KEY,
    cedula        VARCHAR(10) UNIQUE NOT NULL,
    nombres       VARCHAR(80) NOT NULL,
    apellidos     VARCHAR(80) NOT NULL,
    sucursal      VARCHAR(40) NOT NULL,
    estado        VARCHAR(20) NOT NULL DEFAULT 'activo',
    telefono      VARCHAR(20),
    fecha_ingreso DATE NOT NULL
  );

  CREATE TABLE cuentas (
    id            SERIAL PRIMARY KEY,
    socio_id      INT NOT NULL REFERENCES socios(id),
    numero        VARCHAR(20) UNIQUE NOT NULL,
    tipo          VARCHAR(20) NOT NULL,
    saldo         NUMERIC(12,2) NOT NULL DEFAULT 0,
    estado        VARCHAR(20) NOT NULL DEFAULT 'activa'
  );

  CREATE TABLE corresponsales (
    id              SERIAL PRIMARY KEY,
    identificacion  VARCHAR(13) UNIQUE NOT NULL,
    nombre_comercial VARCHAR(80) NOT NULL,
    sucursal        VARCHAR(40) NOT NULL,
    estado          VARCHAR(20) NOT NULL DEFAULT 'activo'
  );

  CREATE TABLE puntos_atencion (
    id              SERIAL PRIMARY KEY,
    corresponsal_id INT NOT NULL REFERENCES corresponsales(id),
    codigo_pos      VARCHAR(20) UNIQUE NOT NULL,
    mac             VARCHAR(17) NOT NULL,
    estado          VARCHAR(20) NOT NULL DEFAULT 'activo'
  );

  CREATE TABLE transacciones (
    id            SERIAL PRIMARY KEY,
    cuenta_id     INT NOT NULL REFERENCES cuentas(id),
    punto_id      INT REFERENCES puntos_atencion(id),
    tipo          VARCHAR(20) NOT NULL,
    monto         NUMERIC(12,2) NOT NULL,
    estado        VARCHAR(20) NOT NULL,
    descripcion   VARCHAR(120),
    fecha         TIMESTAMP NOT NULL
  );

  CREATE INDEX idx_socios_cedula ON socios (cedula);
  CREATE INDEX idx_cuentas_socio ON cuentas (socio_id);
  CREATE INDEX idx_trx_cuenta ON transacciones (cuenta_id);
  CREATE INDEX idx_trx_fecha ON transacciones (fecha DESC);
`);

// ── Los datos ─────────────────────────────────────────────────────────────────
const NOMBRES = [
  ['María Fernanda', 'Tenesaca Quizhpi'], ['Byron Patricio', 'Quizhpe Loja'],
  ['Rosa Elena', 'Pauta Guamán'], ['José Luis', 'Chuchuca Ordóñez'],
  ['Ana Lucía', 'Morocho Sigcha'], ['Carlos Andrés', 'Peñaloza Vera'],
  ['Mercedes Isabel', 'Yupangui Caiza'], ['Segundo Manuel', 'Lema Tigse'],
  ['Jessica Paola', 'Narváez Bermeo'], ['Luis Alberto', 'Cabrera Zhingre'],
  ['Blanca Rocío', 'Sarmiento Ávila'], ['Edison Vinicio', 'Rivera Montalvo'],
];
const SUCURSALES = ['Matriz Cuenca', 'Azogues', 'Paute', 'Gualaceo', 'Sígsig', 'Cañar'];

// Cédulas con un patrón reconocible: 01 (Azuay) + correlativo. Inventadas a propósito.
const cedulaDe = (i) => '01' + String(10000000 + i * 137).slice(0, 8);

const socios = [];
for (let i = 0; i < NOMBRES.length; i += 1) {
  const [nombres, apellidos] = NOMBRES[i];
  const { rows: [s] } = await db.query(
    `INSERT INTO socios (cedula, nombres, apellidos, sucursal, estado, telefono, fecha_ingreso)
     VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id, cedula`,
    [
      cedulaDe(i), nombres, apellidos,
      SUCURSALES[i % SUCURSALES.length],
      // Un par de casos que no son «activo»: una demostración sin casos raros no enseña nada.
      i === 3 ? 'bloqueado' : i === 7 ? 'inactivo' : 'activo',
      '09' + String(80000000 + i * 971).slice(0, 8),
      new Date(2018 + (i % 7), i % 12, 1 + (i % 27)).toISOString().slice(0, 10),
    ],
  );
  socios.push(s);
}

const cuentas = [];
for (const [i, socio] of socios.entries()) {
  for (const tipo of i % 3 === 0 ? ['ahorros', 'corriente'] : ['ahorros']) {
    const { rows: [c] } = await db.query(
      `INSERT INTO cuentas (socio_id, numero, tipo, saldo, estado)
       VALUES ($1,$2,$3,$4,$5) RETURNING id`,
      [
        socio.id,
        '4' + String(100000000 + socio.id * 7919 + (tipo === 'ahorros' ? 0 : 1)).slice(0, 9),
        tipo,
        (((socio.id * 3137) % 480000) / 100).toFixed(2),
        socio.id === 4 ? 'bloqueada' : 'activa',
      ],
    );
    cuentas.push(c);
  }
}

const CORRESPONSALES = [
  ['0102030405001', 'Tienda Don Byron', 'Paute'],
  ['0190123456001', 'Guel Cyber Café', 'Cuenca'],
  ['0105678901001', 'Despensa La Esperanza', 'Gualaceo'],
  ['0103456789001', 'Bazar Mi Rincón', 'Azogues'],
];
const puntos = [];
for (const [i, [ident, nombre, sucursal]] of CORRESPONSALES.entries()) {
  const { rows: [c] } = await db.query(
    `INSERT INTO corresponsales (identificacion, nombre_comercial, sucursal, estado)
     VALUES ($1,$2,$3,$4) RETURNING id`,
    [ident, nombre, sucursal, i === 1 ? 'inactivo' : 'activo'],
  );
  // Los dos primeros comparten MAC a proposito: es el caso real que investigo el equipo
  // —dos POS con la misma MAC— y es el que mejor enseña para que sirve una consulta.
  const mac = i <= 1 ? '00:1B:44:11:3A:B7' : `00:1B:44:11:3A:${(0xc0 + i).toString(16).toUpperCase()}`;
  const { rows: [p] } = await db.query(
    `INSERT INTO puntos_atencion (corresponsal_id, codigo_pos, mac, estado)
     VALUES ($1,$2,$3,$4) RETURNING id`,
    [c.id, 'POS-' + String(1001 + i), mac, i === 1 ? 'inactivo' : 'activo'],
  );
  puntos.push(p);
}

const TIPOS = ['deposito', 'retiro', 'pago_servicio', 'recaudacion'];
const ESTADOS = ['aprobada', 'aprobada', 'aprobada', 'rechazada'];
let n = 0;
for (const cuenta of cuentas) {
  for (let k = 0; k < 6; k += 1) {
    n += 1;
    await db.query(
      `INSERT INTO transacciones (cuenta_id, punto_id, tipo, monto, estado, descripcion, fecha)
       VALUES ($1,$2,$3,$4,$5,$6,$7)`,
      [
        cuenta.id,
        puntos[n % puntos.length].id,
        TIPOS[n % TIPOS.length],
        (((n * 7717) % 95000) / 100 + 5).toFixed(2),
        ESTADOS[n % ESTADOS.length],
        TIPOS[n % TIPOS.length] === 'recaudacion' ? 'MUNICIPIO GAD MEJIA - agua potable' : null,
        new Date(Date.now() - n * 36e5 * 7).toISOString(),
      ],
    );
  }
}

// ── La forma de los casos reales ───────────────────────────────────
//
// Lo de arriba basta para enseñar que el sistema devuelve un dato. No basta para
// enseñar lo que el equipo hace de verdad, que es investigar. Los casos que ya
// investigó hablan de cosas concretas: un usuario de corresponsal cuya MAC no cuadra
// con la del POS, un reporte que se cae pasadas las 300 transacciones, una recaudación
// cobrada que no generó comprobante, un acta que no se puede corregir porque su pago ya
// está cerrado, la política de clave y PIN, y una sucursal desactualizada que descuadra
// la contabilización.
//
// Estas tablas dan esa forma. Los datos siguen siendo inventados; lo que se copia es la
// estructura del problema, que es lo que hace que una demostración se parezca al trabajo.
await db.query(`
  DROP TABLE IF EXISTS pagos_corresponsal, actas, recaudaciones,
                       usuarios_corresponsal, politica_seguridad, clientes_fisja CASCADE;

  CREATE TABLE usuarios_corresponsal (
    id              SERIAL PRIMARY KEY,
    corresponsal_id INT NOT NULL REFERENCES corresponsales(id),
    cedula          VARCHAR(10) NOT NULL,
    usuario         VARCHAR(40) UNIQUE NOT NULL,
    estado          VARCHAR(20) NOT NULL DEFAULT 'activo',
    mac_registrada  VARCHAR(17),
    intentos_fallidos INT NOT NULL DEFAULT 0,
    ultimo_acceso   TIMESTAMP
  );

  CREATE TABLE politica_seguridad (
    id            SERIAL PRIMARY KEY,
    credencial    VARCHAR(20) UNIQUE NOT NULL,
    longitud_min  INT NOT NULL,
    longitud_max  INT NOT NULL,
    solo_numeros  BOOLEAN NOT NULL DEFAULT false,
    exige_mayuscula BOOLEAN NOT NULL DEFAULT false,
    exige_minuscula BOOLEAN NOT NULL DEFAULT false,
    exige_digito  BOOLEAN NOT NULL DEFAULT false,
    exige_simbolo BOOLEAN NOT NULL DEFAULT false,
    no_repetir_ultimas INT NOT NULL DEFAULT 0,
    prohibe_consecutivos BOOLEAN NOT NULL DEFAULT false,
    prohibe_cedula BOOLEAN NOT NULL DEFAULT false,
    vigente       BOOLEAN NOT NULL DEFAULT true
  );

  CREATE TABLE recaudaciones (
    id           SERIAL PRIMARY KEY,
    punto_id     INT NOT NULL REFERENCES puntos_atencion(id),
    institucion  VARCHAR(60) NOT NULL,
    referencia   VARCHAR(30) NOT NULL,
    monto        NUMERIC(12,2) NOT NULL,
    estado       VARCHAR(20) NOT NULL,
    comprobante  VARCHAR(30),
    fecha        TIMESTAMP NOT NULL
  );

  CREATE TABLE actas (
    id              SERIAL PRIMARY KEY,
    numero          INT UNIQUE NOT NULL,
    corresponsal_id INT NOT NULL REFERENCES corresponsales(id),
    periodo         VARCHAR(7) NOT NULL,
    valor           NUMERIC(12,2) NOT NULL,
    estado          VARCHAR(20) NOT NULL
  );

  CREATE TABLE pagos_corresponsal (
    id          SERIAL PRIMARY KEY,
    numero_acta INT NOT NULL REFERENCES actas(numero),
    identificacion VARCHAR(13) NOT NULL,
    estado      CHAR(1) NOT NULL,
    valor       NUMERIC(12,2) NOT NULL,
    fecha       DATE NOT NULL
  );

  CREATE TABLE clientes_fisja (
    id          SERIAL PRIMARY KEY,
    cedula      VARCHAR(10) UNIQUE NOT NULL,
    nombre      VARCHAR(120) NOT NULL,
    sucursal    VARCHAR(40) NOT NULL,
    actualizado DATE NOT NULL
  );

  CREATE INDEX idx_usr_corr_cedula ON usuarios_corresponsal (cedula);
  CREATE INDEX idx_recaud_ref ON recaudaciones (referencia);
  CREATE INDEX idx_trx_punto_fecha ON transacciones (punto_id, fecha DESC);
`);

// Un quinto corresponsal, con su POS y una MAC propia. Hace falta para el caso del
// usuario cuya MAC registrada no corresponde a ningún equipo: si todos los POS
// compartieran MAC no se distinguiría un problema del otro.
const { rows: [corrAna] } = await db.query(
  `INSERT INTO corresponsales (identificacion, nombre_comercial, sucursal, estado)
   VALUES ('0104567890001', 'Minimarket Doña Ana', 'Sígsig', 'activo') RETURNING id`,
);
const { rows: [posAna] } = await db.query(
  `INSERT INTO puntos_atencion (corresponsal_id, codigo_pos, mac, estado)
   VALUES ($1, 'POS-1005', '00:1B:44:11:3A:D4', 'activo') RETURNING id`,
  [corrAna.id],
);
puntos.push(posAna);

// Los usuarios del portal de corresponsal. Dos están bien, y cada uno de los otros dos
// está roto de una forma distinta —la misma distinción que costó dos días aclarar en el
// caso real: una MAC duplicada no es lo mismo que una MAC que no existe.
const USUARIOS = [
  // corresponsal, socio, usuario, estado, MAC registrada
  [1, 2, 'byron.quizhpe', 'activo', '00:1B:44:11:3A:B7'],   // comparte MAC con un POS inactivo
  [3, 9, 'jessica.narvaez', 'activo', '00:1B:44:11:3A:C2'], // correcto
  [4, 10, 'luis.cabrera', 'activo', '00:1B:44:11:3A:C3'],   // correcto
  [5, 5, 'ana.morocho', 'activo', '00:1B:44:11:3A:E1'],     // MAC que no es de ningún POS
];
for (const [iCorr, iSocio, usuario, estado, mac] of USUARIOS) {
  await db.query(
    `INSERT INTO usuarios_corresponsal
       (corresponsal_id, cedula, usuario, estado, mac_registrada, intentos_fallidos, ultimo_acceso)
     VALUES ($1,$2,$3,$4,$5,$6,$7)`,
    [
      iCorr, socios[iSocio - 1].cedula, usuario, estado, mac,
      usuario === 'ana.morocho' ? 7 : 0,
      usuario === 'ana.morocho' ? null : new Date(Date.now() - 864e5).toISOString(),
    ],
  );
}

// La política de clave y PIN. En el caso real la pregunta era exactamente esta —qué
// formato se exige— y la respuesta estaba en una tabla, no en la cabeza de nadie.
await db.query(`
  INSERT INTO politica_seguridad
    (credencial, longitud_min, longitud_max, solo_numeros, exige_mayuscula, exige_minuscula,
     exige_digito, exige_simbolo, no_repetir_ultimas, prohibe_consecutivos, prohibe_cedula, vigente)
  VALUES
    ('contrasena',  8, 15, false, true,  true,  true,  true,  3, false, false, true),
    ('pin',         4,  4, true,  false, false, true,  false, 1, true,  true,  true)
`);

// Recaudaciones. Una cobrada sin comprobante: el caso del comprobante que no se puede
// reimprimir, que es distinto de una recaudación anulada —y la diferencia importa,
// porque en una hay plata cobrada y en la otra no.
const INSTITUCIONES = [
  'MUNICIPIO GAD MEJIA - agua potable',
  'MUNICIPIO GAD MEJIA - predios',
  'EMPRESA ELÉCTRICA REGIONAL CENTRO SUR',
  'CONSEJO PROVINCIAL DEL AZUAY',
];
for (let i = 0; i < 24; i += 1) {
  const cobrada = i % 7 !== 5;
  // La número 11 está cobrada y no generó comprobante: ese es el caso a encontrar.
  const sinComprobante = i === 11;
  await db.query(
    `INSERT INTO recaudaciones (punto_id, institucion, referencia, monto, estado, comprobante, fecha)
     VALUES ($1,$2,$3,$4,$5,$6,$7)`,
    [
      puntos[i % puntos.length].id,
      INSTITUCIONES[i % INSTITUCIONES.length],
      'REC-' + String(240000 + i * 13),
      (((i * 4391) % 18000) / 100 + 3).toFixed(2),
      cobrada ? 'cobrada' : 'anulada',
      cobrada && !sinComprobante ? 'CMP-' + String(900000 + i * 29) : null,
      new Date(Date.now() - i * 36e5 * 11).toISOString(),
    ],
  );
}

// Actas y pagos. El acta 53656 tiene su pago cerrado, que es lo que impide corregirla.
// Y ese caso enseña lo que más cuesta explicar: el sistema puede decir por qué está
// bloqueada, pero desbloquearla no es cosa suya.
const ACTAS = [
  [53655, 1, '2026-06', 'cerrada'],
  [53656, 1, '2026-07', 'cerrada'],
  [53657, 1, '2026-08', 'abierta'],
  [53701, 3, '2026-07', 'cerrada'],
  [53702, 4, '2026-07', 'abierta'],
];
for (const [numero, iCorr, periodo, estado] of ACTAS) {
  const valor = (((numero * 311) % 42000) / 100 + 12).toFixed(2);
  await db.query(
    'INSERT INTO actas (numero, corresponsal_id, periodo, valor, estado) VALUES ($1,$2,$3,$4,$5)',
    [numero, iCorr, periodo, valor, estado],
  );
  if (estado === 'cerrada') {
    const { rows: [corr] } = await db.query('SELECT identificacion FROM corresponsales WHERE id = $1', [iCorr]);
    await db.query(
      `INSERT INTO pagos_corresponsal (numero_acta, identificacion, estado, valor, fecha)
       VALUES ($1,$2,'C',$3,$4)`,
      [numero, corr.identificacion, valor, periodo + '-28'],
    );
  }
}

// La copia desactualizada. Dos socios cambiaron de oficina y aquí sigue la anterior:
// de ahí sale el descuadre, porque la contabilización lee de esta tabla y el reporte de
// la otra. Es el caso que no se ve mirando una sola tabla.
for (const [i, socio] of socios.entries()) {
  const { rows: [s] } = await db.query(
    'SELECT nombres, apellidos, sucursal FROM socios WHERE id = $1',
    [socio.id],
  );
  const desactualizado = i === 0 || i === 5;
  await db.query(
    `INSERT INTO clientes_fisja (cedula, nombre, sucursal, actualizado)
     VALUES ($1,$2,$3,$4)`,
    [
      socio.cedula,
      s.nombres + ' ' + s.apellidos,
      desactualizado ? 'Yanuncay' : s.sucursal,
      desactualizado ? '2021-03-15' : new Date().toISOString().slice(0, 10),
    ],
  );
}

// Y volumen de verdad en un POS: 420 transacciones en el último mes. Sin esto el caso
// del límite de 300 no se puede enseñar, porque no hay con qué pasarse del límite.
const cuentaDeVolumen = cuentas[0].id;
const valores = [];
for (let k = 0; k < 420; k += 1) {
  valores.push(`(${cuentaDeVolumen}, ${puntos[0].id}, '${TIPOS[k % TIPOS.length]}', `
    + `${(((k * 2311) % 64000) / 100 + 2).toFixed(2)}, 'aprobada', NULL, `
    + `NOW() - INTERVAL '${(k * 100) % 43200} minutes')`);
}
await db.query(
  'INSERT INTO transacciones (cuenta_id, punto_id, tipo, monto, estado, descripcion, fecha) VALUES '
  + valores.join(','),
);

// ── Lo que quedó ──────────────────────────────────────────────────────────────
const cuenta = async (t) => (await db.query(`SELECT count(*)::int n FROM ${t}`)).rows[0].n;
console.log('\nDatos cargados:');
for (const t of ['socios', 'cuentas', 'corresponsales', 'puntos_atencion', 'transacciones',
  'usuarios_corresponsal', 'politica_seguridad', 'recaudaciones', 'actas',
  'pagos_corresponsal', 'clientes_fisja']) {
  console.log('  ' + t.padEnd(24) + (await cuenta(t)));
}

const { rows: ejemplo } = await db.query(
  'SELECT cedula, nombres, apellidos, sucursal, estado FROM socios ORDER BY id LIMIT 3',
);
console.log('\nCédulas para la demostración:');
for (const s of ejemplo) {
  console.log(`  ${s.cedula}  ${s.nombres} ${s.apellidos} · ${s.sucursal} · ${s.estado}`);
}

const { rows: [dup] } = await db.query(`
  SELECT mac, count(*)::int n FROM puntos_atencion GROUP BY mac HAVING count(*) > 1 LIMIT 1
`);
if (dup) console.log(`\nCaso preparado: la MAC ${dup.mac} está en ${dup.n} POS distintos.`);

console.log('\n── Entrada para config/multidb.databases.json ──');
console.log(JSON.stringify({
  id: 'demo_cooperativa',
  type: 'postgres',
  label: 'Cooperativa (demostración)',
  host: pg_main.host,
  port: pg_main.port,
  database: BASE,
  user: pg_main.user,
  password: '«la misma de pg_main»',
  enabled: true,
}, null, 2));

await db.end();
