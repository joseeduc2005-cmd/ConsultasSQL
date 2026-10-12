/**
 * Monta en Oracle una base con la forma de la mesa de servicios.
 *
 * Sirve para una cosa concreta: comprobar que el sistema aguanta scripts complicados
 * contra Oracle, que es donde vive la base de la cooperativa y donde el SQL se pone
 * serio —jerarquías con CONNECT BY, PIVOT, LISTAGG, analíticas, REGEXP_SUBSTR—. Contra
 * tres tablas de juguete no se prueba nada de eso.
 *
 * La forma sale del export de GLPI: categorías de tres y cuatro niveles separadas por
 * «>», grupos de técnicos con su jerarquía, entidades, ubicaciones, tiempos de espera y
 * de resolución, escalamientos, y el complemento de socio que solo traen algunos.
 *
 * El catálogo —los nombres de categorías, grupos, entidades y ubicaciones— se lee del
 * export, porque es la taxonomía de la organización y es lo que hace que las consultas
 * se parezcan a las de verdad.
 *
 * Las personas no. Ni los solicitantes, ni los técnicos, ni las cédulas, ni los
 * teléfonos: eso se inventa. Un export de tickets lleva datos de socios reales y no
 * tienen por qué acabar en una base de pruebas que se enseña en una sala.
 *
 *   node herramientas/demo-oracle-mesa.mjs "C:/ruta/al/glpi.csv"
 *   node herramientas/demo-oracle-mesa.mjs --borrar
 */
import oracledb from 'oracledb';
import { readFileSync } from 'node:fs';

const BORRAR = process.argv.includes('--borrar');
const RUTA_CSV = process.argv.find((a) => a.toLowerCase().endsWith('.csv'));

const cfg = JSON.parse(readFileSync('config/multidb.databases.json', 'utf8'));
const o = cfg.databases.find((d) => d.id === 'oracle_local');
if (!o) { console.error('No hay una base oracle_local en config/multidb.databases.json'); process.exit(1); }

const conexion = {
  user: o.user,
  password: o.password,
  connectString: `${o.host}:${o.port}/${o.database}`,
};

// ── Leer el catálogo del export ───────────────────────────────────────────────
//
// Un CSV con comillas dobles y punto y coma, y descripciones con saltos de línea
// dentro. Partir por líneas no sirve: hay que recorrerlo carácter a carácter.
function leerCsv(texto) {
  const filas = [];
  let fila = [];
  let campo = '';
  let enComillas = false;
  for (let i = 0; i < texto.length; i += 1) {
    const c = texto[i];
    if (enComillas) {
      if (c === '"' && texto[i + 1] === '"') { campo += '"'; i += 1; continue; }
      if (c === '"') { enComillas = false; continue; }
      campo += c;
      continue;
    }
    if (c === '"') { enComillas = true; continue; }
    if (c === ';') { fila.push(campo); campo = ''; continue; }
    if (c === '\n') { fila.push(campo); filas.push(fila); fila = []; campo = ''; continue; }
    if (c === '\r') continue;
    campo += c;
  }
  if (campo || fila.length) { fila.push(campo); filas.push(fila); }
  const cabecera = filas.shift().map((x) => x.replace(/^\ufeff/, '').trim());
  return filas
    .filter((f) => f.length > 1)
    .map((f) => Object.fromEntries(cabecera.map((c, i) => [c, (f[i] ?? '').trim()])));
}

/** Lo que se usa del export: la taxonomía, no las personas. */
function catalogoDe(filas) {
  const distintos = (col) => [...new Set(filas.map((f) => f[col]).filter(Boolean))];
  return {
    categorias: distintos('Categoría'),
    grupos: distintos('Asignado a - Grupo de técnicos'),
    entidades: distintos('Entidad'),
    ubicaciones: distintos('Ubicación'),
    tiposSolucion: distintos('Solución - Tipo de solución').filter((x) => !x.includes('<br>')),
    cuantos: filas.length,
  };
}

/** Si no hay export a mano, un catálogo corto con la misma forma. */
const CATALOGO_MINIMO = {
  categorias: [
    'CAPTACIONES > Ahorro a la vista > Retiros',
    'CAPTACIONES > Ahorro a la vista > Depositos',
    'CAPTACIONES > Grupos Organizados > Asignacion SPI',
    'SERVICIOS VIRTUALES > ATM > Conducta de Mercado > Deposito ATM',
    'SERVICIOS VIRTUALES > CashDro > Conducta de Mercado > Retiro Cashdro',
    'SEGURIDAD FISICA > Monitoreo > ATM',
    'SEGURIDAD FISICA > Alarmas > Falla tecnica',
    'GESTION DE USUARIOS > Gestion de usuarios > Otorgar Acceso',
    'SERVICIOS INTERNOS > SRJ > Providencias Judiciales - Levantamiento Coactiva',
    'PERSONAS > Atencion > Certificaciones - Unidades Judiciales',
  ],
  grupos: [
    'Innovación y Desarrollo > Mesa de Servicios > Soporte Operativo',
    'Administración Financiera > Seguridad Física > Monitoreo',
    'Servicio Cooperativo > Gestion de Reclamos',
    'Innovación y Desarrollo > Mesa de Servicios > Mesa de Servicios Tecnológica',
    'Administración Financiera > Dirección Jurídica',
  ],
  entidades: [
    'Jardín Azuayo > Mesa de Servicios > Operativo',
    'Jardín Azuayo > Mesa de Servicios > Seguridad Física',
    'Jardín Azuayo > Mesa de Servicios > Reclamos',
    'Jardín Azuayo > Mesa de Servicios > Tecnología',
  ],
  ubicaciones: [
    'Administrativa Financiera > Financiera > Tesorería',
    'Administrativa Financiera > Seguridad Física > Monitoreo',
    'Zonal Cuenca > Matriz',
    'Zonal Azogues > Agencia Azogues',
    'Zonal Cuenca > Agencia Gualaceo',
  ],
  tiposSolucion: ['Resuelto (no escalado)', 'Favorable', 'Resuelto (escalado)', 'No favorable', 'No aplica'],
  cuantos: 488,
};

// ── Personas inventadas ───────────────────────────────────────────────────────
const NOMBRES = ['Lucía', 'Marco', 'Daniela', 'Fabián', 'Verónica', 'Esteban', 'Paola', 'Andrés', 'Gabriela', 'Nicolás'];
const APELLIDOS = ['Quizhpe', 'Tenesaca', 'Pauta', 'Chuchuca', 'Morocho', 'Peñaloza', 'Yupangui', 'Lema', 'Narváez', 'Cabrera'];
const persona = (n) => `${NOMBRES[n % NOMBRES.length]} ${APELLIDOS[(n * 7) % APELLIDOS.length]}`;
const cedulaInventada = (n) => '01' + String(20000000 + n * 313).slice(0, 8);

const db = await oracledb.getConnection(conexion);
db.autoCommit = false;

const silencioso = async (sql) => { try { await db.execute(sql); } catch { /* no existía */ } };

if (BORRAR) {
  for (const t of ['tareas', 'ticket_socio', 'tickets', 'categorias', 'grupos', 'entidades', 'ubicaciones']) {
    await silencioso(`DROP TABLE ${t} CASCADE CONSTRAINTS`);
  }
  await db.commit();
  console.log('Tablas de la mesa de servicios eliminadas.');
  await db.close();
  process.exit(0);
}

const catalogo = RUTA_CSV
  ? catalogoDe(leerCsv(readFileSync(RUTA_CSV, 'utf8')))
  : CATALOGO_MINIMO;
console.log(RUTA_CSV
  ? `Catálogo leído del export: ${catalogo.categorias.length} categorías, ${catalogo.grupos.length} grupos, ${catalogo.ubicaciones.length} ubicaciones.`
  : 'Sin export: se usa el catálogo mínimo.');

// ── El esquema ────────────────────────────────────────────────────────────────
for (const t of ['tareas', 'ticket_socio', 'tickets', 'categorias', 'grupos', 'entidades', 'ubicaciones']) {
  await silencioso(`DROP TABLE ${t} CASCADE CONSTRAINTS`);
}

const DDL = [
  `CREATE TABLE entidades (
     id        NUMBER PRIMARY KEY,
     nombre    VARCHAR2(200) NOT NULL
   )`,
  `CREATE TABLE ubicaciones (
     id        NUMBER PRIMARY KEY,
     nombre    VARCHAR2(200) NOT NULL
   )`,
  // El grupo lleva su padre: así se puede recorrer con CONNECT BY, que es la forma
  // natural de preguntar «y todo lo que cuelga de Mesa de Servicios» en Oracle.
  `CREATE TABLE grupos (
     id        NUMBER PRIMARY KEY,
     nombre    VARCHAR2(200) NOT NULL,
     padre_id  NUMBER REFERENCES grupos(id),
     nivel     NUMBER NOT NULL
   )`,
  // La ruta entera y además partida: la ruta para las consultas con REGEXP_SUBSTR, los
  // niveles para las que agrupan. Las dos formas aparecen en el trabajo real.
  `CREATE TABLE categorias (
     id        NUMBER PRIMARY KEY,
     ruta      VARCHAR2(400) NOT NULL,
     nivel1    VARCHAR2(120),
     nivel2    VARCHAR2(120),
     nivel3    VARCHAR2(160),
     nivel4    VARCHAR2(160),
     hojas     NUMBER NOT NULL
   )`,
  `CREATE TABLE tickets (
     id              NUMBER PRIMARY KEY,
     titulo          VARCHAR2(300) NOT NULL,
     entidad_id      NUMBER REFERENCES entidades(id),
     categoria_id    NUMBER REFERENCES categorias(id),
     ubicacion_id    NUMBER REFERENCES ubicaciones(id),
     grupo_id        NUMBER REFERENCES grupos(id),
     tipo            VARCHAR2(20)  NOT NULL,
     prioridad       VARCHAR2(20)  NOT NULL,
     estado          VARCHAR2(20)  NOT NULL,
     impacto         VARCHAR2(20),
     urgencia        VARCHAR2(20),
     solicitante     VARCHAR2(120),
     tecnico         VARCHAR2(120),
     abierto_en      DATE NOT NULL,
     asignado_en     DATE,
     resuelto_en     DATE,
     cerrado_en      DATE,
     espera_seg      NUMBER DEFAULT 0 NOT NULL,
     resolucion_min  NUMBER,
     tipo_solucion   VARCHAR2(60),
     excedida        CHAR(1) DEFAULT 'N' NOT NULL,
     escalamientos   NUMBER DEFAULT 1 NOT NULL
   )`,
  `CREATE TABLE ticket_socio (
     ticket_id     NUMBER PRIMARY KEY REFERENCES tickets(id),
     cedula        VARCHAR2(10) NOT NULL,
     telefono      VARCHAR2(20),
     valor         NUMBER(12,2),
     ws_consultado CHAR(1) DEFAULT 'N' NOT NULL
   )`,
  `CREATE TABLE tareas (
     id          NUMBER PRIMARY KEY,
     ticket_id   NUMBER REFERENCES tickets(id),
     descripcion VARCHAR2(400),
     minutos     NUMBER
   )`,
  'CREATE INDEX idx_tk_abierto ON tickets (abierto_en)',
  'CREATE INDEX idx_tk_cat ON tickets (categoria_id)',
  'CREATE INDEX idx_tk_grupo ON tickets (grupo_id)',
];
for (const sql of DDL) await db.execute(sql);

// ── Los datos ─────────────────────────────────────────────────────────────────
const ins = (sql, binds) => db.executeMany(sql, binds, { autoCommit: false });

const entidades = catalogo.entidades.map((nombre, i) => ({ id: i + 1, nombre }));
await ins('INSERT INTO entidades (id, nombre) VALUES (:id, :nombre)', entidades);

const ubicaciones = catalogo.ubicaciones.map((nombre, i) => ({ id: i + 1, nombre }));
await ins('INSERT INTO ubicaciones (id, nombre) VALUES (:id, :nombre)', ubicaciones);

// Los grupos, desarmados en su jerarquía: «A > B > C» son tres filas encadenadas, y así
// un CONNECT BY puede bajar por el árbol.
const grupoId = new Map();
const filasGrupo = [];
for (const ruta of catalogo.grupos) {
  const partes = ruta.split('>').map((p) => p.trim()).filter(Boolean);
  let padre = null;
  for (const [i, parte] of partes.entries()) {
    const clave = partes.slice(0, i + 1).join(' > ');
    if (!grupoId.has(clave)) {
      const id = grupoId.size + 1;
      grupoId.set(clave, id);
      filasGrupo.push({ id, nombre: parte, padre_id: padre, nivel: i + 1 });
    }
    padre = grupoId.get(clave);
  }
}
await ins('INSERT INTO grupos (id, nombre, padre_id, nivel) VALUES (:id, :nombre, :padre_id, :nivel)', filasGrupo);

const categorias = catalogo.categorias.map((ruta, i) => {
  const p = ruta.split('>').map((x) => x.trim());
  return {
    id: i + 1, ruta,
    nivel1: p[0] ?? null, nivel2: p[1] ?? null, nivel3: p[2] ?? null, nivel4: p[3] ?? null,
    hojas: p.length,
  };
});
await ins(
  `INSERT INTO categorias (id, ruta, nivel1, nivel2, nivel3, nivel4, hojas)
   VALUES (:id, :ruta, :nivel1, :nivel2, :nivel3, :nivel4, :hojas)`,
  categorias,
);

// Los grupos hoja son a los que se asigna un ticket: asignar a un nodo intermedio no
// pasa en la realidad y estropearia las consultas jerarquicas.
const hojas = filasGrupo.filter((g) => !filasGrupo.some((h) => h.padre_id === g.id));

const TIPOS = ['Solicitud', 'Incidencia'];
const PRIORIDADES = ['Media', 'Media', 'Media', 'Media', 'Media', 'Media', 'Media', 'Media', 'Alta'];
const NIVELES = ['Bajo', 'Mediano', 'Alto'];
const ESCALAMIENTOS = [1, 1, 1, 1, 1, 1, 1, 1, 2, 3, 3, 5];

const hoy = new Date();
const tickets = [];
const socios = [];
const tareas = [];
for (let i = 0; i < catalogo.cuantos; i += 1) {
  const id = 2026100000 + i * 7;
  const abierto = new Date(hoy.getTime() - ((i * 211) % 240) * 36e5 - (i % 60) * 6e4);
  const esperaSeg = i % 5 === 0 ? (i * 37) % 7200 : 0;
  const resolucionMin = 15 + ((i * 97) % 2600);
  const asignado = new Date(abierto.getTime() + esperaSeg * 1000);
  const resuelto = new Date(asignado.getTime() + resolucionMin * 6e4);
  const cerrado = new Date(resuelto.getTime() + ((i * 53) % 1440) * 6e4);

  tickets.push({
    id,
    titulo: (categorias[i % categorias.length].nivel3 || categorias[i % categorias.length].nivel2 || 'Caso')
      .toUpperCase().slice(0, 60) + ' - ' + (1000 + i),
    entidad_id: entidades[i % entidades.length].id,
    categoria_id: categorias[i % categorias.length].id,
    ubicacion_id: ubicaciones[i % ubicaciones.length].id,
    grupo_id: hojas[i % hojas.length].id,
    tipo: TIPOS[i % TIPOS.length],
    prioridad: PRIORIDADES[i % PRIORIDADES.length],
    estado: 'Resuelto',
    impacto: NIVELES[i % NIVELES.length],
    urgencia: NIVELES[(i + 1) % NIVELES.length],
    solicitante: persona(i),
    tecnico: persona(i * 3 + 1),
    abierto_en: abierto,
    asignado_en: asignado,
    resuelto_en: resuelto,
    cerrado_en: cerrado,
    espera_seg: esperaSeg,
    resolucion_min: resolucionMin,
    tipo_solucion: catalogo.tiposSolucion[i % catalogo.tiposSolucion.length],
    excedida: resolucionMin > 1440 ? 'S' : 'N',
    escalamientos: ESCALAMIENTOS[i % ESCALAMIENTOS.length],
  });

  // Solo uno de cada cinco trae socio, como en el export: eso es lo que hace que un
  // LEFT JOIN y un COUNT se comporten distinto, que es parte de lo que hay que probar.
  if (i % 5 === 0) {
    socios.push({
      ticket_id: id,
      cedula: cedulaInventada(i),
      telefono: '09' + String(70000000 + i * 911).slice(0, 8),
      valor: Number((((i * 7919) % 480000) / 100).toFixed(2)),
      ws_consultado: i % 15 === 0 ? 'S' : 'N',
    });
  }
  if (i % 4 === 0) {
    tareas.push({
      id: tareas.length + 1,
      ticket_id: id,
      descripcion: 'Revisión y verificación del caso en el sistema.',
      minutos: 10 + ((i * 13) % 180),
    });
  }
}

await ins(
  `INSERT INTO tickets (id, titulo, entidad_id, categoria_id, ubicacion_id, grupo_id, tipo,
     prioridad, estado, impacto, urgencia, solicitante, tecnico, abierto_en, asignado_en,
     resuelto_en, cerrado_en, espera_seg, resolucion_min, tipo_solucion, excedida, escalamientos)
   VALUES (:id, :titulo, :entidad_id, :categoria_id, :ubicacion_id, :grupo_id, :tipo,
     :prioridad, :estado, :impacto, :urgencia, :solicitante, :tecnico, :abierto_en, :asignado_en,
     :resuelto_en, :cerrado_en, :espera_seg, :resolucion_min, :tipo_solucion, :excedida, :escalamientos)`,
  tickets,
);
await ins(
  `INSERT INTO ticket_socio (ticket_id, cedula, telefono, valor, ws_consultado)
   VALUES (:ticket_id, :cedula, :telefono, :valor, :ws_consultado)`,
  socios,
);
await ins(
  'INSERT INTO tareas (id, ticket_id, descripcion, minutos) VALUES (:id, :ticket_id, :descripcion, :minutos)',
  tareas,
);
await db.commit();

console.log('\nCargado en Oracle:');
for (const t of ['entidades', 'ubicaciones', 'grupos', 'categorias', 'tickets', 'ticket_socio', 'tareas']) {
  const r = await db.execute(`SELECT count(*) AS n FROM ${t}`, [], { outFormat: oracledb.OUT_FORMAT_OBJECT });
  console.log('  ' + t.padEnd(16) + r.rows[0].N);
}
await db.close();
console.log('\nLos nombres de categorías, grupos y entidades salen del export.');
console.log('Las personas, cédulas y teléfonos son inventados: un export real no va a una base de pruebas.');
