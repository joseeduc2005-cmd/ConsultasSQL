/**
 * Crea el contenido con el que se enseña el sistema.
 *
 * Tres artículos dinámicos que consultan la base de demostración, un aporte esperando
 * revisión y un ticket con sus pasos. Juntos cuentan el recorrido entero: alguien
 * pregunta, el sistema responde con un tema de ayuda, el tema pide un dato y devuelve
 * la respuesta; y por el otro lado, alguien que resolvió algo lo sube y soporte lo
 * convierte en un tema nuevo.
 *
 * Necesita que exista la base de demostración:
 *   node herramientas/demo-cooperativa.mjs
 *   node herramientas/demo-contenido.mjs
 *   node herramientas/demo-contenido.mjs --borrar
 *
 * Todo lo que crea lleva la marca [demo] en el título, y --borrar quita exactamente
 * eso: lo que haya subido gente de verdad no se toca.
 */
import 'dotenv/config';
import pg from 'pg';
import { readFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';

const BORRAR = process.argv.includes('--borrar');
const MARCA = '[demo]';
const BASE_DEMO = 'demo_cooperativa';

const config = JSON.parse(readFileSync('config/multidb.databases.json', 'utf8'));
const pg_main = config.databases.find((d) => d.id === 'pg_main');
const pool = new pg.Pool({
  host: pg_main.host, port: pg_main.port, database: pg_main.database,
  user: pg_main.user, password: pg_main.password,
});

if (!config.databases.some((d) => d.id === BASE_DEMO)) {
  console.error(`Falta la base ${BASE_DEMO} en config/multidb.databases.json.`);
  console.error('Primero: node herramientas/demo-cooperativa.mjs');
  await pool.end();
  process.exit(1);
}

// ── Borrar ────────────────────────────────────────────────────────────────────
if (BORRAR) {
  const { rows: temas } = await pool.query('SELECT id FROM knowledge_base WHERE titulo LIKE $1', [`${MARCA}%`]);
  for (const t of temas) {
    await pool.query('DELETE FROM article_executions WHERE article_id = $1', [String(t.id)]);
    await pool.query('DELETE FROM article_changes WHERE article_id = $1', [String(t.id)]);
  }
  const a = await pool.query('DELETE FROM knowledge_base WHERE titulo LIKE $1', [`${MARCA}%`]);
  const b = await pool.query('DELETE FROM soporte.aportes WHERE titulo LIKE $1', [`${MARCA}%`]);
  await pool.query('DELETE FROM soporte.ticket_pasos WHERE ticket IN (SELECT numero FROM soporte.tickets WHERE titulo LIKE $1)', [`${MARCA}%`]);
  const c = await pool.query('DELETE FROM soporte.tickets WHERE titulo LIKE $1', [`${MARCA}%`]);
  console.log(`Quitado: ${a.rowCount} tema(s), ${b.rowCount} aporte(s), ${c.rowCount} ticket(s).`);
  await pool.end();
  process.exit(0);
}

const { rows: [admin] } = await pool.query("SELECT id, username FROM users WHERE role = 'admin' LIMIT 1");
if (!admin) { console.error('No hay ningún usuario admin.'); await pool.end(); process.exit(1); }

// ── Los artículos dinámicos ───────────────────────────────────────────────────
//
// Cada uno pide lo que le falta y consulta. Nada escribe: el sistema no lo permite, y
// eso es parte de lo que hay que enseñar — se puede dejar que cualquiera los use.
const ARTICULOS = [
  {
    titulo: `${MARCA} Consultar un socio por su cédula`,
    categoria: 'Socios',
    descripcion: 'Devuelve los datos del socio y el estado en que está.',
    contenido: 'Cuando alguien llama preguntando por su cuenta, lo primero es ver si el socio existe '
      + 'y en qué estado está. Escriba la cédula y el sistema trae el resto.',
    sql: `SELECT s.cedula,
       s.nombres || ' ' || s.apellidos AS socio,
       s.sucursal,
       s.estado,
       s.telefono,
       s.fecha_ingreso
  FROM socios s
 WHERE s.cedula = :cedula`,
  },
  {
    titulo: `${MARCA} Ver las cuentas y el saldo de un socio`,
    categoria: 'Socios',
    descripcion: 'Las cuentas de un socio, su tipo, su saldo y si están activas.',
    contenido: 'Para saber por qué un socio no puede retirar: a veces la cuenta está bloqueada y '
      + 'no es un problema del canal. Escriba la cédula.',
    sql: `SELECT c.numero,
       c.tipo,
       c.saldo,
       c.estado,
       s.nombres || ' ' || s.apellidos AS socio
  FROM cuentas c
  JOIN socios s ON s.id = c.socio_id
 WHERE s.cedula = :cedula
 ORDER BY c.tipo`,
  },
  {
    titulo: `${MARCA} Buscar POS que comparten la misma MAC`,
    categoria: 'Corresponsales',
    descripcion: 'Dos puntos de atención con la misma MAC: la causa de que un tendero entre con el usuario de otro.',
    contenido: 'Cuando un corresponsal reporta que entra y ve los datos de otro negocio, lo primero '
      + 'que hay que descartar es una MAC repetida entre dos POS. Esta consulta no pide nada: '
      + 'las lista todas.',
    sql: `SELECT p.mac,
       count(*)::int AS puntos,
       string_agg(c.nombre_comercial || ' (' || p.codigo_pos || ')', ' · ') AS donde
  FROM puntos_atencion p
  JOIN corresponsales c ON c.id = p.corresponsal_id
 GROUP BY p.mac
HAVING count(*) > 1`,
  },
];

for (const a of ARTICULOS) {
  const id = randomUUID();
  await pool.query(
    `INSERT INTO knowledge_base
       (id, titulo, categoria, descripcion, contenido, contenido_md, tipo_solucion,
        script_json, creado_por, creado_por_username, visible_para)
     VALUES ($1,$2,$3,$4,$5,$5,'dinamico',$6,$7,$8,'todos')`,
    [
      id, a.titulo, a.categoria, a.descripcion, a.contenido,
      JSON.stringify({
        modo: 'script', origen: 'manual-sql', sql: a.sql,
        target_database: BASE_DEMO, databaseId: BASE_DEMO,
      }),
      admin.id, admin.username,
    ],
  );
  console.log(`  tema dinámico · ${a.titulo}`);
}

// ── Un aporte esperando revisión ──────────────────────────────────────────────
//
// Es la otra mitad del sistema: alguien que resolvió algo lo sube, y soporte decide si
// se convierte en tema de ayuda. Sin uno pendiente, esa pantalla se ve vacía y no se
// entiende para qué está.
const pasos = [
  { descripcion: 'El socio llamó diciendo que no podía retirar en el corresponsal de Paute.', script: null },
  {
    descripcion: 'Revisé si el socio existía y en qué estado estaba.',
    script: "SELECT cedula, nombres, apellidos, estado FROM socios WHERE cedula = '0110000274'",
    efecto: 'consulta',
  },
  {
    descripcion: 'El socio estaba activo, así que miré la cuenta: estaba bloqueada. Eso era.',
    script: 'SELECT numero, tipo, saldo, estado FROM cuentas WHERE socio_id = 3',
    efecto: 'consulta',
  },
];
await pool.query(
  `INSERT INTO soporte.aportes (autor, titulo, problema, pasos, sql_sugerido, base, estado, pasos_json, creado_en)
   VALUES ($1,$2,$3,$4,$5,$6,'pendiente',$7, now())`,
  [
    'maria.tenesaca',
    `${MARCA} El socio no puede retirar y el corresponsal funciona bien`,
    'Un socio no lograba retirar en el corresponsal. El POS respondía bien con otros socios, '
      + 'así que el problema no era del punto de atención.',
    pasos.map((p, i) => `${i + 1}. ${p.descripcion}`).join('\n'),
    pasos.find((p) => p.script)?.script ?? null,
    BASE_DEMO,
    JSON.stringify(pasos),
  ],
);
console.log(`  aporte pendiente · de maria.tenesaca`);

// ── Un ticket con sus pasos ───────────────────────────────────────────────────
const numero = 'DEMO-0001';
await pool.query('DELETE FROM soporte.ticket_pasos WHERE ticket = $1', [numero]);
await pool.query('DELETE FROM soporte.tickets WHERE numero = $1', [numero]);
await pool.query(
  `INSERT INTO soporte.tickets (numero, titulo, descripcion, autor, sistema, modulo, prioridad, estado, creado_en)
   VALUES ($1,$2,$3,'soporte','Corresponsales','Puntos de atención','Alta','investigacion', now())`,
  [
    numero,
    `${MARCA} Un tendero entra y ve los datos de otro negocio`,
    'El corresponsal de Paute reporta que al ingresar al POS le aparecen las transacciones '
      + 'de otro local. Ocurre de forma intermitente.',
  ],
);
const PASOS_TICKET = [
  ['Confirmé con el corresponsal que pasa solo en su POS y no todos los días.', null],
  ['Busqué si había dos puntos de atención con la misma MAC.',
   'SELECT mac, count(*) FROM puntos_atencion GROUP BY mac HAVING count(*) > 1'],
  ['Salieron dos. Uno es un local que ya está inactivo, por eso solo pasa a veces.',
   "SELECT c.nombre_comercial, c.estado, p.codigo_pos FROM puntos_atencion p JOIN corresponsales c ON c.id = p.corresponsal_id WHERE p.mac = '00:1B:44:11:3A:B7'"],
];
for (const [i, [titulo, sql]] of PASOS_TICKET.entries()) {
  await pool.query(
    `INSERT INTO soporte.ticket_pasos (ticket, numero, titulo, sql, fecha)
     VALUES ($1,$2,$3,$4, now())`,
    [numero, i + 1, titulo, sql],
  );
}
console.log(`  ticket con ${PASOS_TICKET.length} pasos · ${numero}`);

// ── Para la demostración ──────────────────────────────────────────────────────
const demo = new pg.Pool({
  host: pg_main.host, port: pg_main.port, database: BASE_DEMO,
  user: pg_main.user, password: pg_main.password,
});
const { rows: ejemplos } = await demo.query(
  `SELECT s.cedula, s.nombres || ' ' || s.apellidos AS socio, s.estado,
          (SELECT count(*)::int FROM cuentas c WHERE c.socio_id = s.id) AS cuentas
     FROM socios s ORDER BY s.id LIMIT 4`,
);
console.log('\nCédulas que puede escribir en la demostración:');
for (const e of ejemplos) {
  console.log(`  ${e.cedula}  ${String(e.socio).padEnd(32)} ${e.estado.padEnd(10)} ${e.cuentas} cuenta(s)`);
}
await demo.end();
await pool.end();
