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
  {
    titulo: `${MARCA} El corresponsal no puede entrar: revisar su usuario y la MAC`,
    categoria: 'Corresponsales',
    descripcion: 'Dice si el problema es la MAC del equipo, el estado del usuario o el del local.',
    contenido: 'Cuando un corresponsal reporta que no puede entrar, o que al resetear la clave le '
      + 'responde «usuario no encontrado», casi siempre es la MAC: o est\u00e1 registrada una que no '
      + 'corresponde a ning\u00fan equipo, o la que tiene est\u00e1 repetida en otro POS. Son dos problemas '
      + 'distintos y se arreglan distinto, as\u00ed que lo primero es saber cu\u00e1l de los dos es. '
      + 'Escriba la c\u00e9dula del corresponsal.',
    sql: `SELECT u.usuario,
       u.estado AS estado_usuario,
       u.mac_registrada,
       u.intentos_fallidos,
       u.ultimo_acceso,
       c.nombre_comercial,
       c.estado AS estado_local,
       (SELECT count(*)::int FROM puntos_atencion p WHERE p.mac = u.mac_registrada) AS pos_con_esa_mac,
       (SELECT string_agg(p.codigo_pos || ' (' || p.estado || ')', ' \u00b7 ')
          FROM puntos_atencion p WHERE p.mac = u.mac_registrada) AS cuales
  FROM usuarios_corresponsal u
  JOIN corresponsales c ON c.id = u.corresponsal_id
 WHERE u.cedula = :cedula`,
  },
  {
    titulo: `${MARCA} Cu\u00e1ntas transacciones tiene un POS en un rango de fechas`,
    categoria: 'Reportes',
    descripcion: 'El reporte se cae pasadas las 300 transacciones: esto dice si el rango las pasa.',
    contenido: 'El reporte del corresponsal tiene un l\u00edmite de 300 transacciones por consulta, y '
      + 'cuando el rango trae m\u00e1s, en vez de recortar da error. Antes de responder que «el reporte '
      + 'est\u00e1 ca\u00eddo» conviene contar cu\u00e1ntas hay en ese rango: si pasa de 300, la salida es '
      + 'pedirlo por semanas. Escriba el c\u00f3digo del POS y las dos fechas.',
    sql: `SELECT p.codigo_pos,
       c.nombre_comercial,
       count(*)::int AS transacciones,
       min(t.fecha) AS primera,
       max(t.fecha) AS ultima,
       CASE WHEN count(*) > 300
            THEN 'pasa del limite de 300: por eso el reporte da error'
            ELSE 'dentro del limite: el reporte deberia salir' END AS diagnostico
  FROM transacciones t
  JOIN puntos_atencion p ON p.id = t.punto_id
  JOIN corresponsales c ON c.id = p.corresponsal_id
 WHERE p.codigo_pos = :codigo_pos
   AND t.fecha >= :desde
   AND t.fecha <= :hasta
 GROUP BY p.codigo_pos, c.nombre_comercial`,
  },
  {
    titulo: `${MARCA} Una recaudaci\u00f3n se cobr\u00f3 pero no imprime el comprobante`,
    categoria: 'Recaudaciones',
    descripcion: 'Distingue una recaudaci\u00f3n sin comprobante de una anulada: en una hay plata cobrada.',
    contenido: 'Que no salga el comprobante no significa que no se haya cobrado, y es justo lo que hay '
      + 'que aclarar antes de contestarle al socio. Esta consulta dice el estado de la recaudaci\u00f3n y '
      + 'si lleg\u00f3 a generar comprobante. Escriba la referencia.',
    sql: `SELECT r.referencia,
       r.institucion,
       r.monto,
       r.estado,
       coalesce(r.comprobante, 'NO SE GENERO') AS comprobante,
       r.fecha,
       p.codigo_pos,
       c.nombre_comercial,
       CASE WHEN r.estado = 'cobrada' AND r.comprobante IS NULL
            THEN 'cobrada sin comprobante: hay que reimprimirlo desde respaldo'
            WHEN r.estado = 'anulada'
            THEN 'anulada: no hay comprobante porque no se cobro'
            ELSE 'con comprobante' END AS diagnostico
  FROM recaudaciones r
  JOIN puntos_atencion p ON p.id = r.punto_id
  JOIN corresponsales c ON c.id = p.corresponsal_id
 WHERE r.referencia = :referencia`,
  },
  {
    titulo: `${MARCA} Por qu\u00e9 un acta de pago no se puede corregir`,
    categoria: 'Actas y pagos',
    descripcion: 'Un acta con el pago cerrado la bloquea la aplicaci\u00f3n: esto lo confirma.',
    contenido: 'Cuando la pantalla no deja corregir ni eliminar un acta, lo normal es que su pago ya '
      + 'est\u00e9 cerrado. Eso lo decide la aplicaci\u00f3n, no la base. Conviene confirmarlo antes de '
      + 'escalar, porque la correcci\u00f3n la tiene que hacer Administraci\u00f3n de Aplicaciones y hay que '
      + 'decirles cu\u00e1l es el acta y por qu\u00e9. Escriba el n\u00famero del acta.',
    sql: `SELECT a.numero,
       a.periodo,
       a.valor,
       a.estado AS estado_acta,
       c.nombre_comercial,
       c.identificacion,
       coalesce(pc.estado, '-') AS estado_pago,
       pc.fecha AS pagada_el,
       CASE WHEN pc.estado = 'C'
            THEN 'el pago esta cerrado: la aplicacion no deja corregir el acta'
            ELSE 'sin pago cerrado: se puede corregir desde la pantalla' END AS diagnostico
  FROM actas a
  JOIN corresponsales c ON c.id = a.corresponsal_id
  LEFT JOIN pagos_corresponsal pc ON pc.numero_acta = a.numero
 WHERE a.numero = :numero_acta`,
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

// ── Los tickets ────────────────────────────────────────────────────
//
// Con un ticket solo la pantalla se ve vacía y nadie entiende para qué está. Van cinco,
// con la forma de los que el equipo ya investigó: dos resueltos con su causa raíz, uno en
// investigación, y uno abierto sin un solo paso —porque eso es lo que de verdad hay en
// una bandeja, y enseñarlo es más honesto que enseñar todo cerrado.
const TICKETS = [
  {
    numero: 'DEMO-0001',
    titulo: 'Un tendero entra y ve los datos de otro negocio',
    descripcion: 'El corresponsal de Paute reporta que al ingresar al POS le aparecen las '
      + 'transacciones de otro local. Ocurre de forma intermitente.',
    sistema: 'Corresponsales', modulo: 'Puntos de atención', categoria: 'Accesos',
    prioridad: 'Alta', estado: 'investigacion',
    causa: null, solucion: null,
    pasos: [
      ['Confirmé con el corresponsal que pasa solo en su POS y no todos los días.', null,
       'Lo confirma: no es todos los días.'],
      ['Busqué si había dos puntos de atención con la misma MAC.',
       'SELECT mac, count(*) FROM puntos_atencion GROUP BY mac HAVING count(*) > 1',
       'Sale una MAC repetida en dos POS.'],
      ['Salieron dos. Uno es un local que ya está inactivo, por eso solo pasa a veces.',
       "SELECT c.nombre_comercial, c.estado, p.codigo_pos FROM puntos_atencion p JOIN corresponsales c ON c.id = p.corresponsal_id WHERE p.mac = '00:1B:44:11:3A:B7'",
       'Tienda Don Byron (activo) y Guel Cyber Café (inactivo).'],
    ],
  },
  {
    numero: 'DEMO-0002',
    titulo: 'El reporte del corresponsal no muestra más de 300 transacciones',
    descripcion: 'Al pedir el reporte de un mes completo el sistema da error en vez de mostrar '
      + 'los resultados. Con rangos cortos sí sale.',
    sistema: 'Corresponsales', modulo: 'Reportes', categoria: 'Reportes',
    prioridad: 'Media', estado: 'resuelto',
    causa: 'El reporte tiene un límite fijo de 300 transacciones por consulta. Cuando el rango '
      + 'pedido trae más que eso, en lugar de recortar la salida genera un error.',
    solucion: 'Se contó el volumen del rango y efectivamente pasaba de 300. Mientras desarrollo '
      + 'levanta el límite, se le indicó al usuario pedir el reporte por semanas.',
    pasos: [
      ['Conté cuántas transacciones había en el rango que pedía.',
       "SELECT count(*) FROM transacciones t JOIN puntos_atencion p ON p.id = t.punto_id WHERE p.codigo_pos = 'POS-1001' AND t.fecha >= NOW() - INTERVAL '30 days'",
       'Más de 400: pasa del límite.'],
      ['CIERRE: confirmado el límite. Se responde al usuario y se deja el caso como conocido.',
       null, 'Caso cerrado con causa conocida.'],
    ],
  },
  {
    numero: 'DEMO-0003',
    titulo: 'Socia con corresponsal no puede resetear la clave: «usuario no encontrado»',
    descripcion: 'No recuerda la clave ni el PIN. Al intentar resetear, el sistema responde '
      + '«usuario no encontrado», aunque la socia sí tiene corresponsal asignado.',
    sistema: 'Corresponsales', modulo: 'Usuarios / Reset de clave', categoria: 'Autenticación',
    prioridad: 'Alta', estado: 'resuelto',
    causa: 'La MAC registrada para su usuario no corresponde a ningún POS. Al buscar por MAC y '
      + 'usuario no encuentra nada, y el mensaje que devuelve es «usuario no encontrado», que '
      + 'manda a buscar el problema donde no está.',
    solucion: 'Se corrigió la asignación de equipo del usuario, dejando su POS vinculado a la MAC '
      + 'real del dispositivo. Con eso el reset de clave vuelve a identificarla.',
    pasos: [
      ['Busqué el usuario por cédula para ver si existía y en qué estado estaba.',
       "SELECT usuario, estado, mac_registrada, intentos_fallidos FROM usuarios_corresponsal WHERE cedula = '0110000548'",
       'Existe y está activo, con 7 intentos fallidos.'],
      ['Comparé su MAC registrada contra los POS existentes.',
       "SELECT codigo_pos, estado FROM puntos_atencion WHERE mac = '00:1B:44:11:3A:E1'",
       'Ningún POS tiene esa MAC.'],
      ['Descarté que fuera una MAC duplicada, que es el otro caso y se arregla distinto.',
       'SELECT mac, count(*) FROM puntos_atencion GROUP BY mac HAVING count(*) > 1',
       'La MAC repetida es otra, no la suya.'],
      ['CIERRE: era una MAC que no existe, no una duplicada. Corregida la asignación.',
       null, 'La socia ya puede resetear.'],
    ],
  },
  {
    numero: 'DEMO-0004',
    titulo: 'No se puede corregir el acta de julio: la factura salió sin la leyenda',
    descripcion: 'Hay que corregir el acta de julio y volver a generarla junto con agosto, pero '
      + 'la pantalla no deja editarla ni eliminarla.',
    sistema: 'Corresponsales', modulo: 'Actas y pagos', categoria: 'Actas y pagos',
    prioridad: 'Media', estado: 'investigacion',
    causa: null, solucion: null,
    pasos: [
      ['Revisé el estado del acta y de su pago.',
       'SELECT a.numero, a.estado, pc.estado AS pago FROM actas a LEFT JOIN pagos_corresponsal pc ON pc.numero_acta = a.numero WHERE a.numero = 53656',
       'El acta está cerrada y su pago también (estado C).'],
      ['Con el pago cerrado la aplicación bloquea la corrección. La parte de corregirla no se '
       + 'puede hacer desde aquí: hay que escalarla a Administración de Aplicaciones.',
       null, 'Escalado, con el número de acta y el motivo.'],
    ],
  },
  {
    numero: 'DEMO-0005',
    titulo: 'Error al generar el reporte',
    descripcion: 'Reportado por teléfono, sin captura ni detalle del rango. Pendiente de pedir '
      + 'los datos al usuario para poder reproducirlo.',
    sistema: 'Corresponsales', modulo: 'Reportes', categoria: null,
    prioridad: 'Baja', estado: 'abierto',
    causa: null, solucion: null,
    pasos: [],
  },
];

for (const tk of TICKETS) {
  const numero = tk.numero;
  await pool.query('DELETE FROM soporte.ticket_pasos WHERE ticket = $1', [numero]);
  await pool.query('DELETE FROM soporte.tickets WHERE numero = $1', [numero]);
  await pool.query(
    `INSERT INTO soporte.tickets
       (numero, titulo, descripcion, autor, sistema, modulo, categoria, prioridad, estado,
        causa_raiz, solucion, base_datos, creado_en, resuelto_en)
     VALUES ($1,$2,$3,'soporte',$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
    [
      numero, `${MARCA} ${tk.titulo}`, tk.descripcion,
      tk.sistema, tk.modulo, tk.categoria, tk.prioridad, tk.estado,
      tk.causa, tk.solucion, BASE_DEMO,
      new Date(Date.now() - 864e5 * (6 - TICKETS.indexOf(tk))).toISOString().slice(0, 10),
      tk.estado === 'resuelto' ? new Date(Date.now() - 864e5).toISOString().slice(0, 10) : null,
    ],
  );
  for (const [i, [titulo, sql, resultado]] of tk.pasos.entries()) {
    await pool.query(
      `INSERT INTO soporte.ticket_pasos (ticket, numero, titulo, sql, resultado, fecha)
       VALUES ($1,$2,$3,$4,$5,$6)`,
      [numero, i + 1, titulo, sql, resultado, new Date().toISOString().slice(0, 10)],
    );
  }
  console.log(`  ticket ${tk.estado.padEnd(13)} ${numero} · ${tk.pasos.length} paso(s)`);
}

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
