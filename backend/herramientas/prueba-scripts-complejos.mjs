/**
 * Scripts complicados, por el camino de verdad: guardar el artículo y ejecutarlo.
 *
 * La pregunta que contesta es «¿vale para cualquier script que le pongan?». No se puede
 * contestar leyendo el código: hay que pasarle consultas de las que se escriben cuando el
 * problema es de verdad —CTE recursivas, ventanas, LATERAL, pivotes, JSON, arreglos,
 * expresiones regulares, parámetros repetidos, filtros opcionales— y ver si pasan el
 * guardián, si se guardan y si devuelven lo que tienen que devolver.
 *
 * Van en dos grupos, porque son dos cosas distintas:
 *
 *   · fijos     — no piden nada, se ejecutan tal cual;
 *   · dinámicos — piden datos, y ahí entra la inferencia de tipos, que ya ha roto la
 *                 demostración dos veces.
 *
 * Y un tercero: escrituras complicadas, que tienen que ser rechazadas todas. Una consulta
 * complicada y una escritura disfrazada se parecen mucho, y la diferencia es el sistema
 * entero.
 *
 * Necesita el backend levantado y la base de demostración montada.
 *
 *   node herramientas/prueba-scripts-complejos.mjs
 */
import pg from 'pg';
import { readFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';

const BASE = 'http://127.0.0.1:3002';
const DEMO = 'demo_cooperativa';
const cfg = JSON.parse(readFileSync('config/multidb.databases.json', 'utf8'));
const m = cfg.databases.find((d) => d.id === 'pg_main');
const pool = new pg.Pool({ host: m.host, port: m.port, database: m.database, user: m.user, password: m.password });

const { rows: [admin] } = await pool.query("SELECT id, username FROM users WHERE role = 'admin' LIMIT 1");
const token = 'complejos-' + randomUUID();
await pool.query('INSERT INTO sessions (token, user_id, activo, created_at) VALUES ($1,$2,true,NOW())', [token, admin.id]);
const H = { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` };

let fallos = 0;
const creados = [];

/** Guarda el artículo y lo ejecuta. Devuelve las filas, o lanza con el motivo. */
/**
 * Una peticion que espera si el limitador la frena.
 *
 * Son cien por minuto, y cada caso gasta dos. Contar las propias no basta: si la bateria
 * anterior dejo el minuto gastado, esta se come el 429 en el primer caso y reporta
 * fallos que no existen — que es peor que tardar, porque manda a buscar una causa donde
 * no esta.
 */
async function traer(url, opciones) {
  let r = await fetch(url, opciones);
  if (r.status === 429) {
    process.stdout.write('  (el limitador pide calma: 60 s)\n');
    await new Promise((listo) => { setTimeout(listo, 62_000); });
    r = await fetch(url, opciones);
  }
  return r;
}

async function probar(titulo, sql, datos) {
  const alta = await traer(`${BASE}/api/articles`, {
    method: 'POST',
    headers: H,
    body: JSON.stringify({
      titulo: `[complejo] ${titulo}`,
      categoria: 'Prueba',
      descripcion: 'Creado por la prueba de scripts complejos.',
      contenido_md: 'Se borra al terminar.',
      tipo_solucion: 'dinamico',
      visible_para: 'admin',
      script_json: { modo: 'script', origen: 'manual-sql', sql, target_database: DEMO, databaseId: DEMO },
    }),
  });
  const jAlta = await alta.json();
  if (!alta.ok) throw new Error(`no se guarda: ${String(jAlta.error || '').slice(0, 120)}`);
  const id = jAlta.data?.id || jAlta.article?.id || jAlta.id;
  creados.push(id);

  const ej = await traer(`${BASE}/api/execute`, {
    method: 'POST', headers: H, body: JSON.stringify({ articleId: id, formData: datos ?? {} }),
  });
  const jEj = await ej.json();
  if (!ej.ok) throw new Error(`no se ejecuta: ${String(jEj.message || '').slice(0, 160)}`);
  return jEj?.result?.resultado ?? [];
}

async function caso(grupo, titulo, sql, datos, esperado) {
  let bien = false;
  let detalle = '';
  try {
    const filas = await probar(titulo, sql, datos);
    bien = esperado(filas);
    detalle = `${filas.length} fila(s)` + (filas.length ? ` · ${JSON.stringify(filas[0]).slice(0, 72)}` : '');
    if (!bien) detalle = 'resultado inesperado · ' + detalle;
  } catch (e) {
    detalle = e.message;
  }
  if (!bien) fallos += 1;
  console.log(`  ${bien ? 'ok  ' : 'MAL '} ${titulo.padEnd(46)} ${detalle}`);
}

// ── Fijos: no piden nada ──────────────────────────────────────────────────────
console.log('\nSCRIPTS FIJOS — no piden datos\n');

await caso('fijo', 'CTE recursiva: la escalera de saldos',
  `WITH RECURSIVE tramo(desde, hasta) AS (
     SELECT 0::numeric, 1000::numeric
     UNION ALL
     SELECT desde + 1000, hasta + 1000 FROM tramo WHERE desde < 4000
   )
   SELECT t.desde, t.hasta, count(c.id)::int AS cuentas
     FROM tramo t
     LEFT JOIN cuentas c ON c.saldo >= t.desde AND c.saldo < t.hasta
    GROUP BY t.desde, t.hasta
    ORDER BY t.desde`,
  {}, (f) => f.length === 5);

await caso('fijo', 'Ventana: puesto de cada socio en su oficina',
  `SELECT s.sucursal,
          s.nombres || ' ' || s.apellidos AS socio,
          sum(c.saldo) AS total,
          rank() OVER (PARTITION BY s.sucursal ORDER BY sum(c.saldo) DESC) AS puesto,
          round(100.0 * sum(c.saldo) / sum(sum(c.saldo)) OVER (PARTITION BY s.sucursal), 1) AS porcentaje
     FROM socios s JOIN cuentas c ON c.socio_id = s.id
    GROUP BY s.sucursal, s.id, s.nombres, s.apellidos
    ORDER BY s.sucursal, puesto`,
  {}, (f) => f.length > 5 && f[0].puesto !== undefined);

await caso('fijo', 'LATERAL: la última transacción de cada cuenta',
  `SELECT c.numero, u.tipo, u.monto, u.fecha
     FROM cuentas c
     CROSS JOIN LATERAL (
       SELECT t.tipo, t.monto, t.fecha FROM transacciones t
        WHERE t.cuenta_id = c.id ORDER BY t.fecha DESC LIMIT 1
     ) u
    ORDER BY u.fecha DESC`,
  {}, (f) => f.length > 0);

await caso('fijo', 'Pivote con FILTER: tipos por oficina',
  `SELECT s.sucursal,
          count(*) FILTER (WHERE t.tipo = 'deposito')::int      AS depositos,
          count(*) FILTER (WHERE t.tipo = 'retiro')::int        AS retiros,
          count(*) FILTER (WHERE t.estado = 'rechazada')::int   AS rechazadas
     FROM transacciones t
     JOIN cuentas c ON c.id = t.cuenta_id
     JOIN socios s  ON s.id = c.socio_id
    GROUP BY s.sucursal ORDER BY s.sucursal`,
  {}, (f) => f.length > 0 && f[0].depositos !== undefined);

await caso('fijo', 'JSON agregado y arreglos',
  `SELECT s.sucursal,
          json_agg(json_build_object('cedula', s.cedula, 'estado', s.estado)) AS socios,
          array_agg(DISTINCT s.estado) AS estados
     FROM socios s GROUP BY s.sucursal ORDER BY s.sucursal`,
  {}, (f) => f.length > 0 && Array.isArray(f[0].estados));

await caso('fijo', 'GROUPING SETS con el total',
  `SELECT coalesce(s.sucursal, 'TODAS') AS sucursal,
          coalesce(s.estado, 'TODOS') AS estado,
          count(*)::int AS cuantos
     FROM socios s
    GROUP BY GROUPING SETS ((s.sucursal), (s.estado), ())
    ORDER BY 1, 2`,
  {}, (f) => f.length > 3);

await caso('fijo', 'EXISTS y NOT EXISTS',
  `SELECT s.cedula, s.nombres
     FROM socios s
    WHERE EXISTS (SELECT 1 FROM cuentas c WHERE c.socio_id = s.id)
      AND NOT EXISTS (SELECT 1 FROM cuentas c WHERE c.socio_id = s.id AND c.estado = 'bloqueada')`,
  {}, (f) => f.length > 0);

await caso('fijo', 'UNION entre tablas distintas',
  `SELECT 'socio' AS clase, cedula AS identificacion, nombres || ' ' || apellidos AS nombre FROM socios
    UNION ALL
   SELECT 'corresponsal', identificacion, nombre_comercial FROM corresponsales
    ORDER BY clase, nombre`,
  {}, (f) => f.length === 17);

await caso('fijo', 'Expresiones regulares y texto',
  `SELECT cedula, nombres
     FROM socios
    WHERE cedula ~ '^011000[0-9]{4}$' AND nombres !~* 'zzz'
    ORDER BY cedula`,
  {}, (f) => f.length === 12);

await caso('fijo', 'Varias CTE encadenadas',
  `WITH activos AS (SELECT * FROM socios WHERE estado = 'activo'),
        saldos  AS (SELECT socio_id, sum(saldo) AS total FROM cuentas GROUP BY socio_id),
        junto   AS (SELECT a.cedula, a.sucursal, coalesce(s.total, 0) AS total
                      FROM activos a LEFT JOIN saldos s ON s.socio_id = a.id)
   SELECT sucursal, count(*)::int AS socios, round(avg(total), 2) AS promedio
     FROM junto GROUP BY sucursal ORDER BY promedio DESC`,
  {}, (f) => f.length > 0 && f[0].promedio !== undefined);

// ── Dinámicos: piden datos ────────────────────────────────────────────────────
console.log('\nSCRIPTS DINÁMICOS — piden datos\n');

await caso('din', 'Un parámetro usado dos veces',
  `SELECT s.cedula, s.nombres, (SELECT count(*)::int FROM cuentas c WHERE c.socio_id = s.id) AS cuentas
     FROM socios s
    WHERE s.cedula = :cedula OR s.telefono = :cedula`,
  { cedula: '0110000411' }, (f) => f.length === 1);

await caso('din', 'Rango de fechas con agregados',
  `SELECT date_trunc('day', t.fecha)::date AS dia, count(*)::int AS cuantas, sum(t.monto) AS total
     FROM transacciones t
    WHERE t.fecha >= :desde AND t.fecha <= :hasta
    GROUP BY 1 ORDER BY 1 DESC LIMIT 10`,
  { desde: '2026-09-01', hasta: '2026-12-31' }, (f) => f.length > 0 && f[0].cuantas > 0);

await caso('din', 'Texto parcial con ILIKE',
  `SELECT cedula, nombres || ' ' || apellidos AS socio, sucursal
     FROM socios WHERE nombres ILIKE :texto OR apellidos ILIKE :texto`,
  { texto: '%quizh%' }, (f) => f.length >= 1);

await caso('din', 'Cinco parámetros a la vez',
  `SELECT s.cedula, s.sucursal, s.estado, c.tipo, c.saldo
     FROM socios s JOIN cuentas c ON c.socio_id = s.id
    WHERE s.sucursal = :sucursal AND s.estado = :estado AND c.tipo = :tipo
      AND c.saldo >= :minimo AND c.saldo <= :maximo`,
  { sucursal: 'Matriz Cuenca', estado: 'activo', tipo: 'ahorros', minimo: '0', maximo: '999999' },
  (f) => f.length >= 1);

await caso('din', 'Parámetro dentro de una CTE',
  `WITH suyo AS (SELECT id, cedula FROM socios WHERE cedula = :cedula)
   SELECT s.cedula, c.numero, c.tipo, c.saldo, c.estado
     FROM suyo s JOIN cuentas c ON c.socio_id = s.id
    ORDER BY c.tipo`,
  { cedula: '0110000411' }, (f) => f.length === 2);

await caso('din', 'Parámetro en una ventana y un HAVING',
  `SELECT p.codigo_pos, count(*)::int AS cuantas
     FROM transacciones t JOIN puntos_atencion p ON p.id = t.punto_id
    WHERE t.fecha >= :desde
    GROUP BY p.codigo_pos
   HAVING count(*) > :minimo
    ORDER BY cuantas DESC`,
  { desde: '2026-01-01', minimo: '20' }, (f) => f.length > 0);

await caso('din', 'Parámetro comparado con un identificador',
  `SELECT c.numero, c.tipo, c.saldo FROM cuentas c WHERE c.socio_id = :socio_id`,
  { socio_id: '4' }, (f) => f.length === 2);

// ── Escrituras complicadas: todas fuera ───────────────────────────────────────
console.log('\nESCRITURAS DISFRAZADAS — todas tienen que ser rechazadas\n');

const DISFRACES = [
  ['CTE que borra', 'WITH ido AS (DELETE FROM socios WHERE id = 99 RETURNING *) SELECT * FROM ido'],
  ['CTE que actualiza', "WITH u AS (UPDATE socios SET estado='x' WHERE id=99 RETURNING *) SELECT count(*) FROM u"],
  ['Bloque PL/SQL', "BEGIN UPDATE socios SET estado='x'; END;"],
  ['Bloque anónimo de PostgreSQL', "DO $$ BEGIN UPDATE socios SET estado='x'; END $$"],
  ['Lectura y después un DROP', 'SELECT 1; DROP TABLE socios'],
  ['SELECT que crea tabla', 'SELECT * INTO copia_socios FROM socios'],
  ['SELECT que bloquea filas', 'SELECT * FROM socios WHERE id = 1 FOR UPDATE'],
  ['Consulta larga con un UPDATE al final',
   `WITH a AS (SELECT * FROM socios), b AS (SELECT * FROM cuentas)
    SELECT a.cedula, b.saldo FROM a JOIN b ON b.socio_id = a.id;
    UPDATE socios SET estado = 'activo'`],
];

for (const [nombre, sql] of DISFRACES) {
  let rechazada = false;
  let motivo = '';
  try {
    await probar(nombre, sql, {});
  } catch (e) {
    rechazada = e.message.startsWith('no se guarda');
    motivo = e.message.slice(0, 86);
  }
  if (!rechazada) fallos += 1;
  console.log(`  ${rechazada ? 'ok  ' : 'MAL '} ${nombre.padEnd(46)} ${rechazada ? motivo : 'SE GUARDÓ'}`);
}

// ── Limpieza ──────────────────────────────────────────────────────────────────
await pool.query("DELETE FROM article_executions WHERE article_id IN (SELECT id::text FROM knowledge_base WHERE titulo LIKE '[complejo]%')");
await pool.query("DELETE FROM article_changes WHERE article_id IN (SELECT id::text FROM knowledge_base WHERE titulo LIKE '[complejo]%')");
const quitados = await pool.query("DELETE FROM knowledge_base WHERE titulo LIKE '[complejo]%'");
await pool.query('DELETE FROM sessions WHERE token = $1', [token]);
await pool.end();

console.log(`\n  (quitados ${quitados.rowCount} artículos de prueba)`);
console.log('\n' + (fallos === 0
  ? '════ ACEPTA LO COMPLICADO Y RECHAZA LO QUE ESCRIBE ════'
  : `════ ${fallos} FALLO(S) ════`));
process.exit(fallos === 0 ? 0 : 1);
