/**
 * Scripts complicados contra Oracle, MongoDB y MySQL, por el camino de verdad.
 *
 * Hasta ahora lo complicado solo se había probado contra PostgreSQL, y eso deja sin
 * contestar la pregunta que importa: si vale para la base de la cooperativa, que es
 * Oracle, y para las otras que haya conectadas. Un guardián que funciona en un dialecto
 * no funciona en los otros por sí solo, y un motor que ejecuta un SELECT sencillo no
 * ejecuta por eso una consulta jerárquica.
 *
 * Cada caso se guarda como artículo y se ejecuta, igual que lo haría una persona. Y de
 * cada motor se prueban las dos mitades: que lo complicado pase, y que lo que escribe
 * no pase —en el dialecto de ese motor, que es donde se escapaba.
 *
 * Necesita el backend levantado y los tres contenedores arrancados:
 *   node herramientas/demo-oracle-mesa.mjs "ruta/al/glpi.csv"
 *   node herramientas/demo-mongo-mysql.mjs
 *
 *   node herramientas/prueba-motores.mjs
 */
import pg from 'pg';
import { readFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';

const BASE = 'http://127.0.0.1:3002';
const cfg = JSON.parse(readFileSync('config/multidb.databases.json', 'utf8'));
const m = cfg.databases.find((d) => d.id === 'pg_main');
const pool = new pg.Pool({ host: m.host, port: m.port, database: m.database, user: m.user, password: m.password });

const { rows: [admin] } = await pool.query("SELECT id FROM users WHERE role = 'admin' LIMIT 1");
const token = 'motores-' + randomUUID();
await pool.query('INSERT INTO sessions (token, user_id, activo, created_at) VALUES ($1,$2,true,NOW())', [token, admin.id]);
const H = { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` };

let fallos = 0;
let n = 0;

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

async function probar(baseId, titulo, script, datos) {
  const alta = await traer(`${BASE}/api/articles`, {
    method: 'POST',
    headers: H,
    body: JSON.stringify({
      titulo: `[motor] ${titulo}`,
      categoria: 'Prueba de motores',
      descripcion: 'Creado por la prueba de motores.',
      contenido_md: 'Se borra al terminar.',
      tipo_solucion: 'dinamico',
      visible_para: 'admin',
      script_json: { modo: 'script', origen: 'manual-sql', ...script, target_database: baseId, databaseId: baseId },
    }),
  });
  const jAlta = await alta.json();
  if (!alta.ok) throw new Error(`no se guarda: ${String(jAlta.error || '').slice(0, 130)}`);
  const id = jAlta.data?.id || jAlta.article?.id || jAlta.id;

  const ej = await traer(`${BASE}/api/execute`, {
    method: 'POST', headers: H, body: JSON.stringify({ articleId: id, formData: datos ?? {} }),
  });
  const jEj = await ej.json();
  if (!ej.ok) throw new Error(`no se ejecuta: ${String(jEj.message || '').slice(0, 170)}`);
  return jEj?.result?.resultado ?? [];
}

async function caso(baseId, titulo, script, datos, esperado) {
  let bien = false;
  let detalle = '';
  try {
    const filas = await probar(baseId, titulo, script, datos);
    bien = esperado(filas);
    detalle = `${filas.length} fila(s)` + (filas.length ? ` · ${JSON.stringify(filas[0]).slice(0, 78)}` : '');
    if (!bien) detalle = 'inesperado · ' + detalle;
  } catch (e) {
    detalle = e.message;
  }
  if (!bien) fallos += 1;
  console.log(`  ${bien ? 'ok  ' : 'MAL '} ${titulo.padEnd(50)} ${detalle}`);
}

async function rechaza(baseId, titulo, script) {
  let rechazado = false;
  let motivo = '';
  try {
    await probar(baseId, titulo, script, {});
  } catch (e) {
    rechazado = e.message.startsWith('no se guarda');
    motivo = e.message.replace('no se guarda: ', '').slice(0, 78);
  }
  if (!rechazado) fallos += 1;
  console.log(`  ${rechazado ? 'ok  ' : 'MAL '} ${titulo.padEnd(50)} ${rechazado ? motivo : 'SE GUARDÓ'}`);
}

const sql = (texto) => ({ sql: texto });

// ══ ORACLE ═══════════════════════════════════════════════════════════════════
console.log('\nORACLE — la mesa de servicios\n');

await caso('oracle_local', 'CONNECT BY: el árbol de grupos con su ruta',
  sql(`SELECT LEVEL AS nivel,
       LPAD(' ', (LEVEL-1)*2) || g.nombre AS grupo,
       SYS_CONNECT_BY_PATH(g.nombre, ' > ') AS ruta,
       CONNECT_BY_ISLEAF AS es_hoja
  FROM grupos g
 START WITH g.padre_id IS NULL
 CONNECT BY PRIOR g.id = g.padre_id
 ORDER SIBLINGS BY g.nombre`),
  {}, (f) => f.length === 24 && f.some((x) => Number(x.NIVEL) === 3));

await caso('oracle_local', 'Jerarquía + agregado: carga de cada rama',
  sql(`WITH arbol AS (
         SELECT g.id, g.nombre, CONNECT_BY_ROOT g.nombre AS raiz, LEVEL AS nivel
           FROM grupos g
          START WITH g.padre_id IS NULL
          CONNECT BY PRIOR g.id = g.padre_id
       )
   SELECT a.raiz,
          COUNT(t.id) AS tickets,
          ROUND(AVG(t.resolucion_min), 1) AS promedio_min,
          SUM(CASE WHEN t.excedida = 'S' THEN 1 ELSE 0 END) AS excedidos
     FROM arbol a
     LEFT JOIN tickets t ON t.grupo_id = a.id
    GROUP BY a.raiz
    ORDER BY tickets DESC`),
  {}, (f) => f.length >= 3 && f[0].TICKETS !== undefined);

await caso('oracle_local', 'PIVOT: tipos de solución por prioridad',
  sql(`SELECT * FROM (
         SELECT t.prioridad, t.tipo
           FROM tickets t
       )
       PIVOT (COUNT(*) FOR tipo IN ('Solicitud' AS solicitudes, 'Incidencia' AS incidencias))
       ORDER BY prioridad`),
  {}, (f) => f.length === 2 && f[0].SOLICITUDES !== undefined);

await caso('oracle_local', 'LISTAGG: las categorías de cada entidad',
  sql(`SELECT e.nombre AS entidad,
       COUNT(DISTINCT c.id) AS categorias,
       LISTAGG(DISTINCT c.nivel1, ' | ') WITHIN GROUP (ORDER BY c.nivel1) AS areas
  FROM tickets t
  JOIN entidades e ON e.id = t.entidad_id
  JOIN categorias c ON c.id = t.categoria_id
 GROUP BY e.nombre
 ORDER BY categorias DESC`),
  {}, (f) => f.length >= 4 && typeof f[0].AREAS === 'string');

await caso('oracle_local', 'Analíticas: percentiles y KEEP DENSE_RANK',
  sql(`SELECT c.nivel1 AS area,
       COUNT(*) AS tickets,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.resolucion_min), 1) AS mediana,
       ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY t.resolucion_min), 1) AS p90,
       MAX(t.titulo) KEEP (DENSE_RANK FIRST ORDER BY t.resolucion_min DESC) AS el_mas_lento
  FROM tickets t
  JOIN categorias c ON c.id = t.categoria_id
 GROUP BY c.nivel1
HAVING COUNT(*) > 3
 ORDER BY p90 DESC`),
  {}, (f) => f.length > 0 && f[0].MEDIANA !== undefined);

await caso('oracle_local', 'Ventanas: racha y variación contra el día anterior',
  sql(`WITH por_dia AS (
         SELECT TRUNC(t.abierto_en) AS dia, COUNT(*) AS cuantos
           FROM tickets t GROUP BY TRUNC(t.abierto_en)
       )
   SELECT dia, cuantos,
          LAG(cuantos) OVER (ORDER BY dia) AS dia_antes,
          cuantos - LAG(cuantos, 1, 0) OVER (ORDER BY dia) AS variacion,
          ROUND(AVG(cuantos) OVER (ORDER BY dia ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS media_movil,
          RATIO_TO_REPORT(cuantos) OVER () AS porcion
     FROM por_dia
    ORDER BY dia`),
  {}, (f) => f.length > 3 && f[1].DIA_ANTES !== undefined);

await caso('oracle_local', 'REGEXP_SUBSTR: partir la ruta de categoría',
  sql(`SELECT c.ruta,
       REGEXP_SUBSTR(c.ruta, '[^>]+', 1, 1) AS parte1,
       REGEXP_SUBSTR(c.ruta, '[^>]+', 1, 3) AS parte3,
       REGEXP_COUNT(c.ruta, '>') + 1 AS niveles
  FROM categorias c
 WHERE REGEXP_COUNT(c.ruta, '>') >= 3
 FETCH FIRST 10 ROWS ONLY`),
  {}, (f) => f.length > 0 && Number(f[0].NIVELES) >= 4);

await caso('oracle_local', 'ROLLUP con GROUPING_ID: subtotales y total',
  sql(`SELECT NVL(c.nivel1, 'TODAS') AS area,
       NVL(t.prioridad, 'TODAS') AS prioridad,
       COUNT(*) AS cuantos,
       GROUPING_ID(c.nivel1, t.prioridad) AS marca
  FROM tickets t JOIN categorias c ON c.id = t.categoria_id
 GROUP BY ROLLUP (c.nivel1, t.prioridad)
 ORDER BY marca, area, prioridad`),
  {}, (f) => f.length > 5 && f.some((x) => Number(x.MARCA) === 3));

await caso('oracle_local', 'WITH recursivo + intervalos de tiempo',
  sql(`WITH semanas (inicio, k) AS (
         SELECT TRUNC(SYSDATE) - 70, 1 FROM dual
         UNION ALL
         SELECT inicio + 7, k + 1 FROM semanas WHERE k < 10
       )
   SELECT TO_CHAR(s.inicio, 'YYYY-MM-DD') AS semana,
          COUNT(t.id) AS tickets,
          ROUND(AVG(t.espera_seg) / 60, 2) AS espera_min,
          NUMTODSINTERVAL(NVL(AVG(t.resolucion_min), 0) * 60, 'SECOND') AS resolucion
     FROM semanas s
     LEFT JOIN tickets t ON t.abierto_en >= s.inicio AND t.abierto_en < s.inicio + 7
    GROUP BY s.inicio
    ORDER BY s.inicio`),
  {}, (f) => f.length === 10);

await caso('oracle_local', 'Subconsultas correlacionadas y LEFT JOIN con huecos',
  sql(`SELECT t.id, t.titulo, t.prioridad,
       (SELECT COUNT(*) FROM tareas x WHERE x.ticket_id = t.id) AS tareas,
       NVL(ts.cedula, 'sin socio') AS cedula,
       CASE WHEN ts.ticket_id IS NULL THEN 'no' ELSE 'si' END AS lleva_socio
  FROM tickets t
  LEFT JOIN ticket_socio ts ON ts.ticket_id = t.id
 WHERE t.escalamientos > 1
   AND EXISTS (SELECT 1 FROM tareas x WHERE x.ticket_id = t.id)
 ORDER BY t.id
 FETCH FIRST 12 ROWS ONLY`),
  {}, (f) => f.length > 0 && f[0].LLEVA_SOCIO !== undefined);

await caso('oracle_local', 'Con parámetros: cédula y rango de fechas',
  sql(`SELECT t.id, t.titulo, t.prioridad, ts.cedula, ts.valor,
       TO_CHAR(t.abierto_en, 'YYYY-MM-DD HH24:MI') AS abierto
  FROM tickets t JOIN ticket_socio ts ON ts.ticket_id = t.id
 WHERE ts.cedula = :cedula`),
  { cedula: '0120000000' }, (f) => f.length === 1);

await caso('oracle_local', 'Con parámetros: área y mínimo de minutos',
  sql(`SELECT c.nivel1 AS area, COUNT(*) AS cuantos, MAX(t.resolucion_min) AS peor
  FROM tickets t JOIN categorias c ON c.id = t.categoria_id
 WHERE c.nivel1 = :area AND t.resolucion_min >= :minimo
 GROUP BY c.nivel1`),
  { area: 'CAPTACIONES', minimo: '100' }, (f) => f.length === 1 && Number(f[0].CUANTOS) > 0);

console.log('\nORACLE — lo que escribe, en su propio dialecto\n');
for (const [titulo, texto] of [
  ['Bloque PL/SQL con UPDATE', "BEGIN UPDATE tickets SET estado = 'Cerrado'; COMMIT; END;"],
  ['DECLARE con DELETE', 'DECLARE n NUMBER; BEGIN DELETE FROM tareas; COMMIT; END;'],
  ['EXECUTE IMMEDIATE', "BEGIN EXECUTE IMMEDIATE 'DROP TABLE tareas'; END;"],
  ['MERGE', 'MERGE INTO tickets d USING tareas s ON (d.id = s.ticket_id) WHEN MATCHED THEN UPDATE SET d.estado = \'x\''],
  ['CREATE TABLE AS SELECT', 'CREATE TABLE copia_tickets AS SELECT * FROM tickets'],
  ['TRUNCATE', 'TRUNCATE TABLE tareas'],
  ['SELECT ... FOR UPDATE', 'SELECT * FROM tickets WHERE id = 1 FOR UPDATE'],
]) await rechaza('oracle_local', titulo, sql(texto));

// ══ MONGODB ══════════════════════════════════════════════════════════════════
console.log('\nMONGODB — conciliaciones con detalle anidado\n');

const mongo = (texto) => ({ sql: texto });

await caso('mongo_local', 'find con ObjectId y tipos de la consola',
  mongo(`{ collection: 'conciliaciones', find: { institutionCode: NumberInt('4'), statusConciliation: NumberInt('0') }, sort: { cutOffDate: -1 }, limit: 5 }`),
  {}, (f) => f.length > 0 && f[0].cutOffDate !== undefined);

await caso('mongo_local', 'aggregate con $unwind sobre el arreglo anidado',
  mongo(`{ collection: 'conciliaciones', aggregate: [
     { $unwind: '$details' },
     { $match: { 'details.reconciled': NumberInt('0') } },
     { $group: { _id: '$details.observation', cuantas: { $sum: 1 }, valor: { $sum: '$details.transactionValueSwitch' } } },
     { $sort: { cuantas: -1 } }
   ] }`),
  {}, (f) => f.length > 0 && f[0].cuantas > 0);

await caso('mongo_local', 'aggregate con $lookup-less: facetas y proyección',
  mongo(`{ collection: 'conciliaciones', aggregate: [
     { $project: { cutOffDate: 1, total: '$transactionCount', ok: '$transactionOk',
                   tasa: { $cond: [ { $eq: ['$transactionCount', 0] }, 0,
                           { $divide: ['$transactionOk', '$transactionCount'] } ] } } },
     { $match: { tasa: { $lt: 1 } } },
     { $sort: { tasa: 1 } },
     { $limit: 10 }
   ] }`),
  {}, (f) => f.length > 0 && f[0].tasa !== undefined);

await caso('mongo_local', 'countDocuments y distinct',
  mongo(`{ collection: 'conciliaciones', countDocuments: { statusConciliation: NumberInt('1') } }`),
  {}, (f) => f.length === 1 && f[0].count !== undefined);

console.log('\nMONGODB — lo que escribe\n');
for (const [titulo, texto] of [
  ['deleteMany', `{ collection: 'conciliaciones', deleteMany: {} }`],
  ['updateMany', `{ collection: 'conciliaciones', updateMany: { filter: {}, update: { $set: { statusConciliation: 1 } } } }`],
  ['insertOne', `{ collection: 'conciliaciones', insertOne: { a: 1 } }`],
  ['aggregate con $out', `{ collection: 'conciliaciones', aggregate: [ { $match: {} }, { $out: 'copia' } ] }`],
  ['drop', `{ collection: 'conciliaciones', drop: {} }`],
]) await rechaza('mongo_local', titulo, mongo(texto));

// ══ MYSQL ════════════════════════════════════════════════════════════════════
console.log('\nMYSQL — cortes de caja\n');

await caso('mysql_test', 'Ventanas de MySQL 8 y GROUP_CONCAT',
  sql(`SELECT a.nombre AS agencia,
       COUNT(*) AS cortes,
       ROUND(SUM(c.diferencia), 2) AS descuadre,
       RANK() OVER (ORDER BY ABS(SUM(c.diferencia)) DESC) AS puesto,
       GROUP_CONCAT(DISTINCT j.nombre ORDER BY j.nombre SEPARATOR ' | ') AS cajeros
  FROM cortes_caja c
  JOIN cajeros j ON j.id = c.cajero_id
  JOIN agencias a ON a.id = j.agencia_id
 GROUP BY a.nombre
 ORDER BY puesto`),
  {}, (f) => f.length === 5 && f[0].puesto !== undefined);

await caso('mysql_test', 'CTE + ventana corrida + DATE_FORMAT',
  sql(`WITH diario AS (
         SELECT c.fecha, SUM(c.ingresos) AS ingresos, SUM(c.egresos) AS egresos
           FROM cortes_caja c GROUP BY c.fecha
       )
   SELECT DATE_FORMAT(fecha, '%Y-%m-%d') AS dia,
          ingresos, egresos,
          SUM(ingresos - egresos) OVER (ORDER BY fecha) AS acumulado,
          LAG(ingresos) OVER (ORDER BY fecha) AS ingresos_dia_antes
     FROM diario
    ORDER BY fecha DESC
    LIMIT 10`),
  {}, (f) => f.length === 10 && f[0].acumulado !== undefined);

await caso('mysql_test', 'Descuadres con parámetros',
  sql(`SELECT j.nombre AS cajero, a.nombre AS agencia, c.fecha, c.diferencia, c.observacion
  FROM cortes_caja c
  JOIN cajeros j ON j.id = c.cajero_id
  JOIN agencias a ON a.id = j.agencia_id
 WHERE c.diferencia <> 0 AND a.zona = :zona
 ORDER BY ABS(c.diferencia) DESC`),
  { zona: 'Zonal Cuenca' }, (f) => f.length > 0 && f[0].diferencia !== undefined);

console.log('\nMYSQL — lo que escribe\n');
for (const [titulo, texto] of [
  ['REPLACE INTO', "REPLACE INTO agencias (id, nombre, zona) VALUES (1, 'x', 'y')"],
  ['UPDATE', 'UPDATE cortes_caja SET diferencia = 0'],
  ['DELETE', 'DELETE FROM cortes_caja'],
  ['TRUNCATE', 'TRUNCATE TABLE cortes_caja'],
]) await rechaza('mysql_test', titulo, sql(texto));

// ── Limpieza ──────────────────────────────────────────────────────────────────
await pool.query("DELETE FROM article_executions WHERE article_id IN (SELECT id::text FROM knowledge_base WHERE titulo LIKE '[motor]%')");
await pool.query("DELETE FROM article_changes WHERE article_id IN (SELECT id::text FROM knowledge_base WHERE titulo LIKE '[motor]%')");
const quitados = await pool.query("DELETE FROM knowledge_base WHERE titulo LIKE '[motor]%'");
await pool.query('DELETE FROM sessions WHERE token = $1', [token]);
await pool.end();

console.log(`\n  (quitados ${quitados.rowCount} artículos de prueba)`);
console.log('\n' + (fallos === 0
  ? '════ LOS TRES MOTORES: LO COMPLICADO PASA, LO QUE ESCRIBE NO ════'
  : `════ ${fallos} FALLO(S) ════`));
process.exit(fallos === 0 ? 0 : 1);
