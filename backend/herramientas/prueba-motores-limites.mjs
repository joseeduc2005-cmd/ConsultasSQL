/**
 * Hasta dónde llega: lo más exótico de cada motor, y lo más peligroso.
 *
 * `prueba-motores.mjs` cubre lo que se escribe a diario. Esto cubre lo otro: las formas
 * raras que aparecen una vez al año y las que son peligrosas justamente porque no lo
 * parecen — un SELECT de Oracle que lleva PL/SQL dentro, un agregado de Mongo que ejecuta
 * JavaScript en el servidor.
 *
 * Decir «aguanta cualquier script» sin haber probado esto sería decirlo de más.
 *
 *   node herramientas/prueba-motores-limites.mjs
 */
import pg from 'pg';
import { readFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';

const BASE = 'http://127.0.0.1:3002';
const cfg = JSON.parse(readFileSync('config/multidb.databases.json', 'utf8'));
const m = cfg.databases.find((d) => d.id === 'pg_main');
const pool = new pg.Pool({ host: m.host, port: m.port, database: m.database, user: m.user, password: m.password });
const { rows: [admin] } = await pool.query("SELECT id FROM users WHERE role = 'admin' LIMIT 1");
const token = 'limites-' + randomUUID();
await pool.query('INSERT INTO sessions (token, user_id, activo, created_at) VALUES ($1,$2,true,NOW())', [token, admin.id]);
const H = { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` };

let fallos = 0;
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

async function correr(baseId, titulo, sqlTexto, datos) {
  const alta = await traer(`${BASE}/api/articles`, {
    method: 'POST',
    headers: H,
    body: JSON.stringify({
      titulo: `[limite] ${titulo}`,
      categoria: 'Prueba de límites',
      descripcion: 'Creado por la prueba de límites.',
      contenido_md: 'Se borra al terminar.',
      tipo_solucion: 'dinamico',
      visible_para: 'admin',
      script_json: { modo: 'script', origen: 'manual-sql', sql: sqlTexto, target_database: baseId, databaseId: baseId },
    }),
  });
  const jAlta = await alta.json();
  if (!alta.ok) return { guardado: false, motivo: String(jAlta.error || '').slice(0, 110) };
  const id = jAlta.data?.id || jAlta.article?.id || jAlta.id;
  const ej = await traer(`${BASE}/api/execute`, {
    method: 'POST', headers: H, body: JSON.stringify({ articleId: id, formData: datos ?? {} }),
  });
  const jEj = await ej.json();
  return {
    guardado: true,
    ejecutado: ej.ok,
    motivo: ej.ok ? '' : String(jEj.message || '').slice(0, 110),
    filas: jEj?.result?.resultado ?? [],
  };
}

/** Tiene que ejecutarse y devolver algo. */
async function pasa(baseId, titulo, sqlTexto, esperado = (f) => f.length > 0) {
  const r = await correr(baseId, titulo, sqlTexto);
  const bien = r.guardado && r.ejecutado && esperado(r.filas);
  if (!bien) fallos += 1;
  const detalle = !r.guardado ? `NO SE GUARDA · ${r.motivo}`
    : !r.ejecutado ? `NO EJECUTA · ${r.motivo}`
      : `${r.filas.length} fila(s)`;
  console.log(`  ${bien ? 'ok  ' : 'MAL '} ${titulo.padEnd(46)} ${detalle}`);
}

/** No puede llegar a ejecutarse. */
async function frena(baseId, titulo, sqlTexto) {
  const r = await correr(baseId, titulo, sqlTexto);
  const bien = !r.guardado;
  if (!bien) fallos += 1;
  console.log(`  ${bien ? 'ok  ' : 'MAL '} ${titulo.padEnd(46)} ${bien ? r.motivo : (r.ejecutado ? 'SE EJECUTÓ' : 'se guardó; fallo al ejecutar')}`);
}

// ══ ORACLE: lo exótico ═══════════════════════════════════════════════════════
console.log('\nORACLE — formas que aparecen una vez al año\n');

await pasa('oracle_local', 'MATCH_RECOGNIZE: rachas de tickets al alza',
  `SELECT *
     FROM (SELECT TRUNC(abierto_en) AS dia, COUNT(*) AS cuantos
             FROM tickets GROUP BY TRUNC(abierto_en))
    MATCH_RECOGNIZE (
      ORDER BY dia
      MEASURES FIRST(dia) AS desde, LAST(dia) AS hasta, COUNT(*) AS dias
      ONE ROW PER MATCH
      PATTERN (sube+)
      DEFINE sube AS cuantos >= PREV(cuantos)
    )`);

await pasa('oracle_local', 'MODEL: proyección sobre una serie',
  `SELECT mes, cuantos
     FROM (SELECT CAST(TO_CHAR(abierto_en, 'YYYY-MM') AS VARCHAR2(20)) AS mes, COUNT(*) AS cuantos
             FROM tickets GROUP BY TO_CHAR(abierto_en, 'YYYY-MM'))
    MODEL
      DIMENSION BY (mes)
      MEASURES (cuantos)
      RULES (cuantos['PROYECCION'] = AVG(cuantos)[ANY])
    ORDER BY mes`);

await pasa('oracle_local', 'UNPIVOT: pasar columnas a filas',
  `SELECT * FROM (
     SELECT prioridad,
            SUM(CASE WHEN tipo = 'Solicitud' THEN 1 ELSE 0 END) AS solicitudes,
            SUM(CASE WHEN tipo = 'Incidencia' THEN 1 ELSE 0 END) AS incidencias
       FROM tickets GROUP BY prioridad
   )
   UNPIVOT (cuantos FOR clase IN (solicitudes, incidencias))
   ORDER BY prioridad, clase`);

await pasa('oracle_local', 'JSON_TABLE sobre un JSON armado al vuelo',
  `SELECT jt.area, jt.cuantos
     FROM (SELECT JSON_ARRAYAGG(JSON_OBJECT('area' VALUE c.nivel1, 'cuantos' VALUE cnt)) AS doc
             FROM (SELECT c2.nivel1, COUNT(*) AS cnt
                     FROM tickets t2 JOIN categorias c2 ON c2.id = t2.categoria_id
                    GROUP BY c2.nivel1) c) d,
          JSON_TABLE(d.doc, '$[*]' COLUMNS (area VARCHAR2(120) PATH '$.area',
                                            cuantos NUMBER PATH '$.cuantos')) jt
    ORDER BY jt.cuantos DESC`);

await pasa('oracle_local', 'Comillas alternativas q\'[...]\' con dos puntos dentro',
  `SELECT q'[formato HH24:MI, ojo con los dos puntos]' AS nota, COUNT(*) AS tickets
     FROM tickets`);

await pasa('oracle_local', 'Jerarquía con CONNECT_BY_ROOT y LISTAGG anidado',
  `SELECT CONNECT_BY_ROOT g.nombre AS raiz,
          LISTAGG(g.nombre, ' / ') WITHIN GROUP (ORDER BY g.nivel) AS rama
     FROM grupos g
    START WITH g.padre_id IS NULL
    CONNECT BY PRIOR g.id = g.padre_id
    GROUP BY CONNECT_BY_ROOT g.nombre`);

await pasa('oracle_local', 'Consulta larga: seis CTE encadenadas',
  `WITH base AS (SELECT t.*, c.nivel1 AS area FROM tickets t JOIN categorias c ON c.id = t.categoria_id),
        porArea AS (SELECT area, COUNT(*) AS n, AVG(resolucion_min) AS prom FROM base GROUP BY area),
        peores AS (SELECT area, n, prom, RANK() OVER (ORDER BY prom DESC) AS puesto FROM porArea),
        conSocio AS (SELECT b.area, COUNT(ts.ticket_id) AS con_socio FROM base b
                       LEFT JOIN ticket_socio ts ON ts.ticket_id = b.id GROUP BY b.area),
        conTarea AS (SELECT b.area, COUNT(DISTINCT x.ticket_id) AS con_tarea FROM base b
                       LEFT JOIN tareas x ON x.ticket_id = b.id GROUP BY b.area),
        junto AS (SELECT p.area, p.n, ROUND(p.prom,1) AS prom, p.puesto,
                         s.con_socio, r.con_tarea
                    FROM peores p JOIN conSocio s ON s.area = p.area
                                  JOIN conTarea r ON r.area = p.area)
   SELECT * FROM junto WHERE puesto <= 8 ORDER BY puesto`);

console.log('\nORACLE — lo peligroso que no lo parece\n');

await frena('oracle_local', 'WITH FUNCTION: un SELECT que lleva PL/SQL dentro',
  `WITH FUNCTION tocar(p NUMBER) RETURN NUMBER IS
     PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN
     DELETE FROM tareas WHERE id = p;
     COMMIT;
     RETURN p;
   END;
   SELECT tocar(id) FROM tickets WHERE ROWNUM = 1`);

await frena('oracle_local', 'WITH PROCEDURE dentro de un SELECT',
  `WITH PROCEDURE p IS BEGIN DELETE FROM tareas; END;
   SELECT 1 FROM dual`);

await frena('oracle_local', 'INSERT ALL multitabla',
  "INSERT ALL INTO tareas (id, ticket_id) VALUES (999, 1) SELECT * FROM dual");

await frena('oracle_local', 'Comentario delante de un bloque',
  "/* informe mensual */ BEGIN UPDATE tickets SET estado = 'x'; END;");

// ══ MONGO: lo exótico y lo peligroso ═════════════════════════════════════════
console.log('\nMONGODB — agregados de los que duelen\n');

await pasa('mongo_local', '$facet: tres análisis en una pasada',
  `{ collection: 'conciliaciones', aggregate: [
     { $facet: {
         porEstado: [ { $group: { _id: '$statusConciliation', n: { $sum: 1 } } } ],
         totales:   [ { $group: { _id: null, debito: { $sum: '$debitTotal' }, credito: { $sum: '$creditTotal' } } } ],
         peores:    [ { $sort: { transactionError: -1 } }, { $limit: 3 },
                      { $project: { cutOffDate: 1, transactionError: 1 } } ]
     } }
   ] }`);

await pasa('mongo_local', '$unwind + $group + $sort + $limit encadenados',
  `{ collection: 'conciliaciones', aggregate: [
     { $unwind: '$details' },
     { $group: { _id: { obs: '$details.observation', tipo: '$details.transactionTypeSwitch' },
                 n: { $sum: 1 }, valor: { $sum: '$details.transactionValueSwitch' } } },
     { $project: { _id: 0, observacion: '$_id.obs', tipo: '$_id.tipo', n: 1, valor: 1 } },
     { $sort: { n: -1 } },
     { $limit: 8 }
   ] }`);

await pasa('mongo_local', '$lookup consigo misma y $addFields',
  `{ collection: 'conciliaciones', aggregate: [
     { $limit: 5 },
     { $lookup: { from: 'conciliaciones', localField: 'institutionCode',
                  foreignField: 'institutionCode', as: 'hermanas' } },
     { $addFields: { cuantasHermanas: { $size: '$hermanas' } } },
     { $project: { cutOffDate: 1, cuantasHermanas: 1 } }
   ] }`);

await pasa('mongo_local', 'Operadores de fecha y $switch',
  `{ collection: 'conciliaciones', aggregate: [
     { $project: {
         mes: { $dateToString: { format: '%Y-%m', date: '$conciliationDate' } },
         salud: { $switch: { branches: [
             { case: { $eq: ['$transactionError', 0] }, then: 'limpia' },
             { case: { $lt: ['$transactionError', 2] }, then: 'con roces' } ],
           default: 'con problemas' } } } },
     { $group: { _id: { mes: '$mes', salud: '$salud' }, n: { $sum: 1 } } },
     { $sort: { '_id.mes': 1 } }
   ] }`);

console.log('\nMONGODB — lo que ejecuta código o escribe\n');

await frena('mongo_local', '$function: JavaScript en el servidor',
  `{ collection: 'conciliaciones', aggregate: [
     { $addFields: { x: { $function: { body: 'function(){ return 1; }', args: [], lang: 'js' } } } }
   ] }`);

await frena('mongo_local', '$where con JavaScript',
  `{ collection: 'conciliaciones', find: { $where: 'this.transactionCount > 1' } }`);

await frena('mongo_local', '$accumulator con código',
  `{ collection: 'conciliaciones', aggregate: [
     { $group: { _id: null, t: { $accumulator: { init: 'function(){return 0}', accumulate: 'function(s){return s}',
       accumulateArgs: [], merge: 'function(a,b){return a+b}', lang: 'js' } } } }
   ] }`);

await frena('mongo_local', '$out escondido al final de un pipeline largo',
  `{ collection: 'conciliaciones', aggregate: [
     { $match: {} }, { $unwind: '$details' }, { $group: { _id: '$details.id', n: { $sum: 1 } } },
     { $sort: { n: -1 } }, { $out: 'copia_oculta' }
   ] }`);

await frena('mongo_local', 'mapReduce',
  `{ collection: 'conciliaciones', mapReduce: { map: 'function(){}', reduce: 'function(){}', out: 'copia' } }`);

// ── Limpieza ──────────────────────────────────────────────────────────────────
await pool.query("DELETE FROM article_executions WHERE article_id IN (SELECT id::text FROM knowledge_base WHERE titulo LIKE '[limite]%')");
await pool.query("DELETE FROM article_changes WHERE article_id IN (SELECT id::text FROM knowledge_base WHERE titulo LIKE '[limite]%')");
const quitados = await pool.query("DELETE FROM knowledge_base WHERE titulo LIKE '[limite]%'");
await pool.query('DELETE FROM sessions WHERE token = $1', [token]);
await pool.end();

console.log(`\n  (quitados ${quitados.rowCount} artículos de prueba)`);
console.log('\n' + (fallos === 0 ? '════ TAMBIÉN EN LOS BORDES ════' : `════ ${fallos} FALLO(S) ════`));
process.exit(fallos === 0 ? 0 : 1);
