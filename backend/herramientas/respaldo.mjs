/**
 * Respaldo del contenido que no se puede regenerar.
 *
 * El respaldo bueno es el del servicio `db-backup` del docker-compose: un pg_dump
 * completo, con esquema e índices. Esto es otra cosa y más modesta: saca a un JSON el
 * contenido que la gente escribió —los temas de ayuda, los aportes, los tickets con
 * sus pasos, la Biblioteca SQL, los comentarios y lo aprendido— y funciona hoy, desde
 * cualquier máquina con Node, sin Docker y sin que el pg_dump del host tenga que
 * coincidir de versión con el servidor.
 *
 * Sirve para dos cosas concretas:
 *   - tener una copia antes de abrir el sistema a la gente, hoy mismo
 *   - poder mirar qué había el día X sin levantar nada
 *
 * Lo que NO hace: no guarda el esquema, ni los índices, ni los usuarios, ni las
 * contraseñas. No sustituye al pg_dump. Si solo tiene esto, tiene el contenido pero
 * tendría que recrear la base.
 *
 *   node herramientas/respaldo.mjs
 *   node herramientas/respaldo.mjs ../respaldos      (otra carpeta de salida)
 */
import pg from 'pg';
import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';

/** Las tablas cuyo contenido escribieron personas y no se puede recalcular. */
const TABLAS = [
  'public.knowledge_base',
  'public.comments',
  'soporte.aportes',
  'soporte.tickets',
  'soporte.ticket_pasos',
  'soporte.consultas',
  'soporte.diagnosticos',
  'soporte.aprendizaje',
  'public.semantic_learning',
];

const salida = process.argv[2] || path.join('..', 'respaldos');

const base = JSON.parse(await import('node:fs').then((m) => m.promises.readFile('config/multidb.databases.json', 'utf8')))
  .databases.find((d) => d.id === 'pg_main');
if (!base) {
  console.error('No hay una base con id pg_main en config/multidb.databases.json');
  process.exit(1);
}

const pool = new pg.Pool({
  host: base.host, port: base.port, database: base.database,
  user: base.user, password: base.password,
});

const copia = { generado: new Date().toISOString(), base: base.database, tablas: {} };
let filasTotales = 0;
const faltantes = [];

for (const tabla of TABLAS) {
  try {
    const r = await pool.query(`SELECT * FROM ${tabla}`);
    copia.tablas[tabla] = r.rows;
    filasTotales += r.rowCount;
    console.log(`  ${tabla.padEnd(28)} ${String(r.rowCount).padStart(5)} filas`);
  } catch (error) {
    // Una tabla que no existe no debe tumbar el respaldo de las demás, pero sí
    // tiene que constar: un respaldo que calla lo que no pudo copiar no sirve.
    faltantes.push(`${tabla}: ${error.message}`);
    console.log(`  ${tabla.padEnd(28)}     — no se pudo: ${error.message}`);
  }
}
if (faltantes.length) copia.noCopiado = faltantes;

mkdirSync(salida, { recursive: true });
const sello = new Date().toISOString().replace(/[-:]/g, '').replace(/\..+/, '').replace('T', '-');
const destino = path.join(salida, `contenido-${sello}.json`);
writeFileSync(destino, JSON.stringify(copia, null, 1), 'utf8');

const kb = Math.round(Buffer.byteLength(JSON.stringify(copia)) / 1024);
console.log(`\n  ${filasTotales} filas de ${Object.keys(copia.tablas).length} tabla(s) -> ${destino} (${kb} KB)`);
if (faltantes.length) console.log(`  ATENCION: ${faltantes.length} tabla(s) no se pudieron copiar (ver "noCopiado" en el fichero)`);

await pool.end();
