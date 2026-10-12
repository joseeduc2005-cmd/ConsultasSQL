/**
 * Comprueba lo que un usuario normal puede y no puede hacer.
 *
 * Lo que se verifica no es la pantalla, es la API: que un caso solo lo vea quien lo
 * reportó, que la búsqueda no devuelva artículos internos, y que el autor salga de la
 * sesión y no de lo que mande el navegador.
 *
 * Uso, desde `backend/`:  node herramientas/prueba-cliente.mjs
 */
import 'dotenv/config';
import express from 'express';
import pg from 'pg';
import { readFileSync } from 'node:fs';

const b = JSON.parse(readFileSync('config/multidb.databases.json', 'utf8')).databases.find((x) => x.id === 'pg_main');
const pool = new pg.Pool({ host: b.host, port: b.port, database: b.database, user: b.user, password: b.password });
const { configurarPool } = await import('../src/soporte/knowledge/db.ts');
configurarPool(() => pool);
const { crearSoporteClienteRouter } = await import('../src/soporte/adaptadores/http/rutas-cliente.ts');

// Dos personas distintas; la sesión decide quién es, como en el backend real.
let actual = { id: '1', username: 'ana.prueba' };
const app = express();
app.use(express.json());
app.use((req, _res, next) => {
  req.usuarioSoporte = actual;
  next();
});
app.use('/api/soporte-cliente', crearSoporteClienteRouter());
app.use((err, _req, res, _next) => res.status(err.estado ?? 500).json({ error: err.message }));

const servidor = app.listen(0);
const puerto = servidor.address().port;
const api = (ruta, metodo = 'GET', cuerpo) =>
  fetch(`http://127.0.0.1:${puerto}/api/soporte-cliente${ruta}`, {
    method: metodo,
    headers: { 'Content-Type': 'application/json' },
    ...(cuerpo ? { body: JSON.stringify(cuerpo) } : {}),
  }).then(async (r) => ({ estado: r.status, cuerpo: await r.json() }));

const fallos = [];
const comprobar = (ok, que, detalle = '') => {
  console.log(`  ${ok ? 'OK  ' : 'FALLA'} ${que}${detalle ? ' · ' + detalle : ''}`);
  if (!ok) fallos.push(que);
};

await pool.query("DELETE FROM soporte.tickets WHERE autor IN ('ana.prueba','beto.prueba')");

console.log('\nBUSCAR');
const buscado = await api('/solucionar', 'POST', { texto: 'no puedo ingresar me sale que la clave esta bloqueada' });
const internos = await pool.query(
  "SELECT count(*)::int n FROM knowledge_base WHERE visible_para = 'admin' AND id::text = ANY($1)",
  [(buscado.cuerpo.precedentes ?? []).filter((p) => p.tipo === 'articulo').map((p) => p.id)],
);
comprobar(internos.rows[0].n === 0, 'no devuelve artículos internos', `${buscado.cuerpo.precedentes?.length ?? 0} resultados`);

console.log('\nEXPLORAR POR TEMA');
const temas = await api('/temas');
const idsVisibles = temas.cuerpo.flatMap((t) => t.articulos.map((a) => a.id));
const internosEnArbol = await pool.query(
  "SELECT count(*)::int n FROM knowledge_base WHERE visible_para = 'admin' AND id::text = ANY($1)",
  [idsVisibles],
);
comprobar(
  internosEnArbol.rows[0].n === 0,
  'el arbol no trae temas internos',
  `${temas.cuerpo.length} categorias, ${idsVisibles.length} temas`,
);

const interno = await pool.query("SELECT id::text FROM knowledge_base WHERE visible_para = 'admin' LIMIT 1");
if (interno.rows[0]) {
  const directo = await api(`/temas/${interno.rows[0].id}`);
  comprobar(directo.estado === 404, 'pedir un tema interno por su id no lo devuelve', `HTTP ${directo.estado}`);
}

console.log('\nREPORTAR');
const creado = await api('/casos', 'POST', {
  titulo: 'No me carga el listado de aprobaciones',
  descripcion: 'Entro al módulo y la tabla se queda vacía.',
  // Se intenta suplantar al autor a propósito: no debe tener efecto.
  autor: 'otra.persona',
});
comprobar(creado.estado === 201 && Boolean(creado.cuerpo.numero), 'se crea el caso', creado.cuerpo.numero);
const fila = await pool.query('SELECT autor FROM soporte.tickets WHERE numero = $1', [creado.cuerpo.numero]);
comprobar(fila.rows[0]?.autor === 'ana.prueba', 'el autor sale de la sesión, no del cuerpo', fila.rows[0]?.autor);
comprobar(!('causa_raiz' in creado.cuerpo) || creado.cuerpo.causa_raiz === null, 'no expone la investigación interna');

console.log('\nVER LO PROPIO');
const mios = await api('/casos');
comprobar(mios.cuerpo.some((c) => c.numero === creado.cuerpo.numero), 'ana ve su caso', `${mios.cuerpo.length} caso(s)`);

actual = { id: '2', username: 'beto.prueba' };
const ajeno = await api(`/casos/${creado.cuerpo.numero}`);
comprobar(ajeno.estado === 404, 'beto NO puede ver el caso de ana', `HTTP ${ajeno.estado}`);
const deBeto = await api('/casos');
comprobar(deBeto.cuerpo.length === 0, 'el listado de beto no trae los de ana');

await pool.query("DELETE FROM soporte.tickets WHERE autor IN ('ana.prueba','beto.prueba')");
await new Promise((listo) => servidor.close(listo));
await pool.end();

console.log('\n' + '-'.repeat(56));
console.log(fallos.length === 0 ? 'AISLAMIENTO CORRECTO' : `${fallos.length} FALLO(S): ${fallos.join(' · ')}`);
process.exitCode = fallos.length === 0 ? 0 : 1;
