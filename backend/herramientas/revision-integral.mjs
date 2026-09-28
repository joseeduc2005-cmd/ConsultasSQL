/**
 * Revisión de punta a punta, contra la API real y sin Oracle.
 *
 * Levanta el router sobre la base de conocimiento y recorre el ciclo entero como lo
 * haría una persona: registra un caso de un sistema que no existía, lo resuelve, lo
 * publica, y comprueba que un caso nuevo parecido lo encuentra.
 *
 * Es la prueba que no pueden dar los tests unitarios, porque lo que se verifica aquí
 * es que las piezas encajen y que nada esté atado a un dominio concreto.
 *
 * Uso, desde `backend/`:  node herramientas/revision-integral.mjs
 */
import 'dotenv/config';
import express from 'express';
import pg from 'pg';
import { readFileSync } from 'node:fs';

const b = JSON.parse(readFileSync('config/multidb.databases.json', 'utf8')).databases.find((x) => x.id === 'pg_main');
const pool = new pg.Pool({ host: b.host, port: b.port, database: b.database, user: b.user, password: b.password });
const { configurarPool, aplicarEsquema } = await import('../src/soporte/knowledge/db.ts');
configurarPool(() => pool);
await aplicarEsquema();

const admin = (await pool.query("SELECT id, username FROM users WHERE role = 'admin' LIMIT 1")).rows[0];
const { crearSoporteRouter } = await import('../src/soporte/rutas.ts');

const app = express();
app.use(express.json());
app.use((req, _res, next) => {
  req.usuarioSoporte = { id: admin.id, username: admin.username };
  next();
});
app.use('/api/soporte', crearSoporteRouter());
app.use((err, _req, res, _next) => res.status(err.estado ?? 500).json({ error: err.message }));

const servidor = app.listen(0);
const puerto = servidor.address().port;
const api = (ruta, metodo = 'GET', cuerpo) =>
  fetch(`http://127.0.0.1:${puerto}/api/soporte${ruta}`, {
    method: metodo,
    headers: { 'Content-Type': 'application/json' },
    ...(cuerpo ? { body: JSON.stringify(cuerpo) } : {}),
  }).then((r) => r.json());

const fallos = [];
const comprobar = (ok, que, detalle = '') => {
  console.log(`  ${ok ? 'OK  ' : 'FALLA'} ${que}${detalle ? ' · ' + detalle : ''}`);
  if (!ok) fallos.push(que);
};

// Un sistema que no existe en ningún sitio del código: si funciona, es global.
const SISTEMA = 'Tesorería Web';
await pool.query("DELETE FROM soporte.tickets WHERE numero LIKE 'REV-%' OR numero LIKE 'CASO-%'");
await pool.query("DELETE FROM knowledge_base WHERE titulo LIKE 'Tesorería Web%'");

console.log('\n1. REGISTRAR — un caso de un sistema nuevo, con lo mínimo');
const t1 = await api('/tickets', 'POST', {
  titulo: 'La conciliación de tesorería se queda cargando y nunca termina',
  descripcion: 'La analista dice que al generar la conciliación del día la pantalla se queda girando y nunca muestra nada. Pasa desde el cierre de mes.',
  sistema: SISTEMA,
});
comprobar(Boolean(t1.numero), 'se crea sin número ni base', t1.numero);
comprobar(t1.sistema === SISTEMA, 'acepta un sistema que no estaba en ninguna lista', t1.sistema);

console.log('\n2. REVISAR — sin Oracle, responde con conocimiento');
const rev = await api(`/tickets/${t1.numero}/revisar`, 'POST');
comprobar(Array.isArray(rev.precedentes), 'la revisión devuelve precedentes');
comprobar(typeof rev.motivo === 'string' && !/oracle/i.test(rev.motivo ?? '') || rev.ejecutado, 'no promete Oracle si no la hay', rev.motivo?.slice(0, 48));

console.log('\n3. RESOLVER Y PUBLICAR — lo que aprende el equipo vuelve a la base');
await api(`/tickets/${t1.numero}`, 'PATCH', {
  estado: 'resuelto',
  causa_raiz: 'El proceso de conciliación no tiene índice en la fecha de corte y con el volumen del cierre de mes la consulta nunca termina.',
  solucion: 'Pedir a base de datos el índice sobre la fecha de corte. Mientras tanto, generar la conciliación por rangos de una semana.',
});
const art = await api(`/tickets/${t1.numero}/publicar`, 'POST');
comprobar(Boolean(art.id), 'la solución se publica como artículo', art.titulo);
comprobar(String(art.titulo).startsWith(SISTEMA), 'queda clasificado en su sistema, no en uno inventado');

console.log('\n4. ENCONTRAR — otra persona, otras palabras, mismo problema');
const busqueda = await api('/solucionar', 'POST', {
  texto: 'La conciliacion del area de tesoreria no termina de cargar, se queda pensando',
});
const encontrado = (busqueda.precedentes ?? []).some((p) => String(p.titulo).includes('conciliación'));
comprobar(encontrado, 'lo encuentra sin haber creado ticket', `${busqueda.precedentes?.length ?? 0} resultados`);

console.log('\n5. AISLAMIENTO — lo que ve un cliente no es lo que ve un admin');
const { buscarPrecedentes } = await import('../src/soporte/precedentes.ts');
const q = 'no puede ingresar clave bloqueada';
const comoAdmin = (await buscarPrecedentes(q, { limite: 30 })).filter((p) => p.tipo === 'articulo').length;
const comoCliente = (await buscarPrecedentes(q, { limite: 30, soloPublicos: true })).filter((p) => p.tipo === 'articulo').length;
comprobar(comoCliente < comoAdmin, 'el filtro de visibilidad recorta', `admin ${comoAdmin} · cliente ${comoCliente}`);

console.log('\n6. SISTEMAS — la lista sale de los datos');
const sistemas = await api('/sistemas');
comprobar(sistemas.some((s) => s.nombre === SISTEMA), 'el sistema nuevo aparece solo', `${sistemas.length} en total`);

console.log('\n7. ESTADO — dice la verdad sobre lo que hay');
const estado = await api('/estado');
comprobar(typeof estado.correccionesEjecutables === 'boolean', 'informa si la escritura está encendida', String(estado.correccionesEjecutables));
comprobar('baseDiagnosticos' in estado, 'informa contra qué base investiga', String(estado.baseDiagnosticos));

// Limpieza.
await pool.query("DELETE FROM knowledge_base WHERE titulo LIKE 'Tesorería Web%'");
await pool.query('DELETE FROM soporte.tickets WHERE numero = $1', [t1.numero]);
await new Promise((listo) => servidor.close(listo));
await pool.end();

console.log('\n' + '-'.repeat(64));
console.log(fallos.length === 0 ? 'TODO EN ORDEN' : `${fallos.length} FALLO(S): ${fallos.join(' · ')}`);
process.exitCode = fallos.length === 0 ? 0 : 1;
