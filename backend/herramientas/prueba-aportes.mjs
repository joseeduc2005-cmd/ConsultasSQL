/**
 * Comprueba el circuito de los aportes de punta a punta.
 *
 * Lo que interesa no es la pantalla: es que nada de lo que manda un usuario aparezca
 * como tema de ayuda sin que alguien lo acepte, que al aceptarlo sí aparezca, y que un
 * aporte solo lo vea quien lo mandó.
 *
 * Uso, desde `backend/`:  node herramientas/prueba-aportes.mjs
 *
 * Limpia únicamente lo que crea, reconocible por el prefijo de abajo. Borrar por
 * patrones más anchos ya se llevó por delante datos reales una vez.
 */
import 'dotenv/config';
import express from 'express';
import pg from 'pg';
import { readFileSync } from 'node:fs';

const MARCA = '[prueba-aportes]';

const b = JSON.parse(readFileSync('config/multidb.databases.json', 'utf8')).databases.find((x) => x.id === 'pg_main');
const pool = new pg.Pool({ host: b.host, port: b.port, database: b.database, user: b.user, password: b.password });
const { configurarPool, aplicarEsquema } = await import('../src/soporte/knowledge/db.ts');
configurarPool(() => pool);
await aplicarEsquema();

const { crearSoporteClienteRouter } = await import('../src/soporte/adaptadores/http/rutas-cliente.ts');
const { crearSoporteRouter } = await import('../src/soporte/adaptadores/http/rutas-admin.ts');

// `knowledge_base.creado_por` es una clave foránea contra users, así que el revisor
// tiene que existir de verdad. Se toma un admin real de la base; los dos usuarios que
// aportan sí son inventados, porque su nombre solo se guarda como texto.
const [revisor] = (await pool.query("SELECT id, username FROM users WHERE role IN ('admin','superadmin') LIMIT 1")).rows;
if (!revisor) throw new Error('No hay ningún admin en la base contra el que probar.');

let actual = { id: revisor.id, username: 'ana.prueba' };
const app = express();
app.use(express.json());
app.use((req, _res, next) => {
  req.usuarioSoporte = actual;
  next();
});
app.use('/api/soporte-cliente', crearSoporteClienteRouter());
app.use('/api/soporte', crearSoporteRouter());
app.use((err, _req, res, _next) => res.status(err.estado ?? 500).json({ error: err.message }));

const servidor = app.listen(0);
const puerto = servidor.address().port;
const pedir = (base) => (ruta, metodo = 'GET', cuerpo) =>
  fetch(`http://127.0.0.1:${puerto}${base}${ruta}`, {
    method: metodo,
    headers: { 'Content-Type': 'application/json' },
    ...(cuerpo ? { body: JSON.stringify(cuerpo) } : {}),
  }).then(async (r) => ({ estado: r.status, cuerpo: await r.json() }));

const cliente = pedir('/api/soporte-cliente');
const panel = pedir('/api/soporte');

let fallos = 0;
const comprobar = (condicion, texto) => {
  console.log(`${condicion ? '  ok  ' : ' FALLA'}  ${texto}`);
  if (!condicion) fallos++;
};

const titulo = `${MARCA} El listado de aprobaciones sale vacío al filtrar`;

try {
  // 1. Un usuario manda su solución.
  const creado = await cliente('/aportes', 'POST', {
    titulo,
    problema: 'Al filtrar por fecha la tabla queda en blanco, sin error.',
    pasos: [
      { descripcion: 'Entré a Configuración - Periodos y busqué el mes', script: null, efecto: null },
      { descripcion: 'Comprobé cuáles estaban abiertos', script: 'SELECT * FROM periodos WHERE estado = 1;', efecto: 'consulta' },
      { descripcion: 'Lo reabrí y la tabla volvió a cargar', script: 'UPDATE periodos SET estado = 1 WHERE id = 42;', efecto: 'cambio' },
    ],
    base: 'pg_main',
  });
  comprobar(creado.estado === 201 && creado.cuerpo.estado === 'pendiente', 'se registra como pendiente, no publicado');
  const id = creado.cuerpo.id;

  // 2. Lo que manda uno no lo ve otro.
  actual = { id: revisor.id, username: 'bea.prueba' };
  const ajenos = await cliente('/aportes');
  comprobar(!ajenos.cuerpo.some((a) => a.id === id), 'otro usuario no ve el aporte ajeno');

  // 3. Y todavía no es tema de ayuda para nadie.
  const antes = await cliente('/temas');
  const estaAntes = antes.cuerpo.some((c) => c.articulos.some((a) => a.titulo === titulo));
  comprobar(!estaAntes, 'no aparece como tema mientras está pendiente');

  // 4. Aparece en la bandeja de revisión del panel.
  const bandeja = await panel('/aportes?estado=pendiente');
  comprobar(bandeja.cuerpo.some((a) => a.id === id), 'aparece en la bandeja de revisión');

  // 5. Rechazar sin motivo no se puede: el autor tiene que saber por qué.
  const sinMotivo = await panel(`/aportes/${id}/rechazar`, 'POST', {});
  comprobar(sinMotivo.estado >= 400, 'no deja rechazar sin motivo');

  // 6. Al aceptarlo sí se publica.
  actual = { id: revisor.id, username: revisor.username };
  const aceptado = await panel(`/aportes/${id}/aceptar`, 'POST', {
    titulo,
    categoria: `${MARCA} Aportes`,
    visible: 'todos',
  });
  comprobar(aceptado.estado === 200 && aceptado.cuerpo.articulo, `aceptar crea el artículo${aceptado.estado === 200 ? '' : ` — ${JSON.stringify(aceptado.cuerpo)}`}`);

  actual = { id: revisor.id, username: 'ana.prueba' };
  const despues = await cliente('/temas');
  comprobar(
    despues.cuerpo.some((c) => c.articulos.some((a) => a.titulo === titulo)),
    'ya sale como tema de ayuda para el usuario',
  );

  // 7. El texto del aporte llegó entero al artículo, sentencia incluida.
  const tema = await cliente(`/temas/${aceptado.cuerpo.articulo}`);
  comprobar(tema.cuerpo.contenido?.includes('Lo reabrí'), 'el artículo conserva los pasos');
  comprobar(/1\..*2\..*3\./s.test(tema.cuerpo.contenido ?? ''), 'los pasos quedan numerados y en orden');
  comprobar(tema.cuerpo.contenido?.includes('Cambia datos'), 'se ve cuál de los pasos cambia datos');
  comprobar(tema.cuerpo.contenido?.includes('SELECT * FROM periodos'), 'el artículo conserva los scripts');
  comprobar(
    creado.cuerpo.sql_sugerido?.startsWith('UPDATE'),
    'la sentencia principal es la que cambia datos, no la primera que haya',
  );
  comprobar(tema.cuerpo.contenido?.includes('ana.prueba'), 'queda dicho quién lo aportó');

  // 8. El autor ve en qué quedó.
  const mios = await cliente('/aportes');
  comprobar(mios.cuerpo.find((a) => a.id === id)?.estado === 'aceptado', 'el autor ve que se publicó');

  // 9. No se publica dos veces.
  const repetido = await panel(`/aportes/${id}/aceptar`, 'POST', {});
  comprobar(repetido.estado >= 400, 'no deja publicar el mismo aporte dos veces');
} finally {
  await pool.query('DELETE FROM soporte.aportes WHERE titulo LIKE $1', [`${MARCA}%`]);
  await pool.query('DELETE FROM knowledge_base WHERE titulo LIKE $1', [`${MARCA}%`]);
  // Cerrar el servidor antes que el pool, y esperar a que termine: al revés, Node
  // Un respiro, cerrar el servidor y recién después el pool. En Windows, sin eso
  // quedan sockets del keep-alive de fetch a medio soltar y Node aborta al salir,
  // con las comprobaciones ya en verde.
  await new Promise((listo) => setTimeout(listo, 100));
  servidor.closeAllConnections();
  await new Promise((listo) => servidor.close(listo));
  await pool.end();
}

console.log(fallos === 0 ? '\nCIRCUITO CORRECTO' : `\n${fallos} comprobacion(es) fallaron`);
process.exitCode = fallos === 0 ? 0 : 1;
