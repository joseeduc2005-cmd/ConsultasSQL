/**
 * Que un usuario normal pueda ejecutar los temas que ya puede leer — y solo esos.
 *
 * Entra como alguien de rol `user`, no como administrador: lo que se comprueba es
 * justamente lo que un usuario puede y no puede hacer, y hacerlo con una sesión de admin
 * no probaría nada.
 *
 * Lo que tiene que poder:
 *   · ver qué datos le pide un tema dinámico, deducidos de los marcadores;
 *   · ejecutarlo y recibir las filas.
 *
 * Lo que no:
 *   · ejecutar un tema marcado solo para administradores — ni saber que existe;
 *   · ejecutar un tema de lectura, que no tiene nada que ejecutar;
 *   · pasar un dato de más para colar otra cosa.
 *
 * Y que quede registrado: una ejecución hecha desde la pantalla del usuario deja el mismo
 * rastro que una del panel. Si no, la auditoría tendría un agujero con la forma exacta de
 * «lo que hacen los usuarios».
 *
 * Necesita el backend levantado.
 *
 *   node herramientas/prueba-ejecucion-usuario.mjs
 */
import pg from 'pg';
import { readFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';

const BASE = 'http://127.0.0.1:3002/api/soporte-cliente';
const cfg = JSON.parse(readFileSync('config/multidb.databases.json', 'utf8'));
const m = cfg.databases.find((d) => d.id === 'pg_main');
const pool = new pg.Pool({ host: m.host, port: m.port, database: m.database, user: m.user, password: m.password });

let fallos = 0;
const comprobar = (bien, que) => {
  if (!bien) fallos += 1;
  console.log(`  ${bien ? 'ok  ' : 'MAL '} ${que}`);
};

// ── Una sesión de usuario normal, y un tema solo para administradores ─────────
const { rows: [usuario] } = await pool.query(
  "SELECT id, username FROM users WHERE role = 'user' ORDER BY username LIMIT 1",
);
const { rows: [admin] } = await pool.query("SELECT id FROM users WHERE role = 'admin' LIMIT 1");
const token = 'prueba-usuario-' + randomUUID();
await pool.query('INSERT INTO sessions (token, user_id, activo, created_at) VALUES ($1,$2,true,NOW())', [token, usuario.id]);

// El tema restringido se crea aquí en vez de buscar uno: depender de qué haya ese día es
// lo que hizo fallar esta clase de prueba antes.
const idRestringido = randomUUID();
await pool.query(
  `INSERT INTO knowledge_base
     (id, titulo, categoria, contenido, contenido_md, tipo_solucion, script_json,
      creado_por, visible_para)
   VALUES ($1,$2,'Prueba',$3,$3,'dinamico',$4,$5,'admin')`,
  [
    idRestringido, '[prueba] Tema restringido a administradores', 'No lo debe ver un usuario.',
    JSON.stringify({
      modo: 'script', origen: 'manual-sql',
      sql: 'SELECT cedula FROM socios WHERE cedula = :cedula',
      target_database: 'demo_cooperativa', databaseId: 'demo_cooperativa',
    }),
    admin.id,
  ],
);

const H = { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` };
const pedir = async (ruta, cuerpo) => {
  const r = await fetch(BASE + ruta, cuerpo
    ? { method: 'POST', headers: H, body: JSON.stringify(cuerpo) }
    : { headers: H });
  let json = null;
  try { json = JSON.parse(await r.text()); } catch { /* no era json */ }
  return { status: r.status, ok: r.ok, json };
};

console.log(`\nEntrando como ${usuario.username} (rol user)\n`);

// ── Lo que tiene que poder ────────────────────────────────────────────────────
console.log('PUEDE EJECUTAR LO QUE PUEDE LEER');

const { rows: [tema] } = await pool.query(
  "SELECT id::text FROM knowledge_base WHERE titulo = '[demo] Consultar un socio por su cédula'",
);
const visto = await pedir(`/temas/${tema.id}`);
comprobar(visto.ok, 've el tema');
comprobar(visto.json?.ejecutable === true, 'el tema viene marcado como ejecutable');
comprobar(
  JSON.stringify((visto.json?.pide ?? []).map((c) => c.nombre)) === '["cedula"]',
  `le pide la cédula y nada más · ${JSON.stringify((visto.json?.pide ?? []).map((c) => c.nombre))}`,
);

const corrido = await pedir(`/temas/${tema.id}/ejecutar`, { datos: { cedula: '0110000411' } });
comprobar(corrido.ok, `lo ejecuta · HTTP ${corrido.status}`);
comprobar(corrido.json?.filas?.length === 1, `recibe la fila · ${corrido.json?.filas?.length} fila(s)`);
comprobar(corrido.json?.filas?.[0]?.estado === 'bloqueado', 'y el dato es el correcto · bloqueado');
comprobar(Array.isArray(corrido.json?.columnas) && corrido.json.columnas.includes('socio'), 'vienen las columnas para la tabla');

// Un tema con tres datos, para ver que el formulario no es solo de uno.
const { rows: [rango] } = await pool.query(
  "SELECT id::text FROM knowledge_base WHERE titulo LIKE '%transacciones tiene un POS%'",
);
const conRango = await pedir(`/temas/${rango.id}/ejecutar`, {
  datos: { codigo_pos: 'POS-1001', desde: '2026-09-01', hasta: '2026-12-31' },
});
comprobar(conRango.ok && Number(conRango.json?.filas?.[0]?.transacciones) > 300, 'ejecuta uno que pide tres datos, fechas incluidas');

// ── Lo que no ─────────────────────────────────────────────────────────────────
console.log('\nNO PUEDE LO QUE NO LE TOCA');

const restringidoVisto = await pedir(`/temas/${idRestringido}`);
comprobar(restringidoVisto.status === 404, `no ve el tema de administradores · HTTP ${restringidoVisto.status}`);

const restringidoCorrido = await pedir(`/temas/${idRestringido}/ejecutar`, { datos: { cedula: '0110000411' } });
comprobar(restringidoCorrido.status === 404, `no lo ejecuta, y el error no delata que exista · HTTP ${restringidoCorrido.status}`);

const { rows: [lectura] } = await pool.query(
  "SELECT id::text FROM knowledge_base WHERE tipo_solucion = 'lectura' AND coalesce(visible_para,'todos') = 'todos' LIMIT 1",
);
const deLectura = await pedir(`/temas/${lectura.id}/ejecutar`, { datos: {} });
comprobar(deLectura.status === 400, `un tema de lectura no se ejecuta · HTTP ${deLectura.status}`);

const inventado = await pedir(`/temas/${randomUUID()}/ejecutar`, { datos: {} });
comprobar(inventado.status === 404, `un id que no existe tampoco · HTTP ${inventado.status}`);

// Un dato de más no debe cambiar lo que se ejecuta: la sentencia es la del tema.
const conBasura = await pedir(`/temas/${tema.id}/ejecutar`, {
  datos: { cedula: '0110000411', sql: 'DROP TABLE socios', limite: 99999 },
});
comprobar(
  conBasura.ok && conBasura.json?.filas?.length === 1,
  'un dato de más no cambia la sentencia: ejecuta la del tema',
);
const { rows: [siguenLasTablas] } = await pool.query(
  "SELECT count(*)::int n FROM information_schema.tables WHERE table_schema='public'",
  [],
);
comprobar(siguenLasTablas.n > 0, 'y la base sigue entera');

// ── Y queda registrado ────────────────────────────────────────────────────────
console.log('\nQUEDA REGISTRADO');

// El apunte se escribe sin esperar a que termine la respuesta, así que puede llegar un
// instante después: se reintenta en vez de dar por bueno el primer vistazo.
let apunte = null;
for (let i = 0; i < 20 && !apunte; i += 1) {
  const { rows } = await pool.query(
    `SELECT ejecutado_por, filas_resultado, estado, parametros_usados
     FROM article_executions
     WHERE article_id = $1 AND ejecutado_por = $2
     ORDER BY ejecutado_en DESC LIMIT 1`,
    [tema.id, usuario.username],
  );
  apunte = rows[0] ?? null;
  if (!apunte) await new Promise((listo) => { setTimeout(listo, 100); });
}
comprobar(Boolean(apunte), 'la ejecución del usuario quedó en article_executions');
comprobar(apunte?.estado === 'ok' && apunte?.filas_resultado === 1, `con su estado y sus filas · ${apunte?.estado}, ${apunte?.filas_resultado} fila(s)`);
comprobar(
  String(JSON.stringify(apunte?.parametros_usados ?? {})).includes('0110000411'),
  'y con qué cédula la ejecutó',
);

// ── Limpieza ──────────────────────────────────────────────────────────────────
await pool.query("DELETE FROM article_executions WHERE article_id IN (SELECT id::text FROM knowledge_base WHERE titulo LIKE '[prueba]%')");
await pool.query("DELETE FROM knowledge_base WHERE titulo LIKE '[prueba]%'");
await pool.query('DELETE FROM sessions WHERE token = $1', [token]);
await pool.end();

console.log('\n' + (fallos === 0 ? '════ EL USUARIO EJECUTA, Y SOLO LO SUYO ════' : `════ ${fallos} FALLO(S) ════`));
process.exit(fallos === 0 ? 0 : 1);
