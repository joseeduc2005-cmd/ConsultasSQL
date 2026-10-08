/**
 * Recorre la demostración entera por HTTP, como la hará una persona.
 *
 * Busca con palabras naturales, mira qué datos le pide el tema, lo ejecuta y comprueba
 * que el resultado es el esperado. Si algo falla aquí, falla en la exposición — y falla
 * delante de la gente, que es cuando no se puede arreglar.
 *
 * También comprueba lo que no debe poder hacerse: intenta guardar un artículo con UPDATE,
 * con DELETE, con INSERT, con DROP y con TRUNCATE, y los cinco tienen que ser rechazados.
 *
 * Toca la base, y conviene saberlo antes de ejecutarlo:
 *
 *   · crea una sesión de administrador para poder recorrer las pantallas sin la
 *     contraseña de nadie, y la borra al terminar;
 *   · crea los artículos `[prueba-escritura]` del párrafo anterior, y los borra también.
 *
 * Nada de lo que crea sobrevive a la ejecución. Necesita el backend levantado.
 *
 *   node herramientas/prueba-demo.mjs
 */
import pg from 'pg';
import { readFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';

const BASE = 'http://127.0.0.1:3002';
const cfg = JSON.parse(readFileSync('config/multidb.databases.json', 'utf8'));
const m = cfg.databases.find((d) => d.id === 'pg_main');
const pool = new pg.Pool({ host: m.host, port: m.port, database: m.database, user: m.user, password: m.password });

// Una sesión de administrador para poder recorrer todo sin la contraseña de nadie.
const { rows: [admin] } = await pool.query("SELECT id, username FROM users WHERE role = 'admin' LIMIT 1");
const token = 'verificacion-' + randomUUID();
await pool.query(
  'INSERT INTO sessions (token, user_id, activo, created_at) VALUES ($1,$2,true,NOW())',
  [token, admin.id],
);

const H = { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` };
const pedir = async (ruta, cuerpo) => {
  const r = await fetch(BASE + ruta, cuerpo ? { method: 'POST', headers: H, body: JSON.stringify(cuerpo) } : { headers: H });
  const txt = await r.text();
  let json = null;
  try { json = JSON.parse(txt); } catch { /* no era json */ }
  return { ok: r.ok, status: r.status, json, txt };
};

let fallos = 0;
const mal = (q) => { fallos += 1; console.log('   ✗ ' + q); };

// Los temas, con lo que pide cada uno y con qué probarlo.
const CASOS = [
  {
    busqueda: 'el tendero entra y ve los datos de otro negocio',
    tema: 'comparten la misma MAC',
    datos: {},
    esperado: (f) => f.length === 1 && String(f[0].donde || '').includes('Guel Cyber'),
  },
  {
    busqueda: 'quiero saber los datos de un socio por la cedula',
    tema: 'Consultar un socio',
    datos: { cedula: '0110000411' },
    esperado: (f) => f.length === 1 && f[0].estado === 'bloqueado',
  },
  {
    busqueda: 'el socio no puede retirar en el corresponsal',
    tema: 'cuentas y el saldo',
    datos: { cedula: '0110000411' },
    esperado: (f) => f.length === 2 && f.every((x) => x.estado === 'bloqueada'),
  },
  {
    busqueda: 'el corresponsal no puede ingresar le dice usuario no encontrado',
    tema: 'no puede entrar',
    datos: { cedula: '0110000548' },
    esperado: (f) => f.length === 1 && Number(f[0].pos_con_esa_mac) === 0,
  },
  {
    busqueda: 'el corresponsal no puede ingresar le dice usuario no encontrado',
    tema: 'no puede entrar',
    datos: { cedula: '0110000137' },
    esperado: (f) => f.length === 1 && Number(f[0].pos_con_esa_mac) === 2,
  },
  {
    busqueda: 'el reporte de transacciones da error cuando pido un mes',
    tema: 'Cuántas transacciones tiene un POS',
    datos: { codigo_pos: 'POS-1001', desde: '2026-09-01', hasta: '2026-12-31' },
    esperado: (f) => f.length === 1 && Number(f[0].transacciones) > 300 && String(f[0].diagnostico).includes('limite'),
  },
  {
    busqueda: 'el reporte de transacciones da error cuando pido un mes',
    tema: 'Cuántas transacciones tiene un POS',
    datos: { codigo_pos: 'POS-1003', desde: '2026-09-01', hasta: '2026-12-31' },
    esperado: (f) => f.length === 1 && Number(f[0].transacciones) < 300 && String(f[0].diagnostico).includes('dentro'),
  },
  {
    busqueda: 'se cobro la recaudacion pero no sale el comprobante',
    tema: 'no imprime el comprobante',
    datos: { referencia: 'REC-240143' },
    esperado: (f) => f.length === 1 && f[0].comprobante === 'NO SE GENERO',
  },
  {
    busqueda: 'no me deja corregir el acta de pago del corresponsal',
    tema: 'acta de pago no se puede corregir',
    datos: { numero_acta: '53656' },
    esperado: (f) => f.length === 1 && f[0].estado_pago === 'C',
  },
  {
    busqueda: 'no me deja corregir el acta de pago del corresponsal',
    tema: 'acta de pago no se puede corregir',
    datos: { numero_acta: '53657' },
    esperado: (f) => f.length === 1 && String(f[0].diagnostico).includes('se puede corregir'),
  },
];

const { rows: temas } = await pool.query(
  "SELECT id::text, titulo, script_json FROM knowledge_base WHERE titulo LIKE '[demo]%'",
);

console.log('\n── Buscar con palabras propias ──────────────────────────────────');
const yaBuscado = new Set();
for (const c of CASOS) {
  if (yaBuscado.has(c.busqueda)) continue;
  yaBuscado.add(c.busqueda);
  const r = await pedir('/api/soporte-cliente/solucionar', { texto: c.busqueda });
  if (!r.ok) { mal(`la búsqueda falló (${r.status}) · ${c.busqueda}`); continue; }
  const p = r.json?.precedentes ?? [];
  const pos = p.findIndex((x) => String(x.titulo || '').includes(c.tema));
  const etiqueta = pos === 0 ? '1.º' : pos > 0 ? `${pos + 1}.º` : 'NO SALE';
  console.log(`   ${etiqueta.padEnd(8)} «${c.busqueda}»`);
  console.log(`            → ${String(p[0]?.titulo || '(nada)').replace('[demo] ', '')}`);
  if (pos < 0) mal(`no encuentra «${c.tema}» buscando «${c.busqueda}»`);
}

console.log('\n── Qué datos pide cada tema, y qué devuelve ─────────────────────');
for (const c of CASOS) {
  const tema = temas.find((t) => t.titulo.includes(c.tema));
  if (!tema) { mal(`no existe el tema «${c.tema}»`); continue; }

  // Lo que pide: los marcadores del SQL son exactamente los campos del formulario.
  const sql = tema.script_json?.sql || '';
  // Un «::int» es un cast, no un parametro: el regex se lo tragaba y la prueba
  // creia que el tema pedia un dato llamado «int».
  const pide = [...new Set([...sql.matchAll(/(?<!:):(\w+)/g)].map((x) => x[1]))];
  const dados = Object.keys(c.datos);
  if (pide.sort().join(',') !== dados.sort().join(',')) {
    mal(`«${c.tema}» pide [${pide}] y la prueba manda [${dados}]`);
  }

  const r = await pedir('/api/execute', { articleId: tema.id, formData: c.datos });
  if (!r.ok) {
    mal(`${c.tema} → ${r.status} ${String(r.json?.error || r.txt).slice(0, 160)}`);
    continue;
  }
  const filas = r.json?.result?.resultado ?? [];
  const bien = c.esperado(filas);
  console.log(`   ${bien ? '✓' : '✗'} ${tema.titulo.replace('[demo] ', '').padEnd(58)}`
    + `pide ${pide.length ? pide.join(', ') : '(nada)'} → ${filas.length} fila(s)`);
  if (!bien) {
    mal(`el resultado de «${c.tema}» con ${JSON.stringify(c.datos)} no es el esperado`);
    console.log('       ' + JSON.stringify(filas.slice(0, 2)));
  }
}

console.log('\n── Que no se pueda escribir, por ninguna vía ────────────────────');
const ESCRITURAS = [
  ['UPDATE', 'UPDATE socios SET estado = \'activo\' WHERE cedula = :cedula'],
  ['DELETE', 'DELETE FROM socios WHERE cedula = :cedula'],
  ['INSERT', 'INSERT INTO socios (cedula, nombres, apellidos, sucursal, fecha_ingreso) VALUES (:cedula, \'x\', \'y\', \'z\', NOW())'],
  ['DDL', 'DROP TABLE socios'],
  ['TRUNCATE', 'TRUNCATE transacciones'],
];
for (const [etiqueta, sql] of ESCRITURAS) {
  const r = await pedir('/api/articles', {
    titulo: `[prueba-escritura] ${etiqueta}`,
    categoria: 'Prueba',
    contenido_md: 'No debe guardarse.',
    descripcion: 'Prueba de que el sistema no acepta escrituras.',
    tipo_solucion: 'dinamico',
    script_json: { modo: 'script', origen: 'manual-sql', sql, target_database: 'demo_cooperativa', databaseId: 'demo_cooperativa' },
  });
  const rechazado = !r.ok;
  console.log(`   ${rechazado ? '✓' : '✗'} ${etiqueta.padEnd(10)} ${rechazado ? String(r.json?.error || '').slice(0, 92) : 'SE GUARDÓ — el sistema lo aceptó'}`);
  if (!rechazado) mal(`se pudo guardar un artículo con ${etiqueta}`);
}
await pool.query("DELETE FROM knowledge_base WHERE titulo LIKE '[prueba-escritura]%'");

console.log('\n── Los tickets ──────────────────────────────────────────────────');
const t = await pedir('/api/soporte/tickets');
const lista = Array.isArray(t.json) ? t.json : (t.json?.tickets ?? t.json?.data ?? []);
const demo = lista.filter((x) => String(x.titulo || '').includes('[demo]'));
console.log(`   ${demo.length === 5 ? '✓' : '✗'} ${demo.length} ticket(s) de demostración`);
for (const x of demo) console.log(`       ${x.numero}  ${String(x.estado).padEnd(14)} ${String(x.titulo).replace('[demo] ', '').slice(0, 64)}`);
if (demo.length !== 5) mal('no están los cinco tickets');

console.log('\n── Los aportes ──────────────────────────────────────────────────');
const a = await pedir('/api/soporte/aportes?estado=pendiente');
const aportes = Array.isArray(a.json) ? a.json : (a.json?.data ?? []);
console.log(`   ${aportes.length >= 1 ? '✓' : '✗'} ${aportes.length} aporte(s) esperando revisión`);
if (!aportes.length) mal('no hay ningún aporte pendiente que enseñar');

await pool.query('DELETE FROM sessions WHERE token = $1', [token]);
await pool.end();

console.log('\n' + (fallos === 0
  ? '════ LA DEMOSTRACIÓN FUNCIONA DE PUNTA A PUNTA ════'
  : `════ ${fallos} FALLO(S) ════`));
process.exit(fallos === 0 ? 0 : 1);
