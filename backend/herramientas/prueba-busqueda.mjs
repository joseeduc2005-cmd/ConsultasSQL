/**
 * Batería grande contra la búsqueda de la base de conocimiento.
 *
 * Las frases están escritas a partir del problema, no del título: si se copiaran los
 * títulos no se probaría nada más que si el buscador sabe comparar cadenas. Cada una se
 * multiplica por las formas en que llega un caso de verdad —sin tildes, en mayúsculas,
 * con un saludo delante, con una falta de dedo, a medias— porque así es como escribe la
 * gente, no como lo escribiría quien hizo el sistema.
 *
 * Los controles negativos pesan tanto como los aciertos. Un buscador que siempre
 * responde algo es peor que uno que admite no saber: en soporte, una respuesta con cara
 * de segura hace perder más tiempo que un «no hay nada».
 *
 * Uso, desde `backend/`:  node herramientas/prueba-busqueda.mjs [--fallos]
 */
import 'dotenv/config';
import pg from 'pg';
import { readFileSync } from 'node:fs';

const SOLO_FALLOS = process.argv.includes('--fallos');

const b = JSON.parse(readFileSync('config/multidb.databases.json', 'utf8')).databases.find((x) => x.id === 'pg_main');
const pool = new pg.Pool({ host: b.host, port: b.port, database: b.database, user: b.user, password: b.password });
const { configurarPool } = await import('../src/soporte/knowledge/db.ts');
configurarPool(() => pool);
const { buscarPrecedentes } = await import('../src/soporte/precedentes.ts');

/**
 * Lo que se espera de cada frase.
 *
 * `espera` lleva fragmentos de título; vale cualquiera de ellos. Son fragmentos y no
 * títulos enteros para que renombrar un tema —algo que pasa cada vez que alguien lo
 * escribe mejor— no obligue a reescribir la batería entera.
 */
const REPORTE = ['reporte de transacciones'];

const CASOS = [
  // 1 — Auditoría de duplicados
  { espera: ['duplicados o huérfanos'], frases: [
    'necesito que revisen si hay usuarios repetidos en corresponsales',
    'hay representantes que quedaron sin su POS asociado, estan huerfanos',
    'quiero un listado de los registros duplicados de la base de corresponsales',
    'pueden correr unas consultas de solo lectura para revisar la calidad de los datos',
    'tenemos POS que apuntan a dos usuarios distintos, hay que auditarlo',
  ]},
  // 2 — Sucursal desactualizada
  { espera: ['Descuadre contable'], frases: [
    'la socia cambio de oficina pero en contabilidad le sigue saliendo la agencia vieja',
    'cambio de sucursal del corresponsal y los movimientos salen con la oficina anterior',
    'hay descuadre en cuentas por cobrar porque la sucursal no se actualizo',
    'en el admin sale la oficina nueva pero en jasit sale la antigua',
    'las comisiones del corresponsal se contabilizan en la agencia equivocada',
  ]},
  // 3 — Acta y pago
  { espera: ['Comisión no pagada'], frases: [
    'el corresponsal reclama que no le han pagado su comision del mes',
    'en el acta no le aparece el valor completo al tendero',
    'hay un pago pendiente de varios meses a un corresponsal',
    'se genero el acta pero la factura no salio y no se pudo pagar',
    'tengo que corregir una solicitud de pago de un corresponsal',
  ]},
  // 4 y 6 — Error al generar reporte
  { espera: REPORTE, frases: [
    'al corresponsal le sale un mensaje de error cuando quiere generar un reporte',
    'no puede sacar el reporte, le aparece un error en pantalla',
    'me mandaron un video por whatsapp donde sale el error al generar el reporte',
    'el tendero intenta generar su reporte y el sistema le bota error',
  ]},
  // 5 — Recaudación GAD Mejía
  { espera: ['GAD Mejía'], frases: [
    'error al procesar una recaudacion de un municipio en el POS',
    'la tendera no puede cobrar el servicio del municipio, le sale error',
    'falla la recaudacion del GAD en un POS Z90',
    'no se puede recaudar el agua del municipio desde el corresponsal',
  ]},
  // 7 — Recargas
  { espera: ['saldo no llega'], frases: [
    'la señora hizo una recarga de 5 dolares y nunca le entro el saldo al celular',
    'las recargas de claro estan presentando inconvenientes',
    'le descontaron el valor de la recarga pero el telefono no recibio nada',
    'un cliente reclama por una recarga que no se acredito',
    'no se pueden hacer recargas desde el corresponsal',
  ]},
  // 8 — Reimpresión de comprobante
  { espera: ['No imprime el comprobante'], frases: [
    'el POS no imprime el comprobante de la recaudacion local',
    'cobro bien pero no salio el documento de respaldo',
    'no puede reimprimir el comprobante despues de recaudar',
    'la transaccion se proceso pero el papel no sale de la impresora del POS',
    'necesita el respaldo impreso de un cobro de recaudacion local',
  ]},
  // 9 — Formato de clave y PIN
  { espera: ['contraseña o PIN'], frases: [
    'quiero cambiar mi clave y me dice que no cumple el formato',
    'cual es la politica de claves y pines del corresponsal',
    'puse 2008 como PIN y el sistema no me deja',
    'el cambio de contraseña dice que se guardo pero no persiste',
    'que condiciones debe cumplir la contraseña del tendero',
  ]},
  // 10 — QR
  { espera: ['Pagos con QR'], frases: [
    'un cliente pago con el codigo QR y el sistema no registro la transaccion',
    'estan fallando los pagos por QR en el comercio',
    'el cobro con QR no se refleja aunque al cliente si le debitaron',
    'problemas con los pagos QR de la marca Coral',
  ]},
  // 11 — No permite ingresar
  { espera: ['No permite ingresar'], frases: [
    'al corresponsal se le bloquea el usuario cada vez que entra',
    'lo desbloquean y al rato se vuelve a inhabilitar',
    'mete su clave y le sale un mensaje raro, no puede entrar al POS',
    'el usuario se inhabilita solo despues de reactivarlo',
    'la MAC del POS esta duplicada con la de otro dispositivo',
  ]},
  // 14 — Límite de 300
  { espera: ['300 transacciones'], frases: [
    'no me deja sacar el listado completo, se corta a las 300 filas',
    'el reporte solo muestra hasta 300 movimientos',
    'si consulto un rango grande el reporte se queda sin datos',
    'necesito ver mas transacciones de las que me deja el reporte',
  ]},
  // 15 — Resetear clave
  { espera: ['resetear la clave'], frases: [
    'la señora quiere que le reseteen la clave pero dice que el usuario no existe',
    'al buscar a la socia para resetear sale usuario no encontrado',
    'no recuerda su clave ni su contraseña y no se puede resetear',
    'quiere recuperar el acceso pero el sistema no la encuentra',
  ]},
];

/**
 * Temas que existen pero no son para el cliente, y lo que se pregunta por ellos.
 *
 * Son peticiones de proyecto —una pantalla solicitada, un despliegue— no averías con
 * solución, así que se pasaron a visibles solo para soporte. Lo que hay que comprobar
 * no es que la búsqueda se quede muda: preguntar por el PIN y recibir el artículo del
 * formato del PIN está bien. Lo que no puede pasar es que salga el tema oculto.
 */
const OCULTOS = [
  { noDebeSalir: 'Pantalla Cambio de PIN', frases: [
    'solicitan una pantalla para que el tendero cambie su PIN',
    'requerimiento de la pantalla de cambio de PIN para corresponsales',
  ]},
  { noDebeSalir: 'Puesta en producción', frases: [
    'hay que desplegar a produccion el admin de corresponsales',
    'solicitud de puesta en produccion de una orden de trabajo',
  ]},
];

/** Consultas sobre cosas que no están en la base. Deben quedarse sin respuesta firme. */
const NEGATIVOS = [
  'necesito que instalen una impresora nueva en la agencia de Paute y la configuren en red',
  'quiero acceso al modulo de nomina para revisar los roles de pago del personal',
  'como hago una receta de pastel de chocolate',
  'solicito vacaciones para la proxima semana',
  'el aire acondicionado de la oficina no enfria',
  'necesito un proyector para la sala de reuniones',
  'cuando pagan el decimo cuarto sueldo',
  'se daño la silla de mi escritorio',
  'quiero inscribirme en el curso de ingles',
  'cual es la clave del wifi de la agencia',
  'necesito papel y esferos para la oficina',
  'el ascensor del edificio esta parado',
  'quiero cambiar mi correo electronico personal',
  'hay parqueadero disponible para visitantes',
  'necesito el certificado de trabajo para el banco',
  'a que hora abre la cafeteria',
  'me pueden dar un adelanto de sueldo',
  'quien es el jefe del departamento de marketing',
  'necesito reservar la sala para una capacitacion',
  'donde consigo el formato de permiso medico',
];

// ─── Las formas en que llega una frase de verdad ─────────────────────────────

const sinTildes = (t) => t.normalize('NFD').replace(/[̀-ͯ]/g, '');
const saludo = (t) => `Buenos dias, ${t}. Quedo atento, gracias`;
const ruido = (t) => `URGENTE por favor ayuda con esto: ${t} el socio esta reclamando`;
const truncar = (t) => t.split(/\s+/).slice(0, Math.max(5, Math.ceil(t.split(/\s+/).length * 0.6))).join(' ');

/** Una falta de dedo en la palabra más larga, que es la que más suele importar. */
function erratas(texto) {
  const palabras = texto.split(/\s+/);
  let i = 0;
  for (let j = 0; j < palabras.length; j++) if (palabras[j].length > palabras[i].length) i = j;
  const p = palabras[i];
  if (p.length < 5) return texto;
  palabras[i] = p.slice(0, 3) + p[4] + p[3] + p.slice(5);
  return palabras.join(' ');
}

const VARIANTES = [
  ['tal cual', (t) => t],
  ['sin tildes', sinTildes],
  ['mayusculas', (t) => t.toUpperCase()],
  ['con saludo', saludo],
  ['con ruido', ruido],
  ['truncada', truncar],
  ['con errata', erratas],
  ['saludo+sin tildes', (t) => sinTildes(saludo(t))],
  ['ruido+mayusculas', (t) => ruido(t).toUpperCase()],
  ['truncada sin tildes', (t) => sinTildes(truncar(t))],
  ['errata+saludo', (t) => erratas(saludo(t))],
  ['sin puntuacion', (t) => t.replace(/[.,;:()"']/g, '')],
  ['ruido+sin tildes', (t) => sinTildes(ruido(t))],
  ['truncada+mayusculas', (t) => truncar(t).toUpperCase()],
  ['errata+sin tildes', (t) => sinTildes(erratas(t))],
];

// ─── Correr ──────────────────────────────────────────────────────────────────

const acierta = (titulo, espera) => espera.some((e) => titulo.toLowerCase().includes(e.toLowerCase()));

const porVariante = new Map();
const porCaso = new Map();
const fallos = [];
let total = 0;
let bien = 0;
// La pantalla no enseña un resultado, enseña una lista. Que el correcto salga el
// segundo no es un fallo para quien lo usa, así que se mide aparte.
let enLista = 0;

async function evaluar(frase, espera, etiqueta, variante) {
  const r = await buscarPrecedentes(frase, { limite: 3, soloPublicos: true });
  const top = r[0];
  // Un negativo pasa si no devuelve nada, o si lo que devuelve lo marca como lejano:
  // ahí la pantalla ya avisa de que no se fíe.
  const ok = espera === null
    ? r.length === 0 || top.confianza === 'baja'
    : Boolean(top) && acierta(top.titulo, espera);

  const enTop = espera === null
    ? ok
    : r.some((x) => acierta(x.titulo, espera));

  total++;
  if (ok) bien++;
  if (enTop) enLista++;
  else fallos.push({ frase, etiqueta, variante, dio: top ? `${top.titulo} [${top.confianza}]` : '(nada)' });

  for (const [mapa, clave] of [[porVariante, variante], [porCaso, etiqueta]]) {
    const m = mapa.get(clave) ?? { total: 0, bien: 0 };
    m.total++;
    if (ok) m.bien++;
    mapa.set(clave, m);
  }
}

/** Un tema oculto no puede aparecer en la lista del cliente, salga lo que salga. */
async function evaluarOculto(frase, fragmento, variante) {
  const r = await buscarPrecedentes(frase, { limite: 5, soloPublicos: true });
  const asomo = r.find((x) => x.titulo.toLowerCase().includes(fragmento.toLowerCase()));

  total++;
  if (!asomo) {
    bien++;
    enLista++;
  } else {
    fallos.push({ frase, etiqueta: `(oculto) ${fragmento}`, variante, dio: `SE FILTRÓ: ${asomo.titulo}` });
  }

  for (const [mapa, clave] of [[porVariante, variante], [porCaso, '(no debe verlo el cliente)']]) {
    const m = mapa.get(clave) ?? { total: 0, bien: 0 };
    m.total++;
    if (!asomo) m.bien++;
    mapa.set(clave, m);
  }
}

console.log('Corriendo…\n');
for (const caso of CASOS) {
  for (const frase of caso.frases) {
    for (const [nombre, fn] of VARIANTES) await evaluar(fn(frase), caso.espera, caso.espera[0], nombre);
  }
}
for (const frase of NEGATIVOS) {
  for (const [nombre, fn] of VARIANTES) await evaluar(fn(frase), null, '(no debe encontrar)', nombre);
}
for (const o of OCULTOS) {
  for (const frase of o.frases) {
    for (const [nombre, fn] of VARIANTES) await evaluarOculto(fn(frase), o.noDebeSalir, nombre);
  }
}

const pct = (m) => `${((m.bien / m.total) * 100).toFixed(1)}%`.padStart(6);

console.log('Por tema:');
for (const [clave, m] of [...porCaso.entries()].sort((a, b) => a[1].bien / a[1].total - b[1].bien / b[1].total)) {
  console.log(`  ${pct(m)}  ${String(m.bien).padStart(3)}/${String(m.total).padEnd(3)}  ${clave}`);
}

console.log('\nPor forma de escribir:');
for (const [clave, m] of [...porVariante.entries()].sort((a, b) => a[1].bien / a[1].total - b[1].bien / b[1].total)) {
  console.log(`  ${pct(m)}  ${String(m.bien).padStart(3)}/${String(m.total).padEnd(3)}  ${clave}`);
}

if (SOLO_FALLOS && fallos.length) {
  console.log('\nFallos:');
  for (const f of fallos.slice(0, 60)) {
    console.log(`  esperaba ${f.etiqueta}  (${f.variante})`);
    console.log(`     "${f.frase.slice(0, 92)}"`);
    console.log(`     dio: ${f.dio}`);
  }
  if (fallos.length > 60) console.log(`  … y ${fallos.length - 60} más`);
}

console.log(`\nPrimer resultado correcto:     ${bien} de ${total} — ${((bien / total) * 100).toFixed(1)}%`);
console.log(`Correcto entre los 3 primeros: ${enLista} de ${total} — ${((enLista / total) * 100).toFixed(1)}%`);
await new Promise((listo) => setTimeout(listo, 100));
await pool.end();
process.exitCode = bien === total ? 0 : 1;
