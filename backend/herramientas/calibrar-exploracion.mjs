/**
 * Calibra la exploración contra datos reales. SOLO LECTURA.
 *
 * Los umbrales de `exploracion.ts` (mínimo 8 usuarios de referencia, 75 % de cuota)
 * se fijaron contra 41 usuarios inventados en la Oracle de pruebas. Con los
 * corresponsales de verdad la distribución puede ser otra, y dos cosas pueden salir
 * mal: que la exploración no proponga nada nunca, o —peor— que proponga a casi todo
 * el mundo y sus propuestas no signifiquen nada.
 *
 * Este script mide eso sin escribir nada:
 *
 *   1. Cuántos usuarios están «operando» (con transacciones recientes).
 *   2. Con qué valor opera cada columna corregible, y con cuánto peso.
 *   3. A qué porcentaje del total se le propondría una corrección. Si es alto, el
 *      umbral está flojo: una corrección que aplica a todos no informa de nada.
 *
 * Uso, desde `backend/`, conectado a la red del banco:
 *
 *   SOPORTE_ORACLE_DB_ID=oracle node herramientas/calibrar-exploracion.mjs
 *
 * Con `> calibracion.txt` queda el informe para revisarlo con calma.
 */
import 'dotenv/config';

const { fuente } = await import('../src/soporte/oracle/index.ts');
const { config } = await import('../src/soporte/config.ts');

const COLUMNAS = [
  ['usr_estado', 'perfilUsrEstado', 'Estado del usuario'],
  ['usr_intentos', 'perfilUsrIntentos', 'Intentos fallidos de clave'],
  ['usr_intentos_pin', 'perfilUsrIntentosPin', 'Intentos fallidos de PIN'],
  ['usr_is_clave_temporal', 'perfilUsrIsClaveTemporal', 'Marca de clave temporal'],
  ['usr_is_pin_temporal', 'perfilUsrIsPinTemporal', 'Marca de PIN temporal'],
];

const DIAS = 30;
const CUOTA_ACTUAL = 0.75;
const num = (v) => (v === null || v === undefined ? null : Number(v));
const pct = (x) => `${(x * 100).toFixed(1)} %`;

if (fuente.modo !== 'oracle') {
  console.error('Esto necesita una Oracle real. Falta SOPORTE_ORACLE_DB_ID o las credenciales.');
  process.exit(1);
}

console.log('Calibración de la exploración · solo lectura');
console.log('Base:', config.oracle.conexion, '· usuario:', config.oracle.usuario);
console.log('Ventana de actividad:', DIAS, 'días\n');

const perfiles = new Map();
let poblacion = 0;

console.log('DISTRIBUCIÓN ENTRE LOS QUE OPERAN');
console.log('-'.repeat(72));

for (const [campo, consulta, etiqueta] of COLUMNAS) {
  const filas = await fuente.consultar(consulta, { dias: DIAS });
  if (filas.length === 0) {
    console.log(`${etiqueta.padEnd(30)} sin datos`);
    continue;
  }
  const total = num(filas[0].total) ?? 0;
  poblacion = Math.max(poblacion, total);
  const dominante = num(filas[0].valor);
  const cuota = (num(filas[0].veces) ?? 0) / (total || 1);
  perfiles.set(campo, { dominante, cuota });

  const reparto = filas
    .slice(0, 4)
    .map((f) => `${num(f.valor)}: ${pct((num(f.veces) ?? 0) / (total || 1))}`)
    .join('   ');
  const señal = cuota >= CUOTA_ACTUAL ? 'sirve ' : 'FLOJA ';
  console.log(`${etiqueta.padEnd(30)} ${señal} dominante=${dominante} (${pct(cuota)})   ${reparto}`);
}

console.log(`\nPoblación de referencia: ${poblacion} usuarios con transacciones en ${DIAS} días.`);
if (poblacion < 30) {
  console.log('AVISO: población chica. Amplíe la ventana de días o revise el join con transaccion.');
}

// Cuánta gente recibiría propuesta. Se mira sobre los usuarios activos, que son los
// que van a aparecer en tickets; si casi todos "difieren", el umbral no discrimina.
console.log('\nA CUÁNTOS SE LES PROPONDRÍA ALGO');
console.log('-'.repeat(72));

const muestra = await fuente.consultar('usuariosActivosMuestra', { dias: DIAS }).catch(() => null);
if (!muestra) {
  console.log('(Falta la consulta `usuariosActivosMuestra` en el catálogo: vea el comentario al final.)');
} else {
  let conPropuesta = 0;
  const porColumna = new Map(COLUMNAS.map(([c]) => [c, 0]));
  for (const u of muestra) {
    let difiere = 0;
    for (const [campo] of COLUMNAS) {
      const p = perfiles.get(campo);
      if (!p || p.cuota < CUOTA_ACTUAL) continue;
      const actual = num(u[campo]);
      if (actual !== null && actual !== p.dominante) {
        difiere += 1;
        porColumna.set(campo, porColumna.get(campo) + 1);
      }
    }
    if (difiere > 0) conPropuesta += 1;
  }
  const tasa = conPropuesta / (muestra.length || 1);
  console.log(`Muestra: ${muestra.length} usuarios · recibirían propuesta: ${conPropuesta} (${pct(tasa)})`);
  for (const [campo, n] of porColumna) {
    console.log(`   ${campo.padEnd(26)} ${n} usuarios (${pct(n / (muestra.length || 1))})`);
  }
  console.log('');
  if (tasa > 0.4) {
    console.log('SUBA el umbral de cuota (CUOTA_MINIMA en exploracion.ts): a demasiada gente le sale algo,');
    console.log('y una corrección que aplica a la mitad del padrón no está señalando un problema.');
  } else if (tasa < 0.02) {
    console.log('BAJE el umbral, o revise las columnas: casi nadie recibe propuesta y la exploración no sirve de nada.');
  } else {
    console.log('El umbral actual (0.75) se comporta razonable. Contrástelo con tickets reales ya resueltos.');
  }
}

await fuente.cerrar();

/*
 * Nota: `usuariosActivosMuestra` todavía no está en el catálogo porque sacar una
 * muestra de usuarios completa no hace falta para diagnosticar, solo para calibrar.
 * Para correr esta parte, agregue a `dominio/oracle/catalogo.ts`:
 *
 *   usuariosActivosMuestra: {
 *     descripcion: 'Muestra de usuarios que operaron, para calibrar la exploración',
 *     sql: `SELECT u.usr_estado, u.usr_intentos, u.usr_intentos_pin,
 *                  u.usr_is_clave_temporal, u.usr_is_pin_temporal
 *           FROM corresponsales.usuario u
 *           WHERE EXISTS (
 *             SELECT 1 FROM corresponsales.usuario_pos up
 *             JOIN corresponsales.transaccion t ON t.trans_pos = up.up_pos
 *             WHERE up.up_usuario = u.usr_codigo AND t.trans_fecha >= SYSDATE - :dias)`,
 *   },
 *
 * y suba `SOPORTE_ORACLE_MAX_FILAS` para que no recorte la muestra.
 */
