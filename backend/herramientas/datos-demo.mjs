/**
 * Llena el aprendizaje y el registro de búsquedas con datos de muestra.
 *
 * Es para enseñar el sistema cuando todavía no lo ha usado nadie: las dos tablas que
 * mejor cuentan la historia —lo que la gente abre y lo que buscó sin encontrar— están
 * vacías hasta que haya uso real, y vacías no se entiende lo que hacen.
 *
 * Los datos son inventados, pero no se escriben a mano: pasan por las mismas funciones
 * que usa el sistema de verdad, así que las filas quedan exactamente como quedarían con
 * uso real. Lo que se ve en la demostración es el mecanismo, no una maqueta.
 *
 *   node herramientas/datos-demo.mjs            poner los datos
 *   node herramientas/datos-demo.mjs --borrar   quitarlos
 *
 * `--borrar` quita solo lo que este script puso: las búsquedas marcadas como demo y las
 * parejas que aparecen aquí abajo. Lo aprendido con uso real se queda.
 */
import 'dotenv/config';
import pg from 'pg';
import { readFileSync } from 'node:fs';

const BORRAR = process.argv.includes('--borrar');
const AUTOR_DEMO = 'demo.reunion';

const b = JSON.parse(readFileSync('config/multidb.databases.json', 'utf8')).databases.find((x) => x.id === 'pg_main');
const pool = new pg.Pool({ host: b.host, port: b.port, database: b.database, user: b.user, password: b.password });
const { configurarPool } = await import('../src/soporte/knowledge/db.ts');
configurarPool(() => pool);
const { buscarPrecedentes, registrarAcierto, registrarBusqueda, aConsultaParaPruebas } = await import(
  '../src/soporte/precedentes.ts'
);

/**
 * Búsquedas que acabaron bien: alguien escribió esto y abrió ese tema.
 *
 * `veces` es cuánta gente hizo lo mismo. Varias personas describiendo el mismo problema
 * con palabras distintas es justo lo que el aprendizaje aprovecha, así que las frases
 * varían a propósito.
 */
const ACERTADAS = [
  { texto: 'el POS no imprime el comprobante de la recaudacion local', tema: 'No imprime el comprobante%', veces: 4 },
  { texto: 'cobro bien pero no salio el documento de respaldo', tema: 'No imprime el comprobante%', veces: 3 },
  { texto: 'la tendera dice que no sale el papelito del cobro', tema: 'No imprime el comprobante%', veces: 2 },
  { texto: 'al corresponsal se le bloquea el usuario cada vez que entra', tema: 'No permite ingresar%', veces: 5 },
  { texto: 'lo desbloquean y al rato se vuelve a inhabilitar', tema: 'No permite ingresar%', veces: 2 },
  { texto: 'la señora hizo una recarga y nunca le entro el saldo al celular', tema: 'La recarga se cobra%', veces: 3 },
  { texto: 'la socia cambio de oficina y en contabilidad sale la agencia vieja', tema: 'Descuadre contable%', veces: 2 },
  { texto: 'no me deja sacar el listado completo se corta a las 300 filas', tema: 'El reporte del corresponsal se corta%', veces: 3 },
  { texto: 'quiero cambiar mi clave y me dice que no cumple el formato', tema: 'El cambio de contraseña%', veces: 4 },
  { texto: 'reseteen la clave de la socia pero dice que el usuario no existe', tema: 'No se puede resetear%', veces: 2 },
  { texto: 'un cliente pago con el codigo QR y no se registro la transaccion', tema: 'Pagos con QR%', veces: 2 },
  { texto: 'el corresponsal reclama que no le han pagado su comision del mes', tema: 'Comisión no pagada%', veces: 3 },
];

/**
 * Búsquedas que se fueron sin nada.
 *
 * Son las que llenan el panel de «buscaron esto y no encontraron», y las dos clases
 * están representadas: las que no devuelven nada —falta el tema— y las que devuelven
 * algo que nadie abre, donde el tema existe pero no lo encuentran con esas palabras.
 */
const FALLIDAS = [
  { texto: 'cheque protestado de un socio como se reversa', veces: 6 },
  { texto: 'doña rosa pago su planilla pero el papelito no se imprimio', veces: 4 },
  { texto: 'como anulo una transferencia que se hizo dos veces', veces: 3 },
  { texto: 'el aparatito se queda pensando y no termina el cobro', veces: 3 },
  { texto: 'no puedo cerrar la caja del dia me da descuadre', veces: 2 },
  { texto: 'mandaron plata por el codigo de barras cuadrado y no aparece', veces: 2 },
];

async function temaPorTitulo(like) {
  const r = await pool.query('SELECT id::text FROM knowledge_base WHERE titulo LIKE $1 LIMIT 1', [like]);
  return r.rows[0]?.id ?? null;
}

if (BORRAR) {
  const { rowCount: nb } = await pool.query('DELETE FROM soporte.busquedas WHERE autor = $1', [AUTOR_DEMO]);

  // Solo las parejas que este script pudo haber puesto, para no llevarse por delante lo
  // que el sistema haya aprendido de verdad desde entonces.
  let na = 0;
  for (const caso of ACERTADAS) {
    const id = await temaPorTitulo(caso.tema);
    if (!id) continue;
    const terminos = (aConsultaParaPruebas(caso.texto) ?? '').split(' | ').filter(Boolean);
    if (!terminos.length) continue;
    const { rowCount } = await pool.query(
      'DELETE FROM soporte.aprendizaje WHERE articulo = $1 AND termino = ANY($2::text[])',
      [id, terminos],
    );
    na += rowCount;
  }

  console.log(`Quitados: ${nb} búsqueda(s) de demostración y ${na} fila(s) de aprendizaje.`);
} else {
  let aciertos = 0;
  for (const caso of ACERTADAS) {
    const id = await temaPorTitulo(caso.tema);
    if (!id) {
      console.log(`  --   no encontré el tema «${caso.tema}»`);
      continue;
    }
    for (let i = 0; i < caso.veces; i++) {
      const r = await buscarPrecedentes(caso.texto, { limite: 3, soloPublicos: true });
      await registrarBusqueda(caso.texto, AUTOR_DEMO, r.length, r[0]?.confianza ?? null);
      await registrarAcierto(caso.texto, id);
      aciertos++;
    }
    console.log(`  ok   ${String(caso.veces).padStart(2)}×  ${caso.texto.slice(0, 54)}`);
  }

  let fallos = 0;
  for (const caso of FALLIDAS) {
    for (let i = 0; i < caso.veces; i++) {
      const r = await buscarPrecedentes(caso.texto, { limite: 3, soloPublicos: true });
      // Sin `registrarAcierto`: nadie abrió nada, que es justo lo que las hace fallidas.
      await registrarBusqueda(caso.texto, AUTOR_DEMO, r.length, r[0]?.confianza ?? null);
      fallos++;
    }
    console.log(`  --   ${String(caso.veces).padStart(2)}×  ${caso.texto.slice(0, 54)}`);
  }

  const ap = await pool.query('SELECT count(*)::int n FROM soporte.aprendizaje');
  console.log(`\n${aciertos} búsqueda(s) que acabaron en un tema, ${fallos} que se fueron sin nada.`);
  console.log(`La tabla de aprendizaje tiene ahora ${ap.rows[0].n} pareja(s) palabra → tema.`);
  console.log('\nPara quitarlo todo:  node herramientas/datos-demo.mjs --borrar');
}

await new Promise((listo) => setTimeout(listo, 100));
await pool.end();
