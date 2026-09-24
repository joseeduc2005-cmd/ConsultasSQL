import { config } from '../config.ts';
import { CAMPO_MAC, campoFecha } from './campos.ts';
import { numero, texto } from './motor.ts';
import type { Playbook } from './tipos.ts';

const aFecha = (iso: string) => new Date(`${iso}T00:00:00`);

/** "El reporte de transacciones no se genera o sale incompleto". */
export const reporteTransacciones: Playbook = {
  id: 'reporte-transacciones',
  nombre: 'Reporte de transacciones',
  descripcion:
    'Revisa por qué el reporte de transacciones de un POS falla o sale incompleto: límite de registros, campos NULL que rompen el mapeo de Hibernate y valores que anulan los filtros NOT IN.',
  sistema: 'Corresponsales',
  categoria: 'Reportes',
  senales: [/reporte/i, /report/i, /transacciones/i, /hibernate/i, /propertyaccess/i, /movimientotendero/i, /no (carga|genera|muestra)/i, /\b300\b/],
  ticketsReferencia: ['2026084289', '2026091974'],
  campos: [{ ...CAMPO_MAC, requerido: true }, campoFecha('desde', 'Desde'), campoFecha('hasta', 'Hasta')],
  sinDatos: true,

  async ejecutar(ctx) {
    const { mac, desde, hasta } = ctx.entrada;

    // Los tickets reales de este tipo llegan sin MAC ni fechas: «no genera el
    // reporte» y nada más. No se puede medir el volumen, pero la causa ya se
    // investigó dos veces y es siempre la misma, así que se dice.
    if (!mac || !desde || !hasta) {
      await ctx.paso('limite-conocido', 'Causa conocida del reporte', async (p) => {
        p.alerta(`El reporte tiene un límite fijo de ${config.reglas.limiteReporte} transacciones por consulta.`);
      });
      ctx.hallazgo({
        severidad: 'media',
        titulo: `El reporte falla al superar ${config.reglas.limiteReporte} transacciones`,
        detalle:
          `Causa ya confirmada en los tickets 2026084289 y 2026091974: el reporte de corresponsales trae un límite ` +
          `fijo de ${config.reglas.limiteReporte} transacciones y, al pedirle un rango con más, da error en vez de paginar. ` +
          `Desarrollo tiene la mejora en curso.

` +
          `Mientras tanto: pedir al corresponsal rangos de fechas más cortos.

` +
          `Para confirmarlo en este caso hace falta la MAC del POS y el rango de fechas; con eso el diagnóstico ` +
          `mide el volumen real y descarta además los campos NULL que rompen el reporte.`,
        ticketsRelacionados: ['2026084289', '2026091974'],
        acciones: [
          {
            descripcion: 'Pedir al solicitante la MAC del POS y el rango de fechas, y volver a revisar.',
            riesgo: 'lectura',
            responsable: 'Soporte',
          },
        ],
      });
      return;
    }

    const rango = { mac, desde: aFecha(desde), hasta: aFecha(hasta) };

    await ctx.paso('volumen', 'Volumen de transacciones del rango', async (p) => {
      if (desde > hasta) return p.falla('La fecha "desde" es posterior a "hasta".');
      const [resumen] = await p.consultar('resumenTransaccionesPorMac', rango);
      const total = numero(resumen?.total);
      const limite = config.reglas.limiteReporte;
      if (total > limite) {
        p.falla(`${total} transacciones en el rango; el reporte admite hasta ${limite}.`);
        ctx.hallazgo({
          severidad: 'alta',
          titulo: `El rango supera el límite de ${limite} transacciones del reporte`,
          detalle: `Hay ${total} transacciones entre ${desde} y ${hasta}. El reporte falla en lugar de paginar. Mientras desarrollo publica la mejora, pedir al corresponsal rangos más cortos.`,
          ticketsRelacionados: ['2026084289'],
        });
      } else if (total === 0) {
        p.alerta('No hay transacciones en el rango para ese POS.');
      } else {
        p.ok(`${total} transacciones, dentro del límite de ${limite}.`);
      }
    });

    await ctx.paso('tendero-nulo', 'Transacciones con trans_movimiento_tendero en NULL', async (p) => {
      const filas = await p.consultar('transaccionesSinMovimientoTendero', rango);
      if (filas.length === 0) return p.ok('Ninguna transacción del rango tiene el campo en NULL.');
      p.falla(`${filas.length} transacción(es) con trans_movimiento_tendero en NULL.`);
      ctx.hallazgo({
        severidad: 'alta',
        titulo: 'Transacciones con trans_movimiento_tendero en NULL',
        detalle: `El backend mapea transMovimientoTendero a un tipo primitivo; un NULL provoca PropertyAccessException y el reporte no se genera. Transacciones: ${filas
          .slice(0, 10)
          .map((f) => `${texto(f.trans_id)} (tipo ${texto(f.trans_tipo)})`)
          .join(', ')}${filas.length > 10 ? '…' : ''}.`,
        ticketsRelacionados: ['2026091974'],
        acciones: [
          {
            descripcion:
              'Reportar a desarrollo: cambiar el tipo del campo a su clase contenedora (Long/Integer) en la entidad y averiguar qué proceso inserta la transacción sin movimiento.',
            riesgo: 'lectura',
            responsable: 'Equipo de desarrollo',
            biblioteca: 'T3',
          },
        ],
      });
    });

    await ctx.paso('not-in', 'Valores NULL en las subconsultas del reporte', async (p) => {
      const [f] = await p.consultar('nulosEnSubconsultasReporte');
      const fondeo = numero(f?.fondeo_nulos);
      const reverso = numero(f?.reverso_nulos);
      if (fondeo + reverso === 0) return p.ok('Sin valores NULL en movimiento_fondeo ni reverso.');
      p.alerta(`movimiento_fondeo.id_transaccion_rev: ${fondeo} NULL · reverso.rev_transaccion: ${reverso} NULL.`);
      ctx.hallazgo({
        severidad: 'media',
        titulo: 'Posible reporte vacío por NOT IN con valores NULL',
        detalle:
          'La consulta del reporte filtra con "trans_id NOT IN (SELECT …)". En Oracle, si la subconsulta devuelve un solo NULL el resultado completo queda vacío, sin error. Recomendación para desarrollo: reemplazar por NOT EXISTS.',
      });
    });
  },
};
