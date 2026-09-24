import { numero, texto } from './motor.ts';
import { CAMPO_CEDULA } from './campos.ts';
import type { Playbook } from './tipos.ts';

/**
 * "No imprime el comprobante de una recaudación local".
 *
 * Del ticket 2026082703. La avería es solo de la emisión del comprobante: el cobro
 * se registra bien, y desarrollo ya tiene el arreglo en curso.
 *
 * Lo que aporta el diagnóstico es justo lo que tranquiliza a quien reclama: confirmar
 * con datos que la transacción sí entró. En el ticket original eso se respondió de
 * palabra, sin consultar nada; aquí se comprueba.
 */
export const comprobanteRecaudacion: Playbook = {
  id: 'comprobante-recaudacion',
  nombre: 'Comprobante de recaudación',
  descripcion:
    'Revisa el caso de «no imprime el comprobante» de una recaudación local: confirma que el cobro sí se registró y separa la avería de impresión de la transacción.',
  sistema: 'Corresponsales',
  categoria: 'Recaudaciones',
  senales: [
    /reimpres/i,
    /comprobante/i,
    /(no|error).{0,25}imprim/i,
    /recaudaci/i,
    /documentos de respaldo/i,
    /(error|falla).{0,30}(al (momento de la|procesar)).{0,20}recaudaci/i,
    // El 2026085243 es una recaudación de municipio que falla al procesar y sigue
    // abierta. Estas señales existían solo como «tema» de orientación, sin nadie que
    // las atendiera; ahora las cubre este diagnóstico.
    /municipio/i,
    /gad/i,
  ],
  ticketsReferencia: ['2026082703'],
  campos: [{ ...CAMPO_CEDULA, etiqueta: 'Cédula del corresponsal', requerido: false }],
  sinDatos: true,

  async ejecutar(ctx) {
    const { cedula } = ctx.entrada;

    // Con cédula se confirma que el corresponsal existe y que sus transacciones
    // entran: es la prueba de que el problema es solo del comprobante.
    if (cedula) {
      const usuarios = await ctx.paso('corresponsal', 'Identificar al corresponsal', async (p) => {
        const filas = await p.consultar('usuarioPorCedula', { cedula });
        if (filas.length === 0) {
          p.falla('La cédula no corresponde a ningún usuario de corresponsales.');
          return [];
        }
        p.ok(`Usuario ${texto(filas[0].usr_usuario)}, estado ${texto(filas[0].usr_estado)}.`);
        return filas;
      });

      if (usuarios && usuarios.length > 0) {
        await ctx.paso('cobros', 'Transacciones recientes del corresponsal', async (p) => {
          const filas = await p.consultar('transaccionesRecientesPorCedula', { cedula });
          const total = numero(filas[0]?.total);
          if (total > 0) {
            p.ok(`${total} transacción(es) registradas en los últimos 30 días. Los cobros sí entran.`);
          } else {
            p.alerta('Ninguna transacción en los últimos 30 días para este corresponsal.');
          }
        });
      }
    }

    ctx.hallazgo({
      severidad: 'media',
      titulo: 'Separar el cobro de la emisión del comprobante',
      detalle:
        'En Recaudaciones Locales hay dos fallos distintos, y lo primero es saber cuál es. La diferencia está en ' +
        'si el cobro llegó a entrar:' +
        '\n\n' +
        '· Si el cobro SÍ se registró y el error aparece solo al imprimir o reimprimir el comprobante, es la ' +
        'avería del ticket 2026082703. Desarrollo ya la identificó y la corrección está pendiente de salir. Lo ' +
        'primero es tranquilizar a quien reclama: el dinero se cobró.' +
        '\n\n' +
        '· Si el cobro NO llegó a procesarse, es otro caso. El ticket 2026085243 (Municipio GAD Mejía) sigue ' +
        'abierto y sin causa raíz documentada, así que ahí todavía hace falta revisar los logs del POS.',
      ticketsRelacionados: ['2026082703', '2026085243'],
      acciones: [
        {
          descripcion:
            'Confirmar al solicitante que la transacción se cobró y que el fallo es solo del comprobante, con la corrección en curso.',
          riesgo: 'lectura',
          responsable: 'Soporte',
        },
        ...(cedula
          ? []
          : [
              {
                descripcion: 'Pedir la cédula del corresponsal para comprobar en la base que el cobro entró.',
                riesgo: 'lectura' as const,
                responsable: 'Soporte',
              },
            ]),
      ],
    });
  },
};
