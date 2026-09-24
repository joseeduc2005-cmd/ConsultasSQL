import { numero, texto } from './motor.ts';
import { CAMPO_CEDULA } from './campos.ts';
import type { Playbook } from './tipos.ts';

/**
 * "No deja eliminar o corregir un acta de pago al corresponsal".
 *
 * Del ticket 2026090874. No es una avería: es una regla de negocio de la aplicación.
 * Un acta que ya tiene un pago en estado Cerrado no se puede editar desde la pantalla,
 * y cuando hay que rehacerla —por ejemplo porque la factura salió sin la leyenda
 * requerida— el arreglo sale del alcance de la aplicación y lo ejecuta Administración
 * de Aplicaciones directamente en base.
 *
 * El diagnóstico comprueba que efectivamente hay un pago cerrado, que es la
 * verificación que evita borrar el acta equivocada.
 */
export const actasPago: Playbook = {
  id: 'actas-pago',
  nombre: 'Actas y pagos al corresponsal',
  descripcion:
    'Revisa por qué no se puede eliminar o corregir un acta de pago: comprueba si ya tiene un pago en estado Cerrado, que es lo que la aplicación bloquea.',
  sistema: 'Corresponsales',
  categoria: 'Actas y pagos',
  senales: [
    /\bacta(s)?\b/i,
    /(no (se )?(permite|deja|puede)).{0,30}(eliminar|borrar|corregir|editar)/i,
    /solicitud de pago/i,
    /(pago|pagos).{0,25}(pendiente|corresponsal)/i,
    /leyenda/i,
    /comprobante de pago/i,
  ],
  ticketsReferencia: ['2026090874'],
  campos: [
    { ...CAMPO_CEDULA, etiqueta: 'Cédula del corresponsal', requerido: false },
    { clave: 'acta', etiqueta: 'Número de acta', tipo: 'texto', patron: '^\\d{1,10}$', ejemplo: '53656' },
  ],
  sinDatos: true,

  async ejecutar(ctx) {
    const { cedula, acta } = ctx.entrada;

    if (acta || cedula) {
      await ctx.paso('pagos', 'Estado de los pagos asociados al acta', async (p) => {
        const filas = await p.consultar('pagosDeActa', { acta: acta ?? null, cedula: cedula ?? null });
        if (filas.length === 0) {
          return p.ok('El acta no tiene pagos asociados: la aplicación debería permitir editarla.');
        }
        const cerrados = filas.filter((f) => texto(f.estado).toUpperCase() === 'C');
        if (cerrados.length > 0) {
          p.falla(`${cerrados.length} pago(s) en estado Cerrado sobre ${filas.length} asociados.`);
          ctx.hallazgo({
            severidad: 'media',
            titulo: 'El acta está bloqueada por un pago en estado Cerrado',
            detalle:
              `Actas afectadas: ${filas.map((f) => `${texto(f.numero_acta)} (estado ${texto(f.estado)})`).join(', ')}.\n\n` +
              'La aplicación no deja eliminar ni corregir un acta con pago Cerrado. Es una regla de negocio suya, ' +
              'no una restricción de la base: por eso el arreglo tiene que salir por fuera de la aplicación.',
            ticketsRelacionados: ['2026090874'],
            acciones: [
              {
                descripcion:
                  'Escalar a Administración de Aplicaciones: eliminar el acta de actacomprobantes verificando primero que el pago Cerrado corresponda a la cédula del socio, y volver a generarla junto con los meses pendientes.',
                riesgo: 'escritura',
                responsable: 'Administración de Aplicaciones',
              },
            ],
          });
        } else {
          p.alerta(`${filas.length} pago(s) asociados, ninguno Cerrado.`);
        }
      });
      return;
    }

    await ctx.paso('regla', 'Regla que bloquea la edición del acta', async (p) => {
      p.alerta('La aplicación bloquea las actas que ya tienen un pago en estado Cerrado.');
    });

    ctx.hallazgo({
      severidad: 'info',
      titulo: 'Un acta con pago Cerrado no se edita desde la aplicación',
      detalle:
        'Patrón del ticket 2026090874: el acta ya tenía un pago asociado en estado Cerrado, y la pantalla no deja ' +
        'eliminarla ni corregirla. Cuando hay que rehacerla —por ejemplo porque la factura salió sin la leyenda ' +
        'requerida— el arreglo lo ejecuta Administración de Aplicaciones directo en base, verificando antes que el ' +
        'pago Cerrado sea del socio correcto para no borrar el acta equivocada.\n\n' +
        'Con el número de acta o la cédula del corresponsal este diagnóstico comprueba si es ese el caso.',
      ticketsRelacionados: ['2026090874'],
      acciones: [
        {
          descripcion: 'Pedir el número de acta o la cédula del corresponsal y volver a revisar.',
          riesgo: 'lectura',
          responsable: 'Soporte',
        },
      ],
    });

    // Los identificadores numéricos del acta no se extraen del texto todavía, así que
    // este camino es el habitual mientras el ticket no traiga la cédula.
    void numero;
  },
};
