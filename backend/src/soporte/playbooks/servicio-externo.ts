import type { Playbook } from './tipos.ts';

/**
 * "Falla un servicio de un proveedor externo" (recargas, pagos de terceros).
 *
 * No consulta nada: no hay nada que consultar. La causa del ticket 2026091131 fue una
 * caída del proveedor Facilito, ajena a los sistemas de la cooperativa, y se resolvió
 * sola cuando el proveedor restableció el servicio.
 *
 * Existe porque reconocer el caso ya ahorra trabajo: antes caía en «sin hallazgos» y
 * quien atendía se ponía a buscar en Oracle un problema que no estaba ahí.
 */
export const servicioExterno: Playbook = {
  id: 'servicio-externo',
  nombre: 'Servicio de proveedor externo',
  descripcion:
    'Reconoce las fallas de servicios de terceros (recargas Claro, Movistar, pagos de convenios) que no dependen de los sistemas de la cooperativa.',
  sistema: 'Corresponsales',
  categoria: 'Servicios externos',
  senales: [
    /recarga/i,
    /\b(claro|movistar|cnt|tuenti)\b/i,
    /facilito/i,
    /proveedor/i,
    /\bconvenio\b/i,
  ],
  ticketsReferencia: ['2026091131'],
  campos: [],
  sinDatos: true,

  async ejecutar(ctx) {
    await ctx.paso('externo', 'Origen del servicio', async (p) => {
      p.alerta('El servicio lo presta un proveedor externo; la cooperativa solo lo intermedia.');
    });

    ctx.hallazgo({
      severidad: 'media',
      titulo: 'Probable caída del proveedor externo, no de los sistemas propios',
      detalle:
        'Patrón del ticket 2026091131: las recargas fallaron por una caída de Facilito, el proveedor, y se ' +
        'restablecieron solas sin que Sistemas tocara nada. Ese caso se había reportado también el día anterior.\n\n' +
        'Antes de investigar en la base conviene descartar lo de fuera: si varios corresponsales reportan lo mismo ' +
        'a la vez y las demás operaciones funcionan, es del proveedor.',
      ticketsRelacionados: ['2026091131'],
      acciones: [
        {
          descripcion: 'Confirmar con el proveedor si hay incidencia abierta, y preguntar al área si el fallo es general o de un solo corresponsal.',
          riesgo: 'lectura',
          responsable: 'Corresponsales',
        },
      ],
    });
  },
};
