/**
 * Lo que le pasa al propio servicio: que arranca, que se detiene, que se cae.
 *
 * Antes nada de esto dejaba rastro. El proceso se iba y el único sitio donde constaba
 * era la consola de quien lo estuviera mirando en ese momento; un reinicio no planeado
 * de madrugada no dejaba ni una línea en la base.
 *
 * Está aquí y no suelto en app.js por un motivo concreto: escrito allí no se podía
 * comprobar. En Windows `process.kill` con SIGINT mata el proceso sin ejecutar el
 * manejador, así que la única forma de saber si el código era correcto era desplegarlo
 * y esperar. Recibiendo el proceso y los registradores por parámetro se prueba con un
 * emisor de mentira, sin matar nada.
 */

/**
 * @param {object} deps
 * @param {Function} deps.logSystemEvent   dónde se deja el evento
 * @param {Function} deps.logError         dónde se deja el error
 * @param {object}   [deps.proceso]        el proceso; se inyecta para poder probarlo
 */
export function registrarCicloDeVida({ logSystemEvent, logError, proceso = process }) {
  let yaCerrando = false;

  const registrarCierre = async (motivo, detalle = {}) => {
    // Una sola vez: un SIGTERM seguido de una excepción al cerrar no debe dejar dos
    // filas contradictorias sobre la misma parada.
    if (yaCerrando) return false;
    yaCerrando = true;

    // Con await, a diferencia del resto de la auditoría: si no se espera, el proceso
    // se va antes de que la fila llegue a la base y el cierre se pierde igual que antes.
    await logSystemEvent({
      evento: 'SERVICIO_DETENIDO',
      usuario: 'sistema',
      rol: 'sistema',
      detalle: { motivo, enPieSegundos: Math.round(proceso.uptime()), ...detalle },
    }).catch(() => {});
    return true;
  };

  for (const senal of ['SIGTERM', 'SIGINT']) {
    proceso.on(senal, async () => {
      await registrarCierre(senal);
      proceso.exit(0);
    });
  }

  proceso.on('uncaughtException', async (error) => {
    await logError({
      mensaje: error?.message || String(error),
      stack: error?.stack || null,
      capa: 'proceso',
      contexto: 'uncaughtException',
    }).catch(() => {});
    await registrarCierre('uncaughtException', { error: error?.message || String(error) });
    proceso.exit(1);
  });

  proceso.on('unhandledRejection', (motivo) => {
    // Una promesa sin capturar no tumba el proceso, así que aquí no se cierra nada: se
    // deja constancia y el servicio sigue.
    void logError({
      mensaje: motivo?.message || String(motivo),
      stack: motivo?.stack || null,
      capa: 'proceso',
      contexto: 'unhandledRejection',
    });
  });

  return { registrarCierre };
}
