/**
 * El middleware que audita todo lo que cambia algo.
 *
 * El problema que resuelve no es técnico, es de método: hasta ahora auditar era una
 * llamada que había que escribir a mano dentro de cada manejador. De las rutas que
 * cambiaban algo, la mitad no la tenía — incluida la que borra el historial entero,
 * que es precisamente la que más falta hace poder rastrear. Y cada ruta nueva nacía
 * sin auditoría salvo que alguien se acordara.
 *
 * Aquí el rastro no depende de acordarse. Se registra en el camino de salida, cuando
 * ya se sabe cómo acabó, así que también quedan los intentos que fallaron y los que
 * se denegaron — que son los que de verdad interesa poder mirar después.
 *
 * Los registros específicos de antes (logLoginEvent, logArticleChange, logQueryHistory)
 * siguen donde estaban: esto no los sustituye. Ellos cuentan *qué* cambió con detalle;
 * esto garantiza que no haya nada que no deje constancia de *que* pasó.
 */
import { cambiaAlgo, redactarCuerpo, seAudita } from '../../dominio/auditoria/redaccion.js';

const ipDe = (req) => (
  req.headers['x-forwarded-for']?.split(',')[0]?.trim()
  || req.ip
  || req.socket?.remoteAddress
  || null
);

/**
 * @param {object} deps
 * @param {Function} deps.logRequest        dónde se deja el rastro
 * @param {Function} [deps.auditarLecturas] si además se registran los GET; por defecto no
 */
export function crearAuditoriaDePeticiones({ logRequest, auditarLecturas = false }) {
  return function auditarPeticion(req, res, next) {
    const metodo = String(req.method || '').toUpperCase();
    const ruta = String(req.originalUrl || req.url || '').split('?')[0];

    if (!seAudita(metodo, ruta) || (!cambiaAlgo(metodo) && !auditarLecturas)) {
      return next();
    }

    // El cuerpo se redacta ahora, antes de que el manejador lo toque: algunos lo
    // modifican por el camino y lo que interesa es lo que llegó, no lo que quedó.
    const cuerpo = redactarCuerpo(req.body);
    const empezo = Date.now();

    res.on('finish', () => {
      // La sesión se resuelve aquí y no arriba porque `adjuntarSesion` la deja en la
      // petición y, en un login, el usuario solo se conoce al terminar.
      const usuario = req.usuarioSesion?.username || req.usuarioSesion?.usuario || null;
      const rol = req.usuarioSesion?.role || req.usuarioSesion?.rol || null;

      // Sin await: la respuesta ya salió y nadie espera por esto.
      void logRequest({
        metodo,
        ruta,
        usuario,
        rol,
        ip: ipDe(req),
        userAgent: req.headers['user-agent'] || null,
        estado: res.statusCode,
        duracionMs: Date.now() - empezo,
        cuerpo,
      });
    });

    return next();
  };
}
