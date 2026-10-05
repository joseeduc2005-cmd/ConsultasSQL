/**
 * El middleware que audita todo lo que entra.
 *
 * El problema que resuelve no es técnico, es de método: hasta ahora auditar era una
 * llamada que había que escribir a mano dentro de cada manejador. De las rutas que
 * cambiaban algo, la mitad no la tenía — incluida la que borra el historial entero,
 * que es precisamente la que más falta hace poder rastrear. Y cada ruta nueva nacía
 * sin auditoría salvo que alguien se acordara.
 *
 * Va en dos piezas, y el motivo es concreto. La primera versión se montaba detrás de
 * express.json y del limitador de peticiones, para tener el cuerpo y la sesión a mano.
 * Al medirlo se vio lo que eso dejaba fuera: una petición rechazada por exceso devuelve
 * 429 sin llegar nunca al middleware, y un cuerpo JSON mal formado revienta en el
 * analizador antes de llegar también. Las dos cosas quedaban sin una línea, y son
 * justo las que interesa ver: alguien martilleando la API y alguien mandando algo
 * que el sistema no entiende.
 *
 * Así que la garantía va delante de todo y no depende de nada:
 *
 *   auditarTodaPeticion   primero de la cadena. Asegura la fila: método, ruta, estado,
 *                         duración, quién. No necesita el cuerpo ni la sesión, porque
 *                         los lee al terminar, cuando ya están.
 *   capturarCuerpo        detrás de express.json. Añade el cuerpo cuando se pudo leer.
 *                         Si el JSON venía roto no corre, y la fila queda sin cuerpo:
 *                         que es lo correcto, no había cuerpo que guardar.
 *
 * Se registra al terminar la respuesta, así que también quedan los intentos que
 * fallaron y los que se denegaron.
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
 * El cuerpo, redactado en cuanto se puede leer y antes de que nadie lo toque.
 *
 * Se guarda aquí y no al terminar porque algunos manejadores modifican req.body por
 * el camino, y lo que interesa es lo que llegó, no lo que quedó.
 */
export function capturarCuerpoParaAuditoria(req, _res, next) {
  if (req.body && typeof req.body === 'object') {
    req.cuerpoAuditado = redactarCuerpo(req.body);
  }
  next();
}

/**
 * @param {object} deps
 * @param {Function} deps.logRequest        dónde se deja el rastro
 * @param {Function} [deps.auditarLecturas] si además se registran los GET; por defecto no
 */
export function crearAuditoriaDePeticiones({ logRequest, auditarLecturas = false }) {
  return function auditarTodaPeticion(req, res, next) {
    const metodo = String(req.method || '').toUpperCase();
    const ruta = String(req.originalUrl || req.url || '').split('?')[0];

    if (!seAudita(metodo, ruta) || (!cambiaAlgo(metodo) && !auditarLecturas)) {
      return next();
    }

    const empezo = Date.now();
    let yaRegistrada = false;

    const registrar = (comoAcabo) => {
      // Una sola fila por petición: 'finish' y 'close' saltan los dos en una petición
      // normal, y dos filas de lo mismo convertirían el recuento en algo que no se
      // puede leer.
      if (yaRegistrada) return;
      yaRegistrada = true;

      // Se lee al terminar y no al empezar: así este middleware puede ir delante del
      // analizador de JSON y del limitador sin perder quién pidió ni qué mandó.
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
        cuerpo: req.cuerpoAuditado || null,
        comoAcabo,
      });
    };

    res.on('finish', () => registrar('completada'));

    // Y 'close' además de 'finish', por las rutas que mantienen la conexión abierta:
    // la de stream manda el avance de un script por SSE, y si quien la pidió cierra
    // la pestaña a mitad, 'finish' no salta nunca. Sin esto, una ejecución abandonada
    // no dejaba una sola línea — y es la que más interesa, porque nadie vio el final.
    res.on('close', () => registrar('cortada'));

    return next();
  };
}
