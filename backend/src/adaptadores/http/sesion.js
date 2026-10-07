// ─── Session store & request helpers ─────────────────────────────────────────
// Shared between auth, content, and any route that needs the current user.

export const sessions = new Map();

/**
 * Cuánto vale una sesión.
 *
 * Antes no caducaban nunca: la única comprobación era `activo = true`, así que un
 * token emitido hace seis meses seguía sirviendo para entrar. En una cooperativa eso
 * significa que un token copiado una vez vale para siempre.
 *
 * Doce horas por defecto —una jornada— y se puede cambiar con SESION_MAX_HORAS.
 */
const HORAS_MAX_SESION = Math.max(1, Number(process.env.SESION_MAX_HORAS) || 12);

/**
 * Cuánto puede responder la caché en memoria sin volver a preguntar a la base.
 *
 * La caché es el segundo agujero del mismo problema: aunque la consulta mire la
 * caducidad, una sesión ya cacheada se responde sin consultar nada. Con esto, la
 * memoria contesta como mucho cinco minutos y después vuelve a comprobar — así una
 * sesión caducada o un cierre de sesión tardan cinco minutos en notarse, no un
 * reinicio del backend.
 */
const MS_CACHE_SESION = 5 * 60 * 1000;

/** Cuándo entró cada token en la caché. authRoute la llena al iniciar sesión. */
const vistaEn = new Map();

function cacheAunVale(token) {
  const desde = vistaEn.get(token);
  if (desde === undefined) {
    // La puso authRoute al iniciar sesión, así que el reloj empieza ahora.
    vistaEn.set(token, Date.now());
    return true;
  }
  if (Date.now() - desde < MS_CACHE_SESION) return true;
  sessions.delete(token);
  vistaEn.delete(token);
  return false;
}

export function olvidarSesion(token) {
  sessions.delete(token);
  vistaEn.delete(token);
}

/** Pool de PostgreSQL — se inicializa desde app.js para poder restaurar sesiones */
let _pool = null;
export function initSessionStore(pool) { _pool = pool; }

/**
 * El usuario de la petición, sin esperar.
 *
 * Mira primero lo que dejó `adjuntarSesion`, que sí pudo consultar la base. Sin eso
 * solo queda la caché en memoria, y esa está vacía tras reiniciar el backend aunque la
 * sesión siga siendo válida — que es como un administrador acababa recibiendo 403 en
 * sus propias pantallas.
 */
export function getUserFromRequest(req) {
  if (req?.usuarioSesion !== undefined) return req.usuarioSesion;
  const authHeader = req.headers.authorization || '';
  const token = authHeader.replace('Bearer ', '').trim();
  if (!cacheAunVale(token)) return null;
  return sessions.get(token) || null;
}

/**
 * Middleware que resuelve la sesión una vez por petición.
 *
 * Las rutas la leen de forma síncrona en veintiún sitios. Hacerlas asíncronas una por
 * una era tocar ocho archivos para arreglar lo mismo; resolviéndola aquí, todas ven el
 * usuario correcto sin cambiar ni una línea.
 */
export function adjuntarSesion(req, _res, next) {
  getUserFromRequestAsync(req)
    .then((usuario) => {
      req.usuarioSesion = usuario;
      next();
    })
    .catch(() => {
      req.usuarioSesion = null;
      next();
    });
}

/**
 * Versión async: busca en memoria primero, luego en la BD si no está cacheada.
 * Úsala en rutas SSE donde el header Authorization llega desde la cookie.
 */
export async function getUserFromRequestAsync(req) {
  const authHeader = req.headers.authorization || '';
  const token = authHeader.replace('Bearer ', '').trim();
  if (!token) return null;

  // 1. Cache en memoria (hit rápido)
  const cached = cacheAunVale(token) ? sessions.get(token) : null;
  if (cached) return cached;

  // 2. Fallback a la BD (necesario tras reinicios del backend)
  if (_pool) {
    try {
      const r = await _pool.query(
        `SELECT u.id, u.username, u.role
         FROM sessions s
         JOIN users u ON s.user_id = u.id
         WHERE s.token = $1
           AND s.activo = true
           AND s.created_at > NOW() - ($2 || ' hours')::interval
         LIMIT 1`,
        [token, String(HORAS_MAX_SESION)]
      );
      if (r.rowCount > 0) {
        const row = r.rows[0];
        const user = { id: row.id, username: row.username, name: row.username, role: row.role };
        sessions.set(token, user);
        vistaEn.set(token, Date.now()); // el reloj de la caché empieza aquí
        return user;
      }
    } catch { /* ignorar */ }
  }
  return null;
}
