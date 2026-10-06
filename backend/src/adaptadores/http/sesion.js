// ─── Session store & request helpers ─────────────────────────────────────────
// Shared between auth, content, and any route that needs the current user.

export const sessions = new Map();

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
  const cached = sessions.get(token);
  if (cached) return cached;

  // 2. Fallback a la BD (necesario tras reinicios del backend)
  if (_pool) {
    try {
      const r = await _pool.query(
        `SELECT u.id, u.username, u.role
         FROM sessions s
         JOIN users u ON s.user_id = u.id
         WHERE s.token = $1 AND s.activo = true
         LIMIT 1`,
        [token]
      );
      if (r.rowCount > 0) {
        const row = r.rows[0];
        const user = { id: row.id, username: row.username, name: row.username, role: row.role };
        sessions.set(token, user); // Restaurar en cache
        return user;
      }
    } catch { /* ignorar */ }
  }
  return null;
}
