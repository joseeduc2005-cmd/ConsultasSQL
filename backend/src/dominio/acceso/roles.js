/**
 * Quién puede hacer qué.
 *
 * Tres reglas cortas de las que depende todo el control de acceso del panel. Estaban
 * sueltas en `app.js` sin forma de probarlas: el rol se lee de la sesión y nunca de una
 * cabecera que ponga el navegador, y eso es justo lo que conviene dejar fijado por una
 * prueba antes de mover nada.
 */

export const ROLE_LEVELS = {
  user: 1,
  admin: 2,
  superadmin: 3,
};

export function normalizeRole(roleValue) {
  const role = String(roleValue || '').trim().toLowerCase();
  return ROLE_LEVELS[role] ? role : 'user';
}

export function hasRequiredRole(userRole, requiredRole) {
  const current = ROLE_LEVELS[normalizeRole(userRole)] || 0;
  const required = ROLE_LEVELS[normalizeRole(requiredRole)] || 0;
  return current >= required;
}

/**
 * El rol de quien hace la petición.
 *
 * Sale de la sesión y solo de la sesión. Antes se miraba primero la cabecera
 * `x-user-role`, y eso era una escalada de privilegios: la cabecera la pone el
 * navegador, así que una sesión de rol «user» obtenía 200 en rutas de administrador
 * mandando «x-user-role: superadmin» — incluida la que ejecuta SQL manual.
 *
 * La cabecera se sigue aceptando en CORS porque el frontend la manda, pero aquí no
 * decide nada. Lo único que vale es lo que el backend emitió al iniciar sesión.
 */
export function getRoleFromRequest(req, user) {
  return normalizeRole(user?.role || 'user');
}
