/**
 * Quién puede entrar al módulo de soporte.
 *
 * Son dos guardianes y van aparte a propósito. `/api/soporte` es la parte de admin
 * —revisar aportes, publicar tickets, lanzar playbooks— y exige rol admin entero.
 * `/api/soporte-cliente` es la parte de cualquiera con sesión: contar su problema y
 * buscar solución.
 *
 * Tenerlos separados es lo que evita la clase de error que se olvida: si fuera un solo
 * middleware con excepciones, cada ruta nueva de cliente obligaría a añadir un hueco
 * en el guardián de admin, y basta uno mal puesto para que un usuario normal entre a
 * aceptar aportes.
 *
 * Los dos adjuntan `req.usuarioSoporte` para que el módulo pueda atribuir lo que
 * publique sin volver a resolver la sesión.
 */

/**
 * @param {object} deps
 * @param {Function} deps.obtenerUsuario  resuelve la sesión de la petición
 * @param {Function} deps.normalizarRol   deja el rol en su forma canónica
 */
export function crearGuardianesDeSoporte({ obtenerUsuario, normalizarRol }) {
  async function soloAdminSoporte(req, res, next) {
    try {
      const usuario = await obtenerUsuario(req);
      if (!usuario) {
        return res.status(401).json({ error: 'Inicie sesión para acceder al módulo de soporte.' });
      }
      if (normalizarRol(usuario.role) !== 'admin') {
        return res.status(403).json({ error: 'Acceso denegado: el módulo de soporte requiere rol admin.' });
      }
      req.usuarioSoporte = { id: usuario.id, username: usuario.username };
      return next();
    } catch (error) {
      return next(error);
    }
  }

  async function sesionSoporte(req, res, next) {
    try {
      const usuario = await obtenerUsuario(req);
      if (!usuario) {
        return res.status(401).json({ error: 'Inicie sesión para reportar o consultar un caso.' });
      }
      req.usuarioSoporte = { id: usuario.id, username: usuario.username };
      return next();
    } catch (error) {
      return next(error);
    }
  }

  return { soloAdminSoporte, sesionSoporte };
}
