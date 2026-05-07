// ─── Actions route: /api/actions ─────────────────────────────────────────────
import { Router } from 'express';

/**
 * @param {{
 *   BUSINESS_ACTION_CATALOG: Array,
 *   getUserFromRequest: Function,
 * }} deps
 */
export function createActionsRouter({ BUSINESS_ACTION_CATALOG, getUserFromRequest }) {
  const router = Router();

  const normalizeCategory = (value) =>
    String(value || '').trim().toLowerCase().normalize('NFD').replace(/[^\w\s-]/g, '');

  // GET /api/actions
  router.get('/', (req, res) => {
    try {
      const user = getUserFromRequest(req);
      const role = req.headers['x-user-role'] || user?.role;
      if (!role || role !== 'admin') {
        return res.status(403).json({ success: false, error: 'Acceso denegado: se requiere rol admin' });
      }

      const requestedCategory = normalizeCategory(req.query.categoria || req.query.category || '');
      const filtered = requestedCategory
        ? BUSINESS_ACTION_CATALOG.filter((action) => normalizeCategory(action.categoria || '') === requestedCategory)
        : BUSINESS_ACTION_CATALOG;

      return res.status(200).json(filtered);
    } catch (error) {
      console.error('[ACTIONS] ❌ Error obteniendo catálogo de acciones:', error);
      return res.status(500).json({ success: false, error: 'Error interno del servidor' });
    }
  });

  return router;
}
