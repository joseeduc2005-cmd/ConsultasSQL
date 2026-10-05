import { BUSINESS_ACTION_CATALOG, getBusinessActionMetadata, getRequiredParamsForActions }
  from './dominio/acciones/catalogo.js';

// Se reexportan porque el catalogo se mudo a dominio/ y app.js las importa de aqui.
export { BUSINESS_ACTION_CATALOG, getBusinessActionMetadata, getRequiredParamsForActions };

export function createBusinessActionsRegistry(pool) {
  const actions = {
    getUser: async ({ params, context, emit }) => {
      const username = String(params.usuario || params.username || '').trim();
      if (!username) {
        throw new Error('Falta parámetro: usuario');
      }

      emit(`🔍 Buscando usuario ${username}...`);
      const result = await pool.query(
        'SELECT id, username, role, bloqueado, created_at FROM users WHERE username = $1',
        [username]
      );

      if (!result.rowCount) {
        throw new Error('Usuario no encontrado');
      }

      context.user = result.rows[0];
      context.usuario_data = result.rows[0];
      emit(`✅ Usuario encontrado: ${result.rows[0].username}`);
      return result.rows[0];
    },

    unlockUser: async ({ context, emit }) => {
      const currentUser = context.user || context.usuario_data;
      if (!currentUser?.id) {
        throw new Error('Usuario no cargado');
      }

      emit(`🔓 Desbloqueando usuario ${currentUser.username}...`);
      const result = await pool.query(
        'UPDATE users SET bloqueado = false WHERE id = $1 RETURNING id, username, role, bloqueado, created_at',
        [currentUser.id]
      );

      if (!result.rowCount) {
        throw new Error('No se pudo desbloquear el usuario');
      }

      context.user = result.rows[0];
      context.usuario_data = result.rows[0];
      emit(`✅ Usuario desbloqueado: ${result.rows[0].username}`);
      return result.rows[0];
    },

    validateUser: async ({ context, emit }) => {
      const currentUser = context.user || context.usuario_data;
      if (!currentUser) {
        throw new Error('Usuario no existe');
      }

      emit(`✅ Usuario válido: ${currentUser.username}`);
      return currentUser;
    },

    logAction: async ({ params, context, emit }) => {
      const username = String(params.usuario || params.username || context?.user?.username || 'desconocido').trim();
      const actionName = String(params.logAccion || 'workflow_business').trim();
      const detail = String(params.detalle || params.descripcion || 'Sin detalle').trim();

      await pool.query(
        'INSERT INTO logs (accion, usuario, detalle) VALUES ($1, $2, $3)',
        [actionName, username, detail]
      );

      emit(`📝 Auditoría registrada para ${username}`);
      return { logged: true, accion: actionName, usuario: username };
    },
  };

  // Aliases para compatibilidad de scripts empresariales previos
  actions.validarUsuario = actions.getUser;
  actions.desbloquearUsuario = actions.unlockUser;
  actions.registrarLog = actions.logAction;

  return actions;
}
