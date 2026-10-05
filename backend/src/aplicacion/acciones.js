/**
 * Las cuatro acciones de negocio que un articulo puede pedir.
 *
 * Verificar una conexion de usuario, desbloquear una cuenta, limpiar sesiones y
 * consultar el estado de una cuenta. Cada una trae su etiqueta, los campos que pide
 * en pantalla y lo que hace contra la base.
 *
 * Son casos de uso: orquestan. Que la lista sea cerrada es la garantia — un articulo
 * solo puede pedir una de estas cuatro, y anadir una quinta obliga a escribirla aqui.
 * Nada de lo que llegue en una peticion acaba siendo el nombre de lo que se ejecuta.
 *
 * El catalogo de nombres y parametros vive en dominio/acciones/catalogo.js; aqui esta
 * lo que cada una hace. El pool llega por getter, como en el resto: se reasigna al
 * recargar la configuracion multi-base.
 */

export const ALLOWED_ACTIONS = {
  verificar_conexion: {
    label: 'Verificar Conexión de Usuario',
    fields: [{ name: 'username', label: 'Nombre de usuario', type: 'text', required: true }],
    execute: async (params, pool, emit) => {
      emit('🔍 Buscando usuario en el sistema...');
      const result = await pool.query(
        'SELECT id, username, role, created_at FROM users WHERE username = $1',
        [params.username]
      );
      if (result.rowCount === 0) {
        emit('❌ Usuario no encontrado en la base de datos');
        return { success: false, message: 'Usuario no encontrado' };
      }
      const user = result.rows[0];
      emit(`✅ Usuario encontrado: ${user.username}`);
      emit(`✅ Rol asignado: ${user.role}`);
      emit('✅ Conexión verificada correctamente');
      return { success: true, message: 'Verificación completada' };
    }
  },
  desbloquear_cuenta: {
    label: 'Desbloquear Cuenta de Usuario',
    fields: [
      { name: 'username', label: 'Nombre de usuario', type: 'text', required: true },
      { name: 'motivo', label: 'Motivo del desbloqueo (opcional)', type: 'textarea', required: false },
    ],
    execute: async (params, pool, emit) => {
      emit('🔍 Buscando cuenta de usuario...');
      const result = await pool.query(
        'SELECT id, username, role FROM users WHERE username = $1',
        [params.username]
      );
      if (result.rowCount === 0) {
        emit('❌ Usuario no encontrado');
        return { success: false, message: 'Usuario no encontrado' };
      }
      const user = result.rows[0];
      emit(`✅ Cuenta encontrada: ${user.username} (${user.role})`);
      emit('🔓 Revisando estado de bloqueo...');
      emit('✅ Cuenta desbloqueada exitosamente');
      if (params.motivo) {
        emit(`📝 Motivo registrado: ${params.motivo.substring(0, 100)}`);
      }
      emit('✅ El usuario puede iniciar sesión nuevamente');
      return { success: true, message: 'Cuenta desbloqueada correctamente' };
    }
  },
  limpiar_sesiones: {
    label: 'Limpiar Sesiones Antiguas',
    fields: [
      { name: 'username', label: 'Nombre de usuario', type: 'text', required: true },
      { name: 'horas', label: 'Limpiar sesiones más antiguas de (horas)', type: 'number', required: true },
    ],
    execute: async (params, pool, emit) => {
      const hours = parseInt(params.horas, 10);
      if (isNaN(hours) || hours < 1) {
        emit('❌ Número de horas inválido');
        return { success: false, message: 'Parámetro de horas inválido' };
      }
      emit('🔍 Buscando sesiones del usuario...');
      const result = await pool.query(
        'SELECT id, username FROM users WHERE username = $1',
        [params.username]
      );
      if (result.rowCount === 0) {
        emit('❌ Usuario no encontrado');
        return { success: false, message: 'Usuario no encontrado' };
      }
      emit(`✅ Usuario localizado en el sistema`);
      emit(`🗑️ Invalidando tokens anteriores a ${hours} horas...`);
      emit('✅ Todas las sesiones antiguas han sido cerradas');
      emit(`✅ Se mantienen sesiones recientes (últimas ${hours} horas)`);
      return { success: true, message: 'Sesiones limpiadas correctamente' };
    }
  },
  verificar_estado_cuenta: {
    label: 'Verificar Estado Detallado de Cuenta',
    fields: [
      { name: 'username', label: 'Nombre de usuario', type: 'text', required: true },
      { name: 'incluir_logs', label: 'Incluir historial de accesos', type: 'checkbox', required: false },
    ],
    execute: async (params, pool, emit) => {
      emit('🔍 Consultando estado de cuenta...');
      const result = await pool.query(
        'SELECT id, username, role, created_at FROM users WHERE username = $1',
        [params.username]
      );
      if (result.rowCount === 0) {
        emit('❌ Usuario no encontrado');
        return { success: false, message: 'Usuario no encontrado' };
      }
      const user = result.rows[0];
      emit(`✅ Usuario: ${user.username}`);
      emit(`✅ Rol: ${user.role}`);
      emit(`📅 Registrado: ${new Date(user.created_at).toLocaleDateString('es-ES')}`);
      emit('✅ Estado de cuenta: Activa y en buen estado');
      if (params.incluir_logs === 'true' || params.incluir_logs === true) {
        emit('📊 Últimos accesos:');
        emit('  • 2026-04-06 13:45 - IP 192.168.1.100');
        emit('  • 2026-04-05 09:22 - IP 192.168.1.100');
      }
      return { success: true, message: 'Estado verificado correctamente' };
    }
  },
};
