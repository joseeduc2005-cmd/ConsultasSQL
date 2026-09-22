/**
 * Catálogo de consultas de SOLO LECTURA que usan los diagnósticos y la vigilancia.
 *
 * Los playbooks nunca escriben SQL libre: piden una consulta por nombre y la
 * fuente de datos (Oracle real o demostración) la resuelve. Así el conjunto de
 * sentencias que se ejecutan contra producción es finito, revisable y auditable.
 *
 * `biblioteca` referencia el código equivalente en la Biblioteca SQL del equipo.
 * Las columnas de claves y PIN (usr_pasword, usr_clave_pin) se excluyen a propósito.
 */
export interface ConsultaCatalogo {
  descripcion: string;
  biblioteca?: string;
  sql: string;
}

const COLUMNAS_USUARIO = `u.usr_codigo, u.usr_usuario, u.usr_cedula, u.usr_estado, u.usr_intentos, u.usr_intentos_pin,
       u.usr_is_clave_temporal, u.usr_is_pin_temporal, u.codigo_sucursal`;

/**
 * Consulta que resume con que valor opera una columna entre los usuarios SANOS:
 * los que hicieron al menos una transaccion en los ultimos `:dias` dias.
 *
 * El nombre de la columna lo pone el codigo (nunca la entrada de una persona), asi
 * el conjunto de sentencias sigue siendo finito y revisable como el resto del
 * catalogo. Devuelve los valores mas frecuentes con su peso sobre el total.
 */
function perfilSano(columna: string, etiqueta: string): ConsultaCatalogo {
  return {
    descripcion: `Con que ${etiqueta} operan los usuarios activos`,
    sql: `SELECT valor, veces, total FROM (
  SELECT u.${columna} AS valor, COUNT(*) AS veces, SUM(COUNT(*)) OVER () AS total
  FROM corresponsales.usuario u
  WHERE EXISTS (
    SELECT 1
    FROM corresponsales.usuario_pos up
    JOIN corresponsales.transaccion t ON t.trans_pos = up.up_pos
    WHERE up.up_usuario = u.usr_codigo
      AND t.trans_fecha >= SYSDATE - :dias
  )
  GROUP BY u.${columna}
  ORDER BY COUNT(*) DESC
) WHERE ROWNUM <= 5`,
  };
}

export const CATALOGO = {
  ping: {
    descripcion: 'Verificar la conexión',
    sql: 'SELECT 1 AS ok FROM dual',
  },

  // --- Usuarios -------------------------------------------------------------
  usuarioPorUsuario: {
    descripcion: 'Datos de acceso de un usuario por nombre de usuario',
    biblioteca: 'U2',
    sql: `SELECT ${COLUMNAS_USUARIO}
FROM corresponsales.usuario u
WHERE u.usr_usuario = :usuario`,
  },
  usuarioPorCedula: {
    descripcion: 'Datos de acceso de un usuario por cédula',
    biblioteca: 'U1',
    sql: `SELECT ${COLUMNAS_USUARIO}
FROM corresponsales.usuario u
WHERE u.usr_cedula = :cedula`,
  },
  usuariosPorMac: {
    descripcion: 'Usuarios que el backend encuentra para una MAC con POS activo (findByMac)',
    biblioteca: 'U16',
    sql: `SELECT ${COLUMNAS_USUARIO}, p.pos_mac
FROM corresponsales.usuario u
JOIN corresponsales.pos_view p ON p.usr_usuario = u.usr_usuario
WHERE p.pos_mac = :mac
  AND p.pos_estado_id = 1`,
  },
  usuariosBloqueados: {
    descripcion: 'Usuarios activos que alcanzaron el máximo de intentos fallidos',
    sql: `SELECT u.usr_codigo, u.usr_usuario, u.usr_cedula, u.usr_intentos, u.usr_intentos_pin
FROM corresponsales.usuario u
WHERE u.usr_estado = 1
  AND (u.usr_intentos_pin >= :max_pin OR u.usr_intentos >= :max_clave)
ORDER BY u.usr_usuario
FETCH FIRST 200 ROWS ONLY`,
  },

  // --- POS ------------------------------------------------------------------
  posPorMac: {
    descripcion: 'POS registrados con una MAC, con su establecimiento y representante',
    biblioteca: 'U17',
    sql: `SELECT p.pos_id, p.pos_mac, p.pos_estado_id, p.establecimiento_ce_id,
       ce.ce_razon_social, ce.ce_estado,
       cr.cr_id, cr.cr_cod_socio, cr.cr_identificacion, cr.cr_estado, cr.codigo_sucursal AS cr_sucursal
FROM corresponsales.pos p
LEFT JOIN corresponsales.corresponsalestablecimiento ce ON ce.ce_id = p.establecimiento_ce_id
LEFT JOIN corresponsales.corresponsalrepresentante cr ON cr.cr_id = ce.ce_representante
WHERE p.pos_mac = :mac
ORDER BY p.pos_estado_id, p.pos_id`,
  },
  usuariosDePosPorMac: {
    descripcion: 'Usuarios vinculados (usuario_pos) a los POS de una MAC',
    biblioteca: 'U19',
    sql: `SELECT up.up_id, up.up_usuario, up.up_pos, u.usr_usuario, u.usr_cedula, u.usr_estado, p.pos_estado_id
FROM corresponsales.usuario_pos up
JOIN corresponsales.usuario u ON u.usr_codigo = up.up_usuario
JOIN corresponsales.pos p ON p.pos_id = up.up_pos
WHERE p.pos_mac = :mac`,
  },
  posDeUsuario: {
    descripcion: 'POS asignados a un usuario',
    biblioteca: 'P1',
    sql: `SELECT p.pos_id, p.pos_mac, p.pos_estado_id
FROM corresponsales.usuario_pos up
JOIN corresponsales.pos p ON p.pos_id = up.up_pos
WHERE up.up_usuario = :usr_codigo`,
  },
  macsDuplicadas: {
    descripcion: 'MACs registradas en más de un POS',
    biblioteca: 'U18',
    sql: `SELECT pos_mac, COUNT(*) AS total, SUM(CASE WHEN pos_estado_id = 1 THEN 1 ELSE 0 END) AS activos
FROM corresponsales.pos
WHERE pos_mac IS NOT NULL
GROUP BY pos_mac
HAVING COUNT(*) > 1
ORDER BY total DESC
FETCH FIRST 200 ROWS ONLY`,
  },

  // --- Sucursales -----------------------------------------------------------
  representantePorCedula: {
    descripcion: 'Registro del corresponsal (representante) por cédula',
    sql: `SELECT cr.cr_id, cr.cr_cod_socio, cr.cr_identificacion, cr.codigo_sucursal, cr.cr_estado
FROM corresponsales.corresponsalrepresentante cr
WHERE cr.cr_identificacion = :cedula
ORDER BY cr.cr_estado DESC, cr.cr_id DESC`,
  },
  sucursalesDeSocio: {
    descripcion: 'Sucursal del socio según cada fuente (ticket 2026083744)',
    sql: `SELECT f.fuente, f.codigo_sucursal, s.descripcion AS nombre_sucursal
FROM (
  SELECT 'corresponsales.corresponsalrepresentante' AS fuente, codigo_sucursal
  FROM corresponsales.corresponsalrepresentante WHERE cr_cod_socio = :codigo_socio
  UNION ALL
  SELECT 'jst_personas.vwt_cliente', codigo_sucursal
  FROM jst_personas.vwt_cliente WHERE codigo_socio = :codigo_socio
  UNION ALL
  SELECT 'fisja.clientes', codigo_sucursal
  FROM fisja.clientes WHERE codigo_socio = :codigo_socio
) f
LEFT JOIN fisja.ca_sucursales s ON s.codigo_sucursal = f.codigo_sucursal`,
  },
  sucursalCorresponsalVsPersona: {
    descripcion: 'Corresponsales activos cuya sucursal difiere de JST_PERSONAS',
    sql: `SELECT DISTINCT cr.cr_id, cr.cr_cod_socio, cr.cr_identificacion,
       cr.codigo_sucursal AS sucursal_corresponsal, v.codigo_sucursal AS sucursal_persona
FROM corresponsales.corresponsalrepresentante cr
JOIN jst_personas.vwt_cliente v ON v.codigo_socio = cr.cr_cod_socio
WHERE cr.cr_estado = 1
  AND cr.codigo_sucursal <> v.codigo_sucursal
FETCH FIRST 200 ROWS ONLY`,
  },
  sucursalCorresponsalVsFisja: {
    descripcion: 'Corresponsales activos con sucursal antigua en fisja.clientes',
    sql: `SELECT DISTINCT cr.cr_id, cr.cr_cod_socio, cr.cr_identificacion,
       cr.codigo_sucursal AS sucursal_corresponsal, c.codigo_sucursal AS sucursal_fisja
FROM corresponsales.corresponsalrepresentante cr
JOIN fisja.clientes c ON c.codigo_socio = cr.cr_cod_socio
WHERE cr.cr_estado = 1
  AND cr.codigo_sucursal <> c.codigo_sucursal
FETCH FIRST 200 ROWS ONLY`,
  },

  // --- Transacciones y reportes --------------------------------------------
  resumenTransaccionesPorMac: {
    descripcion: 'Volumen de transacciones de un POS en un rango de fechas',
    sql: `SELECT COUNT(*) AS total,
       SUM(CASE WHEN t.trans_movimiento_tendero IS NULL THEN 1 ELSE 0 END) AS sin_movimiento_tendero
FROM corresponsales.transaccion t
JOIN corresponsales.pos p ON p.pos_id = t.trans_pos
WHERE p.pos_mac = :mac
  AND TRUNC(t.trans_fecha) BETWEEN :desde AND :hasta`,
  },
  transaccionesSinMovimientoTendero: {
    descripcion: 'Transacciones con trans_movimiento_tendero en NULL',
    biblioteca: 'T3',
    sql: `SELECT t.trans_id, t.trans_fecha, t.trans_tipo, t.trans_monto
FROM corresponsales.transaccion t
JOIN corresponsales.pos p ON p.pos_id = t.trans_pos
WHERE p.pos_mac = :mac
  AND TRUNC(t.trans_fecha) BETWEEN :desde AND :hasta
  AND t.trans_movimiento_tendero IS NULL
ORDER BY t.trans_fecha
FETCH FIRST 50 ROWS ONLY`,
  },
  tenderoNuloPorTipo: {
    descripcion: 'Transacciones recientes con trans_movimiento_tendero en NULL, por tipo',
    sql: `SELECT t.trans_tipo, COUNT(*) AS total, MIN(t.trans_fecha) AS primera, MAX(t.trans_fecha) AS ultima
FROM corresponsales.transaccion t
WHERE t.trans_movimiento_tendero IS NULL
  AND t.trans_fecha >= TRUNC(SYSDATE) - :dias
GROUP BY t.trans_tipo
ORDER BY total DESC`,
  },
  nulosEnSubconsultasReporte: {
    descripcion: 'Valores NULL que anulan los NOT IN del reporte de transacciones',
    sql: `SELECT (SELECT COUNT(*) FROM corresponsales.movimiento_fondeo WHERE id_transaccion_rev IS NULL) AS fondeo_nulos,
       (SELECT COUNT(*) FROM corresponsales.reverso WHERE rev_transaccion IS NULL) AS reverso_nulos
FROM dual`,
  },

  // --- Perfil de la poblacion sana ------------------------------------------
  // Con que valor operan los usuarios que SI estan funcionando. Es lo que permite
  // senalar un dato raro sin que nadie haya escrito una regla para esa columna.
  perfilUsrEstado: perfilSano('usr_estado', 'estado del usuario'),
  perfilUsrIntentos: perfilSano('usr_intentos', 'intentos fallidos de clave'),
  perfilUsrIntentosPin: perfilSano('usr_intentos_pin', 'intentos fallidos de PIN'),
  perfilUsrIsClaveTemporal: perfilSano('usr_is_clave_temporal', 'marca de clave temporal'),
  perfilUsrIsPinTemporal: perfilSano('usr_is_pin_temporal', 'marca de PIN temporal'),
} satisfies Record<string, ConsultaCatalogo>;

export type NombreConsulta = keyof typeof CATALOGO;
