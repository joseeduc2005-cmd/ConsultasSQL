/**
 * Dejar constancia de lo que pasó: consultas, accesos, cambios en artículos.
 *
 * Adaptador de salida: lo único que hace es escribir en PostgreSQL. Ninguna de estas
 * funciones decide nada, y todas tragan sus errores a propósito — que no se pueda
 * anotar algo no puede impedir que ese algo ocurra.
 *
 * El pool llega por un getter y no por valor. Es una lección que ya costó una vez: al
 * recargar la configuración multi-base el pool se reemplaza, y quien hubiera guardado
 * el anterior se queda escribiendo contra una conexión cerrada.
 */
import { normalizeRole } from '../../dominio/acceso/roles.js';
let obtenerPool = () => {
  throw new Error('La auditoría no tiene pool: falta llamar a configurarPool al arrancar.');
};

/** Lo llama `app.js` al arrancar, con un getter y no con el pool. */
export function configurarPool(getter) {
  obtenerPool = getter;
}


export async function logQueryHistory({
  username,
  role,
  queryText,
  generatedSql,
  queryParams,
  placeholderOrder,
  executionMs,
  rowCount,
  wasCached,
  status,
}) {
  try {
    await obtenerPool().query(
      `INSERT INTO query_history
        (username, user_role, query_text, generated_sql, query_params, placeholder_order, execution_ms, row_count, was_cached, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
      [
        String(username || 'anonymous'),
        normalizeRole(role),
        queryText || null,
        String(generatedSql || ''),
        queryParams === undefined ? null : JSON.stringify(queryParams),
        placeholderOrder === undefined ? null : JSON.stringify(placeholderOrder),
        Math.max(0, Number(executionMs) || 0),
        Math.max(0, Number(rowCount) || 0),
        Boolean(wasCached),
        String(status || 'ok'),
      ]
    );
  } catch (error) {
    console.error('[QUERY_HISTORY] ❌ Error registrando historial:', error.message || error);
  }
}

/**
 * Registra un intento de login (exitoso o fallido) en login_audit.
 */
export async function logLoginEvent({ username, rol, ip, userAgent, estado, detalle }) {
  try {
    await obtenerPool().query(
      `INSERT INTO login_audit (username, rol, ip, user_agent, estado, detalle, fecha)
       VALUES ($1,$2,$3,$4,$5,$6,NOW())`,
      [
        String(username || 'unknown'),
        rol ? String(rol) : null,
        ip ? String(ip).substring(0, 60) : null,
        userAgent ? String(userAgent).substring(0, 300) : null,
        String(estado || 'ok'),
        detalle ? String(detalle) : null,
      ]
    );
  } catch (err) {
    console.error('[LOGIN_AUDIT] ❌ Error registrando login:', err.message || err);
  }
}

/**
 * Registra un evento del sistema (descarga Excel, exportación, etc.) en system_events.
 */
export async function logSystemEvent({ evento, usuario, rol, detalle, ip }) {
  try {
    await obtenerPool().query(
      `INSERT INTO system_events (evento, usuario, rol, detalle, ip, fecha)
       VALUES ($1,$2,$3,$4,$5,NOW())`,
      [
        String(evento || 'evento_desconocido'),
        usuario ? String(usuario) : null,
        rol ? String(rol) : null,
        detalle ? JSON.stringify(detalle) : null,
        ip ? String(ip).substring(0, 60) : null,
      ]
    );
  } catch (err) {
    console.error('[SYSTEM_EVENT_AUDIT] ❌ Error registrando evento:', err.message || err);
  }
}

export async function logArticleChange({ articleId, articleTitulo, accion, realizadoPor, realizadoRol, detalle }) {
  try {
    await obtenerPool().query(
      `INSERT INTO article_changes
        (article_id, article_titulo, accion, realizado_por, realizado_rol, detalle, realizado_en)
       VALUES ($1,$2,$3,$4,$5,$6,NOW())`,
      [
        articleId ? String(articleId) : null,
        String(articleTitulo || ''),
        String(accion || 'editar'),
        String(realizadoPor || 'anonymous'),
        String(realizadoRol || 'admin'),
        detalle ? JSON.stringify(detalle) : null,
      ]
    );
  } catch (err) {
    console.error('[ARTICLE_CHANGE_AUDIT] ❌ Error registrando cambio:', err.message || err);
  }
}
