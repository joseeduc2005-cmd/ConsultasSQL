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

/**
 * El rastro de una petición que cambió algo.
 *
 * A diferencia de las de arriba, a esta no la llama nadie a mano: la llama un
 * middleware por cada petición. Esa es justamente la idea — antes auditar dependía
 * de que quien escribía una ruta se acordara, y de treinta y tantas rutas que
 * cambiaban algo, la mitad no se acordó.
 *
 * Nunca lanza. Una auditoría que tumba la petición que estaba auditando es peor que
 * no tenerla, así que si la escritura falla se queda en la consola y la petición
 * sigue su camino.
 */
export async function logRequest({ metodo, ruta, usuario, rol, ip, userAgent, estado, duracionMs, cuerpo }) {
  try {
    await obtenerPool().query(
      `INSERT INTO auditoria_peticiones
         (metodo, ruta, usuario, rol, ip, user_agent, estado, duracion_ms, cuerpo, fecha)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())`,
      [
        String(metodo || '').slice(0, 10),
        String(ruta || '').slice(0, 300),
        usuario ? String(usuario).slice(0, 150) : null,
        rol ? String(rol).slice(0, 50) : null,
        ip ? String(ip).slice(0, 60) : null,
        userAgent ? String(userAgent).slice(0, 300) : null,
        Number.isFinite(Number(estado)) ? Number(estado) : null,
        Number.isFinite(Number(duracionMs)) ? Math.round(Number(duracionMs)) : null,
        cuerpo ? JSON.stringify(cuerpo) : null,
      ]
    );
  } catch (error) {
    console.error('[AUDITORIA] no se pudo registrar la petición:', error.message);
  }
}

/**
 * Un error, en la tabla que ya existía para ellos.
 *
 * La tabla error_logs se venía creando en cada arranque desde hacía tiempo y no
 * escribía en ella nadie: estaba vacía. Esto la conecta al manejador de errores.
 */
export async function logError({ mensaje, stack, modulo, capa, contexto, archivo, linea, metadata }) {
  try {
    await obtenerPool().query(
      `INSERT INTO error_logs (mensaje, stack, modulo, capa, contexto, archivo, linea, metadata, fecha)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())`,
      [
        String(mensaje || 'error sin mensaje').slice(0, 2000),
        stack ? String(stack).slice(0, 8000) : null,
        modulo ? String(modulo).slice(0, 255) : null,
        capa ? String(capa).slice(0, 120) : null,
        contexto ? String(contexto).slice(0, 255) : null,
        archivo ? String(archivo).slice(0, 255) : null,
        Number.isFinite(Number(linea)) ? Number(linea) : null,
        metadata ? JSON.stringify(metadata) : null,
      ]
    );
  } catch (error) {
    console.error('[AUDITORIA] no se pudo registrar el error:', error.message);
  }
}
