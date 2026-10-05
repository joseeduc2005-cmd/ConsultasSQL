/**
 * Cómo se lee un script guardado.
 *
 * No todos son SQL: los de Mongo llevan `operation`, `collection` y un `pipeline` o un
 * `filter`, y hay formas que todavía no existen. Por eso lo que no se reconoce no se
 * descarta — se muestra como JSON, que es peor que una sentencia bien formateada pero
 * infinitamente mejor que un tema que aparenta no traer nada.
 *
 * Dominio puro: recibe lo guardado y devuelve texto legible. Estaba dentro del archivo
 * de rutas, donde no se podía probar sin levantar un servidor.
 */

/**
 * Un argumento de Mongo, tal como se escribiría.
 *
 * Los pipelines y filtros están guardados unas veces como objeto y otras como el texto
 * JSON de ese objeto. Pasar el texto por `JSON.stringify` lo dejaba escapado y lleno de
 * comillas invertidas, ilegible. Y algunos llevan marcadores como `{{departamento}}`,
 * que no son JSON válido: esos se muestran tal cual, que es como se entienden.
 */
function comoArgumento(valor: unknown): string {
  if (typeof valor === 'string') {
    try {
      return JSON.stringify(JSON.parse(valor), null, 2);
    } catch {
      return valor;
    }
  }
  return JSON.stringify(valor, null, 2);
}

/** Las claves de `script_json` que son plomería del panel y no dicen nada al leerlas. */
const PLOMERIA = new Set(['modo', 'origen', 'databaseId', 'database_id', 'target_database']);

/**
 * Convierte el script guardado en algo que se pueda leer en pantalla.
 *
 * No todos son SQL. Los de Mongo llevan `operation`, `collection` y un `pipeline` o un
 * `filter`, y hay artículos con formas que todavía no existen. Por eso lo que no se
 * reconoce no se descarta: se muestra como JSON, que es peor que una sentencia bien
 * formateada pero infinitamente mejor que un artículo que aparenta no traer nada.
 */
export function comoSeLee(script: Record<string, unknown> | null): { lenguaje: string; texto: string } | null {
  if (!script || typeof script !== 'object') return null;

  if (typeof script.sql === 'string' && script.sql.trim()) {
    return { lenguaje: 'sql', texto: script.sql.trim() };
  }

  const util = Object.fromEntries(Object.entries(script).filter(([k, v]) => !PLOMERIA.has(k) && v != null));
  if (Object.keys(util).length === 0) return null;

  // Mongo se arma como se escribiría en su consola, que es como lo va a reconocer
  // quien lo lea; lo demás cae al JSON de más abajo.
  const { operation, collection, pipeline, filter, sort, limit, ...sobrante } = util as Record<string, unknown>;
  if (typeof collection === 'string' && typeof operation === 'string') {
    const extras = [sort ? `.sort(${comoArgumento(sort)})` : '', limit ? `.limit(${limit})` : ''].join('');
    return {
      lenguaje: 'javascript',
      texto: `db.${collection}.${operation}(${comoArgumento(pipeline ?? filter ?? {})})${extras}`,
    };
  }

  return { lenguaje: 'json', texto: JSON.stringify({ ...util, ...sobrante }, null, 2) };
}
