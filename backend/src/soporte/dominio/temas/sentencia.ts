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

/** Un dato que hay que darle al tema antes de ejecutarlo. */
export interface CampoPedido {
  nombre: string;
  etiqueta: string;
  tipo: string;
  requerido: boolean;
}

/**
 * Qué datos pide un tema para poder ejecutarse.
 *
 * Los marcadores de la sentencia son el formulario: «WHERE s.cedula = :cedula» pide una
 * cédula, y así el tema sirve para el próximo caso en vez de para el que lo originó. Un
 * tema sin marcadores no pide nada y se ejecuta tal cual, que es lo correcto para una
 * consulta que lista algo entero.
 *
 * Un «::int» es un cast, no un marcador. El regex que había no los distinguía, así que
 * un artículo con «count(*)::int» salía pidiendo un dato llamado «int» que nadie sabe
 * rellenar y que dejaba el formulario sin poder enviarse.
 *
 * Mongo usa «{{nombre}}» en vez de «:nombre», y cuenta igual: lo que define un campo es
 * que falte un dato, no con qué símbolo se escriba.
 */
export function loQuePide(script: Record<string, unknown> | null): CampoPedido[] {
  const leido = comoSeLee(script);
  if (!leido) return [];

  const vistos = new Set<string>();
  const campos: CampoPedido[] = [];
  const anotar = (crudo: string) => {
    const nombre = crudo.toLowerCase();
    if (vistos.has(nombre)) return;
    vistos.add(nombre);
    const palabras = nombre.replace(/_/g, ' ');
    campos.push({
      nombre,
      etiqueta: palabras.charAt(0).toUpperCase() + palabras.slice(1),
      tipo: /fecha|desde|hasta|inicio|fin/.test(nombre) ? 'date' : 'text',
      requerido: true,
    });
  };

  for (const m of leido.texto.matchAll(/(?<!:):([a-z_][a-z0-9_]*)/gi)) anotar(m[1]);
  for (const m of leido.texto.matchAll(/\{\{\s*([a-z_][a-z0-9_]*)\s*\}\}/gi)) anotar(m[1]);

  return campos;
}

/**
 * Los campos de un tema: los que alguien escribió, y si no, los deducidos.
 *
 * `loQuePide` saca el nombre del marcador, y un marcador no lleva tildes: `:cedula` da
 * «Cedula», que se lee mal en una pantalla que ve gente de fuera del equipo. La columna
 * `campos_formulario` existe para poner nombres escritos a mano, así que manda cuando la
 * hay.
 *
 * Viene en dos formas según quién la escribiera —el panel usa `name`/`label` y el módulo
 * de soporte `nombre`/`etiqueta`— y se aceptan las dos: discutir cuál es la buena no
 * arregla los artículos que ya están guardados con la otra.
 *
 * Un campo sin nombre se descarta: no hay forma de mandarlo ni de rellenarlo, y dejarlo
 * pasar pinta un recuadro que nunca deja enviar el formulario.
 */
export function camposDelTema(
  guardados: unknown,
  script: Record<string, unknown> | null,
): CampoPedido[] {
  if (Array.isArray(guardados) && guardados.length > 0) {
    const campos = guardados
      .map((crudo) => {
        const c = (crudo ?? {}) as Record<string, unknown>;
        const nombre = String(c.nombre ?? c.name ?? '').trim();
        if (!nombre) return null;
        const etiqueta = String(c.etiqueta ?? c.label ?? '').trim() || nombre.replace(/_/g, ' ');
        return {
          nombre,
          etiqueta: etiqueta.charAt(0).toUpperCase() + etiqueta.slice(1),
          tipo: String(c.tipo ?? c.type ?? 'text'),
          requerido: c.requerido !== false && c.required !== false,
        };
      })
      .filter((c): c is CampoPedido => c !== null);
    if (campos.length > 0) return campos;
  }
  return loQuePide(script);
}
