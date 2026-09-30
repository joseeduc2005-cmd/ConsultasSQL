/**
 * API para quien reporta, no para quien atiende.
 *
 * Va en su propio router y no como excepciones dentro del de administración, a
 * propósito: `/api/soporte` entero exige rol admin, y abrir agujeros en ese guardián
 * es la clase de cosa que se olvida y deja pasar de más. Aquí la lista es explícita —
 * buscar, reportar, y ver lo propio — y todo lo demás sigue sin existir para un
 * usuario normal.
 *
 * Dos reglas que no dependen de la pantalla:
 *
 *   - La búsqueda solo devuelve artículos marcados para todos. Muchos llevan SQL y
 *     nombres de tablas, y no son para cualquiera aunque sea personal interno.
 *   - Un caso solo lo ve quien lo reportó. El listado y el detalle filtran por autor,
 *     no por lo que mande el navegador.
 *   - Lo que se propone como solución no se publica: queda pendiente de revisión.
 */
import { Router, type NextFunction, type Request, type Response } from 'express';
import { aportesDe, crearAporte, type AporteNuevo } from './aportes.ts';
import { solicitudInvalida, textoOpcional, textoRequerido } from './http/errores.ts';
import { filas } from './knowledge/db.ts';
import { guardarTicket, obtenerTicket, type Ticket } from './knowledge/tickets.ts';
import { buscarPrecedentes } from './precedentes.ts';

type Manejador = (req: Request, res: Response) => Promise<unknown>;

const h =
  (manejador: Manejador) =>
  (req: Request, res: Response, next: NextFunction): void => {
    manejador(req, res)
      .then((valor) => {
        if (!res.headersSent && valor !== undefined) res.json(valor);
      })
      .catch(next);
  };

interface Identidad {
  id: string;
  username: string;
}

function quien(req: Request): Identidad {
  const u = (req as unknown as { usuarioSoporte?: Identidad }).usuarioSoporte;
  if (!u) throw solicitudInvalida('No se pudo identificar al usuario.');
  return u;
}

/** Numera los casos reportados desde aquí, aparte de los del sistema de tickets. */
async function siguienteNumero(): Promise<string> {
  const anio = new Date().getFullYear();
  const [fila] = await filas<{ ultimo: string | null }>(
    'SELECT MAX(numero) AS ultimo FROM soporte.tickets WHERE numero LIKE $1',
    [`CASO-${anio}-%`],
  );
  const correlativo = Number(fila?.ultimo?.split('-')[2] ?? 0) + 1;
  return `CASO-${anio}-${String(correlativo).padStart(4, '0')}`;
}

/** Lo que se le devuelve de un caso: sin evidencia interna ni SQL de la investigación. */
function comoLoVeQuienReporta(t: Ticket) {
  return {
    numero: t.numero,
    titulo: t.titulo,
    descripcion: t.descripcion,
    estado: t.estado,
    sistema: t.sistema,
    creado_en: t.creado_en,
    resuelto_en: t.resuelto_en,
    // La solución sí: es la respuesta que esperaba. La causa raíz también, que suele
    // explicar por qué pasó. Lo que no sale son los pasos de investigación.
    causa_raiz: t.causa_raiz,
    solucion: t.solucion,
  };
}

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
function comoSeLee(script: Record<string, unknown> | null): { lenguaje: string; texto: string } | null {
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

export function crearSoporteClienteRouter(
  { log }: { log?: (datos: object, mensaje: string) => void } = {},
): Router {
  const router = Router();
  const registrar = log ?? (() => undefined);

  /** Buscar una solución. Es lo primero que se ofrece, antes de reportar nada. */
  router.post(
    '/solucionar',
    h(async (req) => {
      const cuerpo = (req.body ?? {}) as { texto?: string };
      const consulta = textoRequerido(cuerpo.texto, 'texto');
      const precedentes = await buscarPrecedentes(consulta, { limite: 6, soloPublicos: true });
      return { consulta, precedentes };
    }),
  );

  /** Reportar un caso, cuando la búsqueda no resolvió. */
  router.post(
    '/casos',
    h(async (req, res) => {
      const usuario = quien(req);
      const cuerpo = (req.body ?? {}) as { titulo?: string; descripcion?: string; sistema?: string };
      const titulo = textoRequerido(cuerpo.titulo, 'titulo');

      const ticket = await guardarTicket({
        numero: await siguienteNumero(),
        titulo,
        descripcion: textoOpcional(cuerpo.descripcion),
        sistema: textoOpcional(cuerpo.sistema),
        // El autor sale de la sesión, no del cuerpo: es lo que decide quién puede
        // volver a verlo, así que no puede venir del navegador.
        autor: usuario.username,
        estado: 'abierto',
      });

      registrar({ numero: ticket.numero, autor: usuario.username }, 'caso reportado');
      res.status(201);
      return comoLoVeQuienReporta(ticket);
    }),
  );

  /** Los casos propios. */
  router.get(
    '/casos',
    h(async (req) => {
      const usuario = quien(req);
      return filas(
        `SELECT numero, titulo, estado, sistema, creado_en, resuelto_en,
                (solucion IS NOT NULL AND btrim(solucion) <> '') AS con_solucion
         FROM soporte.tickets
         WHERE autor = $1
         ORDER BY creado_en DESC
         LIMIT 50`,
        [usuario.username],
      );
    }),
  );

  router.get(
    '/casos/:numero',
    h(async (req, res) => {
      const usuario = quien(req);
      const ticket = await obtenerTicket(req.params.numero);
      // Mismo mensaje exista o no: un 404 distinto de un 403 dejaría adivinar qué
      // números de caso existen.
      if (!ticket || ticket.autor !== usuario.username) {
        res.status(404);
        return { error: 'No se encontró ese caso entre los suyos.' };
      }
      return comoLoVeQuienReporta(ticket);
    }),
  );

  /**
   * El árbol de temas, para quien no sabe qué escribir.
   *
   * La búsqueda sirve cuando uno puede describir el problema; a veces no, y entonces
   * hace falta poder mirar qué hay. Solo entran artículos marcados para todos, por lo
   * mismo que la búsqueda: muchos llevan SQL y nombres de tablas.
   */
  router.get(
    '/temas',
    h(async () => {
      const articulos = await filas<{ id: string; titulo: string; descripcion: string | null; categoria: string | null; subcategoria: string | null }>(
        `SELECT id::text, titulo, descripcion, categoria, subcategoria
         FROM knowledge_base
         WHERE coalesce(visible_para, 'todos') = 'todos'
         ORDER BY categoria NULLS LAST, subcategoria NULLS LAST, titulo`,
      );

      // Se agrupa aquí y no en SQL porque el árbol es chico y así el cliente recibe
      // una sola respuesta en vez de pedir cada rama por separado.
      const porCategoria = new Map<string, { categoria: string; articulos: typeof articulos }>();
      for (const a of articulos) {
        const clave = a.categoria?.trim() || 'Otros';
        if (!porCategoria.has(clave)) porCategoria.set(clave, { categoria: clave, articulos: [] });
        porCategoria.get(clave)!.articulos.push(a);
      }
      return [...porCategoria.values()];
    }),
  );

  /** Un artículo, si es de los que puede ver cualquiera. */
  router.get(
    '/temas/:id',
    h(async (req, res) => {
      // El script se devuelve para poder leerlo, no para ejecutarlo. Si el artículo
      // está marcado para todos, alguien ya decidió que su contenido —la sentencia
      // incluida— se puede mostrar; ocultarlo igual dejaba la autorización sin efecto
      // visible. Ejecutar sigue siendo del panel: aquí no hay endpoint que lo haga.
      //
      // Va el objeto entero y no solo `sql` porque no todos son SQL: los de Mongo
      // llevan `operation`, `collection` y un `pipeline` o un `filter`, y quedarse
      // con la clave `sql` dejaba esos artículos mostrando nada.
      const [articulo] = await filas<{
        id: string;
        titulo: string;
        contenido: string | null;
        categoria: string | null;
        script: Record<string, unknown> | null;
        base: string | null;
      }>(
        `SELECT id::text, titulo, coalesce(contenido_md, contenido) AS contenido, categoria,
                script_json AS script,
                coalesce(script_json->>'databaseId', script_json->>'target_database') AS base
         FROM knowledge_base
         WHERE id::text = $1 AND coalesce(visible_para, 'todos') = 'todos'`,
        [req.params.id],
      );
      if (!articulo) {
        res.status(404);
        return { error: 'No se encontró ese tema.' };
      }
      const { script, ...resto } = articulo;
      return { ...resto, sentencia: comoSeLee(script) };
    }),
  );

  /**
   * Proponer una solución.
   *
   * No publica nada: queda pendiente hasta que alguien de soporte la revise. Eso es
   * intencional —lo que se escriba aquí termina siendo un tema que otros van a
   * seguir— y por eso la respuesta lo dice con todas sus letras.
   */
  router.post(
    '/aportes',
    h(async (req, res) => {
      const usuario = quien(req);
      const aporte = await crearAporte((req.body ?? {}) as AporteNuevo, usuario.username);
      registrar({ id: aporte.id, autor: usuario.username }, 'aporte propuesto');
      res.status(201);
      return aporte;
    }),
  );

  /** Los aportes propios, con el resultado de la revisión. */
  router.get(
    '/aportes',
    h(async (req) => aportesDe(quien(req).username)),
  );

  return router;
}
