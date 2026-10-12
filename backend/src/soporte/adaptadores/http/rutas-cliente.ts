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
import { aportesDe, crearAporte, type AporteNuevo } from '../../aportes.ts';
import { basesConectadas } from '../../motor-puente.ts';
import { camposDelTema, comoSeLee } from '../../dominio/temas/sentencia.ts';
import { solicitudInvalida, textoOpcional, textoRequerido } from './errores.ts';
import { filas } from '../../knowledge/db.ts';
import { guardarTicket, obtenerTicket, type Ticket } from '../../knowledge/tickets.ts';
import { buscarPrecedentes, registrarAcierto, registrarBusqueda } from '../../precedentes.ts';

/**
 * Los tipos de tema que ejecutan algo.
 *
 * `dinamico` es el nombre de ahora; los otros tres son como se llamaban antes y siguen
 * existiendo en artículos guardados. Se repite aquí en vez de importarse del dominio del
 * motor porque este módulo no depende de él, y una lista de cuatro palabras no justifica
 * atar los dos lados.
 */
const EJECUTABLES = new Set(['dinamico', 'ejecutable', 'database', 'script']);

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

/** Lo que hace falta de fuera para poder correr un tema: lo pone la composición. */
export type EjecutorDeTema = (
  id: string,
  datos: Record<string, unknown>,
  quien: { id: string; username: string },
) => Promise<{ ok: boolean; mensaje?: string; resumen?: string | null; filas?: Record<string, unknown>[] }>;

export function crearSoporteClienteRouter(
  {
    log,
    ejecutarTema,
  }: {
    log?: (datos: object, mensaje: string) => void;
    /** Sin esto, los temas se leen y no se corren: la ruta responde que no está disponible. */
    ejecutarTema?: EjecutorDeTema;
  } = {},
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
      // Queda apuntada para saber si sirvió: la marca de «sirvió» la pone abrir un
      // resultado, así que no hace falta pedirle a nadie que valore nada.
      await registrarBusqueda(consulta, quien(req).username, precedentes.length, precedentes[0]?.confianza ?? null);
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
      // Si el artículo está marcado para todos, alguien ya decidió que su contenido —la
      // sentencia incluida— se puede mostrar; ocultarlo igual dejaba esa autorización
      // sin efecto visible. Y por la misma razón se puede ejecutar: ver más abajo.
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
        tipo: string | null;
        campos: unknown;
      }>(
        `SELECT id::text, titulo, coalesce(contenido_md, contenido) AS contenido, categoria,
                script_json AS script, tipo_solucion AS tipo, campos_formulario AS campos,
                coalesce(script_json->>'databaseId', script_json->>'target_database') AS base
         FROM knowledge_base
         WHERE id::text = $1 AND coalesce(visible_para, 'todos') = 'todos'`,
        [req.params.id],
      );
      if (!articulo) {
        res.status(404);
        return { error: 'No se encontró ese tema.' };
      }
      const { script, tipo, campos, ...resto } = articulo;
      // `pide` son los datos que hay que rellenar antes de ejecutar: los que alguien
      // escribió al crear el tema, y si no los hay, los deducidos de los marcadores de la
      // sentencia. Van con el tema para que la pantalla no tenga que volver a leer el SQL
      // ni adivinar el formulario.
      return {
        ...resto,
        sentencia: comoSeLee(script),
        ejecutable: EJECUTABLES.has(String(tipo ?? '')),
        pide: camposDelTema(campos, script),
      };
    }),
  );

  /**
   * Ejecutar un tema y ver el resultado.
   *
   * Esto antes no existía, y la pantalla decía «quien la ejecuta es el equipo de
   * soporte». Tenía sentido cuando un artículo podía llevar un UPDATE. Ya no puede:
   * ninguno pasa el guardián con una escritura, contra ninguna base, venga de donde
   * venga. Y entonces la razón para impedirlo se cae — obligaba a que alguien de
   * soporte hiciera de intermediario para correr un SELECT que ya estaba aprobado.
   *
   * Lo que sí se comprueba, y en este orden:
   *
   *   1. que el tema esté marcado para todos — uno de admin sigue siendo de admin, y
   *      por eso la condición va en el WHERE y no en un `if` posterior;
   *   2. que sea de los que ejecutan algo — uno de lectura no tiene nada que correr.
   *
   * Lo demás ya estaba: solo lectura lo garantiza el mismo guardián de siempre, y el
   * límite de filas, el del motor. Queda registrado quién ejecutó qué.
   */
  router.post(
    '/temas/:id/ejecutar',
    h(async (req, res) => {
      const usuario = quien(req);

      const [tema] = await filas<{ id: string; titulo: string; tipo: string | null }>(
        `SELECT id::text, titulo, tipo_solucion AS tipo
         FROM knowledge_base
         WHERE id::text = $1 AND coalesce(visible_para, 'todos') = 'todos'`,
        [req.params.id],
      );
      // Mismo 404 exista o no: distinguirlos dejaría averiguar qué temas restringidos hay.
      if (!tema) {
        res.status(404);
        return { error: 'No se encontró ese tema.' };
      }
      if (!EJECUTABLES.has(String(tema.tipo ?? ''))) {
        throw solicitudInvalida('Ese tema es de lectura: no tiene nada que ejecutar.');
      }
      if (!ejecutarTema) {
        res.status(503);
        return { error: 'Ejecutar temas no está disponible en este momento.' };
      }

      const cuerpo = (req.body ?? {}) as { datos?: Record<string, unknown> };
      const datos = cuerpo.datos && typeof cuerpo.datos === 'object' ? cuerpo.datos : {};

      const salida = await ejecutarTema(tema.id, datos, usuario);
      if (!salida.ok) {
        throw solicitudInvalida(salida.mensaje || 'No se pudo ejecutar el tema.');
      }

      const resultado = Array.isArray(salida.filas) ? salida.filas : [];
      registrar({ tema: tema.id, usuario: usuario.username, filas: resultado.length }, 'tema ejecutado');
      return {
        titulo: tema.titulo,
        resumen: salida.resumen ?? null,
        // Las columnas salen de la primera fila y no del SQL: lo que el motor devuelva es
        // lo que hay que mostrar, y así un alias o una columna calculada aparecen solas.
        columnas: resultado.length > 0 ? Object.keys(resultado[0]) : [],
        filas: resultado,
      };
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

  /**
   * Las bases a las que el backend llegó, para elegir en vez de escribir.
   *
   * Quien reporta muchas veces no sabe cómo se llama la base por dentro, y escribirla a
   * mano producía nombres que no existen. Van solo el id, la etiqueta y el motor: nada
   * de hosts ni de credenciales, que esto lo ve cualquiera con sesión.
   */
  router.get('/bases', h(async () => basesConectadas()));

  return router;
}
