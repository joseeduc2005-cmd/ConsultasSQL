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
 */
import { Router, type NextFunction, type Request, type Response } from 'express';
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

  return router;
}
