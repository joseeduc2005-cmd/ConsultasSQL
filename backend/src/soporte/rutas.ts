/**
 * Rutas HTTP del módulo de soporte, montadas bajo /api/soporte.
 *
 * Reemplaza a los plugins de Fastify de ticket v2.0 (routes/sistema, tickets,
 * consultas, diagnosticos, vigilancia) por un único router de Express, que es
 * lo que usa el backend de ConsultasSQL.
 *
 * Pendiente a propósito: /interpretar, porque depende del modelo de lenguaje.
 */
import { Router, type NextFunction, type Request, type Response } from 'express';
import { config } from './config.ts';
import { ErrorHttp, noEncontrado, solicitudInvalida, textoOpcional, textoRequerido } from './http/errores.ts';
import { cambiarEstadoAlerta, contarAlertasAbiertas, listarAlertas, ultimaEjecucion, type EstadoAlerta } from './knowledge/alertas.ts';
import { categoriasConsultas, contarConsultas, listarConsultas, obtenerConsulta } from './knowledge/consultas.ts';
import { filas } from './knowledge/db.ts';
import { contarDiagnosticos, guardarDiagnostico, listarDiagnosticos, obtenerDiagnostico, vincularDiagnostico } from './knowledge/diagnosticos.ts';
import {
  ESTADOS_TICKET,
  actualizarTicket,
  agregarPaso,
  buscarSimilares,
  guardarTicket,
  listarTickets,
  obtenerTicket,
} from './knowledge/tickets.ts';
import { MONITORES, obtenerMonitor } from './monitors/monitores.ts';
import { ejecutarMonitor, monitorEnCurso } from './monitors/programador.ts';
import { basesParaTickets, fuente, fuentePara } from './oracle/index.ts';
import type { EstadoFuente } from './oracle/fuente.ts';
import { CORRECCIONES, publicarCorrecciones } from './correcciones.ts';
import { explorarDesdeEntrada } from './exploracion.ts';
import { correccionesDelDiagnostico, diagnosticarTicket, publicarSolucion } from './solucion.ts';
import { construirVocabulario, sembrarVocabulario } from './vocabulario.ts';
import { PLAYBOOKS, describirPlaybook, obtenerPlaybook } from './playbooks/index.ts';
import { ejecutarPlaybook, validarEntrada } from './playbooks/motor.ts';
import type { ResultadoDiagnostico } from './playbooks/tipos.ts';

type Manejador = (req: Request, res: Response) => Promise<unknown>;

/**
 * Express 4 no captura promesas rechazadas: sin este envoltorio, cualquier `throw`
 * dentro de un handler async se convierte en una petición colgada.
 * Lo que devuelve el handler se envía como JSON; si ya respondió, no se toca.
 */
const h =
  (manejador: Manejador) =>
  (req: Request, res: Response, next: NextFunction): void => {
    manejador(req, res)
      .then((valor) => {
        if (!res.headersSent && valor !== undefined) res.json(valor);
      })
      .catch(next);
  };

const texto = (v: unknown): string | null => textoOpcional(v);

const CAMPOS_TICKET = [
  'titulo', 'descripcion', 'autor', 'sistema', 'modulo', 'categoria', 'prioridad', 'estado',
  'resultado', 'url_sir', 'causa_raiz', 'solucion', 'observaciones', 'creado_en', 'resuelto_en',
  'base_datos',
] as const;

const ESTADOS_ALERTA: EstadoAlerta[] = ['abierta', 'atendida', 'resuelta'];

const ETIQUETA_ESTADO = { ok: 'OK', alerta: 'ALERTA', falla: 'FALLA', omitido: 'OMITIDO', error: 'ERROR' } as const;

/**
 * Sondeo de Oracle con caché y espera acotada.
 *
 * `/estado` lo consultan la cabecera y la barra lateral cada minuto. Sin esto, con
 * Oracle inalcanzable cada llamada esperaba a que el driver se rindiera y la interfaz
 * se quedaba colgada. Ahora: se responde en 2,5 s como mucho, el sondeo real termina
 * en segundo plano y su resultado queda cacheado para las siguientes llamadas.
 */
const ESTADO_TTL_MS = 30_000;
const ESTADO_ESPERA_MS = 2_500;
let estadoCacheado: { valor: EstadoFuente; expira: number } | null = null;
let sondeoEnCurso: Promise<void> | null = null;

/**
 * Sondea Oracle al arrancar, sin bloquear: así la primera pantalla que pida el
 * estado ya lo encuentra en caché en vez de esperar.
 */
export function precalentarEstadoOracle(): void {
  void lanzarSondeo();
}

function lanzarSondeo(): Promise<void> {
  if (sondeoEnCurso) return sondeoEnCurso;
  sondeoEnCurso = fuente
    .estado()
    .then((valor) => {
      estadoCacheado = { valor, expira: Date.now() + ESTADO_TTL_MS };
    })
    .catch((error: unknown) => {
      estadoCacheado = {
        valor: { modo: fuente.modo, conectado: false, detalle: (error as Error).message },
        expira: Date.now() + ESTADO_TTL_MS,
      };
    })
    .finally(() => {
      sondeoEnCurso = null;
    });
  return sondeoEnCurso;
}

async function estadoOracle(): Promise<EstadoFuente & { comprobando?: boolean }> {
  const fresco = estadoCacheado && estadoCacheado.expira > Date.now();
  if (fresco) return estadoCacheado!.valor;

  const sondeo = lanzarSondeo();

  // Con un valor anterior a mano se devuelve al instante y el sondeo se refresca
  // por detrás: solo la primera llamada de todas llega a esperar.
  if (estadoCacheado) return estadoCacheado.valor;

  await Promise.race([sondeo, new Promise((listo) => setTimeout(listo, ESTADO_ESPERA_MS))]);
  return (
    estadoCacheado?.valor ?? {
      modo: fuente.modo,
      conectado: false,
      detalle: 'Comprobando la conexión…',
      comprobando: true,
    }
  );
}

function validarEstadoTicket(estado: unknown): void {
  if (estado !== undefined && !(ESTADOS_TICKET as readonly unknown[]).includes(estado)) {
    throw solicitudInvalida(`Estado inválido. Valores permitidos: ${ESTADOS_TICKET.join(', ')}.`);
  }
}

/** Convierte un diagnóstico en un paso de investigación del ticket. */
function pasoDesdeDiagnostico(r: ResultadoDiagnostico) {
  const consultas = r.pasos.flatMap((p) => p.consultas);
  return {
    titulo: `Diagnóstico automático: ${r.nombrePlaybook}`,
    fecha: r.inicio.slice(0, 16).replace('T', ' '),
    sql: consultas.length
      ? consultas.map((c) => `-- ${c.descripcion}${c.biblioteca ? ` (${c.biblioteca})` : ''}\n${c.sql};`).join('\n\n')
      : null,
    resultado: r.pasos.map((p) => `[${ETIQUETA_ESTADO[p.estado]}] ${p.titulo}: ${p.detalle}`).join('\n'),
    evidencia: `Datos de entrada: ${Object.entries(r.entrada).map(([k, v]) => `${k}=${v}`).join(', ')} · Fuente: ${r.origenDatos === 'demo' ? 'demostración' : 'Oracle'}`,
    conclusion: [r.resumen, ...r.hallazgos.map((hh) => `- (${hh.severidad}) ${hh.titulo}`)].join('\n'),
    origen: 'diagnostico' as const,
    diagnostico_id: r.id ?? null,
  };
}

/** Lo que el módulo necesita del motor de consultas para sembrar vocabulario. */
export interface PuenteMotor {
  /** La misma huella con la que el motor lee semantic_learning. */
  obtenerHuella: () => string;
  /** Recarga la caché del motor tras escribir. */
  refrescar: () => Promise<unknown>;
}

export function crearSoporteRouter(
  { log, motor }: { log?: (datos: object, mensaje: string) => void; motor?: PuenteMotor } = {},
): Router {
  const router = Router();
  const registrar = log ?? (() => undefined);

  // ─── Sistema ──────────────────────────────────────────────────────────────
  router.get(
    '/estado',
    h(async () => ({
      version: config.version,
      oracle: await estadoOracle(),
      // Base registrada contra la que corren los diagnósticos, o null en modo demo.
      baseDiagnosticos: config.oracle.dbId,
      vigilancia: config.vigilancia.habilitada,
      correccionesEjecutables: config.correccionesEjecutables,
      reglas: config.reglas,
    })),
  );

  router.get(
    '/resumen',
    h(async () => {
      const [porEstado, porSistema, veredictos, alertas, diagnosticos7, consultas] = await Promise.all([
        filas<{ estado: string; total: number }>('SELECT estado, COUNT(*)::int AS total FROM soporte.tickets GROUP BY estado'),
        filas(`SELECT COALESCE(sistema, 'Sin sistema') AS sistema, COUNT(*)::int AS total
               FROM soporte.tickets GROUP BY 1 ORDER BY total DESC`),
        filas(`SELECT veredicto, COUNT(*)::int AS total FROM soporte.diagnosticos
               WHERE creado_en >= now() - interval '30 days' GROUP BY veredicto`),
        contarAlertasAbiertas(),
        contarDiagnosticos(7),
        contarConsultas(),
      ]);
      const total = (e: string) => porEstado.find((x) => x.estado === e)?.total ?? 0;
      return {
        tickets: {
          abiertos: total('abierto'),
          investigacion: total('investigacion'),
          pendientes: total('pendiente'),
          resueltos: total('resuelto'),
          porSistema,
        },
        alertas,
        diagnosticos: { ultimos7dias: diagnosticos7, veredictos30dias: veredictos },
        consultas,
      };
    }),
  );

  // ─── Tickets ──────────────────────────────────────────────────────────────
  router.get(
    '/tickets',
    h(async (req) =>
      listarTickets({
        texto: texto(req.query.q),
        estado: texto(req.query.estado),
        sistema: texto(req.query.sistema),
      }),
    ),
  );

  router.get(
    '/tickets/:numero',
    h(async (req) => {
      const ticket = await obtenerTicket(req.params.numero);
      if (!ticket) throw noEncontrado('Ticket no encontrado.');
      const similares = await buscarSimilares(
        `${ticket.titulo} ${ticket.descripcion ?? ''} ${ticket.modulo ?? ''}`,
        4,
        ticket.numero,
      );
      return { ...ticket, similares };
    }),
  );

  router.post(
    '/tickets',
    h(async (req, res) => {
      const cuerpo = (req.body ?? {}) as Record<string, unknown>;
      const numero = textoRequerido(cuerpo.numero, 'numero');
      if (await obtenerTicket(numero)) throw solicitudInvalida(`El ticket ${numero} ya existe.`);
      validarEstadoTicket(cuerpo.estado);
      const datos = Object.fromEntries(CAMPOS_TICKET.map((c) => [c, texto(cuerpo[c]) ?? undefined]));
      const ticket = await guardarTicket({
        ...datos,
        numero,
        titulo: textoRequerido(cuerpo.titulo, 'titulo'),
        estado: (datos.estado as never) ?? 'abierto',
        creado_en: datos.creado_en ?? new Date().toISOString().slice(0, 10),
      });
      // El ticket se crea y ya: el diagnóstico lo lanza la persona con «Revisar
      // error». Hacerlo en el alta dejaba el botón sin sentido y disparaba
      // consultas a Oracle sin que nadie las pidiera.
      res.status(201);
      return ticket;
    }),
  );

  router.patch(
    '/tickets/:numero',
    h(async (req) => {
      const cuerpo = (req.body ?? {}) as Record<string, unknown>;
      validarEstadoTicket(cuerpo.estado);
      const cambios = Object.fromEntries(
        CAMPOS_TICKET.filter((c) => cuerpo[c] !== undefined).map((c) => [c, texto(cuerpo[c])]),
      );
      if (cambios.titulo === null) throw solicitudInvalida('El título no puede quedar vacío.');
      const ticket = await actualizarTicket(req.params.numero, cambios);
      if (!ticket) throw noEncontrado('Ticket no encontrado.');
      return ticket;
    }),
  );

  router.post(
    '/tickets/:numero/pasos',
    h(async (req, res) => {
      const cuerpo = (req.body ?? {}) as Record<string, unknown>;
      if (!(await obtenerTicket(req.params.numero))) throw noEncontrado('Ticket no encontrado.');
      const paso = await agregarPaso(req.params.numero, {
        titulo: textoRequerido(cuerpo.titulo, 'titulo'),
        sql: texto(cuerpo.sql),
        resultado: texto(cuerpo.resultado),
        evidencia: texto(cuerpo.evidencia),
        conclusion: texto(cuerpo.conclusion),
      });
      res.status(201);
      return paso;
    }),
  );

  /**
   * Revisa el error del ticket: deduce qué comprobar de su texto, ejecuta el
   * diagnóstico y devuelve el resultado completo para mostrarlo en pantalla.
   * El paso queda registrado en la investigación del ticket.
   */
  router.post(
    '/tickets/:numero/revisar',
    h(async (req) => {
      const ticket = await obtenerTicket(req.params.numero);
      if (!ticket) throw noEncontrado('Ticket no encontrado.');
      const quien = (req as unknown as { usuarioSoporte?: { username: string } }).usuarioSoporte?.username ?? null;
      const revision = await diagnosticarTicket(ticket, quien);
      registrar({ ticket: ticket.numero, ...revision, resultado: undefined }, 'ticket revisado');
      return revision;
    }),
  );

  // ─── Biblioteca SQL ───────────────────────────────────────────────────────
  router.get(
    '/consultas',
    h(async (req) => ({
      consultas: await listarConsultas({
        texto: texto(req.query.q),
        categoria: texto(req.query.categoria),
        riesgo: texto(req.query.riesgo),
      }),
      categorias: await categoriasConsultas(),
    })),
  );

  router.get(
    '/consultas/:codigo',
    h(async (req) => {
      const consulta = await obtenerConsulta(req.params.codigo);
      if (!consulta) throw noEncontrado('Consulta no encontrada.');
      return consulta;
    }),
  );

  // ─── Diagnósticos ─────────────────────────────────────────────────────────
  router.get('/playbooks', h(async () => PLAYBOOKS.map(describirPlaybook)));

  router.post(
    '/playbooks/:id/ejecutar',
    h(async (req) => {
      const cuerpo = (req.body ?? {}) as { entrada?: Record<string, unknown>; ticket?: string };
      const playbook = obtenerPlaybook(req.params.id);
      if (!playbook) throw noEncontrado('Diagnóstico no encontrado.');
      const ticket = texto(cuerpo.ticket);
      if (ticket && !(await obtenerTicket(ticket))) throw solicitudInvalida(`El ticket ${ticket} no existe.`);
      const entrada = validarEntrada(playbook, cuerpo.entrada ?? {});
      const resultado = await ejecutarPlaybook(playbook, entrada, fuente);
      const id = await guardarDiagnostico(resultado, ticket);
      registrar({ playbook: playbook.id, veredicto: resultado.veredicto, id }, 'diagnóstico ejecutado');
      return { ...resultado, id, ticket };
    }),
  );

  router.get(
    '/diagnosticos',
    h(async (req) => listarDiagnosticos(Math.min(Number(req.query.limite) || 20, 100))),
  );

  router.get(
    '/diagnosticos/:id',
    h(async (req) => {
      const resultado = await obtenerDiagnostico(Number(req.params.id));
      if (!resultado) throw noEncontrado('Diagnóstico no encontrado.');
      return resultado;
    }),
  );

  router.post(
    '/diagnosticos/:id/registrar',
    h(async (req, res) => {
      const cuerpo = (req.body ?? {}) as { ticket?: string };
      const resultado = await obtenerDiagnostico(Number(req.params.id));
      if (!resultado) throw noEncontrado('Diagnóstico no encontrado.');
      const ticket = texto(cuerpo.ticket);
      if (!ticket) throw solicitudInvalida('Indique el número de ticket.');
      if (!(await obtenerTicket(ticket))) throw noEncontrado(`El ticket ${ticket} no existe.`);
      const paso = await agregarPaso(ticket, pasoDesdeDiagnostico(resultado));
      await vincularDiagnostico(resultado.id!, ticket);
      res.status(201);
      return paso;
    }),
  );

  // ─── Puente con el motor de consultas ─────────────────────────────────────
  // La Biblioteca SQL es un corpus curado que mapea lenguaje de negocio a las
  // tablas reales. Esto lo deriva y lo deja donde el motor ya sabe buscarlo.

  router.get(
    '/vocabulario',
    h(async (req) => {
      const derivado = await construirVocabulario({
        minApariciones: Number(req.query.minApariciones) || undefined,
        minExclusividad: Number(req.query.minExclusividad) || undefined,
      });
      return { ...derivado, huella: motor?.obtenerHuella() ?? null };
    }),
  );

  router.post(
    '/vocabulario',
    h(async (req) => {
      if (!motor) throw solicitudInvalida('El puente con el motor de consultas no está configurado.');
      const cuerpo = (req.body ?? {}) as { minApariciones?: number; minExclusividad?: number };
      const resultado = await sembrarVocabulario(motor.obtenerHuella(), {
        minApariciones: Number(cuerpo.minApariciones) || undefined,
        minExclusividad: Number(cuerpo.minExclusividad) || undefined,
      });
      await motor.refrescar();
      registrar(resultado, 'vocabulario de la Biblioteca sembrado en el motor');
      return resultado;
    }),
  );

  // ─── Correcciones publicadas como artículos ───────────────────────────────

  router.get(
    '/correcciones',
    h(async () =>
      Object.values(CORRECCIONES).map((c) => ({
        id: c.id,
        titulo: c.titulo,
        descripcion: c.descripcion,
        categoria: c.categoria,
        subcategoria: c.subcategoria,
        biblioteca: c.biblioteca ?? null,
        responsable: c.responsable,
        plantilla: c.plantilla,
        campos: c.campos,
        advertencia: c.advertencia ?? null,
      })),
    ),
  );

  router.post(
    '/correcciones/publicar',
    h(async (req) => {
      const cuerpo = (req.body ?? {}) as { databaseId?: string; ejecutable?: boolean };
      const usuario = (req as unknown as { usuarioSoporte?: { id: string; username: string } }).usuarioSoporte;
      if (!usuario) throw solicitudInvalida('No se pudo identificar al usuario que publica.');
      const resultado = await publicarCorrecciones({
        databaseId: texto(cuerpo.databaseId) ?? config.oracle.dbId,
        autorId: usuario.id,
        autorNombre: usuario.username,
        // Aunque lo pidan, sin la llave global no se publica nada ejecutable.
        ejecutable: cuerpo.ejecutable === true && config.correccionesEjecutables,
      });
      registrar(resultado, 'correcciones publicadas como artículos');
      return resultado;
    }),
  );

  router.post(
    '/diagnosticos/:id/publicar',
    h(async (req, res) => {
      const resultado = await obtenerDiagnostico(Number(req.params.id));
      if (!resultado) throw noEncontrado('Diagnóstico no encontrado.');
      if (resultado.hallazgos.length === 0) {
        throw solicitudInvalida('Este diagnóstico no encontró nada que documentar como solución.');
      }
      const usuario = (req as unknown as { usuarioSoporte?: { id: string; username: string } }).usuarioSoporte;
      if (!usuario) throw solicitudInvalida('No se pudo identificar al usuario que publica.');

      // Sin lista se publican todas: el selector de la pantalla siempre manda una,
      // pero la API sigue sirviendo para publicar el diagnóstico entero de un tirón.
      const cuerpo = (req.body ?? {}) as {
        correcciones?: unknown;
        exploracion?: unknown;
        propio?: { titulo?: string; sql?: string; pasos?: string; dinamico?: boolean };
      };
      let elegidas: string[] | null = null;
      if (Array.isArray(cuerpo.correcciones)) {
        const disponibles = new Set(correccionesDelDiagnostico(resultado).map((c) => c.id));
        elegidas = cuerpo.correcciones.map(String);
        const ajenas = elegidas.filter((id) => !disponibles.has(id));
        if (ajenas.length > 0) {
          throw solicitudInvalida(`Este diagnóstico no propone la corrección «${ajenas[0]}».`);
        }
      }

      // La propuesta de exploración se vuelve a derivar aquí en vez de aceptarla del
      // navegador: así el SQL que se publica sale siempre de lo que Oracle dice ahora,
      // y no de un cuerpo que alguien pudo cambiar por el camino.
      const ticket = resultado.ticket ? await obtenerTicket(resultado.ticket) : null;
      let exploracion = null;
      if (cuerpo.exploracion === true) {
        const suFuente = fuentePara(ticket?.base_datos) ?? fuente;
        const r = await explorarDesdeEntrada(suFuente, resultado.entrada);
        if (!r.propuesta) {
          throw solicitudInvalida(r.motivo ?? 'La exploración ya no propone ninguna corrección para este caso.');
        }
        exploracion = r.propuesta;
      }

      const publicado = await publicarSolucion(resultado, {
        sistema: ticket?.sistema ?? null,
        baseDatos: ticket?.base_datos ?? config.oracle.dbId,
        autorId: usuario.id,
        autorNombre: usuario.username,
        correcciones: elegidas,
        exploracion,
        propio:
          texto(cuerpo.propio?.sql) || texto(cuerpo.propio?.pasos)
            ? {
                titulo: texto(cuerpo.propio?.titulo) ?? null,
                sql: texto(cuerpo.propio?.sql) ?? null,
                pasos: texto(cuerpo.propio?.pasos) ?? null,
                dinamico: cuerpo.propio?.dinamico !== false,
              }
            : null,
      });
      registrar(publicado, 'solución publicada como artículos');
      res.status(publicado.articulo.creado ? 201 : 200);
      return publicado;
    }),
  );

  router.get('/bases-tickets', h(async () => basesParaTickets()));

  // ─── Vigilancia ───────────────────────────────────────────────────────────
  router.get(
    '/monitores',
    h(async () =>
      Promise.all(
        MONITORES.map(async (m) => ({
          id: m.id,
          nombre: m.nombre,
          descripcion: m.descripcion,
          intervaloMin: m.intervaloMin,
          ticketsReferencia: m.ticketsReferencia,
          playbook: m.playbook ?? null,
          enCurso: monitorEnCurso(m.id),
          ultimaEjecucion: await ultimaEjecucion(m.id),
        })),
      ),
    ),
  );

  router.post(
    '/monitores/:id/ejecutar',
    h(async (req) => {
      const monitor = obtenerMonitor(req.params.id);
      if (!monitor) throw noEncontrado('Monitor no encontrado.');
      return ejecutarMonitor(monitor, fuente);
    }),
  );

  router.get('/alertas', h(async (req) => listarAlertas(texto(req.query.estado))));

  router.patch(
    '/alertas/:id',
    h(async (req) => {
      const estado = (req.body ?? {}).estado as EstadoAlerta;
      if (!ESTADOS_ALERTA.includes(estado)) {
        throw solicitudInvalida(`Estado inválido. Valores permitidos: ${ESTADOS_ALERTA.join(', ')}.`);
      }
      const alerta = await cambiarEstadoAlerta(Number(req.params.id), estado);
      if (!alerta) throw noEncontrado('Alerta no encontrada.');
      return alerta;
    }),
  );

  // Traduce los ErrorHttp del módulo al formato { error } que espera su frontend.
  router.use((err: unknown, _req: Request, res: Response, next: NextFunction) => {
    if (res.headersSent) return next(err);
    if (err instanceof ErrorHttp) {
      res.status(err.estado).json({ error: err.message });
      return;
    }
    registrar({ err: (err as Error)?.message }, 'error en el módulo de soporte');
    res.status(500).json({ error: 'Error interno del servidor.' });
  });

  return router;
}
