/**
 * Buscar en lo que el equipo ya resolvió.
 *
 * Este es el caso de uso: orquesta el dominio —trocear, pesar, puntuar— pidiéndole los
 * datos a un puerto. No importa ni `pg`, ni `express`, ni ningún archivo de `knowledge/`.
 * Si mañana la base de conocimiento vive en otro motor, este archivo no se toca.
 *
 * Dos fuentes, con criterios distintos:
 *
 *   - Tickets resueltos que dejaron causa raíz o solución escrita. Un ticket sin eso
 *     no es un precedente, es un caso cerrado sin aprendizaje.
 *   - Temas de ayuda, que es donde vive lo que alguien se tomó el trabajo de
 *     documentar. Los que traen script pesan más: no solo explican, dan con qué
 *     arreglarlo.
 */
import { aTerminos } from '../dominio/busqueda/terminos.ts';
import { MINIMO_PARA_MEDIR, confianzaDe, normalizar, pesar } from '../dominio/busqueda/pesaje.ts';
import type { Catalogo, Pregunta } from '../puertos/catalogo.ts';

export interface Precedente {
  tipo: 'ticket' | 'articulo';
  /** Número de ticket o id de tema. */
  id: string;
  titulo: string;
  /** Lo que resume por qué es un precedente: la causa, la solución o la descripción. */
  detalle: string;
  /** Cómo se resolvió, completo. Es lo que la persona necesita leer, no un resumen. */
  solucion?: string | null;
  /** 0..1, comparable entre las dos fuentes. */
  relevancia: number;
  /** Cuánto fiarse, medido sobre la cobertura real y no sobre el primer puesto. */
  confianza: 'alta' | 'media' | 'baja';
  categoria: string | null;
  /** SQL del tema, si lo trae. */
  sql: string | null;
  /** Ruta de la pantalla donde verlo. */
  enlace: string;
}

export interface OpcionesBusqueda {
  excluirTicket?: string | null;
  limite?: number;
  /**
   * Con `true` solo salen temas marcados para todos.
   *
   * Muchos llevan SQL y nombres de tablas, y no son para cualquiera aunque sea
   * personal interno.
   */
  soloPublicos?: boolean;
}

const LIMITE = 5;

/**
 * Puntuaciones mínimas para considerar que un resultado se parece de verdad.
 *
 * Las palabras se unen con OR, así que casi cualquier texto casa con algo. Los umbrales
 * salen de medir casos buenos y malos contra una base real.
 */
const PISO_TICKET = 0.022;
const PISO_TEMA = 0.05;

/**
 * La pregunta con cada término pesado por lo raro que es.
 *
 * Devuelve `null` cuando, quitados los que no distinguen nada, no queda nada con lo que
 * buscar: la frase no tenía ningún término propio, y responder algo sería inventarlo.
 */
async function pesarPregunta(catalogo: Catalogo, texto: string, limite: number): Promise<Pregunta | null> {
  const terminos = aTerminos(texto);
  if (terminos.length === 0) return null;

  try {
    const total = await catalogo.contarTemas();
    // Con muy pocos temas la medida mentiría: todo parecería frecuente.
    if (total < MINIMO_PARA_MEDIR) {
      return { terminos, pesos: terminos.map(() => 1), limite };
    }

    const utiles = pesar(await catalogo.frecuenciaDe(terminos), total);
    if (utiles.length === 0) return null;

    return { terminos: utiles.map((u) => u.termino), pesos: utiles.map((u) => u.peso), limite };
  } catch {
    // Medir es una mejora, no un requisito: si falla, se busca sin pesos.
    return { terminos, pesos: terminos.map(() => 1), limite };
  }
}

/**
 * Precedentes para un texto, de más a menos parecido.
 *
 * Nunca lanza: es un complemento, y si falla no debe tumbar la pantalla que lo llamó.
 */
export async function buscarPrecedentes(
  catalogo: Catalogo,
  texto: string,
  opciones: OpcionesBusqueda = {},
): Promise<Precedente[]> {
  const limite = opciones.limite ?? LIMITE;
  const pregunta = await pesarPregunta(catalogo, texto, limite);
  if (!pregunta) return [];

  try {
    const [tickets, temas] = await Promise.all([
      catalogo.buscarTickets(pregunta, opciones.excluirTicket ?? null),
      catalogo.buscarTemas(pregunta, opciones.soloPublicos === true),
    ]);

    const salida = [
      ...tickets
        .filter((t) => t.relevancia >= PISO_TICKET)
        .map((t) => ({
          tipo: 'ticket' as const,
          id: t.numero,
          titulo: t.titulo,
          detalle: (t.causaRaiz ?? '').trim(),
          solucion: (t.solucion ?? '').trim() || null,
          relevancia: t.relevancia,
          cobertura: t.cobertura,
          categoria: t.sistema,
          sql: null,
          enlace: `/admin/tickets/${encodeURIComponent(t.numero)}`,
        })),
      ...temas
        .filter((a) => a.relevancia >= PISO_TEMA)
        .map((a) => ({
          tipo: 'articulo' as const,
          id: a.id,
          titulo: a.titulo,
          detalle: (a.descripcion ?? '').trim(),
          solucion: null,
          relevancia: a.relevancia,
          cobertura: a.cobertura,
          categoria: a.categoria,
          sql: a.sql,
          enlace: `/admin?articulo=${encodeURIComponent(a.id)}`,
        })),
    ];

    // La confianza se mide sobre la cobertura sin normalizar, antes de que normalizar
    // la relevancia deje al primero en 1 pase lo que pase.
    return normalizar(salida.map((p) => ({ ...p, confianza: confianzaDe(p.cobertura) })))
      .sort((a, b) => b.relevancia - a.relevancia)
      .slice(0, limite);
  } catch {
    return [];
  }
}

/**
 * Apunta que, buscando esto, alguien abrió ese tema.
 *
 * Se guarda por término y no por la frase entera: nadie escribe dos veces igual, y
 * guardar frases completas no serviría para la siguiente persona. Por términos, lo que
 * aprende de «no sale el papel de la impresora» ayuda también a quien escriba «el papel
 * no sale» o «no imprime el papel».
 *
 * Nunca lanza: esto mejora la búsqueda, no puede romper la navegación.
 */
export async function registrarAcierto(catalogo: Catalogo, texto: string, tema: string): Promise<void> {
  try {
    const terminos = aTerminos(texto);
    if (terminos.length === 0) return;
    await catalogo.anotarAcierto(terminos, texto, tema);
  } catch {
    /* que no se aprenda de una visita no le importa a nadie */
  }
}

/** Deja constancia de una búsqueda, para saber después si sirvió de algo. */
export async function registrarBusqueda(
  catalogo: Catalogo,
  texto: string,
  autor: string | null,
  resultados: number,
  confianza: string | null,
): Promise<void> {
  try {
    await catalogo.anotarBusqueda(texto, autor, resultados, confianza);
  } catch {
    /* sin registro se pierde una medida, no una búsqueda */
  }
}

/**
 * Las búsquedas que no acabaron en nada.
 *
 * Son las tres formas de irse con las manos vacías: sin resultados, con resultados que
 * la propia pantalla marcó como lejanos, o con resultados que nadie llegó a abrir.
 */
export function busquedasFallidas(catalogo: Catalogo, dias = 30, limite = 50) {
  return catalogo.busquedasSinRespuesta(dias, limite);
}
