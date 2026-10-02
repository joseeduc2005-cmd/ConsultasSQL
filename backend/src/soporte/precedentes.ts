/**
 * Buscar en lo que el equipo ya resolvió.
 *
 * Los diagnósticos consultan Oracle para ver qué le pasa a un usuario concreto. Esto
 * hace lo otro: busca en la base de conocimiento si un caso parecido ya se resolvió
 * antes, y trae con qué. No toca ninguna base externa — todo sale de `knowledge`.
 *
 * Sirve en dos momentos:
 *
 *   - Sin Oracle conectada, es lo único que puede responder, y responde bastante: el
 *     valor del sistema no está solo en consultar, está en no volver a investigar lo
 *     que ya se investigó.
 *   - Con Oracle, acompaña al diagnóstico: «esto se parece al ticket 2026083700, que
 *     se resolvió así».
 *
 * Dos fuentes, con criterios distintos:
 *
 *   - Tickets resueltos que dejaron causa raíz o solución escrita. Un ticket sin eso
 *     no es un precedente, es un caso cerrado sin aprendizaje.
 *   - Artículos del árbol de categorías, que es donde vive lo que alguien se tomó el
 *     trabajo de documentar. Los que traen script pesan más: no solo explican, dan
 *     con qué arreglarlo.
 */
import { filas } from './knowledge/db.ts';

export interface Precedente {
  tipo: 'ticket' | 'articulo';
  /** Cómo se resolvió, completo. Es lo que la persona necesita leer, no un resumen. */
  solucion?: string | null;
  /** Número de ticket o id de artículo. */
  id: string;
  titulo: string;
  /** Lo que resume por qué es un precedente: la causa, la solución o la descripción. */
  detalle: string;
  /** 0..1, comparable entre las dos fuentes. */
  relevancia: number;
  /** Cuánto fiarse, medido sobre la puntuación real y no sobre el primer puesto. */
  confianza: 'alta' | 'media' | 'baja';
  categoria: string | null;
  /** SQL del artículo, si lo trae. */
  sql: string | null;
  /** Ruta de la pantalla donde verlo. */
  enlace: string;
}

/**
 * Convierte texto libre en una consulta de Postgres.
 *
 * Se unen las palabras con OR y no con AND: el título de un ticket rara vez coincide
 * palabra por palabra con el de otro, y exigir todas devolvía siempre vacío. El orden
 * por relevancia se encarga de que las que más coinciden suban.
 */
function aConsulta(texto: string): string | null {
  const palabras = texto
    .toLowerCase()
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .replace(/[^a-z0-9\s]/g, ' ')
    .split(/\s+/)
    // El mínimo de cuatro letras es para palabras: descarta «de», «la», «si». A los
    // números no se les aplica —bastan dos cifras— porque suelen ser lo más distintivo
    // de un reporte. «No muestra más de 300 transacciones» no se encontraba buscando
    // «se corta a las 300 filas»: el único término que las unía era justo el que se
    // estaba tirando.
    .filter((p) => (p.length >= 4 || /^\d{2,}$/.test(p)) && !VACIAS.has(p));

  const unicas = [...new Set(palabras)].slice(0, 24);
  return unicas.length > 0 ? unicas.join(' | ') : null;
}

/** Palabras que aparecen en todos los tickets y no distinguen nada. */
const VACIAS = new Set([
  'para', 'pero', 'como', 'esta', 'este', 'esto', 'sobre', 'desde', 'hasta', 'cuando',
  'porque', 'tambien', 'favor', 'gracias', 'estimados', 'buenos', 'buenas', 'dias',
  'tardes', 'saludos', 'atentos', 'quedamos', 'adjunto', 'reporta', 'reportado',
  'solicita', 'solicitan', 'comenta', 'indica', 'nos', 'les', 'que', 'con', 'del',
  'los', 'las', 'una', 'por', 'sistema', 'usuario', 'error', 'problema', 'caso',
]);

const LIMITE = 5;

/**
 * Puntaje mínimo para considerar que un resultado se parece de verdad.
 *
 * Las palabras se unen con OR, así que casi cualquier texto casa con algo: un caso
 * sobre adjuntar un PDF devolvía «Error en reimpresión de comprobante» al 100 %,
 * porque compartían «solicitud» y «adjunto». Como el porcentaje se normaliza contra
 * el mejor, el primero siempre salía al 100 % aunque no tuviera nada que ver.
 *
 * Los umbrales salen de medir casos buenos y malos contra esta base: un acierto
 * ronda 0,033 en tickets y 0,4 en artículos; un falso positivo, 0,018 y menos de
 * 0,05. Son de esta base y habrá que revisarlos cuando crezca — el calibrador de
 * `herramientas/` es el sitio donde comprobarlo.
 */
const PISO_TICKET = 0.022;
const PISO_ARTICULO = 0.05;

/**
 * Cuánto fiarse de un resultado, a partir de su puntuación sin normalizar.
 *
 * Los cortes salen de medir contra casos reales: los aciertos claros rondaban 0,20 y
 * los parecidos razonables 0,09, mientras las coincidencias por una palabra suelta se
 * quedaban por debajo de 0,08. No separan perfectamente —un buscador por palabras no
 * sabe que «pago» en «módulo de nómina» es incidental— pero dicho así, quien lee sabe
 * cuándo mirar con cuidado en vez de encontrarse siempre un 100 %.
 */
function confianzaDe(bruta: number): 'alta' | 'media' | 'baja' {
  if (bruta >= 0.15) return 'alta';
  return bruta >= 0.085 ? 'media' : 'baja';
}

/** Expuesto solo para las pruebas: es la pieza que decide qué se busca. */
export const aConsultaParaPruebas = aConsulta;

/**
 * Por encima de esta proporción de artículos, un término no distingue nada.
 *
 * En una base de corresponsales, «corresponsal» sale en casi todos los títulos. Vale
 * tanto como «sistema» o «usuario», que ya estaban en la lista de palabras vacías —
 * solo que esa lista está escrita a mano y no puede saber de qué va cada instalación.
 */
const CUOTA_DEMASIADO_COMUN = 0.5;
/** Por debajo de esto no hay con qué medir: con 6 artículos todo parece frecuente. */
const MINIMO_PARA_MEDIR = 10;

/**
 * La consulta, quitando los términos que aparecen en casi todo.
 *
 * PostgreSQL no penaliza lo frecuente: `ts_rank` cuenta igual un término que está en
 * los quince documentos que uno que está en uno solo. Así, «el corresponsal de Gualaceo
 * hizo una recarga y no entró el saldo» casaba con cualquier artículo que dijera
 * «corresponsal», y la recarga —lo único que de verdad identificaba el caso— se perdía
 * entre el ruido.
 *
 * Se mide contra los datos en vez de mantener otra lista a mano: así funciona igual en
 * una base de corresponsales que en una de nómina, sin que nadie la actualice.
 *
 * Si quitando los comunes no queda nada, se devuelve null: significa que la frase no
 * tenía ningún término propio, y responder algo sería inventarlo.
 */
async function aConsultaDiscriminante(texto: string): Promise<string | null> {
  const base = aConsulta(texto);
  if (!base) return null;

  const terminos = base.split(' | ');
  if (terminos.length < 2) return base;

  try {
    const [{ total } = { total: 0 }] = await filas<{ total: number }>(
      'SELECT count(*)::int AS total FROM knowledge_base',
    );
    if (total < MINIMO_PARA_MEDIR) return base;

    // Una búsqueda por índice por término; son 24 como mucho y van por el GIN.
    const frecuencias = await filas<{ termino: string; docs: number }>(
      `SELECT t.termino,
              (SELECT count(*)::int FROM knowledge_base k
                WHERE k.busqueda @@ to_tsquery('spanish', t.termino)) AS docs
         FROM unnest($1::text[]) AS t(termino)`,
      [terminos],
    );

    const utiles = frecuencias.filter((f) => f.docs / total <= CUOTA_DEMASIADO_COMUN).map((f) => f.termino);
    if (utiles.length === 0) return null;
    return utiles.join(' | ');
  } catch {
    // Medir es una mejora, no un requisito: si falla, se busca como antes.
    return base;
  }
}

/**
 * Precedentes para un texto de ticket, de más a menos parecido.
 *
 * Nunca lanza: es un complemento, y si falla no debe tumbar la revisión del ticket.
 */
export async function buscarPrecedentes(
  texto: string,
  opciones: {
    excluirTicket?: string | null;
    limite?: number;
    /**
     * Con `true` solo salen artículos marcados para todos.
     *
     * Hoy toda la API es de administradores, así que da igual; pero en cuanto exista
     * una pantalla de cliente, sin esto se le mostrarían artículos que llevan SQL y
     * nombres de tablas. Es más barato tenerlo antes que acordarse después.
     */
    soloPublicos?: boolean;
  } = {},
): Promise<Precedente[]> {
  const consulta = await aConsultaDiscriminante(texto);
  if (!consulta) return [];
  const limite = opciones.limite ?? LIMITE;


  try {
    const [tickets, articulos] = await Promise.all([
      filas<{ numero: string; titulo: string; causa_raiz: string | null; solucion: string | null; sistema: string | null; relevancia: number }>(
        `SELECT t.numero, t.titulo, t.causa_raiz, t.solucion, t.sistema,
                ts_rank(t.busqueda, q) AS relevancia
         FROM soporte.tickets t, to_tsquery('spanish', $1) q
         WHERE t.busqueda @@ q
           AND t.numero <> $2
           AND t.estado = 'resuelto'
           AND (t.causa_raiz IS NOT NULL OR t.solucion IS NOT NULL)
         ORDER BY relevancia DESC
         LIMIT $3`,
        [consulta, opciones.excluirTicket ?? '', limite],
      ),
      filas<{ id: string; titulo: string; descripcion: string | null; categoria: string | null; tipo_solucion: string | null; sql: string | null; relevancia: number }>(
        `SELECT k.id::text, k.titulo, k.descripcion, k.categoria, k.tipo_solucion,
                k.script_json->>'sql' AS sql,
                -- Un artículo con script vale más: además de explicar, trae el arreglo.
                ts_rank(k.busqueda, q) * (CASE WHEN k.script_json IS NOT NULL THEN 1.4 ELSE 1 END) AS relevancia
         FROM knowledge_base k, to_tsquery('spanish', $1) q
         WHERE k.busqueda @@ q
           AND ($3 = false OR coalesce(k.visible_para, 'todos') = 'todos')
         ORDER BY relevancia DESC
         LIMIT $2`,
        [consulta, limite, opciones.soloPublicos === true],
      ),
    ]);

    const salida: Precedente[] = [
      ...tickets.filter((t) => Number(t.relevancia) >= PISO_TICKET).map((t) => ({
        tipo: 'ticket' as const,
        id: t.numero,
        titulo: t.titulo,
        detalle: (t.causa_raiz ?? '').trim(),
        solucion: (t.solucion ?? '').trim() || null,
        relevancia: Number(t.relevancia),
        categoria: t.sistema,
        sql: null,
        enlace: `/admin/tickets/${encodeURIComponent(t.numero)}`,
      })),
      ...articulos.filter((a) => Number(a.relevancia) >= PISO_ARTICULO).map((a) => ({
        tipo: 'articulo' as const,
        id: a.id,
        titulo: a.titulo,
        detalle: (a.descripcion ?? '').trim(),
        solucion: null,
        relevancia: Number(a.relevancia),
        categoria: a.categoria,
        sql: a.sql,
        enlace: `/admin?articulo=${encodeURIComponent(a.id)}`,
      })),
    ];

    // La relevancia se normaliza contra el mejor —eso ordena bien— pero por sí sola
    // miente: dividir por el máximo deja siempre al primero en 1, sea un acierto o un
    // disparate. `confianza` se mide sobre la puntuación cruda, que es la que distingue
    // un parecido de verdad de una coincidencia suelta, para que la pantalla pueda
    // decir «esto es lo suyo» o «mire con cuidado» en vez de afirmar siempre lo mismo.
    const mejor = Math.max(...salida.map((p) => p.relevancia), 0);
    return salida
      .map((p) => ({
        ...p,
        confianza: confianzaDe(p.relevancia),
        relevancia: mejor > 0 ? p.relevancia / mejor : 0,
      }))
      .sort((a, b) => b.relevancia - a.relevancia)
      .slice(0, limite);
  } catch {
    return [];
  }
}
