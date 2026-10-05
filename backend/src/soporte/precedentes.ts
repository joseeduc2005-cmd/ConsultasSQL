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
import { ejecutar, filas } from './knowledge/db.ts';

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
    // Dos caracteres, no cuatro. El mínimo alto estaba para quitar «de», «la», «si»,
    // pero de eso ya se encarga el diccionario español de PostgreSQL, que las descarta
    // al convertirlas en lexemas. Lo que sí se llevaba por delante era el vocabulario
    // del dominio —`qr`, `pos`, `pin`, `mac`, `sms`, `otp`— y los números cortos: un
    // pago con QR no encontraba el incidente de QR porque el término se tiraba antes
    // de llegar a la consulta, igual que pasaba con «300».
    .filter((p) => p.length >= 2 && !VACIAS.has(p));

  const unicas = [...new Set(palabras)].slice(0, 24);
  return unicas.length > 0 ? unicas.join(' | ') : null;
}

/**
 * Palabras que no distinguen nada: las de relleno del español y las que salen en todos
 * los tickets. Las cortas están aquí desde que el mínimo de longitud bajó a dos: hacía
 * falta para no perder «qr», «pos» o «pin», pero dejaba entrar «de» y «la».
 *
 * El diccionario español de PostgreSQL también las descarta, así que esto es redundante
 * a propósito: deja la garantía escrita donde se lee, y no dependiendo de qué haga por
 * dentro un motor que puede cambiar de versión.
 */
const VACIAS = new Set([
  'de', 'la', 'el', 'en', 'un', 'lo', 'al', 'se', 'su', 'mi', 'tu', 'es', 'ya', 'si',
  'no', 'me', 'le', 'te', 'ha', 'he', 'va', 'yo', 'eso', 'ese', 'esa', 'ser', 'son',
  'sus', 'mas', 'muy', 'hay', 'dos', 'tres',
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
 * Cuánto fiarse, a partir de qué proporción de lo distintivo cubre el resultado.
 *
 * `cobertura` va de 0 a 1 y es interpretable: cuánto del peso de la pregunta —midiendo
 * cada palabra por lo rara que es— aparece en este artículo. Cubrir «repetidos» y
 * «duplicados» vale mucho; cubrir solo «usuarios», que está en la mitad de la base,
 * vale poco. Sustituye a los umbrales sobre `ts_rank`, que había que recalibrar a mano
 * cada vez que la base crecía porque dependían de cuántos documentos hubiera.
 */
function confianzaDe(cobertura: number): 'alta' | 'media' | 'baja' {
  if (cobertura >= 0.55) return 'alta';
  return cobertura >= 0.3 ? 'media' : 'baja';
}

/** Expuesto solo para las pruebas: es la pieza que decide qué se busca. */
export const aConsultaParaPruebas = aConsulta;

/**
 * Deja constancia de una búsqueda, para saber después si sirvió de algo.
 *
 * Nunca lanza: llevar la cuenta no puede impedir que alguien busque.
 */
export async function registrarBusqueda(
  texto: string,
  autor: string | null,
  resultados: number,
  confianza: string | null,
): Promise<void> {
  try {
    await ejecutar(
      'INSERT INTO soporte.busquedas (texto, autor, resultados, confianza) VALUES ($1, $2, $3, $4)',
      [texto.trim().slice(0, 500), autor, resultados, confianza],
    );
  } catch {
    /* sin registro se pierde una medida, no una búsqueda */
  }
}

/**
 * Las búsquedas que no acabaron en nada, agrupadas por lo que se escribió.
 *
 * Son las tres formas de irse con las manos vacías: sin resultados, con resultados que
 * la propia pantalla marcó como lejanos, o con resultados que nadie llegó a abrir.
 */
export function busquedasFallidas(dias = 30, limite = 50) {
  return filas<{ texto: string; veces: number; resultados: number; confianza: string | null; ultima: string }>(
    `SELECT lower(btrim(texto)) AS texto, count(*)::int AS veces,
            max(resultados) AS resultados, min(confianza) AS confianza,
            max(creado_en) AS ultima
       FROM soporte.busquedas
      WHERE creado_en > now() - ($1 || ' days')::interval
        AND abrio = false
      GROUP BY lower(btrim(texto))
      ORDER BY count(*) DESC, max(creado_en) DESC
      LIMIT $2`,
    [String(dias), limite],
  );
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
export async function registrarAcierto(texto: string, articulo: string): Promise<void> {
  try {
    const consulta = aConsulta(texto);
    if (!consulta) return;
    // La misma apertura cierra la búsqueda que la originó: lo que quede sin cerrar es
    // lo que no le sirvió a nadie.
    await ejecutar(
      `UPDATE soporte.busquedas SET abrio = true
        WHERE id = (SELECT id FROM soporte.busquedas
                     WHERE lower(btrim(texto)) = lower(btrim($1)) AND abrio = false
                     ORDER BY creado_en DESC LIMIT 1)`,
      [texto],
    );
    await ejecutar(
      `INSERT INTO soporte.aprendizaje (termino, articulo)
       SELECT t, $2 FROM unnest($1::text[]) AS t
       ON CONFLICT (termino, articulo)
       DO UPDATE SET veces = soporte.aprendizaje.veces + 1, ultima_vez = now()`,
      [consulta.split(' | '), articulo],
    );
  } catch {
    /* que no se aprenda de una visita no le importa a nadie */
  }
}

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
 * La consulta, con cada término pesado por lo raro que es.
 *
 * PostgreSQL no penaliza lo frecuente: `ts_rank` cuenta igual un término que está en
 * los quince artículos que uno que está en dos. Así «usuarios repetidos» encontraba
 * «usuario no encontrado», porque «usuarios» —que sale en la mitad de la base— pesaba
 * tanto como «repetidos», que es lo único que decía de qué iba la pregunta.
 *
 * La medida es la de toda la vida para esto: el logaritmo de cuántos documentos hay
 * entre los que contienen el término. Lo que está en todos pesa cero y se cae solo, sin
 * necesidad de un umbral; lo raro pesa mucho. Y se mide contra los datos, así que
 * funciona igual en una base de corresponsales que en una de nómina sin que nadie
 * mantenga una lista.
 */
async function aConsultaDiscriminante(
  texto: string,
): Promise<{ consulta: string; terminos: string[]; pesos: number[] } | null> {
  const base = aConsulta(texto);
  if (!base) return null;
  const terminos = base.split(' | ');

  try {
    const [{ total } = { total: 0 }] = await filas<{ total: number }>(
      'SELECT count(*)::int AS total FROM knowledge_base',
    );
    if (total < MINIMO_PARA_MEDIR) {
      return { consulta: base, terminos, pesos: terminos.map(() => 1) };
    }

    // Una búsqueda por índice por término; son 24 como mucho y van por el GIN.
    const frecuencias = await filas<{ termino: string; docs: number }>(
      `SELECT t.termino,
              (SELECT count(*)::int FROM knowledge_base k
                WHERE k.busqueda @@ to_tsquery('spanish', t.termino)) AS docs
         FROM unnest($1::text[]) AS t(termino)`,
      [terminos],
    );

    const utiles = frecuencias
      .map((f) => ({ termino: f.termino, peso: Math.log((total + 1) / (Number(f.docs) + 1)) }))
      // Peso cero es un término que está en todas partes: no distingue, y dejarlo solo
      // añade ruido a la consulta.
      .filter((f) => f.peso > 0.01);

    if (utiles.length === 0) return null;
    return {
      consulta: utiles.map((u) => u.termino).join(' | '),
      terminos: utiles.map((u) => u.termino),
      pesos: utiles.map((u) => u.peso),
    };
  } catch {
    // Medir es una mejora, no un requisito: si falla, se busca como antes.
    return { consulta: base, terminos, pesos: terminos.map(() => 1) };
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
  const pregunta = await aConsultaDiscriminante(texto);
  if (!pregunta) return [];
  const { consulta, terminos, pesos } = pregunta;
  const limite = opciones.limite ?? LIMITE;
  // Cuánto pesa la pregunta entera: la cobertura de cada resultado se mide contra esto.
  const pesoTotal = pesos.reduce((a, b) => a + b, 0) || 1;


  try {
    const [tickets, articulos] = await Promise.all([
      filas<{ numero: string; titulo: string; causa_raiz: string | null; solucion: string | null; sistema: string | null; relevancia: number; cobertura: number }>(
        `WITH p(termino, peso) AS (SELECT * FROM unnest($4::text[], $5::float8[]))
         SELECT t.numero, t.titulo, t.causa_raiz, t.solucion, t.sistema,
                ts_rank(t.busqueda, q) AS relevancia,
                -- Qué parte del peso de la pregunta cubre este ticket.
                (SELECT coalesce(sum(p.peso), 0) FROM p
                  WHERE t.busqueda @@ to_tsquery('spanish', p.termino)) / $6 AS cobertura
         FROM soporte.tickets t, to_tsquery('spanish', $1) q
         WHERE t.busqueda @@ q
           AND t.numero <> $2
           AND t.estado = 'resuelto'
           AND (t.causa_raiz IS NOT NULL OR t.solucion IS NOT NULL)
         ORDER BY cobertura DESC, relevancia DESC
         LIMIT $3`,
        [consulta, opciones.excluirTicket ?? '', limite, terminos, pesos, pesoTotal],
      ),
      filas<{ id: string; titulo: string; descripcion: string | null; categoria: string | null; tipo_solucion: string | null; sql: string | null; relevancia: number; cobertura: number }>(
        `WITH p(termino, peso) AS (SELECT * FROM unnest($4::text[], $5::float8[]))
         SELECT * FROM (
           SELECT k.id::text, k.titulo, k.descripcion, k.categoria, k.tipo_solucion,
                  k.script_json->>'sql' AS sql,
                  (SELECT coalesce(sum(p.peso), 0) FROM p
                    WHERE k.busqueda @@ to_tsquery('spanish', p.termino)) / $6 AS cobertura,
                  -- Cuántas veces se abrió este tema tras buscar con estas palabras.
                  (SELECT coalesce(sum(a.veces), 0) FROM soporte.aprendizaje a
                    WHERE a.articulo = k.id::text AND a.termino = ANY($4::text[])) AS aprendido,
                  -- Un artículo con script vale más: además de explicar, trae el arreglo.
                  ts_rank(k.busqueda, q) * (CASE WHEN k.script_json IS NOT NULL THEN 1.4 ELSE 1 END) AS relevancia
           FROM knowledge_base k, to_tsquery('spanish', $1) q
           WHERE k.busqueda @@ q
             AND ($3 = false OR coalesce(k.visible_para, 'todos') = 'todos')
         ) s
         -- Lo aprendido empuja, no manda: tope de 0,25 sobre una cobertura que va de 0
         -- a 1, así que no puede subir un tema que no tenga nada que ver por mucho que
         -- lo hayan abierto. Y lo que de verdad coincide sigue ganando sin ayuda.
         ORDER BY cobertura + least(aprendido * 0.03, 0.25) DESC, relevancia DESC
         LIMIT $2`,
        [consulta, limite, opciones.soloPublicos === true, terminos, pesos, pesoTotal],
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
        cobertura: Number(t.cobertura),
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
        cobertura: Number(a.cobertura),
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
        confianza: confianzaDe(p.cobertura),
        relevancia: mejor > 0 ? p.relevancia / mejor : 0,
      }))
      .sort((a, b) => b.relevancia - a.relevancia)
      .slice(0, limite);
  } catch {
    return [];
  }
}
