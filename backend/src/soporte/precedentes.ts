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
    .filter((p) => p.length >= 4 && !VACIAS.has(p));

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

/** Expuesto solo para las pruebas: es la pieza que decide qué se busca. */
export const aConsultaParaPruebas = aConsulta;

/**
 * Precedentes para un texto de ticket, de más a menos parecido.
 *
 * Nunca lanza: es un complemento, y si falla no debe tumbar la revisión del ticket.
 */
export async function buscarPrecedentes(
  texto: string,
  opciones: { excluirTicket?: string | null; limite?: number } = {},
): Promise<Precedente[]> {
  const consulta = aConsulta(texto);
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
         ORDER BY relevancia DESC
         LIMIT $2`,
        [consulta, limite],
      ),
    ]);

    const salida: Precedente[] = [
      ...tickets.map((t) => ({
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
      ...articulos.map((a) => ({
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

    // Se normaliza contra el mejor para que el porcentaje signifique «cuánto se parece
    // al más parecido», que es lo que la persona interpreta al verlo.
    const mejor = Math.max(...salida.map((p) => p.relevancia), 0);
    return salida
      .map((p) => ({ ...p, relevancia: mejor > 0 ? p.relevancia / mejor : 0 }))
      .sort((a, b) => b.relevancia - a.relevancia)
      .slice(0, limite);
  } catch {
    return [];
  }
}
