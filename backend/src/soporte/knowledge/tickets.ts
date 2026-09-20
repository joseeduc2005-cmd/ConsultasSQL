import { type Cliente, ejecutar, enTransaccion, fila, filas } from './db.ts';

export const ESTADOS_TICKET = ['abierto', 'investigacion', 'pendiente', 'resuelto'] as const;
export type EstadoTicket = (typeof ESTADOS_TICKET)[number];

export interface Ticket {
  numero: string;
  titulo: string;
  descripcion: string | null;
  autor: string | null;
  sistema: string | null;
  modulo: string | null;
  categoria: string | null;
  prioridad: string | null;
  estado: EstadoTicket;
  resultado: string | null;
  url_sir: string | null;
  causa_raiz: string | null;
  solucion: string | null;
  observaciones: string | null;
  creado_en: string | null;
  resuelto_en: string | null;
  actualizado_en: string;
}

export interface PasoTicket {
  id: number;
  ticket: string;
  numero: number;
  fecha: string | null;
  titulo: string;
  sql: string | null;
  resultado: string | null;
  evidencia: string | null;
  conclusion: string | null;
  origen: 'manual' | 'diagnostico' | 'importado';
  diagnostico_id: number | null;
}

export type TicketNuevo = Pick<Ticket, 'numero' | 'titulo'> &
  Partial<Omit<Ticket, 'numero' | 'titulo' | 'actualizado_en'>>;

export type PasoNuevo = Pick<PasoTicket, 'titulo'> &
  Partial<Pick<PasoTicket, 'fecha' | 'sql' | 'resultado' | 'evidencia' | 'conclusion' | 'origen' | 'diagnostico_id'>>;

const CAMPOS_EDITABLES = [
  'titulo', 'descripcion', 'autor', 'sistema', 'modulo', 'categoria', 'prioridad', 'estado',
  'resultado', 'url_sir', 'causa_raiz', 'solucion', 'observaciones', 'creado_en', 'resuelto_en',
] as const;

/** Columnas del ticket sin el tsvector generado, que no se devuelve al cliente. */
const COLUMNAS_TICKET = [
  'numero', 'titulo', 'descripcion', 'autor', 'sistema', 'modulo', 'categoria', 'prioridad', 'estado',
  'resultado', 'url_sir', 'causa_raiz', 'solucion', 'observaciones', 'creado_en', 'resuelto_en', 'actualizado_en',
] as const;

const COLUMNAS = COLUMNAS_TICKET.join(', ');
const COLUMNAS_T = COLUMNAS_TICKET.map((c) => `t.${c}`).join(', ');

/** Prioridad de estados en los listados: primero lo que está en curso. */
const ORDEN_ESTADO = `CASE t.estado WHEN 'investigacion' THEN 0 WHEN 'abierto' THEN 1 WHEN 'pendiente' THEN 2 ELSE 3 END`;

export interface FiltroTickets {
  texto?: string | null;
  estado?: string | null;
  sistema?: string | null;
}

export async function listarTickets(filtro: FiltroTickets = {}): Promise<(Ticket & { pasos: number })[]> {
  const condiciones: string[] = [];
  const valores: string[] = [];

  if (filtro.estado) {
    valores.push(filtro.estado);
    condiciones.push(`t.estado = $${valores.length}`);
  }
  if (filtro.sistema) {
    valores.push(filtro.sistema);
    condiciones.push(`t.sistema = $${valores.length}`);
  }
  if (filtro.texto) {
    valores.push(`%${filtro.texto}%`);
    const p = `$${valores.length}`;
    condiciones.push(`(t.numero ILIKE ${p} OR t.titulo ILIKE ${p} OR t.descripcion ILIKE ${p} OR t.modulo ILIKE ${p})`);
  }

  const where = condiciones.length ? `WHERE ${condiciones.join(' AND ')}` : '';
  return filas<Ticket & { pasos: number }>(
    `SELECT ${COLUMNAS_T},
            (SELECT COUNT(*) FROM soporte.ticket_pasos p WHERE p.ticket = t.numero)::int AS pasos
     FROM soporte.tickets t ${where}
     ORDER BY ${ORDEN_ESTADO},
              COALESCE(t.creado_en, to_char(t.actualizado_en, 'YYYY-MM-DD')) DESC`,
    valores,
  );
}

export async function obtenerTicket(numero: string): Promise<(Ticket & { pasos: PasoTicket[] }) | null> {
  const ticket = await fila<Ticket>(`SELECT ${COLUMNAS} FROM soporte.tickets WHERE numero = $1`, [numero]);
  if (!ticket) return null;
  const pasos = await filas<PasoTicket>('SELECT * FROM soporte.ticket_pasos WHERE ticket = $1 ORDER BY numero', [numero]);
  return { ...ticket, pasos };
}

export async function guardarTicket(datos: TicketNuevo, cliente?: Cliente): Promise<Ticket> {
  const columnas = ['numero', ...CAMPOS_EDITABLES.filter((c) => datos[c] !== undefined)];
  const valores = columnas.map((c) => (datos as Record<string, unknown>)[c] ?? null);
  const actualizaciones = columnas
    .filter((c) => c !== 'numero')
    .map((c) => `${c} = excluded.${c}`)
    .concat('actualizado_en = now()');

  const sql = `INSERT INTO soporte.tickets (${columnas.join(', ')})
       VALUES (${columnas.map((_, i) => `$${i + 1}`).join(', ')})
       ON CONFLICT (numero) DO UPDATE SET ${actualizaciones.join(', ')}
       RETURNING ${COLUMNAS}`;

  // El tsvector de búsqueda es una columna generada: se actualiza solo.
  if (cliente) {
    const { rows } = await cliente.query(sql, valores);
    return rows[0] as unknown as Ticket;
  }
  return (await fila<Ticket>(sql, valores))!;
}

export async function actualizarTicket(
  numero: string,
  cambios: Partial<Record<(typeof CAMPOS_EDITABLES)[number], string | null>>,
): Promise<Ticket | null> {
  const actual = await obtenerTicket(numero);
  if (!actual) return null;
  const campos = CAMPOS_EDITABLES.filter((c) => cambios[c] !== undefined);
  if (cambios.estado === 'resuelto' && !actual.resuelto_en && cambios.resuelto_en === undefined) {
    campos.push('resuelto_en');
    cambios.resuelto_en = new Date().toISOString().slice(0, 10);
  }
  if (campos.length === 0) return actual;

  const valores = campos.map((c) => cambios[c] ?? null);
  await ejecutar(
    `UPDATE soporte.tickets
     SET ${campos.map((c, i) => `${c} = $${i + 1}`).join(', ')}, actualizado_en = now()
     WHERE numero = $${campos.length + 1}`,
    [...valores, numero],
  );
  return obtenerTicket(numero);
}

export async function agregarPaso(ticket: string, paso: PasoNuevo): Promise<PasoTicket> {
  return enTransaccion(async (cliente) => {
    const { rows } = await cliente.query(
      `INSERT INTO soporte.ticket_pasos (ticket, numero, fecha, titulo, sql, resultado, evidencia, conclusion, origen, diagnostico_id)
       SELECT $1, COALESCE(MAX(numero), 0) + 1, $2, $3, $4, $5, $6, $7, $8, $9
       FROM soporte.ticket_pasos WHERE ticket = $1
       RETURNING *`,
      [
        ticket,
        paso.fecha ?? new Date().toISOString().slice(0, 16).replace('T', ' '),
        paso.titulo,
        paso.sql ?? null,
        paso.resultado ?? null,
        paso.evidencia ?? null,
        paso.conclusion ?? null,
        paso.origen ?? 'manual',
        paso.diagnostico_id ?? null,
      ],
    );
    await cliente.query('UPDATE soporte.tickets SET actualizado_en = now() WHERE numero = $1', [ticket]);
    return rows[0] as unknown as PasoTicket;
  });
}

const PALABRAS_VACIAS = new Set([
  'para', 'pero', 'como', 'esta', 'este', 'estos', 'estas', 'porque', 'cuando', 'donde', 'tiene', 'tengo',
  'hace', 'desde', 'hasta', 'sobre', 'entre', 'todo', 'todos', 'nada', 'algo', 'buenos', 'buenas', 'dias',
  'tardes', 'favor', 'gracias', 'saludos', 'estimados', 'socio', 'socia', 'señor', 'senor', 'señora', 'senora',
]);

/**
 * Convierte texto libre en una tsquery segura: palabras de 4+ letras unidas con OR,
 * cada una como prefijo. Equivale a la consulta FTS5 del SQLite original.
 * El filtrado a [a-z0-9] garantiza que no puede inyectarse sintaxis de tsquery.
 */
function aTsquery(texto: string): string | null {
  const palabras = texto
    .toLowerCase()
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .split(/[^a-z0-9]+/)
    .filter((p) => p.length >= 4 && !PALABRAS_VACIAS.has(p));
  const unicas = [...new Set(palabras)].slice(0, 24);
  return unicas.length ? unicas.map((p) => `${p}:*`).join(' | ') : null;
}

export type TicketSimilar = Pick<Ticket, 'numero' | 'titulo' | 'estado' | 'sistema' | 'modulo'> & { relevancia: number };

export async function buscarSimilares(texto: string, limite = 5, excluir?: string): Promise<TicketSimilar[]> {
  const consulta = aTsquery(texto);
  if (!consulta) return [];
  return filas<TicketSimilar>(
    `SELECT t.numero, t.titulo, t.estado, t.sistema, t.modulo, ts_rank(t.busqueda, q) AS relevancia
     FROM soporte.tickets t, to_tsquery('spanish', $1) q
     WHERE t.busqueda @@ q AND t.numero <> $2
     ORDER BY relevancia DESC
     LIMIT $3`,
    [consulta, excluir ?? '', limite],
  );
}

export async function contarTickets(): Promise<number> {
  const r = await fila<{ n: string }>('SELECT COUNT(*) AS n FROM soporte.tickets');
  return Number(r?.n ?? 0);
}
