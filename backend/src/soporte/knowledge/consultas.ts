import { type Cliente, fila, filas } from './db.ts';

export interface ConsultaBiblioteca {
  codigo: string;
  nombre: string;
  categoria: string | null;
  tipo: string | null;
  riesgo: 'lectura' | 'escritura';
  sql: string;
  parametros: string | null;
  uso: string | null;
  sistema: string | null;
  tickets: string | null;
}

export interface FiltroConsultas {
  texto?: string | null;
  categoria?: string | null;
  riesgo?: string | null;
}

/** Orden natural por código: letras primero, luego el número (U2 antes que U10). */
const ORDEN_NATURAL = `categoria NULLS LAST,
  regexp_replace(codigo, '[0-9]+$', ''),
  COALESCE(NULLIF(substring(codigo from '[0-9]+$'), '')::int, 0)`;

export async function listarConsultas(filtro: FiltroConsultas = {}): Promise<ConsultaBiblioteca[]> {
  const condiciones: string[] = [];
  const valores: string[] = [];
  if (filtro.categoria) {
    valores.push(filtro.categoria);
    condiciones.push(`categoria = $${valores.length}`);
  }
  if (filtro.riesgo) {
    valores.push(filtro.riesgo);
    condiciones.push(`riesgo = $${valores.length}`);
  }
  if (filtro.texto) {
    valores.push(`%${filtro.texto}%`);
    const p = `$${valores.length}`;
    condiciones.push(`(codigo ILIKE ${p} OR nombre ILIKE ${p} OR sql ILIKE ${p} OR uso ILIKE ${p} OR tickets ILIKE ${p})`);
  }
  const where = condiciones.length ? `WHERE ${condiciones.join(' AND ')}` : '';
  return filas<ConsultaBiblioteca>(`SELECT * FROM soporte.consultas ${where} ORDER BY ${ORDEN_NATURAL}`, valores);
}

export async function obtenerConsulta(codigo: string): Promise<ConsultaBiblioteca | null> {
  return fila<ConsultaBiblioteca>('SELECT * FROM soporte.consultas WHERE codigo = $1', [codigo]);
}

export async function guardarConsulta(c: ConsultaBiblioteca, cliente?: Cliente): Promise<void> {
  const sql = `INSERT INTO soporte.consultas (codigo, nombre, categoria, tipo, riesgo, sql, parametros, uso, sistema, tickets)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
     ON CONFLICT (codigo) DO UPDATE SET
       nombre = excluded.nombre, categoria = excluded.categoria, tipo = excluded.tipo, riesgo = excluded.riesgo,
       sql = excluded.sql, parametros = excluded.parametros, uso = excluded.uso, sistema = excluded.sistema,
       tickets = excluded.tickets`;
  const valores = [c.codigo, c.nombre, c.categoria, c.tipo, c.riesgo, c.sql, c.parametros, c.uso, c.sistema, c.tickets];
  if (cliente) await cliente.query(sql, valores);
  else await fila(sql, valores);
}

export async function categoriasConsultas(): Promise<{ categoria: string; total: number }[]> {
  return filas<{ categoria: string; total: number }>(
    'SELECT categoria, COUNT(*)::int AS total FROM soporte.consultas GROUP BY categoria ORDER BY categoria',
  );
}

export async function contarConsultas(): Promise<number> {
  const r = await fila<{ n: string }>('SELECT COUNT(*) AS n FROM soporte.consultas');
  return Number(r?.n ?? 0);
}
