/**
 * Acceso a la base de conocimiento de soporte sobre PostgreSQL.
 *
 * Sustituye al `node:sqlite` de ticket v2.0. No crea su propio pool: reutiliza el
 * que el backend ya tiene abierto contra knowledge_base, por eso hay que llamar a
 * `configurarPool()` en el arranque antes de usar cualquier función de este módulo.
 *
 * Todas las consultas califican el esquema (`soporte.tabla`) a propósito: el pool es
 * compartido con el resto del backend y cambiar su search_path afectaría a A.
 */
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

export type Pool = {
  query: (texto: string, valores?: unknown[]) => Promise<{ rows: Record<string, unknown>[]; rowCount: number | null }>;
  connect: () => Promise<Cliente>;
};

export type Cliente = {
  query: (texto: string, valores?: unknown[]) => Promise<{ rows: Record<string, unknown>[]; rowCount: number | null }>;
  release: () => void;
};

let obtenerPool: (() => Pool | null) | null = null;

/**
 * Recibe un *getter*, no el pool: el backend reemplaza su pool en caliente cuando
 * cambia multidb.databases.json, y capturarlo una sola vez dejaría a soporte
 * hablando con una conexión ya cerrada.
 */
export function configurarPool(getter: () => Pool | null): void {
  obtenerPool = getter;
}

function activo(): Pool {
  const pool = obtenerPool?.() ?? null;
  if (!pool) throw new Error('El módulo de soporte no tiene un pool de PostgreSQL disponible.');
  return pool;
}

/** Ejecuta una consulta y devuelve las filas. */
export async function filas<T>(sql: string, valores: unknown[] = []): Promise<T[]> {
  const { rows } = await activo().query(sql, valores);
  return rows as T[];
}

/** Ejecuta una consulta y devuelve la primera fila, o null. */
export async function fila<T>(sql: string, valores: unknown[] = []): Promise<T | null> {
  const { rows } = await activo().query(sql, valores);
  return (rows[0] as T) ?? null;
}

/** Ejecuta una sentencia sin leer resultados y devuelve cuántas filas tocó. */
export async function ejecutar(sql: string, valores: unknown[] = []): Promise<number> {
  const { rowCount } = await activo().query(sql, valores);
  return rowCount ?? 0;
}

/**
 * Corre `fn` dentro de una transacción. El cliente que recibe es el mismo para
 * todas las sentencias del bloque; usarlo es obligatorio para que participen.
 */
export async function enTransaccion<T>(fn: (cliente: Cliente) => Promise<T>): Promise<T> {
  const cliente = await activo().connect();
  try {
    await cliente.query('BEGIN');
    const resultado = await fn(cliente);
    await cliente.query('COMMIT');
    return resultado;
  } catch (error) {
    await cliente.query('ROLLBACK').catch(() => undefined);
    throw error;
  } finally {
    cliente.release();
  }
}

/** Crea el esquema si falta. Es idempotente, así que se puede correr en cada arranque. */
export async function aplicarEsquema(): Promise<void> {
  const ruta = fileURLToPath(new URL('./esquema.sql', import.meta.url));
  await activo().query(readFileSync(ruta, 'utf8'));
}
