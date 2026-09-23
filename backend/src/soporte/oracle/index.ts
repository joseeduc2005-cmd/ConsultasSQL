/**
 * Registro de fuentes de datos, una por base registrada.
 *
 * Antes había una sola fuente global: el campo «base de datos» del ticket solo
 * etiquetaba a dónde apuntaba el script del artículo, pero el diagnóstico siempre
 * consultaba la de `SOPORTE_ORACLE_DB_ID`. Ahora cada base marcada en
 * `multidb.databases.json` con `soporte.tickets` tiene su propia fuente, con su pool
 * y su recuperación, y el ticket decide contra cuál se investiga.
 *
 * Las fuentes se crean cuando alguien las pide, no al arrancar: una base marcada
 * pero apagada no debe costar nada hasta que se use.
 */
import { config } from '../config.ts';
import { FuenteDemo } from './fuente-demo.ts';
import { FuenteOracle } from './fuente-oracle.ts';
import type { FuenteDatos } from './fuente.ts';

const registro = new Map<string, FuenteDatos>();

/** La que se usa cuando el ticket no dice contra cuál investigar. */
export const fuente: FuenteDatos = config.oracle.modo === 'oracle' ? new FuenteOracle() : new FuenteDemo();

/**
 * Fuente de la base indicada, o la de por defecto si no se indica ninguna.
 *
 * Una base que no esté marcada para tickets no devuelve fuente: apuntar el catálogo
 * a una base sin los esquemas de Corresponsales no da un diagnóstico malo, da un
 * error, y es mejor decirlo que dejar que reviente a mitad de camino.
 */
export function fuentePara(dbId?: string | null): FuenteDatos | null {
  if (!dbId || dbId === config.oracle.dbId) return fuente;
  if (config.oracle.modo !== 'oracle') return fuente; // en demostración todo es la misma

  const base = config.basesTickets.find((b) => b.id === dbId);
  if (!base) return null;

  let existente = registro.get(base.id);
  if (!existente) {
    existente = new FuenteOracle({ usuario: base.usuario, clave: base.clave, conexion: base.conexion });
    registro.set(base.id, existente);
  }
  return existente;
}

/** Bases que la pantalla del ticket puede ofrecer, sin credenciales. */
export function basesParaTickets(): { id: string; etiqueta: string; catalogo: string; porDefecto: boolean }[] {
  return config.basesTickets.map(({ id, etiqueta, catalogo, porDefecto }) => ({ id, etiqueta, catalogo, porDefecto }));
}

export async function cerrarFuentes(): Promise<void> {
  await Promise.all([...registro.values()].map((f) => f.cerrar().catch(() => undefined)));
  registro.clear();
}

export type { FuenteDatos, Fila, Parametros } from './fuente.ts';
