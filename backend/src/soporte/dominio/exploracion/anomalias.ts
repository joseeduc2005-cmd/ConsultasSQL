/**
 * Qué cuenta como una anomalía, y con qué umbrales.
 *
 * La idea es sencilla: comparar una fila contra el resto de su tabla. Si el 97 % tiene
 * un valor y esta fila tiene otro, eso es lo raro. Los umbrales están aquí sueltos
 * porque son la diferencia entre señalar algo útil y llenar de ruido un diagnóstico.
 *
 * Dominio puro: tipos y números, ninguna consulta.
 */

export interface DiferenciaGenerica {
  columna: string;
  valorActual: unknown;
  valorHabitual: unknown;
  /** Proporción de filas de la tabla que tienen el valor habitual (0..1). */
  cuota: number;
}

export interface HallazgoGenerico {
  tabla: string;
  columna: string;
  identificador: string;
  /** Columnas donde esta fila se sale de lo que hace el resto. */
  diferencias: DiferenciaGenerica[];
  /** Cuántas filas se compararon. */
  poblacion: number;
  /** La fila encontrada, para mostrarla tal cual. */
  fila: Record<string, unknown>;
  /**
   * Cómo se llegó hasta aquí desde la tabla donde estaba el identificador.
   *
   * Vacío en la tabla de partida. En las demás explica la ruta —«USUARIO → AGENCIA por
   * AGE_CODIGO»— porque un hallazgo en una tabla que el ticket ni menciona no se
   * entiende sin saber qué la ata al caso.
   */
  camino: string[];
}

export const CUOTA_MINIMA = 0.8;
export const MINIMO_POBLACION = 20;
/** Más valores distintos que esto y la columna no describe un estado, describe un dato. */
export const MAX_VALORES_DISTINTOS = 12;

/** Cita un identificador para que un nombre con mayúsculas o reservado no rompa el SQL. */

/** Columnas que nunca se comparan: cambian en cada fila y su rareza no significa nada. */
export const IGNORADAS =
  /(^id$|_id$|^uuid|fecha|date|_at$|timestamp|hora|monto|valor|saldo|importe|clave|pass|pin|token|hash|secret)/i;
