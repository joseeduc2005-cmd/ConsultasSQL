/**
 * Cuánto vale cada palabra, y cuánto fiarse de un resultado.
 *
 * Dominio puro: aritmética sobre números que alguien le pasa. No consulta nada. Las
 * decisiones que vive este archivo son las que más afectan a lo que ve una persona, y
 * por eso están aquí sueltas, donde se pueden leer y probar sin una base de datos.
 */

/** Por debajo de esto no hay con qué medir: con pocos temas todo parece frecuente. */
export const MINIMO_PARA_MEDIR = 10;

/** Un término por debajo de este peso está en todas partes y solo añade ruido. */
const PESO_DESPRECIABLE = 0.01;

export interface TerminoPesado {
  termino: string;
  /** Cuánto distingue: alto si aparece en pocos temas, cero si aparece en todos. */
  peso: number;
}

/**
 * El peso de un término según en cuántos temas aparece.
 *
 * Es la medida clásica para esto: el logaritmo de cuántos documentos hay entre los que
 * contienen el término. PostgreSQL no hace esto por su cuenta — `ts_rank` cuenta igual
 * uno que está en los quince temas que uno que está en dos. Así «usuarios repetidos»
 * encontraba «usuario no encontrado», porque «usuarios» sale en la mitad de la base y
 * pesaba tanto como «repetidos», que era lo único que decía de qué iba la pregunta.
 *
 * Lo que está en todos pesa cero y se cae solo, sin umbral que calibrar; lo raro manda.
 */
export function pesoDe(apareceEn: number, totalDeTemas: number): number {
  return Math.log((totalDeTemas + 1) / (apareceEn + 1));
}

/**
 * Los términos que vale la pena buscar, con su peso.
 *
 * Se mide contra los datos en vez de mantener una lista a mano: así funciona igual en
 * una base de corresponsales que en una de nómina, sin que nadie la actualice.
 */
export function pesar(
  frecuencias: { termino: string; apareceEn: number }[],
  totalDeTemas: number,
): TerminoPesado[] {
  return frecuencias
    .map((f) => ({ termino: f.termino, peso: pesoDe(f.apareceEn, totalDeTemas) }))
    .filter((f) => f.peso > PESO_DESPRECIABLE);
}

/**
 * Cuánto fiarse, a partir de qué proporción de lo distintivo cubre el resultado.
 *
 * `cobertura` va de 0 a 1 y es interpretable: cuánto del peso de la pregunta —midiendo
 * cada palabra por lo rara que es— aparece en este tema. Cubrir «repetidos» y
 * «duplicados» vale mucho; cubrir solo «usuarios», que está en la mitad de la base,
 * vale poco.
 *
 * Sustituye a los umbrales sobre `ts_rank`, que había que recalibrar a mano cada vez
 * que la base crecía porque dependían de cuántos documentos hubiera.
 */
export function confianzaDe(cobertura: number): 'alta' | 'media' | 'baja' {
  if (cobertura >= 0.55) return 'alta';
  return cobertura >= 0.3 ? 'media' : 'baja';
}

/**
 * Normaliza la relevancia contra la mejor del grupo.
 *
 * Eso ordena bien, pero por sí solo miente: dividir por el máximo deja siempre al
 * primero en 1, sea un acierto o un disparate. Por eso la confianza se calcula aparte,
 * sobre la cobertura sin normalizar, y es la que la pantalla usa para decidir si
 * anuncia «esto es lo suyo» o pide que se mire con cuidado.
 */
export function normalizar<T extends { relevancia: number }>(items: T[]): T[] {
  const mejor = Math.max(...items.map((p) => p.relevancia), 0);
  return items.map((p) => ({ ...p, relevancia: mejor > 0 ? p.relevancia / mejor : 0 }));
}
