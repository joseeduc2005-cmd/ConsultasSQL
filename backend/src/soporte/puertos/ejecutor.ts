/**
 * Leer de una base que se está investigando.
 *
 * Esto ya era un puerto antes de que la arquitectura tuviera nombre: una función que el
 * dominio recibe y usa sin saber qué hay detrás. Hoy la cumple el motor multi-base —
 * PostgreSQL, MySQL, Oracle— y en las pruebas la cumple una función de tres líneas que
 * devuelve filas inventadas.
 *
 * Que sea una sola función y no una interfaz con métodos es a propósito: la exploración
 * no necesita nada más que leer, y un puerto que no puede escribir es un puerto que no
 * va a escribir por accidente.
 */
export type Ejecutor = (sql: string, parametros: unknown[]) => Promise<Record<string, unknown>[]>;
