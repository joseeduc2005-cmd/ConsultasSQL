/**
 * Exploración sin catálogo: punto de entrada.
 *
 * Todo lo que decide algo vive repartido según quién lo necesita:
 *
 *   dominio/exploracion/grafo.ts            tablas y claves foráneas  puro
 *   dominio/exploracion/identificadores.ts  dónde vive una cédula     puro
 *   dominio/exploracion/anomalias.ts        qué cuenta como raro      puro
 *   puertos/ejecutor.ts                     leer de la base
 *   aplicacion/explorar-base.ts             el caso de uso
 *
 * Aquí solo se reexporta, con las mismas firmas de siempre, para que las pruebas y
 * quien lo importe no tengan que cambiar.
 */
export type { Ejecutor } from './puertos/ejecutor.ts';
export type { Arista, ClaveForanea, TablaEsquema } from './dominio/exploracion/grafo.ts';
export { vecinas } from './dominio/exploracion/grafo.ts';
export type { Candidata, TipoIdentificador } from './dominio/exploracion/identificadores.ts';
export { tablasCandidatas } from './dominio/exploracion/identificadores.ts';
export type { DiferenciaGenerica, HallazgoGenerico } from './dominio/exploracion/anomalias.ts';
export { explorarTabla, explorarVecindario } from './aplicacion/explorar-base.ts';
