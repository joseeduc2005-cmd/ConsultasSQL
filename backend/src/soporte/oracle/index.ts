import { config } from '../config.ts';
import { FuenteDemo } from './fuente-demo.ts';
import { FuenteOracle } from './fuente-oracle.ts';
import type { FuenteDatos } from './fuente.ts';

export const fuente: FuenteDatos = config.oracle.modo === 'oracle' ? new FuenteOracle() : new FuenteDemo();

export type { FuenteDatos, Fila, Parametros } from './fuente.ts';
