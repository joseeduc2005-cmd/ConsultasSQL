import type { ModoOracle } from '../config.ts';
import type { NombreConsulta } from '../dominio/oracle/catalogo.ts';

export type Fila = Record<string, unknown>;
export type Parametros = Record<string, string | number | Date | null>;

export interface EstadoFuente {
  modo: ModoOracle;
  conectado: boolean;
  detalle: string;
}

/** Contrato común para Oracle real y el modo demostración. */
export interface FuenteDatos {
  readonly modo: ModoOracle;
  consultar(nombre: NombreConsulta, parametros?: Parametros): Promise<Fila[]>;
  estado(): Promise<EstadoFuente>;
  cerrar(): Promise<void>;
}
