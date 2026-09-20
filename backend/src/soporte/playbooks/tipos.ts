import type { ModoOracle } from '../config.ts';
import type { NombreConsulta } from '../oracle/catalogo.ts';
import type { Fila, Parametros } from '../oracle/fuente.ts';

export type EstadoPaso = 'ok' | 'alerta' | 'falla' | 'omitido' | 'error';
export type Severidad = 'critica' | 'alta' | 'media' | 'info';
export type Veredicto = 'problema' | 'sin_hallazgos' | 'incompleto';

export interface CampoEntrada {
  clave: string;
  etiqueta: string;
  tipo: 'texto' | 'fecha';
  ayuda?: string;
  ejemplo?: string;
  /** Expresión regular (como texto) que valida el valor. */
  patron?: string;
  requerido?: boolean;
}

export interface ConsultaEjecutada {
  nombre: NombreConsulta;
  descripcion: string;
  biblioteca?: string;
  sql: string;
  parametros: Parametros;
  filas: Fila[];
  duracionMs: number;
}

export interface PasoDiagnostico {
  id: string;
  titulo: string;
  estado: EstadoPaso;
  detalle: string;
  consultas: ConsultaEjecutada[];
  duracionMs: number;
}

/** Acción que el diagnóstico propone. Las de escritura NUNCA se ejecutan desde el sistema. */
export interface AccionSugerida {
  descripcion: string;
  riesgo: 'lectura' | 'escritura';
  responsable: string;
  biblioteca?: string;
  sql?: string;
}

export interface Hallazgo {
  severidad: Severidad;
  titulo: string;
  detalle: string;
  ticketsRelacionados?: string[];
  acciones?: AccionSugerida[];
}

export interface ResultadoDiagnostico {
  id?: number;
  ticket?: string | null;
  playbook: string;
  nombrePlaybook: string;
  entrada: Record<string, string>;
  veredicto: Veredicto;
  resumen: string;
  pasos: PasoDiagnostico[];
  hallazgos: Hallazgo[];
  origenDatos: ModoOracle;
  inicio: string;
  duracionMs: number;
}

export interface ControlPaso {
  consultar(nombre: NombreConsulta, parametros?: Parametros): Promise<Fila[]>;
  ok(detalle: string): void;
  alerta(detalle: string): void;
  falla(detalle: string): void;
  omitir(detalle: string): void;
}

export interface Contexto {
  entrada: Record<string, string>;
  paso<T>(id: string, titulo: string, fn: (control: ControlPaso) => Promise<T>): Promise<T | undefined>;
  hallazgo(h: Hallazgo): void;
}

export interface Playbook {
  id: string;
  nombre: string;
  descripcion: string;
  sistema: string;
  categoria: string;
  /** Palabras y frases que ayudan a elegir este playbook al interpretar un mensaje. */
  senales: RegExp[];
  ticketsReferencia: string[];
  campos: CampoEntrada[];
  /** Grupos de campos de los que se necesita al menos uno (ej. MAC, usuario o cédula). */
  requiereAlguno?: string[][];
  ejecutar(ctx: Contexto): Promise<void>;
}
