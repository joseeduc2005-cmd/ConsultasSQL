export type EstadoTicket = 'abierto' | 'investigacion' | 'pendiente' | 'resuelto';
export type Severidad = 'critica' | 'alta' | 'media' | 'info';
export type EstadoPaso = 'ok' | 'alerta' | 'falla' | 'omitido' | 'error';
export type Veredicto = 'problema' | 'sin_hallazgos' | 'incompleto';
export type EstadoAlerta = 'abierta' | 'atendida' | 'resuelta';
export type Fila = Record<string, unknown>;

export interface EstadoSistema {
  version: string;
  oracle: { modo: 'demo' | 'oracle'; conectado: boolean; detalle: string };
  /** Base registrada contra la que corren los diagnósticos, o null en modo demostración. */
  baseDiagnosticos: string | null;
  /** Solo si se configura un modelo de lenguaje; hoy el backend no lo expone. */
  modelo?: { configurado: boolean; conectado: boolean; detalle: string };
  vigilancia: boolean;
  /**
   * Si esta apagado, los articulos de solucion se publican como documentacion y el
   * panel no muestra el boton de ejecutar. Es lo normal hasta que se confirmen los
   * permisos de escritura en Oracle.
   */
  correccionesEjecutables: boolean;
  reglas: { maxIntentosPin: number; maxIntentosClave: number; limiteReporte: number };
}

export interface Resumen {
  tickets: {
    abiertos: number;
    investigacion: number;
    pendientes: number;
    resueltos: number;
    porSistema: { sistema: string; total: number }[];
  };
  alertas: { total: number; criticas: number };
  diagnosticos: { ultimos7dias: number; veredictos30dias: { veredicto: Veredicto; total: number }[] };
  consultas: number;
}

export interface Ticket {
  numero: string;
  titulo: string;
  descripcion: string | null;
  autor: string | null;
  sistema: string | null;
  modulo: string | null;
  categoria: string | null;
  prioridad: string | null;
  estado: EstadoTicket;
  resultado: string | null;
  url_sir: string | null;
  causa_raiz: string | null;
  solucion: string | null;
  observaciones: string | null;
  creado_en: string | null;
  resuelto_en: string | null;
  /** Id de la base registrada contra la que se investiga el caso. */
  base_datos: string | null;
  actualizado_en: string;
}

/** Una base registrada en multidb.databases.json, tal como la expone el motor. */
export interface BaseRegistrada {
  id: string;
  label?: string;
  type: string;
  primary?: boolean;
  connected?: boolean;
}

export interface TicketListado extends Ticket {
  pasos: number;
}

export interface PasoTicket {
  id: number;
  ticket: string;
  numero: number;
  fecha: string | null;
  titulo: string;
  sql: string | null;
  resultado: string | null;
  evidencia: string | null;
  conclusion: string | null;
  origen: 'manual' | 'diagnostico' | 'importado';
  diagnostico_id: number | null;
}

export interface TicketSimilar {
  numero: string;
  titulo: string;
  estado: EstadoTicket;
  sistema: string | null;
  modulo: string | null;
  relevancia: number;
}

export interface TicketDetalle extends Ticket {
  pasos: PasoTicket[];
  similares: TicketSimilar[];
}

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

export interface CampoEntrada {
  clave: string;
  etiqueta: string;
  tipo: 'texto' | 'fecha';
  ayuda?: string;
  ejemplo?: string;
  patron?: string;
  requerido?: boolean;
}

export interface PlaybookInfo {
  id: string;
  nombre: string;
  descripcion: string;
  sistema: string;
  categoria: string;
  ticketsReferencia: string[];
  campos: CampoEntrada[];
  requiereAlguno: string[][];
}

export interface ConsultaEjecutada {
  nombre: string;
  descripcion: string;
  biblioteca?: string;
  sql: string;
  parametros: Record<string, unknown>;
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

export interface AccionSugerida {
  descripcion: string;
  riesgo: 'lectura' | 'escritura';
  responsable: string;
  biblioteca?: string;
  sql?: string;
  /** Id en el catalogo de correcciones: lo que el selector de publicacion ofrece marcar. */
  correccion?: string;
  parametros?: Record<string, string | number | null>;
}

export interface BaseTickets {
  id: string;
  etiqueta: string;
  /** produccion | pruebas | otras */
  grupo: string;
  /** rojo | ambar | verde | azul | gris. Rojo avisa de que toca datos reales. */
  color: string;
  nota: string | null;
  catalogo: string;
  porDefecto: boolean;
}

export interface Anomalia {
  columna: string;
  etiqueta: string;
  valorActual: number;
  valorSano: number;
  cuota: number;
}

export interface PropuestaExploracion {
  plantilla: string;
  sql: string;
  tabla: string;
  clave: { campo: string; etiqueta: string; valor: number };
  anomalias: Anomalia[];
  confianza: number;
  resumen: string;
  poblacion: number;
  dias: number;
}

export interface ResultadoExploracion {
  explorado: boolean;
  motivo?: string;
  propuesta?: PropuestaExploracion;
}

export interface SolucionPublicada {
  articulo: { id: string; titulo: string; creado: boolean };
  scripts: { id: string; titulo: string; correccion: string; creado: boolean }[];
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
  origenDatos: 'demo' | 'oracle';
  inicio: string;
  duracionMs: number;
}

export interface DiagnosticoResumen {
  id: number;
  playbook: string;
  entrada: Record<string, string>;
  veredicto: Veredicto;
  resumen: string | null;
  ticket: string | null;
  origen_datos: string | null;
  duracion_ms: number | null;
  creado_en: string;
}

export interface EjecucionMonitor {
  id: number;
  monitor: string;
  estado: 'ok' | 'alerta' | 'error';
  hallazgos: number;
  detalle: string | null;
  duracion_ms: number | null;
  creado_en: string;
}

export interface MonitorInfo {
  id: string;
  nombre: string;
  descripcion: string;
  intervaloMin: number;
  ticketsReferencia: string[];
  playbook: string | null;
  enCurso: boolean;
  ultimaEjecucion: EjecucionMonitor | null;
}

export interface Alerta {
  id: number;
  monitor: string;
  clave: string;
  titulo: string;
  detalle: string | null;
  severidad: Severidad;
  datos: Record<string, string> | null;
  estado: EstadoAlerta;
  primera_vez: string;
  ultima_vez: string;
  veces: number;
}

export interface Interpretacion {
  motor: 'reglas' | 'modelo';
  modelo: string | null;
  avisoModelo: string | null;
  resumen: string | null;
  sistema: string | null;
  categoria: string | null;
  orientacion: string | null;
  respuestaSugerida: string | null;
  identificadores: {
    macs: string[];
    cedulas: string[];
    usuarios: string[];
    tickets: string[];
    fechas: string[];
    cuentas: string[];
  };
  sugerencia: {
    playbook: PlaybookInfo;
    confianza: number;
    motivos: string[];
    entrada: Record<string, string>;
    faltantes: string[];
  } | null;
  similares: TicketSimilar[];
}
