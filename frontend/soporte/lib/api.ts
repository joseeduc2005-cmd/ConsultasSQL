import type {
  Alerta,
  BaseRegistrada,
  BaseTickets,
  ConsultaBiblioteca,
  DiagnosticoResumen,
  EjecucionMonitor,
  EstadoAlerta,
  EstadoSistema,
  Interpretacion,
  MonitorInfo,
  PasoTicket,
  PlaybookInfo,
  ResultadoDiagnostico,
  ResultadoExploracion,
  Resumen,
  SolucionPublicada,
  Ticket,
  TicketDetalle,
  TicketListado,
} from './types';

export class ErrorApi extends Error {
  readonly estado: number;
  constructor(estado: number, mensaje: string) {
    super(mensaje);
    this.estado = estado;
  }
}

/**
 * Cabeceras de autenticación de ConsultasSQL.
 *
 * En ticket v2.0 la API no llevaba ninguna: el backend era un servicio interno sin
 * usuarios. Aquí el backend identifica a la persona por `Authorization: Bearer`, que
 * es lo que guarda el login, y el proxy de Next lo reenvía tal cual.
 */
function cabecerasAuth(): Record<string, string> {
  if (typeof window === 'undefined') return {};
  try {
    const token = localStorage.getItem('token');
    const rol = localStorage.getItem('userRole');
    return {
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...(rol ? { 'x-user-role': rol } : {}),
    };
  } catch {
    return {}; // almacenamiento no disponible
  }
}

async function solicitar<T>(ruta: string, init?: RequestInit): Promise<T> {
  const respuesta = await fetch(`/api/soporte${ruta}`, {
    ...init,
    headers: {
      ...cabecerasAuth(),
      ...(init?.body ? { 'Content-Type': 'application/json' } : {}),
      ...init?.headers,
    },
  });
  const cuerpo = await respuesta.json().catch(() => null);
  if (!respuesta.ok) {
    throw new ErrorApi(respuesta.status, cuerpo?.error ?? `Error ${respuesta.status}`);
  }
  return cuerpo as T;
}

const consulta = (params: Record<string, string | undefined | null>) => {
  const q = new URLSearchParams(Object.entries(params).filter((e): e is [string, string] => Boolean(e[1])));
  return q.size ? `?${q}` : '';
};

const json = (datos: unknown) => JSON.stringify(datos);

export const api = {
  estado: () => solicitar<EstadoSistema>('/estado'),
  resumen: () => solicitar<Resumen>('/resumen'),

  tickets: (f: { q?: string; estado?: string; sistema?: string } = {}) => solicitar<TicketListado[]>(`/tickets${consulta(f)}`),
  ticket: (numero: string) => solicitar<TicketDetalle>(`/tickets/${encodeURIComponent(numero)}`),
  crearTicket: (datos: Partial<Ticket> & { numero: string; titulo: string }) =>
    solicitar<Ticket>('/tickets', { method: 'POST', body: json(datos) }),
  actualizarTicket: (numero: string, cambios: Partial<Ticket>) =>
    solicitar<Ticket>(`/tickets/${encodeURIComponent(numero)}`, { method: 'PATCH', body: json(cambios) }),
  agregarPaso: (numero: string, paso: Partial<PasoTicket> & { titulo: string }) =>
    solicitar<PasoTicket>(`/tickets/${encodeURIComponent(numero)}/pasos`, { method: 'POST', body: json(paso) }),

  revisarTicket: (numero: string) =>
    solicitar<{
      ejecutado: boolean;
      motivo?: string;
      diagnosticoId?: number;
      resultado?: ResultadoDiagnostico;
      exploracion?: ResultadoExploracion;
    }>(`/tickets/${encodeURIComponent(numero)}/revisar`, { method: 'POST' }),

  consultas: (f: { q?: string; categoria?: string; riesgo?: string } = {}) =>
    solicitar<{ consultas: ConsultaBiblioteca[]; categorias: { categoria: string; total: number }[] }>(`/consultas${consulta(f)}`),

  /** Bases habilitadas para investigar tickets, con su grupo y color. */
  basesTickets: () => solicitar<BaseTickets[]>('/bases-tickets'),

  playbooks: () => solicitar<PlaybookInfo[]>('/playbooks'),
  ejecutarPlaybook: (id: string, entrada: Record<string, string>, ticket?: string) =>
    solicitar<ResultadoDiagnostico>(`/playbooks/${id}/ejecutar`, { method: 'POST', body: json({ entrada, ticket }) }),
  diagnosticos: (limite = 20) => solicitar<DiagnosticoResumen[]>(`/diagnosticos?limite=${limite}`),
  diagnostico: (id: number) => solicitar<ResultadoDiagnostico>(`/diagnosticos/${id}`),
  /**
   * Sin `correcciones` se publican todas las que proponga el diagnóstico.
   *
   * `exploracion` es un sí/no: la propuesta se vuelve a derivar en el backend, no se
   * manda desde aquí, para que el SQL publicado salga siempre de la base.
   */
  publicarSolucion: (
    id: number,
    opciones: {
      correcciones?: string[];
      exploracion?: boolean;
      propio?: { titulo?: string; sql?: string; pasos?: string; dinamico?: boolean };
    } = {},
  ) => solicitar<SolucionPublicada>(`/diagnosticos/${id}/publicar`, { method: 'POST', body: json(opciones) }),
  registrarDiagnostico: (id: number, ticket: string) =>
    solicitar<PasoTicket>(`/diagnosticos/${id}/registrar`, { method: 'POST', body: json({ ticket }) }),

  monitores: () => solicitar<MonitorInfo[]>('/monitores'),
  ejecutarMonitor: (id: string) => solicitar<EjecucionMonitor>(`/monitores/${id}/ejecutar`, { method: 'POST' }),
  alertas: (estado?: EstadoAlerta) => solicitar<Alerta[]>(`/alertas${consulta({ estado })}`),
  cambiarAlerta: (id: number, estado: EstadoAlerta) =>
    solicitar<Alerta>(`/alertas/${id}`, { method: 'PATCH', body: json({ estado }) }),

  interpretar: (texto: string) => solicitar<Interpretacion>('/interpretar', { method: 'POST', body: json({ texto }) }),

  /**
   * Bases registradas en el motor de consultas. No cuelga de /api/soporte: es un
   * endpoint de ConsultasSQL, y el módulo solo lo lee para ofrecerlas como contexto
   * de investigación del ticket.
   *
   * `showAll` es necesario: por defecto el endpoint devuelve solo las conectadas y
   * no primarias, y un ticket se registra igual aunque la base esté caída.
   */
  basesDatos: async (): Promise<BaseRegistrada[]> => {
    const r = await fetch('/api/distributed/databases?showAll=true', { headers: cabecerasAuth() });
    if (!r.ok) return [];
    const cuerpo = await r.json().catch(() => null);
    return Array.isArray(cuerpo?.databases) ? cuerpo.databases : [];
  },
};
