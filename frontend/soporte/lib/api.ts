import type {
  Alerta,
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
  Resumen,
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

async function solicitar<T>(ruta: string, init?: RequestInit): Promise<T> {
  const respuesta = await fetch(`/api/soporte${ruta}`, {
    ...init,
    headers: init?.body ? { 'Content-Type': 'application/json', ...init.headers } : init?.headers,
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

  consultas: (f: { q?: string; categoria?: string; riesgo?: string } = {}) =>
    solicitar<{ consultas: ConsultaBiblioteca[]; categorias: { categoria: string; total: number }[] }>(`/consultas${consulta(f)}`),

  playbooks: () => solicitar<PlaybookInfo[]>('/playbooks'),
  ejecutarPlaybook: (id: string, entrada: Record<string, string>, ticket?: string) =>
    solicitar<ResultadoDiagnostico>(`/playbooks/${id}/ejecutar`, { method: 'POST', body: json({ entrada, ticket }) }),
  diagnosticos: (limite = 20) => solicitar<DiagnosticoResumen[]>(`/diagnosticos?limite=${limite}`),
  diagnostico: (id: number) => solicitar<ResultadoDiagnostico>(`/diagnosticos/${id}`),
  registrarDiagnostico: (id: number, ticket: string) =>
    solicitar<PasoTicket>(`/diagnosticos/${id}/registrar`, { method: 'POST', body: json({ ticket }) }),

  monitores: () => solicitar<MonitorInfo[]>('/monitores'),
  ejecutarMonitor: (id: string) => solicitar<EjecucionMonitor>(`/monitores/${id}/ejecutar`, { method: 'POST' }),
  alertas: (estado?: EstadoAlerta) => solicitar<Alerta[]>(`/alertas${consulta({ estado })}`),
  cambiarAlerta: (id: number, estado: EstadoAlerta) =>
    solicitar<Alerta>(`/alertas/${id}`, { method: 'PATCH', body: json({ estado }) }),

  interpretar: (texto: string) => solicitar<Interpretacion>('/interpretar', { method: 'POST', body: json({ texto }) }),
};
