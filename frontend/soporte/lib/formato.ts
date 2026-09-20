import type { EstadoAlerta, EstadoPaso, EstadoTicket, Severidad, Veredicto } from './types';

export type Tono = 'neutral' | 'primary' | 'success' | 'warning' | 'orange' | 'danger' | 'info';

export const ESTADO_TICKET: Record<EstadoTicket, { etiqueta: string; tono: Tono }> = {
  abierto: { etiqueta: 'Abierto', tono: 'danger' },
  investigacion: { etiqueta: 'En investigación', tono: 'warning' },
  pendiente: { etiqueta: 'Pendiente', tono: 'info' },
  resuelto: { etiqueta: 'Resuelto', tono: 'success' },
};

export const SEVERIDAD: Record<Severidad, { etiqueta: string; tono: Tono }> = {
  critica: { etiqueta: 'Crítica', tono: 'danger' },
  alta: { etiqueta: 'Alta', tono: 'orange' },
  media: { etiqueta: 'Media', tono: 'warning' },
  info: { etiqueta: 'Informativo', tono: 'info' },
};

export const ESTADO_PASO: Record<EstadoPaso, { etiqueta: string; tono: Tono }> = {
  ok: { etiqueta: 'Correcto', tono: 'success' },
  alerta: { etiqueta: 'Revisar', tono: 'warning' },
  falla: { etiqueta: 'Problema', tono: 'danger' },
  omitido: { etiqueta: 'Omitido', tono: 'neutral' },
  error: { etiqueta: 'Error', tono: 'danger' },
};

export const VEREDICTO: Record<Veredicto, { etiqueta: string; tono: Tono }> = {
  problema: { etiqueta: 'Problema detectado', tono: 'danger' },
  sin_hallazgos: { etiqueta: 'Sin hallazgos', tono: 'success' },
  incompleto: { etiqueta: 'Incompleto', tono: 'warning' },
};

export const ESTADO_ALERTA: Record<EstadoAlerta, { etiqueta: string; tono: Tono }> = {
  abierta: { etiqueta: 'Abierta', tono: 'danger' },
  atendida: { etiqueta: 'Atendida', tono: 'info' },
  resuelta: { etiqueta: 'Resuelta', tono: 'success' },
};

/** Convierte "AAAA-MM-DD HH:MM:SS" (hora local de SQLite) o ISO a Date. */
export function aFecha(valor: string | null | undefined): Date | null {
  if (!valor) return null;
  const d = new Date(/^\d{4}-\d{2}-\d{2} /.test(valor) ? valor.replace(' ', 'T') : valor);
  return Number.isNaN(d.getTime()) ? null : d;
}

const formatoFecha = new Intl.DateTimeFormat('es-EC', { day: '2-digit', month: 'short', year: 'numeric' });
const formatoFechaHora = new Intl.DateTimeFormat('es-EC', { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit' });
const relativo = new Intl.RelativeTimeFormat('es', { numeric: 'auto' });

export function fechaCorta(valor: string | null | undefined): string {
  if (valor && /^\d{4}-\d{2}-\d{2}$/.test(valor)) return formatoFecha.format(new Date(`${valor}T00:00:00`));
  const d = aFecha(valor);
  return d ? formatoFecha.format(d) : '—';
}

export function fechaHora(valor: string | null | undefined): string {
  const d = aFecha(valor);
  return d ? formatoFechaHora.format(d) : '—';
}

export function haceCuanto(valor: string | null | undefined): string {
  const d = aFecha(valor);
  if (!d) return '—';
  const segundos = Math.round((d.getTime() - Date.now()) / 1000);
  const abs = Math.abs(segundos);
  if (abs < 45) return 'hace un momento';
  if (abs < 3600) return relativo.format(Math.round(segundos / 60), 'minute');
  if (abs < 86_400) return relativo.format(Math.round(segundos / 3600), 'hour');
  if (abs < 86_400 * 30) return relativo.format(Math.round(segundos / 86_400), 'day');
  return formatoFecha.format(d);
}

export function intervalo(minutos: number): string {
  if (minutos < 60) return `cada ${minutos} min`;
  const horas = minutos / 60;
  return horas === 1 ? 'cada hora' : `cada ${horas} h`;
}

export function valorCelda(v: unknown): string {
  if (v === null || v === undefined) return 'NULL';
  if (typeof v === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/.test(v)) return v.slice(0, 16).replace('T', ' ');
  if (typeof v === 'object') return JSON.stringify(v);
  return String(v);
}

export const ETIQUETA_CAMPO: Record<string, string> = {
  mac: 'MAC',
  usuario: 'Usuario',
  cedula: 'Cédula',
  codigo_socio: 'Código de socio',
  desde: 'Desde',
  hasta: 'Hasta',
  tipo: 'Tipo',
};
