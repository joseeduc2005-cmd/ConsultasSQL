import { readFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import vm from 'node:vm';
import { config } from '../config.ts';
import { enTransaccion } from './db.ts';
import { ESTADOS_TICKET, agregarPaso, contarTickets, guardarTicket, obtenerTicket, type EstadoTicket } from './tickets.ts';
import { contarConsultas, guardarConsulta } from './consultas.ts';

/** Formato de los arreglos TICKETS y SQL_LIBRARY del dashboard.html original. */
interface TicketDashboard {
  numero_ticket: string;
  url_sir?: string;
  titulo: string;
  descripcion?: string;
  autor?: string;
  sistema?: string;
  modulo?: string;
  categoria?: string;
  prioridad?: string;
  estado?: string;
  fecha_creacion?: string;
  fecha_resolucion?: string;
  causa_raiz?: string;
  solucion_aplicada?: string;
  observaciones?: string;
  resultado?: string;
  investigacion?: {
    numero_paso?: number;
    fecha_hora?: string;
    descripcion?: string;
    sql_utilizado?: string;
    resultado?: string;
    evidencia?: string;
    conclusion?: string;
  }[];
}

interface ConsultaDashboard {
  id: string;
  nombre: string;
  categoria?: string;
  tipo?: string;
  riesgo?: string;
  sql: string;
  parametros?: string;
  uso?: string;
  linea?: string;
  tickets?: string[];
  sistema?: string;
}

const vacio = (v: string | undefined | null): string | null => (v && v.trim() !== '' ? v.trim() : null);

/** Normaliza variantes históricas de nombres de sistema y categoría. */
const SISTEMAS: Record<string, string> = {
  JaWeb: 'Ja Web',
  JaPagos: 'Ja Pagos',
  JaMovil: 'Ja Movil',
  'Ja Móvil': 'Ja Movil',
};
const CATEGORIAS: Record<string, string> = { Parámetros: 'Parametros', Auditoría: 'Auditoria' };

export async function importarDatos(
  tickets: TicketDashboard[],
  consultas: ConsultaDashboard[],
): Promise<{ tickets: number; consultas: number }> {
  await enTransaccion(async (cliente) => {
    for (const c of consultas) {
      await guardarConsulta(
        {
          codigo: c.id,
          nombre: c.nombre,
          categoria: CATEGORIAS[c.categoria ?? ''] ?? vacio(c.categoria),
          tipo: vacio(c.tipo),
          riesgo: c.riesgo === 'escritura' ? 'escritura' : 'lectura',
          sql: c.sql,
          parametros: vacio(c.parametros),
          uso: vacio(c.uso),
          sistema: SISTEMAS[c.sistema ?? ''] ?? vacio(c.sistema),
          tickets: c.tickets?.length ? c.tickets.join(', ') : null,
        },
        cliente,
      );
    }
  });

  for (const t of tickets) {
    const estado = (ESTADOS_TICKET as readonly string[]).includes(t.estado ?? '') ? (t.estado as EstadoTicket) : 'abierto';
    const existia = (await obtenerTicket(t.numero_ticket)) !== null;
    await guardarTicket({
      numero: t.numero_ticket,
      titulo: t.titulo,
      descripcion: vacio(t.descripcion),
      autor: vacio(t.autor),
      sistema: SISTEMAS[t.sistema ?? ''] ?? vacio(t.sistema),
      modulo: vacio(t.modulo),
      categoria: vacio(t.categoria),
      prioridad: vacio(t.prioridad),
      estado,
      resultado: vacio(t.resultado),
      url_sir: vacio(t.url_sir),
      causa_raiz: vacio(t.causa_raiz),
      solucion: vacio(t.solucion_aplicada),
      observaciones: vacio(t.observaciones),
      creado_en: vacio(t.fecha_creacion?.slice(0, 10)),
      resuelto_en: vacio(t.fecha_resolucion?.slice(0, 10)),
    });
    // Los pasos solo se importan una vez para no duplicar la investigación.
    if (!existia) {
      for (const p of t.investigacion ?? []) {
        await agregarPaso(t.numero_ticket, {
          titulo: vacio(p.descripcion)?.split('\n')[0].slice(0, 240) ?? `Paso ${p.numero_paso ?? ''}`.trim(),
          fecha: vacio(p.fecha_hora),
          sql: vacio(p.sql_utilizado),
          resultado: [vacio(p.descripcion), vacio(p.resultado)].filter(Boolean).join('\n\n') || null,
          evidencia: vacio(p.evidencia),
          conclusion: vacio(p.conclusion),
          origen: 'importado',
        });
      }
    }
  }
  return { tickets: tickets.length, consultas: consultas.length };
}

/** Extrae los arreglos TICKETS y SQL_LIBRARY de un dashboard.html sin ejecutar el resto de la página. */
export function leerDashboardHtml(ruta: string): { tickets: TicketDashboard[]; consultas: ConsultaDashboard[] } {
  const html = readFileSync(ruta, 'utf8');
  const extraer = (marcador: string): unknown[] => {
    const inicio = html.indexOf(marcador);
    if (inicio < 0) throw new Error(`No se encontró "${marcador}" en ${ruta}`);
    const apertura = html.indexOf('[', inicio);
    let profundidad = 0;
    let comilla: string | null = null;
    let escape = false;
    for (let i = apertura; i < html.length; i++) {
      const c = html[i];
      if (comilla) {
        if (escape) escape = false;
        else if (c === '\\') escape = true;
        else if (c === comilla) comilla = null;
        continue;
      }
      if (c === "'" || c === '"' || c === '`') comilla = c;
      else if (c === '[') profundidad++;
      else if (c === ']' && --profundidad === 0) {
        // Se evalúa solo el literal del arreglo, en un contexto aislado.
        return vm.runInNewContext(`(${html.slice(apertura, i + 1)})`, {}, { timeout: 2000 }) as unknown[];
      }
    }
    throw new Error(`El arreglo "${marcador}" no está cerrado en ${ruta}`);
  };
  return {
    tickets: extraer('let TICKETS') as TicketDashboard[],
    consultas: extraer('const SQL_LIBRARY') as ConsultaDashboard[],
  };
}

/** Carga las semillas incluidas en el proyecto la primera vez que arranca el servidor. */
export async function sembrarSiVacio(): Promise<{ tickets: number; consultas: number } | null> {
  if ((await contarTickets()) > 0 || (await contarConsultas()) > 0) return null;
  const rutaTickets = join(config.rutas.semillas, 'tickets.json');
  const rutaConsultas = join(config.rutas.semillas, 'sql-library.json');
  if (!existsSync(rutaTickets) || !existsSync(rutaConsultas)) return null;
  return importarDatos(
    JSON.parse(readFileSync(rutaTickets, 'utf8')) as TicketDashboard[],
    JSON.parse(readFileSync(rutaConsultas, 'utf8')) as ConsultaDashboard[],
  );
}
