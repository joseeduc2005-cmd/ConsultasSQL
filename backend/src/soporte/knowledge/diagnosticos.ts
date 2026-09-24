import { ejecutar, fila, filas } from './db.ts';
import type { ResultadoDiagnostico } from '../playbooks/tipos.ts';

export interface DiagnosticoResumen {
  id: number;
  playbook: string;
  entrada: Record<string, string>;
  veredicto: ResultadoDiagnostico['veredicto'];
  resumen: string | null;
  ticket: string | null;
  origen_datos: string | null;
  duracion_ms: number | null;
  creado_en: string;
}

/** `entrada` y `resultado` son jsonb: el driver ya los entrega deserializados. */
const COLUMNAS_RESUMEN = 'id, playbook, entrada, veredicto, resumen, ticket, origen_datos, duracion_ms, creado_en';

export async function guardarDiagnostico(
  r: ResultadoDiagnostico,
  ticket: string | null,
  ejecutadoPor?: string | null,
): Promise<number> {
  const creado = await fila<{ id: number }>(
    `INSERT INTO soporte.diagnosticos
       (playbook, entrada, veredicto, resumen, resultado, ticket, origen_datos, duracion_ms, ejecutado_por)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
     RETURNING id`,
    [r.playbook, JSON.stringify(r.entrada), r.veredicto, r.resumen, JSON.stringify(r), ticket, r.origenDatos,
     r.duracionMs, ejecutadoPor ?? null],
  );
  return creado!.id;
}

export async function listarDiagnosticos(limite = 20): Promise<DiagnosticoResumen[]> {
  return filas<DiagnosticoResumen>(
    `SELECT ${COLUMNAS_RESUMEN} FROM soporte.diagnosticos ORDER BY id DESC LIMIT $1`,
    [limite],
  );
}

export async function obtenerDiagnostico(id: number): Promise<ResultadoDiagnostico | null> {
  const f = await fila<{ id: number; ticket: string | null; resultado: ResultadoDiagnostico }>(
    'SELECT id, ticket, resultado FROM soporte.diagnosticos WHERE id = $1',
    [id],
  );
  if (!f) return null;
  return { ...f.resultado, id: f.id, ticket: f.ticket };
}

export async function vincularDiagnostico(id: number, ticket: string): Promise<void> {
  await ejecutar('UPDATE soporte.diagnosticos SET ticket = $1 WHERE id = $2', [ticket, id]);
}

export async function contarDiagnosticos(dias: number): Promise<number> {
  const r = await fila<{ n: string }>(
    'SELECT COUNT(*) AS n FROM soporte.diagnosticos WHERE creado_en >= now() - make_interval(days => $1)',
    [dias],
  );
  return Number(r?.n ?? 0);
}
