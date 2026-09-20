import { ejecutar, enTransaccion, fila, filas } from './db.ts';

export type Severidad = 'critica' | 'alta' | 'media' | 'info';
export type EstadoAlerta = 'abierta' | 'atendida' | 'resuelta';

export interface Alerta {
  id: number;
  monitor: string;
  clave: string;
  titulo: string;
  detalle: string | null;
  severidad: Severidad;
  datos: Record<string, unknown> | null;
  estado: EstadoAlerta;
  primera_vez: string;
  ultima_vez: string;
  veces: number;
}

export interface AlertaDetectada {
  clave: string;
  titulo: string;
  detalle: string;
  severidad: Severidad;
  datos?: Record<string, unknown>;
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

/**
 * Registra el resultado de un monitor:
 * - alertas nuevas se crean como abiertas,
 * - alertas que reaparecen actualizan su conteo (y se reabren si estaban resueltas),
 * - alertas que ya no aparecen se marcan como resueltas automáticamente.
 */
export async function registrarAlertas(monitor: string, detectadas: AlertaDetectada[]): Promise<void> {
  await enTransaccion(async (cliente) => {
    for (const a of detectadas) {
      await cliente.query(
        `INSERT INTO soporte.alertas (monitor, clave, titulo, detalle, severidad, datos)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (monitor, clave) DO UPDATE SET
           titulo = excluded.titulo, detalle = excluded.detalle, severidad = excluded.severidad,
           datos = excluded.datos, ultima_vez = now(), veces = alertas.veces + 1,
           estado = CASE WHEN alertas.estado = 'resuelta' THEN 'abierta' ELSE alertas.estado END`,
        [monitor, a.clave, a.titulo, a.detalle, a.severidad, a.datos ? JSON.stringify(a.datos) : null],
      );
    }
    // Lo que el monitor ya no reporta se da por resuelto.
    await cliente.query(
      `UPDATE soporte.alertas SET estado = 'resuelta'
       WHERE monitor = $1 AND estado <> 'resuelta' AND clave <> ALL($2::text[])`,
      [monitor, detectadas.map((a) => a.clave)],
    );
  });
}

export async function registrarEjecucion(e: Omit<EjecucionMonitor, 'id' | 'creado_en'>): Promise<void> {
  await ejecutar(
    `INSERT INTO soporte.vigilancia_ejecuciones (monitor, estado, hallazgos, detalle, duracion_ms)
     VALUES ($1, $2, $3, $4, $5)`,
    [e.monitor, e.estado, e.hallazgos, e.detalle, e.duracion_ms],
  );
  // Se conserva solo el historial reciente de cada monitor.
  await ejecutar(
    `DELETE FROM soporte.vigilancia_ejecuciones
     WHERE monitor = $1 AND id NOT IN (
       SELECT id FROM soporte.vigilancia_ejecuciones WHERE monitor = $1 ORDER BY id DESC LIMIT 200)`,
    [e.monitor],
  );
}

export async function ultimaEjecucion(monitor: string): Promise<EjecucionMonitor | null> {
  return fila<EjecucionMonitor>(
    'SELECT * FROM soporte.vigilancia_ejecuciones WHERE monitor = $1 ORDER BY id DESC LIMIT 1',
    [monitor],
  );
}

export async function listarAlertas(estado?: string | null, limite = 200): Promise<Alerta[]> {
  const where = estado ? 'WHERE estado = $2' : '';
  return filas<Alerta>(
    `SELECT * FROM soporte.alertas ${where}
     ORDER BY CASE estado WHEN 'abierta' THEN 0 WHEN 'atendida' THEN 1 ELSE 2 END,
              CASE severidad WHEN 'critica' THEN 0 WHEN 'alta' THEN 1 WHEN 'media' THEN 2 ELSE 3 END,
              ultima_vez DESC
     LIMIT $1`,
    estado ? [limite, estado] : [limite],
  );
}

export async function cambiarEstadoAlerta(id: number, estado: EstadoAlerta): Promise<Alerta | null> {
  return fila<Alerta>('UPDATE soporte.alertas SET estado = $1 WHERE id = $2 RETURNING *', [estado, id]);
}

export async function contarAlertasAbiertas(): Promise<{ total: number; criticas: number }> {
  const r = await fila<{ total: number; criticas: number }>(
    `SELECT COUNT(*)::int AS total,
            COALESCE(SUM(CASE WHEN severidad IN ('critica', 'alta') THEN 1 ELSE 0 END), 0)::int AS criticas
     FROM soporte.alertas WHERE estado = 'abierta'`,
  );
  return r ?? { total: 0, criticas: 0 };
}
