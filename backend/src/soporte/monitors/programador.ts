import { config } from '../config.ts';
import { registrarAlertas, registrarEjecucion, ultimaEjecucion, type EjecucionMonitor } from '../knowledge/alertas.ts';
import type { Auditoria } from '../puertos/auditoria.ts';
import type { FuenteDatos } from '../puertos/fuente-datos.ts';
import { MONITORES, type Monitor } from './monitores.ts';

const enCurso = new Set<string>();
let temporizador: NodeJS.Timeout | null = null;

/**
 * Dónde deja rastro la vigilancia.
 *
 * Esto es lo único del sistema que escribe sin que haya una petición detrás, así que
 * el middleware de auditoría no la ve: corre sola cada minuto. Los monitores sí
 * dejaban su propia tabla de ejecuciones, pero que la vigilancia arrancara, se
 * detuviera o que un monitor fallara no constaba en ningún sitio consultable.
 */
let auditoria: Auditoria | null = null;

export async function ejecutarMonitor(monitor: Monitor, fuente: FuenteDatos): Promise<EjecucionMonitor | null> {
  if (enCurso.has(monitor.id)) return ultimaEjecucion(monitor.id);
  enCurso.add(monitor.id);
  const t0 = performance.now();
  try {
    const alertas = await monitor.revisar(fuente);
    await registrarAlertas(monitor.id, alertas);
    await registrarEjecucion({
      monitor: monitor.id,
      estado: alertas.length ? 'alerta' : 'ok',
      hallazgos: alertas.length,
      detalle: alertas.length ? `${alertas.length} hallazgo(s)` : 'Sin hallazgos',
      duracion_ms: Math.round(performance.now() - t0),
    });
  } catch (error) {
    await registrarEjecucion({
      monitor: monitor.id,
      estado: 'error',
      hallazgos: 0,
      detalle: (error as Error).message,
      duracion_ms: Math.round(performance.now() - t0),
    });
    // Además de su tabla: un monitor que falla en silencio cada minuto durante una
    // semana es el tipo de avería que nadie ve hasta que alguien pregunta por qué no
    // salió una alerta.
    await auditoria?.registrarError({
      mensaje: (error as Error).message,
      stack: (error as Error).stack ?? null,
      capa: 'vigilancia',
      contexto: `monitor:${monitor.id}`,
    }).catch(() => undefined);
  } finally {
    enCurso.delete(monitor.id);
  }
  return ultimaEjecucion(monitor.id);
}

/** PostgreSQL devuelve timestamptz como Date; el SQLite anterior devolvía texto. */
function comoFecha(valor: string | Date): number {
  return valor instanceof Date ? valor.getTime() : new Date(valor.replace(' ', 'T')).getTime();
}

async function vencido(monitor: Monitor): Promise<boolean> {
  const ultima = await ultimaEjecucion(monitor.id);
  if (!ultima) return true;
  return Date.now() - comoFecha(ultima.creado_en) >= monitor.intervaloMin * 60_000;
}

/** Revisa cada minuto qué monitores están vencidos y los ejecuta uno a la vez. */
export function iniciarVigilancia(
  fuente: FuenteDatos,
  log: (msg: string) => void,
  auditor?: Auditoria,
): void {
  if (!config.vigilancia.habilitada || temporizador) return;
  auditoria = auditor ?? null;
  const ciclo = async () => {
    for (const monitor of MONITORES) {
      try {
        if (await vencido(monitor)) {
          const r = await ejecutarMonitor(monitor, fuente);
          log(`Vigilancia ${monitor.id}: ${r?.estado ?? 'sin resultado'} (${r?.hallazgos ?? 0} hallazgos)`);
        }
      } catch (error) {
        // Un monitor caído no debe detener el ciclo de los demás.
        log(`Vigilancia ${monitor.id}: error — ${(error as Error).message}`);
      }
    }
  };
  setTimeout(ciclo, 5_000);
  temporizador = setInterval(ciclo, 60_000);

  void auditoria?.registrarEvento({
    evento: 'VIGILANCIA_ACTIVADA',
    usuario: 'sistema',
    rol: 'sistema',
    detalle: { monitores: MONITORES.length, cicloSegundos: 60 },
  });
}

export function detenerVigilancia(): void {
  if (temporizador) {
    clearInterval(temporizador);
    void auditoria?.registrarEvento({
      evento: 'VIGILANCIA_DETENIDA',
      usuario: 'sistema',
      rol: 'sistema',
      detalle: { monitores: MONITORES.length },
    });
  }
  temporizador = null;
}

export const monitorEnCurso = (id: string) => enCurso.has(id);
