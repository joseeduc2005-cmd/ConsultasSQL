/**
 * Módulo de soporte: tickets, diagnósticos automáticos, biblioteca SQL y vigilancia.
 * Absorbido desde ticket v2.0 — ver SOPORTE.md.
 *
 * Uso desde app.js:
 *
 *   import { crearSoporteRouter, iniciarSoporte } from './soporte/index.ts';
 *   app.use('/api/soporte', crearSoporteRouter());        // al registrar rutas
 *   await iniciarSoporte({ obtenerPool: () => pool });    // dentro de startServer()
 */
import { config } from './config.ts';
import { aplicarEsquema, configurarPool, type Pool } from './knowledge/db.ts';
import { sembrarSiVacio } from './knowledge/semillas.ts';
import { iniciarVigilancia } from './monitors/programador.ts';
import { fuente } from './oracle/index.ts';
import { sembrarVocabulario, vocabularioSembrado } from './vocabulario.ts';

export { crearSoporteRouter } from './rutas.ts';
export { config as configSoporte } from './config.ts';

export interface OpcionesSoporte {
  /** Getter del pool de PostgreSQL del backend (se relee en cada consulta). */
  obtenerPool: () => Pool;
  log?: (mensaje: string) => void;
  /** Puente con el motor de consultas, para sembrar el vocabulario de la Biblioteca. */
  motor?: { obtenerHuella: () => string; refrescar: () => Promise<unknown> };
}

/**
 * Prepara el módulo: crea el esquema si falta, siembra la base de conocimiento la
 * primera vez y arranca la vigilancia si está encendida.
 *
 * No lanza: un fallo aquí deja el módulo de soporte inutilizable pero no impide que
 * el resto del backend arranque.
 */
export async function iniciarSoporte({ obtenerPool, log, motor }: OpcionesSoporte): Promise<boolean> {
  const registrar = log ?? ((m: string) => console.log(m));
  configurarPool(obtenerPool);

  try {
    await aplicarEsquema();
  } catch (error) {
    registrar(`⚠ [soporte] No se pudo preparar el esquema: ${(error as Error).message}`);
    return false;
  }

  try {
    const sembrado = await sembrarSiVacio();
    if (sembrado) {
      registrar(`✓ [soporte] Base de conocimiento inicializada: ${sembrado.tickets} tickets, ${sembrado.consultas} consultas`);
    }
  } catch (error) {
    registrar(`⚠ [soporte] No se pudieron cargar las semillas: ${(error as Error).message}`);
  }

  // La Biblioteca SQL enseña al motor qué palabra de negocio apunta a qué tabla.
  // Solo la primera vez: después se regenera a propósito desde /api/soporte/vocabulario.
  if (motor && config.vocabulario.automatico) {
    try {
      const huella = motor.obtenerHuella();
      if ((await vocabularioSembrado(huella)) === 0) {
        const r = await sembrarVocabulario(huella);
        await motor.refrescar();
        registrar(`✓ [soporte] Vocabulario de la Biblioteca sembrado en el motor: ${r.tablas} tablas, ${r.columnas} columnas`);
      }
    } catch (error) {
      registrar(`⚠ [soporte] No se pudo sembrar el vocabulario: ${(error as Error).message}`);
    }
  }

  registrar(
    `✓ [soporte] Listo · fuente de diagnósticos: ${config.oracle.modo === 'demo' ? 'DEMOSTRACIÓN (datos ficticios)' : `Oracle ${config.oracle.conexion}`}`,
  );

  if (config.vigilancia.habilitada) {
    iniciarVigilancia(fuente, (m) => registrar(`  ${m}`));
    registrar('✓ [soporte] Vigilancia activada');
  }
  return true;
}

export async function detenerSoporte(): Promise<void> {
  const { detenerVigilancia } = await import('./monitors/programador.ts');
  detenerVigilancia();
  await fuente.cerrar().catch(() => undefined);
}
